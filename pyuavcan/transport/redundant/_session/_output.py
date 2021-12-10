# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import logging
import asyncio
import pyuavcan.transport
from ._base import RedundantSession, RedundantSessionStatistics


_logger = logging.getLogger(__name__)


class RedundantFeedback(pyuavcan.transport.Feedback):
    """
    This is the output feedback extended with the reference to the inferior transport session
    that this feedback originates from.

    A redundant output session provides one feedback entry per inferior session;
    for example, if there are three inferiors in a redundant transport group,
    each outgoing transfer will generate three feedback entries
    (unless inferior sessions fail to provide their feedback entries for whatever reason).
    """

    def __init__(
        self, inferior_feedback: pyuavcan.transport.Feedback, inferior_session: pyuavcan.transport.OutputSession
    ):
        self._inferior_feedback = inferior_feedback
        self._inferior_session = inferior_session

    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._inferior_feedback.original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._inferior_feedback.first_frame_transmission_timestamp

    @property
    def inferior_feedback(self) -> pyuavcan.transport.Feedback:
        """
        The original feedback instance from the inferior session.
        """
        assert isinstance(self._inferior_feedback, pyuavcan.transport.Feedback)
        return self._inferior_feedback

    @property
    def inferior_session(self) -> pyuavcan.transport.OutputSession:
        """
        The inferior session that generated this feedback entry.
        """
        assert isinstance(self._inferior_session, pyuavcan.transport.OutputSession)
        return self._inferior_session


class RedundantOutputSession(RedundantSession, pyuavcan.transport.OutputSession):
    """
    This is a composite of a group of :class:`pyuavcan.transport.OutputSession`.
    Every outgoing transfer is simply forked into each of the inferior sessions.
    The result aggregation policy is documented in :func:`send`.
    """

    def __init__(
        self,
        specifier: pyuavcan.transport.OutputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly! Use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.OutputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert callable(self._finalizer)

        self._inferiors: typing.List[pyuavcan.transport.OutputSession] = []
        self._feedback_handler: typing.Optional[typing.Callable[[RedundantFeedback], None]] = None
        self._idle_send_future: typing.Optional[asyncio.Future[None]] = None
        self._lock = asyncio.Lock()

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0
        self._stat_drops = 0

    def _add_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.OutputSession)
        assert self._finalizer is not None, "The session was supposed to be unregistered"
        assert session.specifier == self.specifier and session.payload_metadata == self.payload_metadata
        if session not in self._inferiors:
            # Synchronize the feedback state.
            if self._feedback_handler is not None:
                self._enable_feedback_on_inferior(session)
            else:
                session.disable_feedback()
            # If and only if all went well, add the new inferior to the set.
            self._inferiors.append(session)
            # Unlock the pending transmission because now we have an inferior to work with.
            if self._idle_send_future is not None:
                self._idle_send_future.set_result(None)

    def _close_inferior(self, session_index: int) -> None:
        assert session_index >= 0, "Negative indexes may lead to unexpected side effects"
        assert self._finalizer is not None, "The session was supposed to be unregistered"
        try:
            session = self._inferiors.pop(session_index)
        except LookupError:
            pass
        else:
            session.close()  # May raise.

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.OutputSession]:
        return self._inferiors[:]

    def enable_feedback(self, handler: typing.Callable[[RedundantFeedback], None]) -> None:
        """
        The operation is atomic on all inferiors.
        If at least one inferior fails to enable feedback, all inferiors are rolled back into the disabled state.
        """
        self.disable_feedback()  # For state determinism.
        try:
            self._feedback_handler = handler
            for ses in self._inferiors:
                self._enable_feedback_on_inferior(ses)
        except Exception as ex:
            _logger.info("%s could not enable feedback, rolling back into the disabled state: %r", self, ex)
            self.disable_feedback()
            raise

    def disable_feedback(self) -> None:
        """
        The method implements the best-effort policy if any of the inferior sessions fail to disable feedback.
        """
        self._feedback_handler = None
        for ses in self._inferiors:
            try:
                ses.disable_feedback()
            except Exception as ex:
                _logger.exception("%s could not disable feedback on %r: %s", self, ses, ex)

    async def send(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        """
        Sends the transfer via all of the inferior sessions concurrently.
        Returns when all of the inferior calls return and/or raise exceptions.
        Edge cases:

        - If there are no inferiors, the method will await until either the deadline is expired
          or an inferior(s) is (are) added. In the former case, the method returns False.
          In the latter case, the transfer is transmitted via the new inferior(s) using the remaining time
          until the deadline.

        - If at least one inferior succeeds, True is returned (logical OR).
          If the other inferiors raise exceptions, they are logged as errors and suppressed.

        - If all inferiors raise exceptions, the exception from the first one is propagated,
          the rest are logged as errors and suppressed.

        - If all inferiors time out, False is returned (logical OR).

        In other words, the error handling strategy is optimistic: if one inferior reported success,
        the call is assumed to have succeeded; best result is always returned.
        """
        if self._finalizer is None:
            raise pyuavcan.transport.ResourceClosedError(f"{self} is closed")

        loop = asyncio.get_running_loop()
        async with self._lock:  # Serialize access to the inferiors and the idle future.
            # It is required to create a local copy to prevent disruption of the logic when
            # the set of inferiors is changed in the background. Oh, Rust, where art thou.
            inferiors = list(self._inferiors)

            # This part is a bit tricky. If there are no inferiors, we have nowhere to send the transfer.
            # Instead of returning immediately, we hang out here until the deadline is expired hoping that
            # an inferior is added while we're waiting here.
            assert not self._idle_send_future
            if not inferiors and monotonic_deadline > loop.time():
                try:
                    _logger.debug("%s has no inferiors; suspending the send method...", self)
                    self._idle_send_future = loop.create_future()
                    try:
                        await asyncio.wait_for(self._idle_send_future, timeout=monotonic_deadline - loop.time())
                    except asyncio.TimeoutError:
                        pass
                    else:
                        self._idle_send_future.result()  # Collect the empty result to prevent asyncio from complaining.
                    # The set of inferiors may have been updated.
                    inferiors = list(self._inferiors)
                    _logger.debug(
                        "%s send method unsuspended; available inferiors: %r; remaining time: %f",
                        self,
                        inferiors,
                        monotonic_deadline - loop.time(),
                    )
                finally:
                    self._idle_send_future = None
            assert not self._idle_send_future

            if not inferiors:
                self._stat_drops += 1
                return False  # Still nothing.

            results = await asyncio.gather(
                *[ses.send(transfer, monotonic_deadline) for ses in inferiors], return_exceptions=True
            )
            assert results and len(results) == len(inferiors)
            _logger.debug("%s send results: %s", self, results)

            exceptions = [ex for ex in results if isinstance(ex, Exception)]

            # Result consolidation logic as described in the doc.
            if exceptions:
                # Taking great efforts to make the error message very understandable to the user.
                _logger.error(  # pylint: disable=logging-not-lazy
                    f"{self}: {len(exceptions)} of {len(results)} inferiors have failed: "
                    + ", ".join(f"{i}:{self._describe_send_result(r)}" for i, r in enumerate(results))
                )
                if len(exceptions) >= len(results):
                    self._stat_errors += 1
                    raise exceptions[0]

            if any(x is True for x in results):
                self._stat_transfers += 1
                self._stat_payload_bytes += sum(map(len, transfer.fragmented_payload))
                return True
            self._stat_drops += 1
            return False

    @property
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> RedundantSessionStatistics:
        """
        - ``transfers``     - the number of redundant transfers where at least ONE inferior succeeded (success count).
        - ``errors``        - the number of redundant transfers where ALL inferiors raised exceptions (failure count).
        - ``payload_bytes`` - the number of payload bytes in successful redundant transfers counted in ``transfers``.
        - ``drops``         - the number of redundant transfers where ALL inferiors timed out (timeout count).
        - ``frames``        - the total number of frames summed from all inferiors (i.e., replicated frame count).
          This value is invalidated when the set of inferiors is changed. The semantics may change later.
        """
        inferiors = [s.sample_statistics() for s in self._inferiors]
        return RedundantSessionStatistics(
            transfers=self._stat_transfers,
            frames=sum(s.frames for s in inferiors),
            payload_bytes=self._stat_payload_bytes,
            errors=self._stat_errors,
            drops=self._stat_drops,
            inferiors=inferiors,
        )

    def close(self) -> None:
        for s in self._inferiors:
            try:
                s.close()
            except Exception as ex:
                _logger.exception("%s could not close inferior %s: %s", self, s, ex)
        self._inferiors.clear()

        fin, self._finalizer = self._finalizer, None
        if fin is not None:
            fin()

    def _enable_feedback_on_inferior(self, inferior_session: pyuavcan.transport.OutputSession) -> None:
        def proxy(fb: pyuavcan.transport.Feedback) -> None:
            """
            Intercepts a feedback report from an inferior session,
            constructs a higher-level redundant feedback instance from it,
            and then passes it along to the higher-level handler.
            """
            if inferior_session not in self._inferiors:
                _logger.warning(
                    "%s got unexpected feedback %s from %s which is not a registered inferior. "
                    "The transport or its underlying software or hardware are probably misbehaving, "
                    "or this inferior has just been removed.",
                    self,
                    fb,
                    inferior_session,
                )
                return

            handler = self._feedback_handler
            if handler is not None:
                new_fb = RedundantFeedback(fb, inferior_session)
                try:
                    handler(new_fb)
                except Exception as ex:
                    _logger.exception("%s: Unhandled exception in the feedback handler %s: %s", self, handler, ex)
            else:
                _logger.debug("%s ignoring unattended feedback %r from %r", self, fb, inferior_session)

        inferior_session.enable_feedback(proxy)

    @staticmethod
    def _describe_send_result(result: typing.Union[bool, Exception]) -> str:
        if isinstance(result, Exception):
            return repr(result)
        if isinstance(result, bool):
            return "success" if result else "timeout"
        assert False
