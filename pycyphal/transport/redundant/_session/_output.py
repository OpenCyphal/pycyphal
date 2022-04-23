# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Callable, Optional, Sequence
import logging
import asyncio
import dataclasses
import pycyphal.transport
from ._base import RedundantSession, RedundantSessionStatistics


_logger = logging.getLogger(__name__)


class RedundantFeedback(pycyphal.transport.Feedback):
    """
    This is the output feedback extended with the reference to the inferior transport session
    that this feedback originates from.

    A redundant output session provides one feedback entry per inferior session;
    for example, if there are three inferiors in a redundant transport group,
    each outgoing transfer will generate three feedback entries
    (unless inferior sessions fail to provide their feedback entries for whatever reason).
    """

    def __init__(
        self, inferior_feedback: pycyphal.transport.Feedback, inferior_session: pycyphal.transport.OutputSession
    ):
        self._inferior_feedback = inferior_feedback
        self._inferior_session = inferior_session

    @property
    def original_transfer_timestamp(self) -> pycyphal.transport.Timestamp:
        return self._inferior_feedback.original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pycyphal.transport.Timestamp:
        return self._inferior_feedback.first_frame_transmission_timestamp

    @property
    def inferior_feedback(self) -> pycyphal.transport.Feedback:
        """
        The original feedback instance from the inferior session.
        """
        assert isinstance(self._inferior_feedback, pycyphal.transport.Feedback)
        return self._inferior_feedback

    @property
    def inferior_session(self) -> pycyphal.transport.OutputSession:
        """
        The inferior session that generated this feedback entry.
        """
        assert isinstance(self._inferior_session, pycyphal.transport.OutputSession)
        return self._inferior_session


@dataclasses.dataclass(frozen=True)
class _WorkItem:
    """
    Send the transfer before the deadline, then notify the future unless it is already canceled.
    """

    transfer: pycyphal.transport.Transfer
    monotonic_deadline: float
    future: asyncio.Future[bool]


@dataclasses.dataclass(frozen=True)
class _Inferior:
    """
    Each inferior runs a dedicated worker task.
    The worker takes work items from the queue one by one and attempts to transmit them.
    Upon completion (timeout/exception/success) the future is materialized unless cancelled.
    """

    session: pycyphal.transport.OutputSession
    worker: asyncio.Task[None]
    queue: asyncio.Queue[_WorkItem]

    def close(self) -> None:
        # Ensure correct finalization order to avoid https://github.com/OpenCyphal/pycyphal/issues/204
        try:
            if self.worker.done():
                self.worker.result()
            else:
                self.worker.cancel()
            while True:
                try:
                    self.queue.get_nowait().future.cancel()
                except asyncio.QueueEmpty:
                    break
        finally:
            self.session.close()


class RedundantOutputSession(RedundantSession, pycyphal.transport.OutputSession):
    """
    This is a composite of a group of :class:`pycyphal.transport.OutputSession`.
    Every outgoing transfer is simply forked into each of the inferior sessions.
    The result aggregation policy is documented in :func:`send`.
    """

    def __init__(
        self,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        finalizer: Callable[[], None],
    ):
        """
        Do not call this directly! Use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._finalizer: Optional[Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pycyphal.transport.OutputSessionSpecifier)
        assert isinstance(self._payload_metadata, pycyphal.transport.PayloadMetadata)
        assert callable(self._finalizer)

        self._inferiors: list[_Inferior] = []
        self._feedback_handler: Optional[Callable[[RedundantFeedback], None]] = None
        self._idle_send_future: Optional[asyncio.Future[None]] = None
        self._lock = asyncio.Lock()

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0
        self._stat_drops = 0

    def _add_inferior(self, session: pycyphal.transport.Session) -> None:
        assert isinstance(session, pycyphal.transport.OutputSession)
        assert self._finalizer is not None, "The session was supposed to be unregistered"
        assert session.specifier == self.specifier and session.payload_metadata == self.payload_metadata
        if session in self.inferiors:
            return
        # Synchronize the feedback state.
        if self._feedback_handler is not None:
            self._enable_feedback_on_inferior(session)
        else:
            session.disable_feedback()
        # If all went well, add the new inferior to the set.
        que: asyncio.Queue[_WorkItem] = asyncio.Queue()
        tsk = asyncio.get_event_loop().create_task(self._inferior_worker_task(session, que))
        self._inferiors.append(_Inferior(session, tsk, que))
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
    def inferiors(self) -> Sequence[pycyphal.transport.OutputSession]:
        return [x.session for x in self._inferiors]

    def enable_feedback(self, handler: Callable[[RedundantFeedback], None]) -> None:
        """
        The operation is atomic on all inferiors.
        If at least one inferior fails to enable feedback, all inferiors are rolled back into the disabled state.
        """
        self.disable_feedback()  # For state determinism.
        try:
            self._feedback_handler = handler
            for ses in self._inferiors:
                self._enable_feedback_on_inferior(ses.session)
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
                ses.session.disable_feedback()
            except Exception as ex:
                _logger.exception("%s could not disable feedback on %r: %s", self, ses, ex)

    async def send(self, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        """
        Sends the transfer via all of the inferior sessions concurrently.
        Returns when the first of the inferior calls succeeds; the remaining will keep sending in the background;
        that is, the redundant transport operates at the rate of the fastest inferior, delegating the slower ones
        to background tasks.
        Edge cases:

        - If there are no inferiors, the method will await until either the deadline is expired
          or an inferior(s) is (are) added. In the former case, the method returns False.
          In the latter case, the transfer is transmitted via the new inferior(s) using the remaining time
          until the deadline.

        - If at least one inferior succeeds, True is returned (logical OR).
          If the other inferiors raise exceptions, they are logged as errors and suppressed.

        - If all inferiors raise exceptions, one of them is propagated, the rest are logged as errors and suppressed.

        - If all inferiors time out, False is returned (logical OR).

        In other words, the error handling strategy is optimistic: if one inferior reported success,
        the call is assumed to have succeeded; best result is always returned.
        """
        if self._finalizer is None:
            raise pycyphal.transport.ResourceClosedError(f"{self} is closed")

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

            # We have at least one inferior so we can handle this transaction. Create the work items.
            pending: set[asyncio.Future[bool]] = set()
            for inf in self._inferiors:
                fut: asyncio.Future[bool] = asyncio.Future()
                inf.queue.put_nowait(_WorkItem(transfer, monotonic_deadline, fut))
                pending.add(fut)

            # Execute the work items concurrently and unblock as soon as at least one inferior is done transmitting.
            # Those that are still pending are detached because we're not going to wait around for the slow ones
            # (they will continue transmitting in the background of course).
            done: set[asyncio.Future[bool]] = set()
            while pending and not any(f.exception() is None for f in done):
                done_subset, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                done |= done_subset
            _logger.debug("%s send results: done=%s, pending=%s", self, done, pending)
            for p in pending:
                p.cancel()  # We will no longer need this.

            # Extract the results to determine the final outcome of the transaction.
            results = [x.result() for x in done if x.exception() is None]
            exceptions = [x.exception() for x in done if x.exception() is not None]
            assert 0 < (len(results) + len(exceptions)) <= len(inferiors)  # Some tasks may be not yet done.
            assert not results or all(isinstance(x, bool) for x in results)
            if exceptions and not results:
                self._stat_errors += 1
                exc = exceptions[0]
                assert isinstance(exc, BaseException)
                raise exc
            if results and any(results):
                self._stat_transfers += 1
                self._stat_payload_bytes += sum(map(len, transfer.fragmented_payload))
                return True
            self._stat_drops += 1
            return False

    @property
    def specifier(self) -> pycyphal.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
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
        inferiors = [s.session.sample_statistics() for s in self._inferiors]
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

    async def _inferior_worker_task(self, ses: pycyphal.transport.OutputSession, que: asyncio.Queue[_WorkItem]) -> None:
        try:
            _logger.debug("%s: Task for inferior %r is starting", self, ses)
            while self._finalizer:
                wrk = await que.get()
                try:
                    result = await ses.send(wrk.transfer, wrk.monotonic_deadline)
                except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError):
                    break  # Do not cancel the future because we don't want to unblock the master task.
                except Exception as ex:
                    _logger.error("%s: Inferior %r failed: %s: %s", self, ses, type(ex).__name__, ex)
                    _logger.debug("%s: Stack trace for the above inferior failure:", self, exc_info=True)
                    if not wrk.future.done():
                        wrk.future.set_exception(ex)
                else:
                    _logger.debug(
                        "%s: Inferior %r send result: %s; future %s",
                        self,
                        ses,
                        "success" if result else "timeout",
                        wrk.future,
                    )
                    if not wrk.future.done():
                        wrk.future.set_result(result)
        except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError):
            pass
        except Exception as ex:
            _logger.exception("%s: Task for %r has encountered an unhandled exception: %s", self, ses, ex)
        finally:
            _logger.debug("%s: Task for %r is stopping", self, ses)

    def _enable_feedback_on_inferior(self, inferior_session: pycyphal.transport.OutputSession) -> None:
        def proxy(fb: pycyphal.transport.Feedback) -> None:
            """
            Intercepts a feedback report from an inferior session,
            constructs a higher-level redundant feedback instance from it,
            and then passes it along to the higher-level handler.
            """
            if inferior_session not in self.inferiors:
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
