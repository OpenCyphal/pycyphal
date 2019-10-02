#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

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
    The user can then map the feedback to the inferior transport instance if necessary.

    A redundant output session provides one feedback entry per inferior session;
    for example, if there are three inferiors in a redundant transport group,
    each outgoing transfer will generate three feedback entries
    (unless inferior sessions fail to provide their feedback entries for whatever reason).
    """
    def __init__(self,
                 inferior_feedback: pyuavcan.transport.Feedback,
                 inferior_session:  pyuavcan.transport.OutputSession):
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
    This is a standard composite over a set of :class:`pyuavcan.transport.OutputSession`.
    Every sent transfer is simply forked into each of the inferior sessions.
    """
    def __init__(self,
                 specifier:        pyuavcan.transport.OutputSessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 loop_provider:    typing.Callable[[], asyncio.AbstractEventLoop],
                 finalizer:        typing.Callable[[], None]):
        """
        Do not call this directly! Use the factory method instead.

        Observe that we can't pass a loop directly because it may be changed when the set of
        redundant transports is changed, so we have to request it each time it's needed separately.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop_provider = loop_provider
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.OutputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert isinstance(self._loop_provider(), asyncio.AbstractEventLoop)
        assert callable(self._finalizer)

        self._inferiors: typing.List[pyuavcan.transport.OutputSession] = []
        self._feedback_handler = typing.Optional[typing.Callable[[RedundantFeedback], None]]

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0
        self._stat_drops = 0

    def _add_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.OutputSession), 'Internal error'
        assert self._finalizer is not None, 'Internal logic error: the session was supposed to be unregistered'
        assert session.specifier == self.specifier, 'Internal error'
        assert session.payload_metadata == self.payload_metadata, 'Internal error'
        if session not in self._inferiors:
            # Synchronize the feedback state.
            if self._feedback_handler is not None:
                self._enable_feedback_on_inferior(session)
            else:
                session.disable_feedback()
            # If and only if all went well, add the new inferior to the set.
            self._inferiors.append(session)

    def _close_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.OutputSession), 'Internal error'
        assert self._finalizer is not None, 'Internal logic error: the session was supposed to be unregistered'
        assert session.specifier == self.specifier, 'Internal error'
        assert session.payload_metadata == self.payload_metadata, 'Internal error'
        try:
            self._inferiors.remove(session)
        except ValueError:
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
            _logger.info('%s could not enable feedback, rolling back into the disabled state: %r', self, ex)
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
                _logger.exception('%s could not disable feedback on %r: %s', self, ses, ex)

    async def send_until(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        """
        Sends the transfer via all of the inferior sessions concurrently.
        Returns when all of the inferior calls return and/or raise exceptions.
        Edge cases:

        - If there are no inferiors, returns False immediately.
        - If at least one inferior succeeds, True is returned.
          False is returned only if all redundant sessions fail to send the transfer.
        - If any of the inferiors raises an exception, it is postponed until all other inferiors succeed or fail.
        - If more than one inferior raises an exception, only one of them will be raised;
          which one is raised is undefined. The other exceptions will be logged instead.
        """
        if self._finalizer is None:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')

        if not self._inferiors:
            return False

        out = await asyncio.gather(
            *[
                ses.send_until(transfer, monotonic_deadline) for ses in self._inferiors
            ],
            loop=self._loop_provider(),
            return_exceptions=True
        )
        assert len(out) == len(self._inferiors)
        exceptions = [ex for ex in out if isinstance(ex, Exception)]

        self._stat_transfers += 1
        self._stat_payload_bytes += sum(map(len, transfer.fragmented_payload))
        self._stat_drops += sum(1 for x in out if not x)
        self._stat_errors += len(exceptions)

        if exceptions:
            if len(exceptions) > 1:
                # This is because we can only raise one. All of them are counted in the stats, however.
                _logger.error('%s: Suppressed exceptions: %r', self, exceptions[1:])
            raise exceptions[0]

        assert all(isinstance(x, bool) for x in out)
        return any(out)

    @property
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> RedundantSessionStatistics:
        """
        - ``transfers``     - the number of *redundant* (i.e., unique) transfers.
        - ``frames``        - the total number of frames summed from all inferiors.
        - ``payload_bytes`` - the number of payload bytes before redundant transfer replication.
        - ``errors``        - the total number of exceptions thrown from the inferiors.
        - ``drops``         - how many times an inferior was unable to complete transmission before the deadline.
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
        """
        Closes and detaches all inferior sessions.
        If any of the sessions fail to close, an error message will be logged, but no exception will be raised.
        The instance will no longer be usable afterward.
        """
        for s in self._inferiors:
            try:
                s.close()
            except Exception as ex:
                _logger.exception('%s could not close inferior %s: %s', self, s, ex)
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
                _logger.warning('%s got unexpected feedback %s from %s which is not a registered inferior. '
                                'The transport or its underlying software or hardware are probably misbehaving.',
                                self, fb, inferior_session)
                return

            handler = self._feedback_handler
            if handler is not None:
                new_fb = RedundantFeedback(fb, inferior_session)
                try:
                    handler(new_fb)
                except Exception as ex:
                    _logger.exception('%s: Unhandled exception in the feedback handler %s: %s', self, handler, ex)
            else:
                _logger.debug('%s ignoring unattended feedback %r from %r', self, fb, inferior_session)

        inferior_session.enable_feedback(proxy)
