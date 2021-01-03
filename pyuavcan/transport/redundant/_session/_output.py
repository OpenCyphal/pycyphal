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
        loop: asyncio.AbstractEventLoop,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly! Use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.OutputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert isinstance(self._loop, asyncio.AbstractEventLoop)
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

        async with self._lock:  # Serialize access to the inferiors and the idle future.
            # It is required to create a local copy to prevent disruption of the logic when
            # the set of inferiors is changed in the background. Oh, Rust, where art thou.
            inferiors = list(self._inferiors)

            # This part is a bit tricky. If there are no inferiors, we have nowhere to send the transfer.
            # Instead of returning immediately, we hang out here until the deadline is expired hoping that
            # an inferior is added while we're waiting here.
            assert not self._idle_send_future
            if not inferiors and monotonic_deadline > self._loop.time():
                try:
                    _logger.debug("%s has no inferiors; suspending the send method...", self)
                    self._idle_send_future = self._loop.create_future()
                    try:
                        await asyncio.wait_for(self._idle_send_future, timeout=monotonic_deadline - self._loop.time())
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
                        monotonic_deadline - self._loop.time(),
                    )
                finally:
                    self._idle_send_future = None
            assert not self._idle_send_future

            if not inferiors:
                self._stat_drops += 1
                return False  # Still nothing.

            results = await asyncio.gather(
                *[ses.send(transfer, monotonic_deadline) for ses in inferiors], loop=self._loop, return_exceptions=True
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


def _unittest_redundant_output() -> None:
    import time
    import pytest
    from pyuavcan.transport import Transfer, Timestamp, Priority, SessionStatistics, ResourceClosedError
    from pyuavcan.transport import TransferFrom
    from pyuavcan.transport.loopback import LoopbackTransport, LoopbackFeedback

    loop = asyncio.get_event_loop()
    await_ = loop.run_until_complete

    spec = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_rx = pyuavcan.transport.InputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30 * 1024 * 1024)

    ts = Timestamp.now()

    is_retired = False

    def retire() -> None:
        nonlocal is_retired
        is_retired = True

    ses = RedundantOutputSession(spec, meta, loop=loop, finalizer=retire)
    assert not is_retired
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()

    # Transmit with an empty set of inferiors.
    time_before = loop.time()
    assert not await_(
        ses.send(
            Transfer(
                timestamp=ts,
                priority=Priority.IMMEDIATE,
                transfer_id=1234567890,
                fragmented_payload=[memoryview(b"abc")],
            ),
            loop.time() + 2.0,
        )
    )
    assert 1.0 < loop.time() - time_before < 5.0, "The method should have returned in about two seconds."
    assert ses.sample_statistics() == RedundantSessionStatistics(
        drops=1,
    )

    # Create inferiors.
    tr_a = LoopbackTransport(111)
    tr_b = LoopbackTransport(111)
    inf_a = tr_a.get_output_session(spec, meta)
    inf_b = tr_b.get_output_session(spec, meta)
    rx_a = tr_a.get_input_session(spec_rx, meta)
    rx_b = tr_b.get_input_session(spec_rx, meta)

    # Begin transmission, then add an inferior while it is in progress.
    async def add_inferior(inferior: pyuavcan.transport.OutputSession) -> None:
        _logger.debug("Test: sleeping before adding the inferior...")
        await asyncio.sleep(2.0)
        _logger.debug("Test: adding the inferior...")
        ses._add_inferior(inferior)  # pylint: disable=protected-access
        _logger.debug("Test: inferior has been added.")

    assert await_(
        asyncio.gather(
            # Start transmission here. It would stall for up to five seconds because no inferiors.
            ses.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.IMMEDIATE,
                    transfer_id=9876543210,
                    fragmented_payload=[memoryview(b"def")],
                ),
                loop.time() + 5.0,
            ),
            # While the transmission is stalled, add one inferior with a 2-sec delay. It will unlock the stalled task.
            add_inferior(inf_a),
            # Then make sure that the transmission has actually taken place about after two seconds from the start.
        )
    ), "Transmission should have succeeded"
    assert 1.0 < loop.time() - time_before < 5.0, "The method should have returned in about two seconds."
    assert ses.sample_statistics() == RedundantSessionStatistics(
        transfers=1,
        frames=1,
        payload_bytes=3,
        drops=1,
        inferiors=[
            SessionStatistics(
                transfers=1,
                frames=1,
                payload_bytes=3,
            ),
        ],
    )
    tf_rx = await_(rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 9876543210
    assert tf_rx.fragmented_payload == [memoryview(b"def")]
    assert None is await_(rx_b.receive(loop.time() + 0.1))

    # Enable feedback.
    feedback: typing.List[RedundantFeedback] = []
    ses.enable_feedback(feedback.append)
    assert await_(
        ses.send(
            Transfer(
                timestamp=ts,
                priority=Priority.LOW,
                transfer_id=555555555555,
                fragmented_payload=[memoryview(b"qwerty")],
            ),
            loop.time() + 1.0,
        )
    )
    assert ses.sample_statistics() == RedundantSessionStatistics(
        transfers=2,
        frames=2,
        payload_bytes=9,
        drops=1,
        inferiors=[
            SessionStatistics(
                transfers=2,
                frames=2,
                payload_bytes=9,
            ),
        ],
    )
    assert len(feedback) == 1
    assert feedback[0].inferior_session is inf_a
    assert feedback[0].original_transfer_timestamp == ts
    assert ts.system <= feedback[0].first_frame_transmission_timestamp.system <= time.time()
    assert ts.monotonic <= feedback[0].first_frame_transmission_timestamp.monotonic <= time.monotonic()
    assert isinstance(feedback[0].inferior_feedback, LoopbackFeedback)
    feedback.pop()
    assert not feedback
    tf_rx = await_(rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 555555555555
    assert tf_rx.fragmented_payload == [memoryview(b"qwerty")]
    assert None is await_(rx_b.receive(loop.time() + 0.1))

    # Add a new inferior and ensure that its feedback is auto-enabled!
    ses._add_inferior(inf_b)  # pylint: disable=protected-access
    assert ses.inferiors == [
        inf_a,
        inf_b,
    ]
    # Double-add has no effect.
    ses._add_inferior(inf_b)  # pylint: disable=protected-access
    assert ses.inferiors == [
        inf_a,
        inf_b,
    ]
    assert await_(
        ses.send(
            Transfer(
                timestamp=ts,
                priority=Priority.FAST,
                transfer_id=777777777777,
                fragmented_payload=[memoryview(b"fgsfds")],
            ),
            loop.time() + 1.0,
        )
    )
    assert ses.sample_statistics() == RedundantSessionStatistics(
        transfers=3,
        frames=3 + 1,
        payload_bytes=15,
        drops=1,
        inferiors=[
            SessionStatistics(
                transfers=3,
                frames=3,
                payload_bytes=15,
            ),
            SessionStatistics(
                transfers=1,
                frames=1,
                payload_bytes=6,
            ),
        ],
    )
    assert len(feedback) == 2
    assert feedback[0].inferior_session is inf_a
    assert feedback[0].original_transfer_timestamp == ts
    assert ts.system <= feedback[0].first_frame_transmission_timestamp.system <= time.time()
    assert ts.monotonic <= feedback[0].first_frame_transmission_timestamp.monotonic <= time.monotonic()
    assert isinstance(feedback[0].inferior_feedback, LoopbackFeedback)
    feedback.pop(0)
    assert len(feedback) == 1
    assert feedback[0].inferior_session is inf_b
    assert feedback[0].original_transfer_timestamp == ts
    assert ts.system <= feedback[0].first_frame_transmission_timestamp.system <= time.time()
    assert ts.monotonic <= feedback[0].first_frame_transmission_timestamp.monotonic <= time.monotonic()
    assert isinstance(feedback[0].inferior_feedback, LoopbackFeedback)
    feedback.pop()
    assert not feedback
    tf_rx = await_(rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 777777777777
    assert tf_rx.fragmented_payload == [memoryview(b"fgsfds")]
    tf_rx = await_(rx_b.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 777777777777
    assert tf_rx.fragmented_payload == [memoryview(b"fgsfds")]

    # Remove the first inferior.
    ses._close_inferior(0)  # pylint: disable=protected-access
    assert ses.inferiors == [inf_b]
    ses._close_inferior(1)  # Out of range, no effect.  # pylint: disable=protected-access
    assert ses.inferiors == [inf_b]
    # Make sure the removed inferior has been closed.
    assert not tr_a.output_sessions

    # Transmission test with the last inferior.
    assert await_(
        ses.send(
            Transfer(
                timestamp=ts,
                priority=Priority.HIGH,
                transfer_id=88888888888888,
                fragmented_payload=[memoryview(b"hedgehog")],
            ),
            loop.time() + 1.0,
        )
    )
    assert ses.sample_statistics().transfers == 4
    # We don't check frames because this stat metric is computed quite clumsily atm, this may change later.
    assert ses.sample_statistics().payload_bytes == 23
    assert ses.sample_statistics().drops == 1
    assert ses.sample_statistics().inferiors == [
        SessionStatistics(
            transfers=2,
            frames=2,
            payload_bytes=14,
        ),
    ]
    assert len(feedback) == 1
    assert feedback[0].inferior_session is inf_b
    assert feedback[0].original_transfer_timestamp == ts
    assert ts.system <= feedback[0].first_frame_transmission_timestamp.system <= time.time()
    assert ts.monotonic <= feedback[0].first_frame_transmission_timestamp.monotonic <= time.monotonic()
    assert isinstance(feedback[0].inferior_feedback, LoopbackFeedback)
    feedback.pop()
    assert not feedback
    assert None is await_(rx_a.receive(loop.time() + 1))
    tf_rx = await_(rx_b.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 88888888888888
    assert tf_rx.fragmented_payload == [memoryview(b"hedgehog")]

    # Disable the feedback.
    ses.disable_feedback()
    # A diversion - enable the feedback in the inferior and make sure it's not propagated.
    ses._enable_feedback_on_inferior(inf_b)  # pylint: disable=protected-access
    assert await_(
        ses.send(
            Transfer(
                timestamp=ts,
                priority=Priority.OPTIONAL,
                transfer_id=666666666666666,
                fragmented_payload=[memoryview(b"horse")],
            ),
            loop.time() + 1.0,
        )
    )
    assert ses.sample_statistics().transfers == 5
    # We don't check frames because this stat metric is computed quite clumsily atm, this may change later.
    assert ses.sample_statistics().payload_bytes == 28
    assert ses.sample_statistics().drops == 1
    assert ses.sample_statistics().inferiors == [
        SessionStatistics(
            transfers=3,
            frames=3,
            payload_bytes=19,
        ),
    ]
    assert not feedback
    assert None is await_(rx_a.receive(loop.time() + 1))
    tf_rx = await_(rx_b.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 666666666666666
    assert tf_rx.fragmented_payload == [memoryview(b"horse")]

    # Retirement.
    assert not is_retired
    ses.close()
    assert is_retired
    # Make sure the inferiors have been closed.
    assert not tr_a.output_sessions
    assert not tr_b.output_sessions
    # Idempotency.
    is_retired = False
    ses.close()
    assert not is_retired

    # Use after close.
    with pytest.raises(ResourceClosedError):
        await_(
            ses.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.OPTIONAL,
                    transfer_id=1111111111111,
                    fragmented_payload=[memoryview(b"cat")],
                ),
                loop.time() + 1.0,
            )
        )

    assert None is await_(rx_a.receive(loop.time() + 1))
    assert None is await_(rx_b.receive(loop.time() + 1))


def _unittest_redundant_output_exceptions(caplog: typing.Any) -> None:
    import pytest
    from pyuavcan.transport import Transfer, Timestamp, Priority, SessionStatistics
    from pyuavcan.transport import TransferFrom
    from pyuavcan.transport.loopback import LoopbackTransport

    loop = asyncio.get_event_loop()
    await_ = loop.run_until_complete

    spec = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_rx = pyuavcan.transport.InputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30 * 1024 * 1024)

    ts = Timestamp.now()

    is_retired = False

    def retire() -> None:
        nonlocal is_retired
        is_retired = True

    ses = RedundantOutputSession(spec, meta, loop=loop, finalizer=retire)
    assert not is_retired
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()

    tr_a = LoopbackTransport(111)
    tr_b = LoopbackTransport(111)
    inf_a = tr_a.get_output_session(spec, meta)
    inf_b = tr_b.get_output_session(spec, meta)
    rx_a = tr_a.get_input_session(spec_rx, meta)
    rx_b = tr_b.get_input_session(spec_rx, meta)
    ses._add_inferior(inf_a)  # pylint: disable=protected-access
    ses._add_inferior(inf_b)  # pylint: disable=protected-access

    # Transmission with exceptions.
    # If at least one transmission succeeds, the call succeeds.
    with caplog.at_level(logging.CRITICAL, logger=__name__):
        inf_a.exception = RuntimeError("INTENDED EXCEPTION")
        assert await_(
            ses.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.FAST,
                    transfer_id=444444444444,
                    fragmented_payload=[memoryview(b"INTENDED EXCEPTION")],
                ),
                loop.time() + 1.0,
            )
        )
        assert ses.sample_statistics() == RedundantSessionStatistics(
            transfers=1,
            frames=1,
            payload_bytes=len("INTENDED EXCEPTION"),
            errors=0,
            drops=0,
            inferiors=[
                SessionStatistics(
                    transfers=0,
                    frames=0,
                    payload_bytes=0,
                ),
                SessionStatistics(
                    transfers=1,
                    frames=1,
                    payload_bytes=len("INTENDED EXCEPTION"),
                ),
            ],
        )
        assert None is await_(rx_a.receive(loop.time() + 1))
        tf_rx = await_(rx_b.receive(loop.time() + 1))
        assert isinstance(tf_rx, TransferFrom)
        assert tf_rx.transfer_id == 444444444444
        assert tf_rx.fragmented_payload == [memoryview(b"INTENDED EXCEPTION")]

        # Transmission timeout.
        # One times out, one raises an exception --> the result is timeout.
        inf_b.should_timeout = True
        assert not await_(
            ses.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.FAST,
                    transfer_id=2222222222222,
                    fragmented_payload=[memoryview(b"INTENDED EXCEPTION")],
                ),
                loop.time() + 1.0,
            )
        )
        assert ses.sample_statistics().transfers == 1
        assert ses.sample_statistics().payload_bytes == len("INTENDED EXCEPTION")
        assert ses.sample_statistics().errors == 0
        assert ses.sample_statistics().drops == 1
        assert None is await_(rx_a.receive(loop.time() + 1))
        assert None is await_(rx_b.receive(loop.time() + 1))

        # Transmission with exceptions.
        # If all transmissions fail, the call fails.
        inf_b.exception = RuntimeError("INTENDED EXCEPTION")
        with pytest.raises(RuntimeError, match="INTENDED EXCEPTION"):
            assert await_(
                ses.send(
                    Transfer(
                        timestamp=ts,
                        priority=Priority.FAST,
                        transfer_id=3333333333333,
                        fragmented_payload=[memoryview(b"INTENDED EXCEPTION")],
                    ),
                    loop.time() + 1.0,
                )
            )
        assert ses.sample_statistics().transfers == 1
        assert ses.sample_statistics().payload_bytes == len("INTENDED EXCEPTION")
        assert ses.sample_statistics().errors == 1
        assert ses.sample_statistics().drops == 1
        assert None is await_(rx_a.receive(loop.time() + 1))
        assert None is await_(rx_b.receive(loop.time() + 1))

    # Retirement.
    assert not is_retired
    ses.close()
    assert is_retired
    # Make sure the inferiors have been closed.
    assert not tr_a.output_sessions
    assert not tr_b.output_sessions
    # Idempotency.
    is_retired = False
    ses.close()
    assert not is_retired
