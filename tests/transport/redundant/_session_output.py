# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import time
import typing
import logging
import asyncio
import pytest
import pyuavcan
from pyuavcan.transport import ResourceClosedError
from pyuavcan.transport import Transfer, Timestamp, Priority, SessionStatistics
from pyuavcan.transport import TransferFrom
from pyuavcan.transport.loopback import LoopbackTransport, LoopbackFeedback
from pyuavcan.transport.redundant._session._output import RedundantOutputSession
from pyuavcan.transport.redundant import RedundantSessionStatistics, RedundantFeedback

pytestmark = pytest.mark.asyncio


async def _unittest_redundant_output() -> None:
    loop = asyncio.get_event_loop()

    spec = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_rx = pyuavcan.transport.InputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30 * 1024 * 1024)

    ts = Timestamp.now()

    is_retired = False

    def retire() -> None:
        nonlocal is_retired
        is_retired = True

    ses = RedundantOutputSession(spec, meta, finalizer=retire)
    assert not is_retired
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()

    # Transmit with an empty set of inferiors.
    time_before = loop.time()
    assert not await (
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
        print("sleeping before adding the inferior...")
        await asyncio.sleep(2.0)
        print("adding the inferior...")
        ses._add_inferior(inferior)  # pylint: disable=protected-access
        print("inferior has been added.")

    assert await (
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
    tf_rx = await (rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 9876543210
    assert tf_rx.fragmented_payload == [memoryview(b"def")]
    assert None is await (rx_b.receive(loop.time() + 0.1))

    # Enable feedback.
    feedback: typing.List[RedundantFeedback] = []
    ses.enable_feedback(feedback.append)
    assert await (
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
    tf_rx = await (rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 555555555555
    assert tf_rx.fragmented_payload == [memoryview(b"qwerty")]
    assert None is await (rx_b.receive(loop.time() + 0.1))

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
    assert await (
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
    tf_rx = await (rx_a.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 777777777777
    assert tf_rx.fragmented_payload == [memoryview(b"fgsfds")]
    tf_rx = await (rx_b.receive(loop.time() + 1))
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
    assert await (
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
    assert None is await (rx_a.receive(loop.time() + 1))
    tf_rx = await (rx_b.receive(loop.time() + 1))
    assert isinstance(tf_rx, TransferFrom)
    assert tf_rx.transfer_id == 88888888888888
    assert tf_rx.fragmented_payload == [memoryview(b"hedgehog")]

    # Disable the feedback.
    ses.disable_feedback()
    # A diversion - enable the feedback in the inferior and make sure it's not propagated.
    ses._enable_feedback_on_inferior(inf_b)  # pylint: disable=protected-access
    assert await (
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
    assert None is await (rx_a.receive(loop.time() + 1))
    tf_rx = await (rx_b.receive(loop.time() + 1))
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
        await (
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

    assert None is await (rx_a.receive(loop.time() + 1))
    assert None is await (rx_b.receive(loop.time() + 1))


async def _unittest_redundant_output_exceptions(caplog: typing.Any) -> None:
    loop = asyncio.get_event_loop()

    spec = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_rx = pyuavcan.transport.InputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30 * 1024 * 1024)

    ts = Timestamp.now()

    is_retired = False

    def retire() -> None:
        nonlocal is_retired
        is_retired = True

    ses = RedundantOutputSession(spec, meta, finalizer=retire)
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
        assert await (
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
        assert None is await (rx_a.receive(loop.time() + 1))
        tf_rx = await (rx_b.receive(loop.time() + 1))
        assert isinstance(tf_rx, TransferFrom)
        assert tf_rx.transfer_id == 444444444444
        assert tf_rx.fragmented_payload == [memoryview(b"INTENDED EXCEPTION")]

        # Transmission timeout.
        # One times out, one raises an exception --> the result is timeout.
        inf_b.should_timeout = True
        assert not await (
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
        assert None is await (rx_a.receive(loop.time() + 1))
        assert None is await (rx_b.receive(loop.time() + 1))

        # Transmission with exceptions.
        # If all transmissions fail, the call fails.
        inf_b.exception = RuntimeError("INTENDED EXCEPTION")
        with pytest.raises(RuntimeError, match="INTENDED EXCEPTION"):
            assert await (
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
        assert None is await (rx_a.receive(loop.time() + 1))
        assert None is await (rx_b.receive(loop.time() + 1))

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
