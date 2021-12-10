# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import pytest
from pytest import raises, approx
import pyuavcan
from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority, ServiceDataSpecifier
from pyuavcan.transport import PayloadMetadata, SessionStatistics, Timestamp, Feedback, Transfer
from pyuavcan.transport.serial._session._output import SerialOutputSession
from pyuavcan.transport.serial import SerialFrame

pytestmark = pytest.mark.asyncio


async def _unittest_output_session() -> None:
    ts = Timestamp.now()
    loop = asyncio.get_event_loop()

    tx_timestamp: typing.Optional[Timestamp] = Timestamp.now()
    tx_exception: typing.Optional[Exception] = None
    last_sent_frames: typing.List[SerialFrame] = []
    last_monotonic_deadline = 0.0
    finalized = False

    async def do_send(frames: typing.Sequence[SerialFrame], monotonic_deadline: float) -> typing.Optional[Timestamp]:
        nonlocal last_sent_frames
        nonlocal last_monotonic_deadline
        last_sent_frames = list(frames)
        last_monotonic_deadline = monotonic_deadline
        if tx_exception:
            raise tx_exception
        return tx_timestamp

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        SerialOutputSession(
            specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 1111),
            payload_metadata=PayloadMetadata(1024),
            mtu=10,
            local_node_id=None,
            send_handler=do_send,
            finalizer=do_finalize,
        )

    sos = SerialOutputSession(
        specifier=OutputSessionSpecifier(MessageDataSpecifier(3210), None),
        payload_metadata=PayloadMetadata(1024),
        mtu=11,
        local_node_id=None,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    assert sos.specifier == OutputSessionSpecifier(MessageDataSpecifier(3210), None)
    assert sos.destination_node_id is None
    assert sos.payload_metadata == PayloadMetadata(1024)
    assert sos.sample_statistics() == SessionStatistics()

    assert await (
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            999999999.999,
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 1

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        await (
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three four five")],
                ),
                loop.time() + 10.0,
            )
        )

    last_feedback: typing.Optional[Feedback] = None

    def feedback_handler(feedback: Feedback) -> None:
        nonlocal last_feedback
        last_feedback = feedback

    sos.enable_feedback(feedback_handler)

    assert last_feedback is None
    assert await (
        sos.send(
            Transfer(timestamp=ts, priority=Priority.NOMINAL, transfer_id=12340, fragmented_payload=[]), 999999999.999
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 1
    assert last_feedback is not None
    assert last_feedback.original_transfer_timestamp == ts
    assert last_feedback.first_frame_transmission_timestamp == tx_timestamp

    sos.disable_feedback()
    sos.disable_feedback()  # Idempotency check

    assert sos.sample_statistics() == SessionStatistics(transfers=2, frames=2, payload_bytes=11, errors=0, drops=0)

    assert not finalized
    sos.close()
    assert finalized
    finalized = False

    sos = SerialOutputSession(
        specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(1024),
        mtu=10,
        local_node_id=1234,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    # Induced failure
    tx_timestamp = None
    assert not await (
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            999999999.999,
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 2

    assert sos.sample_statistics() == SessionStatistics(transfers=0, frames=0, payload_bytes=0, errors=0, drops=2)

    tx_exception = RuntimeError()
    with raises(RuntimeError):
        _ = await (
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )

    assert sos.sample_statistics() == SessionStatistics(transfers=0, frames=0, payload_bytes=0, errors=1, drops=2)

    assert not finalized
    sos.close()
    assert finalized
    sos.close()  # Idempotency

    with raises(pyuavcan.transport.ResourceClosedError):
        await (
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )
