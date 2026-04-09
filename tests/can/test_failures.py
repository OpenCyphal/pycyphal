from __future__ import annotations

import asyncio

import pytest

import pycyphal2
from pycyphal2 import ClosedError, Instant, Priority, SendError
from pycyphal2.can import CANTransport
from pycyphal2.can._wire import TransferKind, make_tail_byte, serialize_transfer
from tests.can._support import MockCANBus, MockCANInterface, wait_for


def _remote_source_id(transport: CANTransport) -> int:
    return 1 if transport.id != 1 else 2


async def test_all_transient_enqueue_failures_raise_send_error() -> None:
    bus = MockCANBus()
    a = MockCANInterface(bus, "a", transient_enqueue_failures=1)
    b = MockCANInterface(bus, "b", transient_enqueue_failures=1)
    transport = CANTransport.new([a, b])
    writer = transport.subject_advertise(7)

    with pytest.raises(SendError) as exc_info:
        await writer(Instant.now() + 1.0, Priority.NOMINAL, b"x")

    assert not isinstance(exc_info.value, ClosedError)
    assert isinstance(exc_info.value.__cause__, OSError)
    assert transport.closed is False
    assert len(transport.interfaces) == 2
    writer.close()
    transport.close()


async def test_closed_enqueue_failure_evicts_last_interface_and_closes_transport() -> None:
    bus = MockCANBus()
    iface = MockCANInterface(bus, "a", fail_enqueue_closed=True)
    transport = CANTransport.new(iface)
    writer = transport.subject_advertise(7)

    with pytest.raises(ClosedError):
        await writer(Instant.now() + 1.0, Priority.NOMINAL, b"x")

    assert transport.closed is True
    assert transport.interfaces == []
    writer.close()


async def test_garbage_can_id_dropped() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(7, arrivals.append)

    bogus_id = (4 << 26) | (1 << 23) | (7 << 8) | 5
    tail = make_tail_byte(True, True, True, 0)
    pub_if.enqueue(bogus_id, [memoryview(b"hello" + bytes([tail]))], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    assert arrivals == []
    sub.close()


async def test_truncated_frame_dropped() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(7, arrivals.append)

    frame_id, _ = serialize_transfer(
        kind=TransferKind.MESSAGE_16,
        priority=0,
        port_id=7,
        source_id=_remote_source_id(sub),
        payload=b"x",
        transfer_id=0,
        fd=False,
    )
    pub_if.enqueue(frame_id, [memoryview(b"")], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    assert arrivals == []
    sub.close()


async def test_corrupted_multiframe_crc_is_dropped() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(7, arrivals.append)

    frame_id, frames = serialize_transfer(
        kind=TransferKind.MESSAGE_16,
        priority=0,
        port_id=7,
        source_id=_remote_source_id(sub),
        payload=bytes(range(20)),
        transfer_id=3,
        fd=False,
    )
    bad = bytearray(frames[1])
    bad[0] ^= 0xFF
    frames[1] = bytes(bad)
    pub_if.enqueue(frame_id, [memoryview(frame) for frame in frames], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    assert arrivals == []
    sub.close()


async def test_anonymous_single_frame_reports_remote_id_255() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(123, arrivals.append)

    anonymous_id = (3 << 21) | (1 << 24) | (123 << 8)
    tail = make_tail_byte(True, True, True, 0)
    pub_if.enqueue(anonymous_id, [memoryview(b"anon" + bytes([tail]))], Instant.now() + 1.0)
    await wait_for(lambda: len(arrivals) == 1)

    assert arrivals[0].remote_id == 0xFF
    assert arrivals[0].message[24:] == b"anon"
    sub.close()


async def test_wrong_destination_unicast_is_dropped() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.unicast_listen(arrivals.append)

    destination = 1 if sub.id != 1 else 2
    if destination == sub.id:
        destination = 3
    frame_id, frames = serialize_transfer(
        kind=TransferKind.REQUEST,
        priority=0,
        port_id=511,
        source_id=_remote_source_id(sub),
        destination_id=destination,
        payload=b"ping",
        transfer_id=0,
        fd=False,
    )
    pub_if.enqueue(frame_id, [memoryview(frames[0])], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    assert arrivals == []
    sub.close()


async def test_service_response_is_dropped() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.unicast_listen(arrivals.append)

    frame_id, frames = serialize_transfer(
        kind=TransferKind.RESPONSE,
        priority=0,
        port_id=511,
        source_id=_remote_source_id(sub),
        destination_id=sub.id,
        payload=b"pong",
        transfer_id=0,
        fd=False,
    )
    pub_if.enqueue(frame_id, [memoryview(frames[0])], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    assert arrivals == []
    sub.close()
