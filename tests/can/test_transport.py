from __future__ import annotations

import asyncio
import logging

import pytest

import pycyphal2
from pycyphal2 import Instant, Priority
from pycyphal2._header import MsgBeHeader
from pycyphal2.can import CANTransport
from pycyphal2.can._wire import HEARTBEAT_SUBJECT_ID, TransferKind, make_filter, parse_frame, serialize_transfer
from tests.can._support import MockCANBus, MockCANInterface, wait_for


def _force_distinct_ids(a: CANTransport, b: CANTransport) -> None:
    if a.id != b.id:
        return
    impl = b
    impl._local_node_id = (a.id % 127) + 1  # type: ignore[attr-defined]
    impl._refresh_filters()  # type: ignore[attr-defined]


async def test_pinned_best_effort_uses_13bit_fast_path() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub_if = MockCANInterface(bus, "sub")
    pub = CANTransport.new(pub_if)
    sub = CANTransport.new(sub_if)
    _force_distinct_ids(pub, sub)
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(123, arrivals.append)
    writer = pub.subject_advertise(123)
    payload = b"\x11\x22\x33"
    message = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=0x1234, tag=99).serialize() + payload

    await writer(Instant.now() + 1.0, Priority.NOMINAL, message)
    await wait_for(lambda: len(arrivals) == 1)

    assert len(pub_if.tx_history) == 1
    parsed = parse_frame(pub_if.tx_history[0].id, pub_if.tx_history[0].data)
    assert parsed is not None
    assert parsed.kind is TransferKind.MESSAGE_13
    assert arrivals[0].remote_id == pub.id
    assert arrivals[0].message[:1] == b"\x00"
    assert arrivals[0].message[24:] == payload

    writer.close()
    pub.close()
    sub.close()


async def test_verbatim_subject_uses_16bit_path_and_multiframe_on_classic() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub_if = MockCANInterface(bus, "sub")
    pub = CANTransport.new(pub_if)
    sub = CANTransport.new(sub_if)
    _force_distinct_ids(pub, sub)
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(9000, arrivals.append)
    writer = pub.subject_advertise(9000)
    message = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=0x1234, tag=77).serialize() + b"\xaa\xbb\xcc"

    await writer(Instant.now() + 1.0, Priority.NOMINAL, message)
    await wait_for(lambda: len(arrivals) == 1)

    assert len(pub_if.tx_history) > 1
    parsed = parse_frame(pub_if.tx_history[0].id, pub_if.tx_history[0].data)
    assert parsed is not None
    assert parsed.kind is TransferKind.MESSAGE_16
    assert arrivals[0].message == message

    writer.close()
    pub.close()
    sub.close()


async def test_unicast_roundtrip_uses_service_511_request() -> None:
    bus = MockCANBus()
    a_if = MockCANInterface(bus, "a")
    b_if = MockCANInterface(bus, "b")
    a = CANTransport.new(a_if)
    b = CANTransport.new(b_if)
    _force_distinct_ids(a, b)
    arrivals: list[pycyphal2.TransportArrival] = []
    b.unicast_listen(arrivals.append)

    await a.unicast(Instant.now() + 1.0, Priority.HIGH, b.id, b"hello")
    await wait_for(lambda: len(arrivals) == 1)

    assert len(a_if.tx_history) == 1
    parsed = parse_frame(a_if.tx_history[0].id, a_if.tx_history[0].data)
    assert parsed is not None
    assert parsed.kind is TransferKind.REQUEST
    assert parsed.port_id == 511
    assert parsed.destination_id == b.id
    assert arrivals[0].message == b"hello"

    a.close()
    b.close()


async def test_filter_failure_is_logged_and_retried_promptly(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0", fail_filter_calls=1)
    transport = CANTransport.new(iface)

    await wait_for(lambda: iface.filter_calls >= 1, timeout=0.3)

    assert transport.interfaces == [iface]
    assert any("filter apply failed" in record.message for record in caplog.records)
    transport.close()


async def test_listener_close_refreshes_filters_immediately() -> None:
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0")
    transport = CANTransport.new(iface)
    handle = transport.subject_listen(123, lambda _: None)
    await wait_for(lambda: iface.filter_calls >= 2)
    before = list(iface.filter_history[-1])

    handle.close()
    await wait_for(lambda: iface.filter_calls >= 3)
    after = list(iface.filter_history[-1])

    assert before != after
    subject_filter_16 = make_filter(TransferKind.MESSAGE_16, 123, transport.id)
    subject_filter_13 = make_filter(TransferKind.MESSAGE_13, 123, transport.id)
    assert all(flt.id != subject_filter_16.id or flt.mask != subject_filter_16.mask for flt in after)
    assert all(flt.id != subject_filter_13.id or flt.mask != subject_filter_13.mask for flt in after)
    transport.close()


async def test_collision_intentionally_purges_backend_queue_before_flush() -> None:
    bus = MockCANBus()
    tx_if = MockCANInterface(bus, "tx", defer_tx=True)
    probe = MockCANInterface(bus, "probe")
    transport = CANTransport.new(tx_if)
    writer = transport.subject_advertise(9000)
    payload = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=1, tag=1).serialize() + bytes(range(16))

    await writer(Instant.now() + 1.0, Priority.NOMINAL, payload)
    assert tx_if.tx_history == []
    old_id = transport.id

    collision_id, collision_frames = serialize_transfer(
        kind=TransferKind.MESSAGE_13,
        priority=0,
        port_id=HEARTBEAT_SUBJECT_ID,
        source_id=old_id,
        payload=b"x",
        transfer_id=0,
        fd=False,
    )
    probe.enqueue(collision_id, [memoryview(collision_frames[0])], Instant.now() + 1.0)
    await wait_for(lambda: transport.collision_count == 1)

    assert transport.id != old_id
    assert tx_if.purge_calls >= 1
    tx_if.flush_tx()
    assert tx_if.tx_history == []

    await writer(Instant.now() + 1.0, Priority.NOMINAL, payload)
    tx_if.flush_tx()
    assert tx_if.tx_history
    first = parse_frame(tx_if.tx_history[0].id, tx_if.tx_history[0].data)
    assert first is not None
    assert first.source_id == transport.id

    writer.close()
    transport.close()


async def test_transport_exposes_closed_and_collision_count() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))

    assert transport.closed is False
    assert transport.collision_count == 0

    transport.close()
    assert transport.closed is True


async def test_no_self_loopback_means_publish_does_not_reroll() -> None:
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0")
    transport = CANTransport.new(iface)
    writer = transport.subject_advertise(123)
    old_id = transport.id

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hello")
    await wait_for(lambda: len(iface.tx_history) == 1)

    assert transport.id == old_id
    assert transport.collision_count == 0
    writer.close()
    transport.close()


async def test_reassembly_state_is_not_created_for_non_start_frame() -> None:
    bus = MockCANBus()
    pub_if = MockCANInterface(bus, "pub")
    sub_if = MockCANInterface(bus, "sub")
    sub = CANTransport.new(sub_if)
    sub.subject_listen(7, lambda _: None)

    frame_id, _ = serialize_transfer(
        kind=TransferKind.MESSAGE_16,
        priority=0,
        port_id=7,
        source_id=55,
        payload=b"abcdefghi",
        transfer_id=3,
        fd=False,
    )
    raw = b"abcdefg" + bytes([0x03])
    pub_if.enqueue(frame_id, [memoryview(raw)], Instant.now() + 1.0)
    await asyncio.sleep(0.05)

    endpoint = sub._endpoints[(TransferKind.MESSAGE_16, 7)]  # type: ignore[attr-defined]
    assert endpoint.sessions == {}
    sub.close()
