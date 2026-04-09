from __future__ import annotations

import pycyphal2
from pycyphal2 import Instant, Priority
from pycyphal2.can import CANTransport
from pycyphal2.can._wire import (
    HEARTBEAT_SUBJECT_ID,
    LEGACY_NODE_STATUS_SUBJECT_ID,
    TransferKind,
    make_tail_byte,
    parse_frame,
    serialize_transfer,
)
from tests.can._support import MockCANBus, MockCANInterface, wait_for


def _heartbeat_from(source_id: int) -> tuple[int, bytes]:
    identifier, frames = serialize_transfer(
        kind=TransferKind.MESSAGE_13,
        priority=Priority.NOMINAL,
        port_id=HEARTBEAT_SUBJECT_ID,
        source_id=source_id,
        payload=b"x",
        transfer_id=0,
        fd=False,
    )
    return identifier, frames[0]


async def test_collision_triggers_reroll_and_counts() -> None:
    bus = MockCANBus()
    probe = MockCANInterface(bus, "probe")
    transport = CANTransport.new(MockCANInterface(bus, "sub"))
    old_id = transport.id

    identifier, frame = _heartbeat_from(old_id)
    probe.enqueue(identifier, [memoryview(frame)], Instant.now() + 1.0)
    await wait_for(lambda: transport.id != old_id)

    assert transport.id != old_id
    assert transport.collision_count == 1
    transport.close()


async def test_v0_node_status_collision_triggers_reroll_and_counts() -> None:
    bus = MockCANBus()
    probe = MockCANInterface(bus, "probe")
    transport = CANTransport.new(MockCANInterface(bus, "sub"))
    old_id = transport.id

    identifier = (int(Priority.NOMINAL) << 26) | (LEGACY_NODE_STATUS_SUBJECT_ID << 8) | old_id
    frame = b"x" + bytes([make_tail_byte(True, True, False, 0)])
    probe.enqueue(identifier, [memoryview(frame)], Instant.now() + 1.0)
    await wait_for(lambda: transport.id != old_id)

    assert transport.id != old_id
    assert transport.collision_count == 1
    transport.close()


async def test_unicast_filter_refresh_is_immediate_after_reroll() -> None:
    bus = MockCANBus()
    collision = MockCANInterface(bus, "collision")
    sender = MockCANInterface(bus, "sender")
    transport = CANTransport.new(MockCANInterface(bus, "sub"))
    arrivals: list[pycyphal2.TransportArrival] = []
    old_id = transport.id
    transport.unicast_listen(arrivals.append)

    identifier, frame = _heartbeat_from(old_id)
    collision.enqueue(identifier, [memoryview(frame)], Instant.now() + 1.0)
    await wait_for(lambda: transport.id != old_id)

    request_id, request_frames = serialize_transfer(
        kind=TransferKind.REQUEST,
        priority=Priority.FAST,
        port_id=511,
        source_id=1 if transport.id != 1 else 2,
        destination_id=transport.id,
        payload=b"ping",
        transfer_id=0,
        fd=False,
    )
    sender.enqueue(request_id, [memoryview(request_frames[0])], Instant.now() + 1.0)
    await wait_for(lambda: len(arrivals) == 1, timeout=0.2)

    assert arrivals[0].message == b"ping"
    assert transport.collision_count == 1
    transport.close()


async def test_publish_does_not_reroll_without_self_loopback() -> None:
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0")
    transport = CANTransport.new(iface)
    writer = transport.subject_advertise(1234)
    old_id = transport.id

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hello")
    await wait_for(lambda: len(iface.tx_history) == 1)

    parsed = parse_frame(iface.tx_history[0].id, iface.tx_history[0].data)
    assert parsed is not None
    assert parsed.source_id == old_id
    assert transport.id == old_id
    assert transport.collision_count == 0

    writer.close()
    transport.close()


async def test_dense_occupancy_probabilistic_purge_resets_bitmap() -> None:
    class _AlwaysPurgeRNG:
        def __init__(self) -> None:
            self.calls = 0

        def randrange(self, stop: int) -> int:
            assert stop > 0
            self.calls += 1
            return 0

    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "sub"))
    transport._local_node_id = 120  # type: ignore[attr-defined]
    transport._node_id_occupancy = sum(1 << i for i in range(64))  # type: ignore[attr-defined]
    rng = _AlwaysPurgeRNG()
    transport._rng = rng  # type: ignore[attr-defined]

    transport._node_id_occupancy_update(64)  # type: ignore[attr-defined]

    assert transport.id == 120
    assert transport.collision_count == 0
    assert rng.calls == 1
    assert transport._node_id_occupancy == ((1 << 0) | (1 << 64))  # type: ignore[attr-defined]
    transport.close()
