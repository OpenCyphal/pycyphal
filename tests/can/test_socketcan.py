from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import pytest

from pycyphal2 import Instant, Priority
from pycyphal2._transport import TransportArrival
from pycyphal2.can import CANTransport
from pycyphal2.can._wire import HEARTBEAT_SUBJECT_ID, TransferKind, serialize_transfer
from tests.can._support import wait_for

socketcan = pytest.importorskip("pycyphal2.can.socketcan", reason="SocketCAN backend unavailable")
SocketCANInterface = socketcan.SocketCANInterface
list_interfaces = SocketCANInterface.list_interfaces

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not Path("/sys/class/net/vcan0").exists(),
    reason="SocketCAN live tests require Linux with vcan0",
)


def test_list_interfaces_includes_vcan0() -> None:
    assert "vcan0" in list_interfaces()


async def test_socketcan_pubsub_smoke() -> None:
    a = CANTransport.new(SocketCANInterface("vcan0"))
    b = CANTransport.new(SocketCANInterface("vcan0"))
    arrivals: list[TransportArrival] = []
    b.subject_listen(1234, arrivals.append)
    writer = a.subject_advertise(1234)

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hello")
    await wait_for(lambda: len(arrivals) == 1, timeout=2.0)
    assert arrivals[0].message == b"hello"

    writer.close()
    a.close()
    b.close()


async def test_socketcan_unicast_smoke() -> None:
    a = CANTransport.new(SocketCANInterface("vcan0"))
    b = CANTransport.new(SocketCANInterface("vcan0"))
    arrivals: list[TransportArrival] = []
    b.unicast_listen(arrivals.append)

    await a.unicast(Instant.now() + 1.0, Priority.FAST, b.id, b"ping")
    await wait_for(lambda: len(arrivals) == 1, timeout=2.0)
    assert arrivals[0].message == b"ping"

    a.close()
    b.close()


async def test_socketcan_reroll_then_immediate_unicast() -> None:
    target = CANTransport.new(SocketCANInterface("vcan0"))
    collision = SocketCANInterface("vcan0")
    sender = SocketCANInterface("vcan0")
    arrivals: list[TransportArrival] = []
    old_id = target.id
    target.unicast_listen(arrivals.append)

    heartbeat_id, heartbeat_frames = serialize_transfer(
        kind=TransferKind.MESSAGE_13,
        priority=0,
        port_id=HEARTBEAT_SUBJECT_ID,
        source_id=old_id,
        payload=b"x",
        transfer_id=0,
        fd=False,
    )
    collision.enqueue(heartbeat_id, [memoryview(heartbeat_frames[0])], Instant.now() + 1.0)
    await wait_for(lambda: target.id != old_id, timeout=2.0)

    request_id, request_frames = serialize_transfer(
        kind=TransferKind.REQUEST,
        priority=int(Priority.FAST),
        port_id=511,
        source_id=1 if target.id != 1 else 2,
        destination_id=target.id,
        payload=b"ping",
        transfer_id=0,
        fd=False,
    )
    sender.enqueue(request_id, [memoryview(request_frames[0])], Instant.now() + 1.0)
    await wait_for(lambda: len(arrivals) == 1, timeout=2.0)

    assert arrivals[0].message == b"ping"
    assert target.collision_count == 1
    collision.close()
    sender.close()
    target.close()


async def test_socketcan_self_publish_does_not_reroll() -> None:
    transport = CANTransport.new(SocketCANInterface("vcan0"))
    writer = transport.subject_advertise(1234)
    old_id = transport.id

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hello")
    await asyncio.sleep(0.1)

    assert transport.id == old_id
    assert transport.collision_count == 0
    writer.close()
    transport.close()
