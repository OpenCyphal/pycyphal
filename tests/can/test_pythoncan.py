"""Tests for pycyphal2.can.pythoncan -- python-can Interface backend."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import threading
from typing import cast
from unittest.mock import MagicMock

import pytest

from pycyphal2 import ClosedError, Instant, Priority
from pycyphal2._transport import TransportArrival
from pycyphal2.can import CANTransport, Filter, TimestampedFrame
from tests.can._support import wait_for

can = pytest.importorskip("can", reason="python-can is not installed")

import can as _can  # noqa: E402  (re-import after skip gate for mypy)

import pycyphal2.can.pythoncan as pythoncan  # noqa: E402

PythonCANInterface = pythoncan.PythonCANInterface

# ============================================================================
# Helpers
# ============================================================================

_CHANNEL_SEQ = 0


def _unique_channel() -> str:
    global _CHANNEL_SEQ
    _CHANNEL_SEQ += 1
    return f"pycyphal2_test_{_CHANNEL_SEQ}"


def _force_distinct_ids(a: CANTransport, b: CANTransport) -> None:
    if a.id != b.id:
        return
    b._local_node_id = (a.id % 127) + 1  # type: ignore[attr-defined]
    b._refresh_filters()  # type: ignore[attr-defined]


def _virtual_pair(
    *, fd: bool = False, receive_own_messages: bool = False
) -> tuple[PythonCANInterface, PythonCANInterface]:
    """Create a pair of PythonCANInterface instances on the same virtual channel."""
    ch = _unique_channel()
    a = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=receive_own_messages),
        fd=fd,
    )
    b = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=receive_own_messages),
        fd=fd,
    )
    return a, b


def _close_all(*interfaces: PythonCANInterface) -> None:
    for itf in interfaces:
        itf.close()


# ============================================================================
# Tier 1: Virtual bus tests (cross-platform, always runnable)
# ============================================================================


async def test_virtual_send_receive_classic() -> None:
    """Two interfaces on the same virtual channel: A sends extended frame, B receives it."""
    a, b = _virtual_pair()
    try:
        ts_before = Instant.now()
        a.enqueue(0x1BADC0DE, [memoryview(b"hello")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        ts_after = Instant.now()
        assert frame.id == 0x1BADC0DE
        assert frame.data == b"hello"
        assert ts_before.ns <= frame.timestamp.ns <= ts_after.ns
    finally:
        _close_all(a, b)


async def test_virtual_send_receive_fd() -> None:
    """CAN FD mode with >8 byte payload."""
    a, b = _virtual_pair(fd=True)
    try:
        payload = bytes(range(48))
        a.enqueue(0x00112233, [memoryview(payload)], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00112233
        assert frame.data == payload
    finally:
        _close_all(a, b)


async def test_virtual_send_receive_classic_8_bytes() -> None:
    """Classic CAN with exactly 8 bytes -- the maximum for non-FD."""
    a, b = _virtual_pair()
    try:
        payload = bytes(range(8))
        a.enqueue(0x00000001, [memoryview(payload)], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000001
        assert frame.data == payload
    finally:
        _close_all(a, b)


async def test_virtual_send_receive_empty_payload() -> None:
    """Frame with zero-length data field."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x12345678, [memoryview(b"")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x12345678
        assert frame.data == b""
    finally:
        _close_all(a, b)


async def test_virtual_multi_frame_enqueue() -> None:
    """Multiple frames from a single enqueue() call arrive in order."""
    a, b = _virtual_pair()
    try:
        frames_data = [memoryview(bytes([i]) * 4) for i in range(5)]
        a.enqueue(0x00AABBCC, frames_data, Instant.now() + 2.0)
        received = []
        for _ in range(5):
            frame = await asyncio.wait_for(b.receive(), timeout=2.0)
            received.append(frame)
        assert len(received) == 5
        for i, frame in enumerate(received):
            assert frame.id == 0x00AABBCC
            assert frame.data == bytes([i]) * 4
    finally:
        _close_all(a, b)


async def test_virtual_multi_frame_different_payloads() -> None:
    """Enqueue frames with varying payload sizes."""
    a, b = _virtual_pair()
    try:
        payloads = [b"", b"\x01", b"\x02\x03", b"\x04\x05\x06\x07\x08\x09\x0a\x0b"]
        views = [memoryview(p) for p in payloads]
        a.enqueue(0x10000000, views, Instant.now() + 2.0)
        for expected in payloads:
            frame = await asyncio.wait_for(b.receive(), timeout=2.0)
            assert frame.data == expected
    finally:
        _close_all(a, b)


async def test_virtual_bidirectional() -> None:
    """Both sides can send and receive."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x00000001, [memoryview(b"from_a")], Instant.now() + 2.0)
        b.enqueue(0x00000002, [memoryview(b"from_b")], Instant.now() + 2.0)
        frame_at_b = await asyncio.wait_for(b.receive(), timeout=2.0)
        frame_at_a = await asyncio.wait_for(a.receive(), timeout=2.0)
        assert frame_at_b.id == 0x00000001
        assert frame_at_b.data == b"from_a"
        assert frame_at_a.id == 0x00000002
        assert frame_at_a.data == b"from_b"
    finally:
        _close_all(a, b)


async def test_virtual_deadline_expired() -> None:
    """Frames with an already-expired deadline are dropped."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x1FFFFFFF, [memoryview(b"expired")], Instant.now() + (-1.0))
        # Send a second frame with a valid deadline so we can verify the first was dropped.
        await asyncio.sleep(0.05)
        a.enqueue(0x00000042, [memoryview(b"valid")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000042
        assert frame.data == b"valid"
    finally:
        _close_all(a, b)


async def test_virtual_purge() -> None:
    """Purged frames are not transmitted."""
    ch = _unique_channel()
    a = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        # Enqueue a bunch of frames but purge before the TX loop processes them.
        # Using a very distant deadline to ensure they won't expire on their own.
        for i in range(10):
            a.enqueue(0x00000010 + i, [memoryview(b"purge_me")], Instant.now() + 60.0)
        a.purge()
        # Send a sentinel frame to prove the bus is still functional.
        a.enqueue(0x000000FF, [memoryview(b"sentinel")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x000000FF
        assert frame.data == b"sentinel"
    finally:
        _close_all(a, b)


async def test_virtual_filter_acceptance() -> None:
    """Hardware filter configuration: only matching frames pass through."""
    a, b = _virtual_pair()
    try:
        # Accept only id=0x100 with exact mask for the lower 12 bits.
        b.filter([Filter(id=0x00000100, mask=0x00000FFF)])
        a.enqueue(0x00000100, [memoryview(b"pass")], Instant.now() + 2.0)
        a.enqueue(0x00000200, [memoryview(b"reject")], Instant.now() + 2.0)
        a.enqueue(0x00000100, [memoryview(b"pass2")], Instant.now() + 2.0)
        # We expect exactly two frames through.
        frame1 = await asyncio.wait_for(b.receive(), timeout=2.0)
        frame2 = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame1.data == b"pass"
        assert frame2.data == b"pass2"
    finally:
        _close_all(a, b)


async def test_virtual_filter_promiscuous() -> None:
    """Promiscuous filter accepts all frames."""
    a, b = _virtual_pair()
    try:
        b.filter([Filter.promiscuous()])
        a.enqueue(0x00000001, [memoryview(b"one")], Instant.now() + 2.0)
        a.enqueue(0x1FFFFFFF, [memoryview(b"two")], Instant.now() + 2.0)
        f1 = await asyncio.wait_for(b.receive(), timeout=2.0)
        f2 = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert f1.data == b"one"
        assert f2.data == b"two"
    finally:
        _close_all(a, b)


async def test_virtual_filter_multiple() -> None:
    """Multiple filters: frame must match at least one.
    Note: TX PriorityQueue sorts by CAN ID, so arrival order may differ from enqueue order across different IDs.
    """
    a, b = _virtual_pair()
    try:
        b.filter(
            [
                Filter(id=0x00000100, mask=0x1FFFFFFF),
                Filter(id=0x00000200, mask=0x1FFFFFFF),
            ]
        )
        a.enqueue(0x00000100, [memoryview(b"match1")], Instant.now() + 2.0)
        a.enqueue(0x00000200, [memoryview(b"match2")], Instant.now() + 2.0)
        a.enqueue(0x00000300, [memoryview(b"nomatch")], Instant.now() + 2.0)
        a.enqueue(0x00000100, [memoryview(b"sentinel")], Instant.now() + 2.0)
        received = []
        for _ in range(3):
            received.append(await asyncio.wait_for(b.receive(), timeout=2.0))
        rx_data = sorted(f.data for f in received)
        assert b"match1" in rx_data
        assert b"match2" in rx_data
        assert b"sentinel" in rx_data
        assert all(f.id in (0x00000100, 0x00000200) for f in received)
    finally:
        _close_all(a, b)


async def test_virtual_close_idempotent() -> None:
    """Calling close() multiple times does not raise."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    itf.close()
    itf.close()


async def test_virtual_operations_after_close_enqueue() -> None:
    """enqueue() after close raises ClosedError."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    with pytest.raises(ClosedError):
        itf.enqueue(0x100, [memoryview(b"x")], Instant.now() + 1.0)


async def test_virtual_operations_after_close_filter() -> None:
    """filter() after close raises ClosedError."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    with pytest.raises(ClosedError):
        itf.filter([Filter.promiscuous()])


async def test_virtual_operations_after_close_receive() -> None:
    """receive() after close raises ClosedError."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    with pytest.raises(ClosedError):
        await itf.receive()


async def test_virtual_receive_unblocks_on_close() -> None:
    """A pending receive() call raises ClosedError when the interface is closed."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))

    async def close_later() -> None:
        await asyncio.sleep(0.1)
        itf.close()

    closer = asyncio.ensure_future(close_later())
    with pytest.raises(ClosedError):
        await asyncio.wait_for(itf.receive(), timeout=2.0)
    await closer


async def test_virtual_properties() -> None:
    """Verify name, fd, and repr properties."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch)
    itf = PythonCANInterface(bus, fd=False)
    try:
        assert itf.fd is False
        assert "PythonCANInterface" in repr(itf)
        assert "fd=False" in repr(itf)
    finally:
        itf.close()


async def test_virtual_properties_fd() -> None:
    """Verify fd property when FD mode is enabled."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch, fd=True)
    itf = PythonCANInterface(bus, fd=True)
    try:
        assert itf.fd is True
        assert "fd=True" in repr(itf)
    finally:
        itf.close()


async def test_virtual_fd_default_from_protocol() -> None:
    """fd defaults from bus.protocol; virtual bus reports CAN_20 so fd=False."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        assert itf.fd is False
    finally:
        itf.close()


async def test_virtual_fd_explicit_true() -> None:
    """Explicit fd=True overrides bus.protocol."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch), fd=True)
    try:
        assert itf.fd is True
    finally:
        itf.close()


async def test_virtual_fd_explicit_false() -> None:
    """Explicit fd=False overrides bus.protocol."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch), fd=False)
    try:
        assert itf.fd is False
    finally:
        itf.close()


async def test_virtual_non_extended_dropped() -> None:
    """Standard (non-extended) ID frames are silently dropped by the receiver."""
    ch = _unique_channel()
    bus_a = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    b = PythonCANInterface(bus_b)
    try:
        # Send a standard-ID frame directly via the raw bus (bypass PythonCANInterface which always sets extended).
        std_msg = _can.Message(arbitration_id=0x100, is_extended_id=False, data=b"std")
        bus_a.send(std_msg)
        # Now send an extended-ID frame that should arrive.
        ext_msg = _can.Message(arbitration_id=0x00000200, is_extended_id=True, data=b"ext")
        bus_a.send(ext_msg)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000200
        assert frame.data == b"ext"
    finally:
        b.close()
        bus_a.shutdown()


async def test_virtual_remote_frame_dropped() -> None:
    """Remote (RTR) frames are silently dropped."""
    ch = _unique_channel()
    bus_a = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    b = PythonCANInterface(bus_b)
    try:
        rtr_msg = _can.Message(arbitration_id=0x00000300, is_extended_id=True, is_remote_frame=True, dlc=8)
        bus_a.send(rtr_msg)
        ext_msg = _can.Message(arbitration_id=0x00000400, is_extended_id=True, data=b"ok")
        bus_a.send(ext_msg)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000400
        assert frame.data == b"ok"
    finally:
        b.close()
        bus_a.shutdown()


async def test_virtual_overlength_frame_dropped() -> None:
    """A malformed >64-byte frame from the bus is silently dropped, not crash the RX thread."""
    ch = _unique_channel()
    bus_a = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    b = PythonCANInterface(bus_b)
    try:
        # Inject a malformed overlength message directly through the raw bus.
        bad_msg = _can.Message(arbitration_id=0x00000600, is_extended_id=True, data=bytes(65))
        bus_a.send(bad_msg)
        # Send a valid frame afterwards to prove the RX thread survived.
        good_msg = _can.Message(arbitration_id=0x00000601, is_extended_id=True, data=b"ok")
        bus_a.send(good_msg)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000601
        assert frame.data == b"ok"
    finally:
        b.close()
        bus_a.shutdown()


async def test_virtual_self_loopback() -> None:
    """With receive_own_messages, the sender also receives its own frames."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True)
    itf = PythonCANInterface(bus)
    try:
        itf.enqueue(0x00000500, [memoryview(b"echo")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(itf.receive(), timeout=2.0)
        assert frame.id == 0x00000500
        assert frame.data == b"echo"
    finally:
        itf.close()


async def test_virtual_many_frames_throughput() -> None:
    """Send many frames in sequence to exercise the TX/RX path under load."""
    a, b = _virtual_pair()
    n = 50
    try:
        for i in range(n):
            a.enqueue(0x00001000 + i, [memoryview(i.to_bytes(2, "big"))], Instant.now() + 5.0)
        received = []
        for _ in range(n):
            frame = await asyncio.wait_for(b.receive(), timeout=5.0)
            received.append(frame)
        assert len(received) == n
        for i, frame in enumerate(received):
            assert frame.id == 0x00001000 + i
            assert frame.data == i.to_bytes(2, "big")
    finally:
        _close_all(a, b)


async def test_virtual_timestamp_ordering() -> None:
    """Timestamps of received frames are monotonically non-decreasing."""
    a, b = _virtual_pair()
    n = 20
    try:
        for i in range(n):
            a.enqueue(0x00002000, [memoryview(bytes([i]))], Instant.now() + 5.0)
        prev_ts = 0
        for _ in range(n):
            frame = await asyncio.wait_for(b.receive(), timeout=5.0)
            assert frame.timestamp.ns >= prev_ts
            prev_ts = frame.timestamp.ns
    finally:
        _close_all(a, b)


async def test_virtual_max_extended_id() -> None:
    """Frame with the maximum 29-bit extended CAN ID."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x1FFFFFFF, [memoryview(b"max")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x1FFFFFFF
        assert frame.data == b"max"
    finally:
        _close_all(a, b)


async def test_virtual_min_extended_id() -> None:
    """Frame with CAN ID = 0."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x00000000, [memoryview(b"min")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00000000
        assert frame.data == b"min"
    finally:
        _close_all(a, b)


async def test_virtual_transport_pubsub() -> None:
    """Full transport-level publish/subscribe through PythonCANInterface."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    arrivals: list[TransportArrival] = []
    b.subject_listen(1234, arrivals.append)
    writer = a.subject_advertise(1234)
    try:
        await writer(Instant.now() + 2.0, Priority.NOMINAL, b"hello_pythoncan")
        await wait_for(lambda: len(arrivals) == 1, timeout=3.0)
        assert arrivals[0].message == b"hello_pythoncan"
    finally:
        writer.close()
        a.close()
        b.close()


async def test_virtual_transport_unicast() -> None:
    """Full transport-level unicast through PythonCANInterface."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    arrivals: list[TransportArrival] = []
    b.unicast_listen(arrivals.append)
    try:
        await a.unicast(Instant.now() + 2.0, Priority.FAST, b.id, b"ping_pythoncan")
        await wait_for(lambda: len(arrivals) == 1, timeout=3.0)
        assert arrivals[0].message == b"ping_pythoncan"
    finally:
        a.close()
        b.close()


async def test_virtual_transport_multi_message() -> None:
    """Multiple messages through the transport layer."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    arrivals: list[TransportArrival] = []
    b.subject_listen(5678, arrivals.append)
    writer = a.subject_advertise(5678)
    try:
        for i in range(5):
            await writer(Instant.now() + 2.0, Priority.NOMINAL, f"msg{i}".encode())
        await wait_for(lambda: len(arrivals) == 5, timeout=5.0)
        for i, arrival in enumerate(arrivals):
            assert arrival.message == f"msg{i}".encode()
    finally:
        writer.close()
        a.close()
        b.close()


async def test_virtual_purge_does_not_raise_when_closed() -> None:
    """purge() on a closed interface is a no-op."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    itf.purge()  # Should not raise.


async def test_virtual_fd_various_payload_sizes() -> None:
    """CAN FD with various payload sizes up to 64 bytes."""
    a, b = _virtual_pair(fd=True)
    try:
        sizes = [0, 1, 8, 12, 16, 20, 24, 32, 48, 64]
        for size in sizes:
            payload = bytes(range(size)) if size <= 256 else bytes(range(256))[:size]
            a.enqueue(0x00003000, [memoryview(payload)], Instant.now() + 2.0)
        for size in sizes:
            frame = await asyncio.wait_for(b.receive(), timeout=2.0)
            expected = bytes(range(size)) if size <= 256 else bytes(range(256))[:size]
            assert frame.data == expected, f"Mismatch for size {size}"
    finally:
        _close_all(a, b)


async def test_virtual_interleaved_enqueue_receive() -> None:
    """Interleaved enqueue and receive operations."""
    a, b = _virtual_pair()
    try:
        for i in range(10):
            a.enqueue(0x00004000 + i, [memoryview(bytes([i]))], Instant.now() + 2.0)
            frame = await asyncio.wait_for(b.receive(), timeout=2.0)
            assert frame.id == 0x00004000 + i
            assert frame.data == bytes([i])
    finally:
        _close_all(a, b)


# ============================================================================
# Tier 2: Unit tests (mocking python-can internals)
# ============================================================================


def test_parse_message_valid_extended() -> None:
    """_parse_message accepts a valid extended-ID data frame."""
    msg = _can.Message(arbitration_id=0x1BADC0DE, is_extended_id=True, data=b"valid")
    frame = pythoncan._parse_message(msg)
    assert frame is not None
    assert frame.id == 0x1BADC0DE
    assert frame.data == b"valid"
    assert isinstance(frame, TimestampedFrame)


def test_parse_message_error_frame() -> None:
    """_parse_message drops error frames."""
    msg = _can.Message(arbitration_id=0x100, is_extended_id=True, is_error_frame=True)
    assert pythoncan._parse_message(msg) is None


def test_parse_message_non_extended() -> None:
    """_parse_message drops standard (non-extended) frames."""
    msg = _can.Message(arbitration_id=0x100, is_extended_id=False, data=b"std")
    assert pythoncan._parse_message(msg) is None


def test_parse_message_remote_frame() -> None:
    """_parse_message drops remote (RTR) frames."""
    msg = _can.Message(arbitration_id=0x100, is_extended_id=True, is_remote_frame=True, dlc=4)
    assert pythoncan._parse_message(msg) is None


def test_parse_message_id_mask() -> None:
    """_parse_message masks the arbitration_id to 29 bits."""
    msg = _can.Message(arbitration_id=0xFFFFFFFF, is_extended_id=True, data=b"")
    frame = pythoncan._parse_message(msg)
    assert frame is not None
    assert frame.id == 0x1FFFFFFF


async def test_close_unblocks_pending_receive() -> None:
    """A receive() that's already awaiting must raise ClosedError on close."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    task = asyncio.ensure_future(itf.receive())
    await asyncio.sleep(0.05)
    assert not task.done()
    itf.close()
    with pytest.raises(ClosedError):
        await asyncio.wait_for(task, timeout=2.0)


async def test_fail_records_first_exception_only() -> None:
    """_fail() only records the first exception."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    ex1 = OSError("first")
    ex2 = OSError("second")
    itf._fail(ex1)
    itf._fail(ex2)
    assert itf._failure is ex1


async def test_raise_if_closed_with_failure() -> None:
    """_raise_if_closed chains the original failure exception."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    original = OSError("root cause")
    itf._fail(original)
    with pytest.raises(ClosedError) as exc_info:
        itf._raise_if_closed()
    assert exc_info.value.__cause__ is original


async def test_raise_if_closed_without_failure() -> None:
    """_raise_if_closed without a failure gives a clean ClosedError."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    with pytest.raises(ClosedError):
        itf._raise_if_closed()


async def test_enqueue_creates_tx_task_lazily() -> None:
    """TX task is not created until the first enqueue()."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        assert itf._tx_task is None
        itf.enqueue(0x00000001, [memoryview(b"x")], Instant.now() + 1.0)
        assert itf._tx_task is not None
    finally:
        itf.close()


async def test_rx_thread_exits_on_bus_error() -> None:
    """If bus.recv() raises, the RX thread pushes the exception and exits."""
    mock_bus = MagicMock(spec=_can.BusABC)
    mock_bus.recv.side_effect = _can.CanError("hardware failure")
    mock_bus.channel_info = "mock:0"
    itf = PythonCANInterface(mock_bus)
    with pytest.raises(ClosedError):
        await asyncio.wait_for(itf.receive(), timeout=2.0)
    itf.close()


async def test_filter_on_closed_raises() -> None:
    """filter() on a closed interface raises ClosedError."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.close()
    with pytest.raises(ClosedError):
        itf.filter([Filter.promiscuous()])


async def test_filter_can_error_raises_oserror() -> None:
    """can.CanError from set_filters is wrapped as OSError."""
    mock_bus = MagicMock(spec=_can.BusABC)
    mock_bus.recv.return_value = None
    mock_bus.channel_info = "mock:0"
    mock_bus.set_filters.side_effect = _can.CanError("filter error")
    itf = PythonCANInterface(mock_bus)
    try:
        with pytest.raises(OSError, match="filter configuration failed"):
            itf.filter([Filter.promiscuous()])
    finally:
        itf.close()


async def test_filter_waits_for_rx_thread_before_reconfiguring() -> None:
    """Filter changes must wait until the RX thread leaves recv()."""
    recv_entered = threading.Event()
    allow_recv_return = threading.Event()
    filter_started = threading.Event()
    set_filters_called = threading.Event()
    applied_filters: list[list[_can.typechecking.CanFilter]] = []

    class BlockingRecvBus:
        channel_info = "blocking:0"
        protocol = _can.CanProtocol.CAN_20

        def recv(self, timeout: float | None = None) -> _can.Message | None:
            recv_entered.set()
            allow_recv_return.wait()
            return None

        def set_filters(self, filters: list[_can.typechecking.CanFilter] | None = None) -> None:
            applied_filters.append(list(filters or []))
            set_filters_called.set()

        def shutdown(self) -> None:
            allow_recv_return.set()

    itf = PythonCANInterface(cast(_can.BusABC, BlockingRecvBus()))

    def apply_filters() -> None:
        filter_started.set()
        itf.filter([Filter.promiscuous()])

    try:
        assert await asyncio.to_thread(recv_entered.wait, 1.0)
        task = asyncio.create_task(asyncio.to_thread(apply_filters))
        assert await asyncio.to_thread(filter_started.wait, 1.0)
        assert not set_filters_called.is_set()
        allow_recv_return.set()
        await asyncio.wait_for(task, timeout=1.0)
        assert len(applied_filters) == 1
        assert applied_filters[0][0]["can_id"] == 0
        assert applied_filters[0][0]["can_mask"] == 0
        assert applied_filters[0][0]["extended"] is True
    finally:
        allow_recv_return.set()
        itf.close()


async def test_purge_empty_queue() -> None:
    """Purging an empty queue is harmless."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        itf.purge()  # Should not raise.
    finally:
        itf.close()


# ============================================================================
# Tier 2b: More unit/integration tests (extended coverage)
# ============================================================================


async def test_unit_tx_loop_multiple_deadline_drops() -> None:
    """Multiple consecutive expired frames are all dropped."""
    ch = _unique_channel()
    a = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        expired = Instant.now() + (-10.0)
        for i in range(10):
            a.enqueue(0x00005000 + i, [memoryview(b"expired")], expired)
        a.enqueue(0x00006000, [memoryview(b"good")], Instant.now() + 5.0)
        frame = await asyncio.wait_for(b.receive(), timeout=5.0)
        assert frame.id == 0x00006000
        assert frame.data == b"good"
    finally:
        _close_all(a, b)


async def test_unit_enqueue_after_purge_still_works() -> None:
    """After purge, new enqueue'd frames are still sent."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x00007000, [memoryview(b"before")], Instant.now() + 60.0)
        a.purge()
        a.enqueue(0x00007001, [memoryview(b"after")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x00007001
        assert frame.data == b"after"
    finally:
        _close_all(a, b)


async def test_unit_close_cancels_tx_task() -> None:
    """Closing the interface cancels the TX task."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    itf.enqueue(0x00000001, [memoryview(b"x")], Instant.now() + 1.0)
    assert itf._tx_task is not None
    tx_task = itf._tx_task
    itf.close()
    assert itf._tx_task is None
    assert tx_task.cancelling() or tx_task.cancelled() or tx_task.done()


async def test_unit_rx_thread_stops_on_close() -> None:
    """The RX thread exits promptly after close."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    assert itf._rx_thread.is_alive()
    itf.close()
    itf._rx_thread.join(timeout=1.0)
    assert not itf._rx_thread.is_alive()


async def test_unit_prebuilt_bus_name_from_channel_info() -> None:
    """When constructed with a pre-built bus, name comes from channel_info."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch)
    itf = PythonCANInterface(bus)
    try:
        assert isinstance(itf.name, str)
        assert len(itf.name) > 0
    finally:
        itf.close()


async def test_unit_prebuilt_bus_fd_default_false() -> None:
    """Pre-built bus defaults to fd=False when not specified."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch)
    itf = PythonCANInterface(bus)
    try:
        assert itf.fd is False
    finally:
        itf.close()


async def test_unit_prebuilt_bus_fd_true() -> None:
    """Pre-built bus with fd=True."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch, fd=True)
    itf = PythonCANInterface(bus, fd=True)
    try:
        assert itf.fd is True
    finally:
        itf.close()


async def test_unit_repr_includes_class_name() -> None:
    """repr() always includes the class name."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        r = repr(itf)
        assert r.startswith("PythonCANInterface(")
    finally:
        itf.close()


async def test_unit_filter_empty_list() -> None:
    """Setting an empty filter list does not raise."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        itf.filter([])
    finally:
        itf.close()


async def test_unit_filter_many_filters() -> None:
    """Setting many filters at once does not raise."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        filters = [Filter(id=i, mask=0x1FFFFFFF) for i in range(50)]
        itf.filter(filters)
    finally:
        itf.close()


async def test_unit_enqueue_single_byte_payloads() -> None:
    """Single-byte payloads are handled correctly."""
    a, b = _virtual_pair()
    try:
        for byte_val in range(256):
            a.enqueue(0x00008000, [memoryview(bytes([byte_val]))], Instant.now() + 10.0)
        for byte_val in range(256):
            frame = await asyncio.wait_for(b.receive(), timeout=10.0)
            assert frame.data == bytes([byte_val])
    finally:
        _close_all(a, b)


async def test_unit_concurrent_receive_and_enqueue() -> None:
    """receive() and enqueue() can be used concurrently from different coroutines."""
    a, b = _virtual_pair()
    received: list[TimestampedFrame] = []

    async def receiver() -> None:
        for _ in range(20):
            frame = await asyncio.wait_for(b.receive(), timeout=5.0)
            received.append(frame)

    async def sender() -> None:
        for i in range(20):
            a.enqueue(0x00009000, [memoryview(i.to_bytes(2, "big"))], Instant.now() + 5.0)
            await asyncio.sleep(0.01)

    try:
        await asyncio.gather(receiver(), sender())
        assert len(received) == 20
    finally:
        _close_all(a, b)


async def test_unit_concurrent_receivers() -> None:
    """Multiple tasks awaiting receive() on the same interface each get distinct frames."""
    ch = _unique_channel()
    bus_a = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    a = PythonCANInterface(bus_a)
    b = PythonCANInterface(bus_b)
    results: list[TimestampedFrame] = []

    async def rx_task() -> None:
        frame = await asyncio.wait_for(b.receive(), timeout=5.0)
        results.append(frame)

    try:
        tasks = [asyncio.ensure_future(rx_task()) for _ in range(3)]
        await asyncio.sleep(0.05)
        for i in range(3):
            a.enqueue(0x0000A000 + i, [memoryview(bytes([i]))], Instant.now() + 2.0)
        await asyncio.gather(*tasks)
        assert len(results) == 3
        ids = sorted(f.id for f in results)
        assert ids == [0x0000A000, 0x0000A001, 0x0000A002]
    finally:
        _close_all(a, b)


async def test_unit_receive_timeout_does_not_drop_frames() -> None:
    """A timeout on receive does not cause subsequent frames to be lost."""
    ch = _unique_channel()
    a = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(b.receive(), timeout=0.2)
        a.enqueue(0x0000B000, [memoryview(b"after_timeout")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.data == b"after_timeout"
    finally:
        _close_all(a, b)


async def test_unit_multiple_enqueue_calls() -> None:
    """Multiple separate enqueue() calls accumulate in the TX queue."""
    a, b = _virtual_pair()
    try:
        a.enqueue(0x0000C001, [memoryview(b"first")], Instant.now() + 2.0)
        a.enqueue(0x0000C002, [memoryview(b"second")], Instant.now() + 2.0)
        a.enqueue(0x0000C003, [memoryview(b"third")], Instant.now() + 2.0)
        received = []
        for _ in range(3):
            received.append(await asyncio.wait_for(b.receive(), timeout=2.0))
        data_set = {f.data for f in received}
        assert data_set == {b"first", b"second", b"third"}
    finally:
        _close_all(a, b)


async def test_unit_tx_priority_ordering() -> None:
    """TX PriorityQueue sends lower CAN IDs first (bus arbitration approximation)."""
    ch = _unique_channel()
    a = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        # Enqueue high-ID first, then low-ID.
        a.enqueue(0x1FFFFFFF, [memoryview(b"high")], Instant.now() + 5.0)
        a.enqueue(0x00000001, [memoryview(b"low")], Instant.now() + 5.0)
        f1 = await asyncio.wait_for(b.receive(), timeout=5.0)
        f2 = await asyncio.wait_for(b.receive(), timeout=5.0)
        assert f1.id <= f2.id
    finally:
        _close_all(a, b)


async def test_unit_close_during_tx() -> None:
    """Closing the interface while the TX loop is processing frames does not hang."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    for i in range(100):
        itf.enqueue(0x0000D000 + i, [memoryview(b"close_me")], Instant.now() + 60.0)
    itf.close()


async def test_unit_rapid_open_close() -> None:
    """Rapidly opening and closing interfaces does not leak threads or tasks."""
    for _ in range(20):
        ch = _unique_channel()
        itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
        itf.enqueue(0x0000E000, [memoryview(b"x")], Instant.now() + 1.0)
        itf.close()


async def test_unit_interface_with_can_transport_close() -> None:
    """CANTransport.close() properly closes the underlying PythonCANInterface."""
    ch = _unique_channel()
    itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    transport = CANTransport.new(itf)
    transport.close()
    assert itf._closed


async def test_unit_transport_multiple_subjects() -> None:
    """Transport can handle multiple subject subscriptions and publications."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    arrivals_1: list[TransportArrival] = []
    arrivals_2: list[TransportArrival] = []
    b.subject_listen(100, arrivals_1.append)
    b.subject_listen(200, arrivals_2.append)
    w1 = a.subject_advertise(100)
    w2 = a.subject_advertise(200)
    try:
        await w1(Instant.now() + 2.0, Priority.NOMINAL, b"subject_100")
        await w2(Instant.now() + 2.0, Priority.NOMINAL, b"subject_200")
        await wait_for(lambda: len(arrivals_1) == 1 and len(arrivals_2) == 1, timeout=5.0)
        assert arrivals_1[0].message == b"subject_100"
        assert arrivals_2[0].message == b"subject_200"
    finally:
        w1.close()
        w2.close()
        a.close()
        b.close()


async def test_unit_transport_writer_close_allows_readvertise() -> None:
    """After closing a writer, the same subject can be re-advertised."""
    ch = _unique_channel()
    itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    transport = CANTransport.new(itf)
    try:
        w1 = transport.subject_advertise(300)
        w1.close()
        w2 = transport.subject_advertise(300)
        w2.close()
    finally:
        transport.close()


async def test_unit_transport_listener_close_allows_relisten() -> None:
    """After closing a listener, the same subject can be re-subscribed."""
    ch = _unique_channel()
    itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    transport = CANTransport.new(itf)
    try:
        listener1 = transport.subject_listen(400, lambda _: None)
        listener1.close()
        listener2 = transport.subject_listen(400, lambda _: None)
        listener2.close()
    finally:
        transport.close()


def test_parse_message_max_data() -> None:
    """_parse_message with 64-byte (max FD) payload."""
    payload = bytes(range(64))
    msg = _can.Message(arbitration_id=0x00000001, is_extended_id=True, data=payload, is_fd=True)
    frame = pythoncan._parse_message(msg)
    assert frame is not None
    assert len(frame.data) == 64
    assert frame.data == payload


def test_parse_message_empty_data() -> None:
    """_parse_message with empty payload."""
    msg = _can.Message(arbitration_id=0x00000001, is_extended_id=True, data=b"")
    frame = pythoncan._parse_message(msg)
    assert frame is not None
    assert frame.data == b""


def test_parse_message_timestamp_is_recent() -> None:
    """_parse_message generates a recent timestamp."""
    ts_before = Instant.now()
    msg = _can.Message(arbitration_id=0x00000001, is_extended_id=True, data=b"ts")
    frame = pythoncan._parse_message(msg)
    ts_after = Instant.now()
    assert frame is not None
    assert ts_before.ns <= frame.timestamp.ns <= ts_after.ns


async def test_unit_filter_then_refilter() -> None:
    """Filters can be changed after initial configuration."""
    a, b = _virtual_pair()
    try:
        b.filter([Filter(id=0x00000100, mask=0x1FFFFFFF)])
        a.enqueue(0x00000100, [memoryview(b"pass1")], Instant.now() + 2.0)
        f1 = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert f1.data == b"pass1"

        b.filter([Filter(id=0x00000200, mask=0x1FFFFFFF)])
        a.enqueue(0x00000100, [memoryview(b"fail")], Instant.now() + 2.0)
        a.enqueue(0x00000200, [memoryview(b"pass2")], Instant.now() + 2.0)
        f2 = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert f2.data == b"pass2"
    finally:
        _close_all(a, b)


async def test_unit_three_way_communication() -> None:
    """Three interfaces on the same bus: A sends, B and C both receive."""
    ch = _unique_channel()
    a = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    c = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        a.enqueue(0x0000F000, [memoryview(b"broadcast")], Instant.now() + 2.0)
        fb = await asyncio.wait_for(b.receive(), timeout=2.0)
        fc = await asyncio.wait_for(c.receive(), timeout=2.0)
        assert fb.data == b"broadcast"
        assert fc.data == b"broadcast"
    finally:
        _close_all(a, b, c)


async def test_unit_large_multi_frame_transfer() -> None:
    """Multi-frame transfer with many frames in a single enqueue."""
    a, b = _virtual_pair()
    try:
        n = 100
        views = [memoryview(bytes([i % 256]) * 8) for i in range(n)]
        a.enqueue(0x00010000, views, Instant.now() + 10.0)
        for i in range(n):
            frame = await asyncio.wait_for(b.receive(), timeout=10.0)
            assert frame.id == 0x00010000
            assert frame.data == bytes([i % 256]) * 8
    finally:
        _close_all(a, b)


async def test_unit_purge_partial() -> None:
    """Purge drops all pending frames, including those from multiple enqueue calls."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        itf.enqueue(0x00020001, [memoryview(b"a")], Instant.now() + 60.0)
        itf.enqueue(0x00020002, [memoryview(b"b")], Instant.now() + 60.0)
        itf.enqueue(0x00020003, [memoryview(b"c")], Instant.now() + 60.0)
        itf.purge()
        assert itf._tx_queue.empty()
    finally:
        itf.close()


async def test_unit_mixed_fd_and_classic_payloads() -> None:
    """In FD mode, both small (<=8) and large (>8) payloads work."""
    a, b = _virtual_pair(fd=True)
    try:
        a.enqueue(0x00030000, [memoryview(b"short")], Instant.now() + 2.0)
        a.enqueue(0x00030001, [memoryview(bytes(range(32)))], Instant.now() + 2.0)
        a.enqueue(0x00030002, [memoryview(b"tiny")], Instant.now() + 2.0)
        # PriorityQueue sorts by ID so order is preserved for same-ID, but we have different IDs.
        received = []
        for _ in range(3):
            received.append(await asyncio.wait_for(b.receive(), timeout=2.0))
        data_set = {f.data for f in received}
        assert b"short" in data_set
        assert bytes(range(32)) in data_set
        assert b"tiny" in data_set
    finally:
        _close_all(a, b)


async def test_unit_enqueue_same_id_preserves_order() -> None:
    """Frames with the same CAN ID preserve their enqueue order."""
    a, b = _virtual_pair()
    try:
        views = [memoryview(bytes([i])) for i in range(10)]
        a.enqueue(0x00040000, views, Instant.now() + 5.0)
        for i in range(10):
            frame = await asyncio.wait_for(b.receive(), timeout=5.0)
            assert frame.data == bytes([i])
    finally:
        _close_all(a, b)


async def test_unit_filter_coalesce_passthrough() -> None:
    """Python-CAN receives all filters even if there are many (no coalescing limit in PythonCANInterface)."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        filters = [Filter(id=i, mask=0x1FFFFFFF) for i in range(100)]
        itf.filter(filters)
    finally:
        itf.close()


async def test_unit_rx_queue_ordering() -> None:
    """Frames arrive in the RX queue in the order they were received from the bus."""
    ch = _unique_channel()
    bus_a = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    b = PythonCANInterface(bus_b)
    try:
        for i in range(5):
            msg = _can.Message(arbitration_id=0x00050000 + i, is_extended_id=True, data=bytes([i]))
            bus_a.send(msg)
        received_ids = []
        for _ in range(5):
            frame = await asyncio.wait_for(b.receive(), timeout=2.0)
            received_ids.append(frame.id)
        assert received_ids == [0x00050000 + i for i in range(5)]
    finally:
        b.close()
        bus_a.shutdown()


async def test_unit_tx_bus_error_mock() -> None:
    """A bus.send() that raises CanError is logged and retried."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch)
    bus_b = _can.ThreadSafeBus(interface="virtual", channel=ch)
    a = PythonCANInterface(bus)
    b = PythonCANInterface(bus_b)
    call_count = 0
    orig_send = bus.send

    def flaky_send(msg, timeout=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _can.CanError("transient")
        return orig_send(msg, timeout)

    bus.send = flaky_send  # type: ignore[assignment]
    try:
        a.enqueue(0x00060000, [memoryview(b"retry")], Instant.now() + 5.0)
        frame = await asyncio.wait_for(b.receive(), timeout=5.0)
        assert frame.data == b"retry"
        assert call_count >= 2
    finally:
        _close_all(a, b)


async def test_unit_rx_bus_error_propagates() -> None:
    """A bus.recv() exception propagates as ClosedError from receive()."""
    mock_bus = MagicMock(spec=_can.BusABC)
    mock_bus.recv.side_effect = OSError("hardware gone")
    mock_bus.channel_info = "mock:err"
    itf = PythonCANInterface(mock_bus)
    with pytest.raises(ClosedError, match="receive failed"):
        await asyncio.wait_for(itf.receive(), timeout=2.0)
    itf.close()


async def test_unit_multiple_close_with_failure() -> None:
    """Multiple close() calls after failure are harmless."""
    mock_bus = MagicMock(spec=_can.BusABC)
    mock_bus.recv.side_effect = _can.CanError("fail")
    mock_bus.channel_info = "mock:multiclose"
    itf = PythonCANInterface(mock_bus)
    with pytest.raises(ClosedError):
        await asyncio.wait_for(itf.receive(), timeout=2.0)
    itf.close()
    itf.close()
    itf.close()


async def test_unit_tx_os_error_fails_interface() -> None:
    """A non-CAN OSError during TX fails the interface permanently."""
    ch = _unique_channel()
    bus = _can.ThreadSafeBus(interface="virtual", channel=ch)
    itf = PythonCANInterface(bus)

    def os_error_send(msg, timeout=None):
        raise OSError("bus error")

    bus.send = os_error_send  # type: ignore[assignment]
    itf.enqueue(0x00070000, [memoryview(b"oserr")], Instant.now() + 5.0)
    await asyncio.sleep(0.3)
    assert itf._closed
    assert itf._failure is not None
    itf.close()


async def test_unit_filter_set_clear_set() -> None:
    """Filters can be set, cleared (empty), then set again."""
    ch = _unique_channel()
    itf = PythonCANInterface(_can.ThreadSafeBus(interface="virtual", channel=ch))
    try:
        itf.filter([Filter(id=0x100, mask=0x1FFFFFFF)])
        itf.filter([])
        itf.filter([Filter(id=0x200, mask=0x1FFFFFFF)])
    finally:
        itf.close()


async def test_unit_transport_pubsub_large_message() -> None:
    """Transport pub/sub with a message larger than one CAN frame (multi-frame transfer)."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    arrivals: list[TransportArrival] = []
    b.subject_listen(500, arrivals.append)
    writer = a.subject_advertise(500)
    try:
        large_payload = bytes(range(1, 200)) * 3
        await writer(Instant.now() + 5.0, Priority.NOMINAL, large_payload)
        await wait_for(lambda: len(arrivals) == 1, timeout=10.0)
        assert arrivals[0].message == large_payload
    finally:
        writer.close()
        a.close()
        b.close()


async def test_unit_transport_bidirectional_unicast() -> None:
    """Both nodes can send unicast to each other."""
    ch = _unique_channel()
    a_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    b_itf = PythonCANInterface(
        _can.ThreadSafeBus(interface="virtual", channel=ch, receive_own_messages=True),
    )
    a = CANTransport.new(a_itf)
    b = CANTransport.new(b_itf)
    _force_distinct_ids(a, b)
    a_rx: list[TransportArrival] = []
    b_rx: list[TransportArrival] = []
    a.unicast_listen(a_rx.append)
    b.unicast_listen(b_rx.append)
    try:
        await a.unicast(Instant.now() + 2.0, Priority.NOMINAL, b.id, b"a_to_b")
        await b.unicast(Instant.now() + 2.0, Priority.NOMINAL, a.id, b"b_to_a")
        await wait_for(lambda: len(a_rx) == 1 and len(b_rx) == 1, timeout=5.0)
        assert b_rx[0].message == b"a_to_b"
        assert a_rx[0].message == b"b_to_a"
    finally:
        a.close()
        b.close()


# ============================================================================
# Tier 3: SocketCAN vcan integration tests (Linux-only)
# ============================================================================

pytestmark_socketcan = pytest.mark.skipif(
    sys.platform != "linux" or not Path("/sys/class/net/vcan0").exists(),
    reason="SocketCAN live tests require Linux with vcan0",
)


@pytestmark_socketcan
async def test_pythoncan_socketcan_pubsub_smoke() -> None:
    """PythonCANInterface with SocketCAN backend: transport pub/sub."""
    a = CANTransport.new(PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0")))
    b = CANTransport.new(PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0")))
    arrivals: list[TransportArrival] = []
    b.subject_listen(1234, arrivals.append)
    writer = a.subject_advertise(1234)
    try:
        await writer(Instant.now() + 2.0, Priority.NOMINAL, b"socketcan_pubsub")
        await wait_for(lambda: len(arrivals) == 1, timeout=3.0)
        assert arrivals[0].message == b"socketcan_pubsub"
    finally:
        writer.close()
        a.close()
        b.close()


@pytestmark_socketcan
async def test_pythoncan_socketcan_unicast_smoke() -> None:
    """PythonCANInterface with SocketCAN backend: transport unicast."""
    a = CANTransport.new(PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0")))
    b = CANTransport.new(PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0")))
    arrivals: list[TransportArrival] = []
    b.unicast_listen(arrivals.append)
    try:
        await a.unicast(Instant.now() + 2.0, Priority.FAST, b.id, b"socketcan_unicast")
        await wait_for(lambda: len(arrivals) == 1, timeout=3.0)
        assert arrivals[0].message == b"socketcan_unicast"
    finally:
        a.close()
        b.close()


@pytestmark_socketcan
async def test_pythoncan_socketcan_send_receive_raw() -> None:
    """Raw frame send/receive on SocketCAN vcan0."""
    a = PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0"))
    b = PythonCANInterface(_can.ThreadSafeBus(interface="socketcan", channel="vcan0"))
    try:
        a.enqueue(0x1BADC0DE, [memoryview(b"vcan")], Instant.now() + 2.0)
        frame = await asyncio.wait_for(b.receive(), timeout=2.0)
        assert frame.id == 0x1BADC0DE
        assert frame.data == b"vcan"
    finally:
        _close_all(a, b)
