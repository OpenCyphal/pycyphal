"""Regression tests for the TX completion contract of Cyphal/CAN."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import pycyphal2.can._transport as can_transport
from pycyphal2 import ClosedError, Instant, Priority, SendError
from pycyphal2._header import MsgBeHeader
from pycyphal2.can import CANTransport
from tests.can._support import MockCANBus, MockCANInterface, wait_for

_SUBJECT_ID = 9000


def _payload(size: int = 16) -> bytes:
    header = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=1, tag=1)
    return header.serialize() + bytes(range(size))


async def test_await_returns_only_after_the_frames_reach_the_media() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 5.0, Priority.NOMINAL, _payload()))
    await wait_for(lambda: bool(tx.enqueue_history))
    assert not publish.done()
    assert tx.tx_history == []

    tx.flush_tx()
    await asyncio.wait_for(publish, timeout=1.0)
    assert tx.tx_history

    transport.close()


async def test_publish_then_immediate_teardown_keeps_the_frames() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx")
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    await writer(Instant.now() + 5.0, Priority.NOMINAL, _payload())
    transport.close()  # No sleep, no drain, no flush.

    assert tx.tx_history
    assert bus.history


async def test_multi_frame_transfer_is_complete_when_the_await_returns() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)  # Classic CAN: 8-byte frames, so this spans several.
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 5.0, Priority.NOMINAL, _payload(64)))
    await wait_for(lambda: bool(tx.enqueue_history))
    _, chunks, _ = tx.enqueue_history[0]
    assert len(chunks) > 1

    tx.flush_tx()
    await asyncio.wait_for(publish, timeout=1.0)
    assert [f.data for f in tx.tx_history] == list(chunks)

    transport.close()


async def test_deadline_expiry_raises_instead_of_dropping_silently() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)  # Never flushed, so the media never accepts anything.
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    with pytest.raises(SendError):
        await writer(Instant.now() + 0.05, Priority.NOMINAL, _payload())
    assert tx.tx_history == []

    transport.close()


async def test_purge_wakes_the_publisher_instead_of_stranding_it() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload()))
    await wait_for(lambda: bool(tx.enqueue_history))
    tx.purge()  # As a node-ID collision would do.

    with pytest.raises(SendError):
        await asyncio.wait_for(publish, timeout=1.0)

    transport.close()


async def test_interface_failure_wakes_the_publisher() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload()))
    await wait_for(lambda: bool(tx.enqueue_history))
    tx.close()

    with pytest.raises(SendError):  # ClosedError is a SendError.
        await asyncio.wait_for(publish, timeout=1.0)

    transport.close()


async def test_cancelling_the_publisher_still_transmits_the_whole_transfer() -> None:
    """A cancelled publisher must not pull frames back out: that would emit a transfer without its tail."""
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload(64)))
    await wait_for(lambda: bool(tx.enqueue_history))
    _, chunks, _ = tx.enqueue_history[0]
    assert len(chunks) > 1

    publish.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publish
    assert tx.pending_tx == 1

    tx.flush_tx()
    assert [f.data for f in tx.tx_history] == list(chunks)

    transport.close()


async def test_redundancy_returns_on_first_success_without_cancelling_the_others() -> None:
    bus = MockCANBus()
    fast = MockCANInterface(bus, "fast")  # Hands frames over synchronously.
    slow = MockCANInterface(bus, "slow", defer_tx=True)  # Wedged.
    transport = CANTransport.new([fast, slow])
    writer = transport.subject_advertise(_SUBJECT_ID)

    await asyncio.wait_for(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload()), timeout=1.0)
    assert fast.tx_history
    assert slow.tx_history == []
    assert slow.pending_tx == 1  # The loser was not cancelled.

    slow.flush_tx()
    assert slow.tx_history

    transport.close()


async def test_redundancy_raises_when_no_interface_can_send() -> None:
    bus = MockCANBus()
    a = MockCANInterface(bus, "a", defer_tx=True)
    b = MockCANInterface(bus, "b", defer_tx=True)
    transport = CANTransport.new([a, b])
    writer = transport.subject_advertise(_SUBJECT_ID)

    with pytest.raises(SendError):
        await writer(Instant.now() + 0.05, Priority.NOMINAL, _payload())
    assert a.tx_history == []
    assert b.tx_history == []

    transport.close()


async def test_discarded_completion_future_does_not_log_unretrieved_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The loser's future in a redundant send is never awaited, so its failure must still be retrieved."""
    bus = MockCANBus()
    fast = MockCANInterface(bus, "fast")
    slow = MockCANInterface(bus, "slow", defer_tx=True)
    transport = CANTransport.new([fast, slow])
    writer = transport.subject_advertise(_SUBJECT_ID)

    await asyncio.wait_for(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload()), timeout=1.0)
    with caplog.at_level("ERROR", logger="asyncio"):
        slow.close()  # Fails the loser's transfer; nobody is listening.
        transport.close()
        await asyncio.sleep(0)
    assert not [r for r in caplog.records if "never retrieved" in r.getMessage()]


async def test_interface_closed_at_enqueue_is_dropped_and_reported() -> None:
    """A synchronous rejection drops the interface inline."""
    bus = MockCANBus()
    good = MockCANInterface(bus, "good")
    dead = MockCANInterface(bus, "dead", fail_enqueue_closed=True)
    transport = CANTransport.new([good, dead])
    writer = transport.subject_advertise(_SUBJECT_ID)

    await writer(Instant.now() + 5.0, Priority.NOMINAL, _payload())
    assert good.tx_history
    assert dead not in transport.interfaces

    transport.close()


async def test_handover_at_the_deadline_boundary_is_not_reported_as_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    An interface that hands the transfer over just as the deadline elapses did send it; reporting that as
    SendError would invite a needless retransmission. send_transfer reads the clock once before enqueueing
    and once to size its wait, so scripting those two readings pins the boundary without relying on timing.
    """
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx")  # Hands frames over synchronously, inside enqueue().
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    deadline = Instant.now() + 30.0
    readings = iter([deadline - 1.0])  # First reading is in time, so the frames are enqueued and sent.
    monkeypatch.setattr(can_transport, "Instant", SimpleNamespace(now=lambda: next(readings, deadline + 1.0)))

    await writer(deadline, Priority.NOMINAL, _payload())  # Deadline elapsed, yet the transfer is on the media.
    assert tx.tx_history

    transport.close()


async def test_wait_for_on_the_enqueue_future_does_not_tear_the_transfer() -> None:
    """`asyncio.wait_for` cancels the future it is given; that must not suppress the tail of the transfer."""
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    chunks = [b"f1", b"f2", b"f3"]
    future = tx.enqueue(0x10, [memoryview(c) for c in chunks], Instant.now() + 30.0)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(future, timeout=0.01)
    assert future.cancelled()

    tx.flush_tx()  # The media catches up after the observer gave up.
    assert [f.data for f in tx.tx_history] == chunks

    tx.close()


async def test_starved_low_priority_transfer_bounds_the_publisher_and_never_reaches_the_wire() -> None:
    """
    A frame is checked against its deadline only when popped, so a busy queue can hold a transfer past it.
    The publisher must still be bounded by that deadline, and the starved frames dropped rather than sent.
    """
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)  # Nothing drains: an over-subscribed bus.
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    started = asyncio.get_running_loop().time()
    with pytest.raises(SendError):
        await asyncio.wait_for(writer(Instant.now() + 0.1, Priority.OPTIONAL, _payload()), timeout=3.0)
    assert asyncio.get_running_loop().time() - started < 1.0  # Bounded by the deadline, not by the queue.

    await asyncio.sleep(0.02)
    assert tx.pending_tx == 1  # Never pulled back out of the queue.
    tx.flush_tx()  # The queue finally drains, well past the deadline.
    assert tx.tx_history == []  # Expired frames are dropped, never transmitted late.

    transport.close()


async def test_transport_close_during_publish_reports_closed() -> None:
    bus = MockCANBus()
    tx = MockCANInterface(bus, "tx", defer_tx=True)
    transport = CANTransport.new(tx)
    writer = transport.subject_advertise(_SUBJECT_ID)

    publish = asyncio.create_task(writer(Instant.now() + 30.0, Priority.NOMINAL, _payload()))
    await wait_for(lambda: bool(tx.enqueue_history))
    transport.close()

    with pytest.raises(ClosedError):
        await asyncio.wait_for(publish, timeout=1.0)
