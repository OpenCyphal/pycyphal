from __future__ import annotations

import asyncio
import logging
from typing import cast

import pytest

from pycyphal2 import ClosedError, Instant, Priority, SendError
from pycyphal2._transport import SUBJECT_ID_MODULUS_16bit, TransportArrival
from pycyphal2.can import CANTransport, TimestampedFrame
from pycyphal2.can._transport import _CANTransportImpl, _PinnedSubjectState
from pycyphal2.can._wire import NODE_ID_ANONYMOUS, TransferKind
from tests.can._support import MockCANBus, MockCANInterface, wait_for


class _OneShotInterface(MockCANInterface):
    def __post_init__(self) -> None:
        super().__post_init__()
        self._receive_event = asyncio.Event()
        self._receive_error: BaseException | None = None
        self._receive_frame: TimestampedFrame | None = None

    async def receive(self) -> TimestampedFrame:
        await self._receive_event.wait()
        if self._receive_error is not None:
            raise self._receive_error
        assert self._receive_frame is not None
        return self._receive_frame

    def release(self, frame: TimestampedFrame | None = None, error: BaseException | None = None) -> None:
        self._receive_frame = frame
        self._receive_error = error
        self._receive_event.set()


async def test_transport_factory_and_constructor_validation() -> None:
    bus = MockCANBus()

    with pytest.raises(ValueError, match="interfaces must contain at least one Interface instance"):
        CANTransport.new([])

    with pytest.raises(ValueError, match="At least one CAN interface is required"):
        _CANTransportImpl([])

    with pytest.raises(ValueError, match="interfaces must contain at least one Interface instance"):
        CANTransport.new([object()])  # type: ignore[list-item]

    with pytest.raises(ValueError, match="Mixed Classic-CAN and CAN FD interface sets are not supported"):
        CANTransport.new([MockCANInterface(bus, "a"), MockCANInterface(bus, "b", _fd=True)])


async def test_transport_validation_repr_and_idempotent_closers() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))
    impl = cast(_CANTransportImpl, transport)
    listener = transport.subject_listen(10, lambda _: None)
    writer = transport.subject_advertise(20)

    assert transport.subject_id_modulus == SUBJECT_ID_MODULUS_16bit
    assert "CANTransport" in repr(transport)
    assert f"id={transport.id}" in repr(transport)

    with pytest.raises(ValueError, match="Invalid subject-ID"):
        transport.subject_listen(-1, lambda _: None)

    with pytest.raises(ValueError, match="already has an active listener"):
        transport.subject_listen(10, lambda _: None)

    with pytest.raises(ValueError, match="Invalid subject-ID"):
        transport.subject_advertise(1 << 16)

    with pytest.raises(ValueError, match="already has an active writer"):
        transport.subject_advertise(20)

    impl.remove_subject_listener(10, lambda _: None)
    assert 10 in transport._subject_handlers  # type: ignore[attr-defined]

    listener.close()
    listener.close()
    writer.close()
    writer.close()
    transport.close()
    transport.close()


async def test_writer_unicast_and_send_transfer_error_paths() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))
    writer = transport.subject_advertise(123)
    writer.close()

    with pytest.raises(ClosedError, match="CAN subject writer closed"):
        await writer(Instant.now() + 1.0, Priority.NOMINAL, b"x")

    writer2 = transport.subject_advertise(124)
    transport.close()

    with pytest.raises(ClosedError, match="CAN transport closed"):
        await writer2(Instant.now() + 1.0, Priority.NOMINAL, b"x")

    live = CANTransport.new(MockCANInterface(bus, "if1"))
    with pytest.raises(ValueError, match="Invalid remote node-ID"):
        await live.unicast(Instant.now() + 1.0, Priority.NOMINAL, 0, b"x")

    live_impl = cast(_CANTransportImpl, live)
    with pytest.raises(SendError, match="Deadline exceeded"):
        await live_impl.send_transfer(
            deadline=Instant(ns=0),
            priority=Priority.NOMINAL,
            kind=TransferKind.MESSAGE_16,
            port_id=1,
            payload=b"x",
            transfer_id=0,
        )

    live.close()

    with pytest.raises(ClosedError, match="CAN transport closed"):
        await live.unicast(Instant.now() + 1.0, Priority.NOMINAL, 1, b"x")

    with pytest.raises(ClosedError, match="CAN transport closed"):
        await live_impl.send_transfer(
            deadline=Instant.now() + 1.0,
            priority=Priority.NOMINAL,
            kind=TransferKind.MESSAGE_16,
            port_id=1,
            payload=b"x",
            transfer_id=0,
        )


async def test_pinned_subject_state_wraps_payloads() -> None:
    state = _PinnedSubjectState.new(123)
    first = state.wrap(b"a")
    second = state.wrap(b"b")

    assert first[:16] == second[:16] == state.header_prefix
    assert first[-1:] == b"a"
    assert second[-1:] == b"b"
    assert first[16:24] != second[16:24]


async def test_mark_filters_dirty_unicast_handler_and_apply_dirty_filter_edges() -> None:
    bus = MockCANBus()
    a = MockCANInterface(bus, "a")
    b = MockCANInterface(bus, "b")
    extra = MockCANInterface(bus, "extra")
    transport = CANTransport.new([a, b])

    transport._on_unicast_transfer(Instant(ns=1), 99, Priority.FAST, b"ignored")  # type: ignore[attr-defined]

    transport._filter_dirty.clear()  # type: ignore[attr-defined]
    transport._mark_filters_dirty([a, extra])  # type: ignore[attr-defined]
    assert transport._filter_dirty == {a}  # type: ignore[attr-defined]

    transport._interfaces.remove(a)  # type: ignore[attr-defined]
    transport._filter_dirty = {a}  # type: ignore[attr-defined]
    transport._filter_failures = {a: 3}  # type: ignore[attr-defined]
    transport._apply_dirty_filters()  # type: ignore[attr-defined]
    assert transport._filter_dirty == set()  # type: ignore[attr-defined]
    assert transport._filter_failures == {}  # type: ignore[attr-defined]

    transport.close()
    transport._apply_dirty_filters()  # type: ignore[attr-defined]
    extra.close()


async def test_filter_retry_logs_second_failure_and_recovery(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0", fail_filter_calls=2)
    transport = CANTransport.new(iface)

    await wait_for(lambda: iface.filter_calls >= 1, timeout=1.0)

    assert any("filter apply failed" in record.message for record in caplog.records)
    assert any("filter retry failed #2" in record.message for record in caplog.records)
    assert any("filter apply recovered" in record.message for record in caplog.records)

    transport.close()
    await transport._filter_retry_loop()  # type: ignore[attr-defined]


async def test_filter_retry_loop_wait_branch() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))

    class _Event:
        def clear(self) -> None:
            pass

        async def wait(self) -> bool:
            transport._closed = True  # type: ignore[attr-defined]
            return True

    transport._filter_dirty.clear()  # type: ignore[attr-defined]
    transport._filter_retry_event = _Event()  # type: ignore[attr-defined]

    await transport._filter_retry_loop()  # type: ignore[attr-defined]


async def test_reader_loop_exit_paths() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "host"))

    unknown = MockCANInterface(bus, "unknown")
    unknown._rx_queue.put_nowait(TimestampedFrame(id=1, data=b"x", timestamp=Instant(ns=1)))  # type: ignore[attr-defined]
    await transport._reader_loop(unknown)  # type: ignore[attr-defined]

    delayed = _OneShotInterface(bus, "delayed")
    task = asyncio.create_task(transport._reader_loop(delayed))  # type: ignore[attr-defined]
    await asyncio.sleep(0)
    transport.close()
    delayed.release(error=OSError("closed after receive started"))
    await task

    await transport._reader_loop(unknown)  # type: ignore[attr-defined]
    unknown.close()
    delayed.close()


async def test_drop_interface_and_node_id_occupancy_edges(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    bus = MockCANBus()
    iface = MockCANInterface(bus, "if0")
    other = MockCANInterface(bus, "other")
    transport = CANTransport.new(iface)

    transport._drop_interface(other, RuntimeError("not tracked"))  # type: ignore[attr-defined]
    assert transport.interfaces == [iface]

    before = transport._node_id_occupancy  # type: ignore[attr-defined]
    transport._node_id_occupancy_update(NODE_ID_ANONYMOUS)  # type: ignore[attr-defined]
    assert transport._node_id_occupancy == before  # type: ignore[attr-defined]

    foreign = 1 if transport.id != 1 else 2
    transport._node_id_occupancy |= 1 << foreign  # type: ignore[attr-defined]
    before = transport._node_id_occupancy  # type: ignore[attr-defined]
    transport._node_id_occupancy_update(foreign)  # type: ignore[attr-defined]
    assert transport._node_id_occupancy == before  # type: ignore[attr-defined]

    transport._node_id_occupancy = (1 << 128) - 1  # type: ignore[attr-defined]
    transport._node_id_occupancy_update(transport.id)  # type: ignore[attr-defined]
    assert any("no free slot remains" in record.message for record in caplog.records)

    transport.close()
    other.close()


async def test_cleanup_loop_executes_once_then_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))
    calls: list[int] = []

    async def fake_sleep(_delay: float) -> None:
        transport._closed = True  # type: ignore[attr-defined]

    def fake_cleanup(endpoints: object, now_ns: int) -> None:
        del endpoints
        calls.append(now_ns)

    monkeypatch.setattr("pycyphal2.can._transport.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pycyphal2.can._transport.Reassembler.cleanup_sessions", fake_cleanup)

    await transport._cleanup_loop()  # type: ignore[attr-defined]
    assert len(calls) == 1

    await transport._cleanup_loop()  # type: ignore[attr-defined]
    transport.close()


async def test_unicast_handler_delivers_when_present() -> None:
    bus = MockCANBus()
    transport = CANTransport.new(MockCANInterface(bus, "if0"))
    arrivals: list[TransportArrival] = []
    transport.unicast_listen(arrivals.append)

    transport._on_unicast_transfer(Instant(ns=3), 123, Priority.HIGH, b"payload")  # type: ignore[attr-defined]

    assert arrivals == [TransportArrival(Instant(ns=3), Priority.HIGH, 123, b"payload")]
    transport.close()
