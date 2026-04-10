from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from pycyphal2 import ClosedError, Instant
from pycyphal2.can import Filter, Frame, Interface, TimestampedFrame
from pycyphal2.can._wire import match_filters


class MockCANBus:
    def __init__(self) -> None:
        self._interfaces: list[MockCANInterface] = []
        self.history: list[tuple[str, Frame]] = []

    def attach(self, interface: MockCANInterface) -> None:
        self._interfaces.append(interface)

    def detach(self, interface: MockCANInterface) -> None:
        try:
            self._interfaces.remove(interface)
        except ValueError:
            pass

    def deliver(self, sender: MockCANInterface, frame: Frame, deadline: Instant) -> None:
        if Instant.now().ns > deadline.ns:
            return
        self.history.append((sender.name, frame))
        for interface in tuple(self._interfaces):
            if interface is sender and not interface.self_loopback:
                continue
            interface.ingest(frame)


@dataclass(eq=False)
class MockCANInterface(Interface):
    bus: MockCANBus
    _name: str
    _fd: bool = False
    filter_limit: int | None = None
    fail_filter_calls: int = 0
    transient_enqueue_failures: int = 0
    fail_enqueue_closed: bool = False
    fail_receive: bool = False
    defer_tx: bool = False
    self_loopback: bool = False

    def __post_init__(self) -> None:
        self.closed = False
        self.filters = [Filter.promiscuous()]
        self.filter_calls = 0
        self.filter_history: list[list[Filter]] = []
        self.enqueue_history: list[tuple[int, tuple[bytes, ...], Instant]] = []
        self.tx_history: list[Frame] = []
        self.purge_calls = 0
        self._pending_tx: list[tuple[Frame, Instant]] = []
        self._rx_queue: asyncio.Queue[TimestampedFrame | None] = asyncio.Queue()
        self.bus.attach(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def fd(self) -> bool:
        return self._fd

    def filter(self, filters: Iterable[Filter]) -> None:
        if self.closed:
            raise ClosedError(f"{self._name} closed")
        if self.fail_filter_calls > 0:
            self.fail_filter_calls -= 1
            raise OSError(f"{self._name} filter failed")
        self.filter_calls += 1
        flt = list(filters)
        if self.filter_limit is not None and len(flt) > self.filter_limit:
            flt = Filter.coalesce(flt, self.filter_limit)
        self.filters = flt
        self.filter_history.append(list(flt))

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> None:
        if self.closed:
            raise ClosedError(f"{self._name} closed")
        if self.fail_enqueue_closed:
            self.close()
            raise ClosedError(f"{self._name} closed during enqueue")
        if self.transient_enqueue_failures > 0:
            self.transient_enqueue_failures -= 1
            raise OSError(f"{self._name} enqueue failed")
        chunks = tuple(bytes(item) for item in data)
        self.enqueue_history.append((id, chunks, deadline))
        for item in chunks:
            frame = Frame(id=id, data=item)
            if self.defer_tx:
                self._pending_tx.append((frame, deadline))
            else:
                self._emit(frame, deadline)

    def purge(self) -> None:
        self.purge_calls += 1
        self._pending_tx.clear()

    def flush_tx(self) -> None:
        pending = list(self._pending_tx)
        self._pending_tx.clear()
        for frame, deadline in pending:
            self._emit(frame, deadline)

    async def receive(self) -> TimestampedFrame:
        if self.closed:
            raise ClosedError(f"{self._name} closed")
        if self.fail_receive:
            raise OSError(f"{self._name} receive failed")
        item = await self._rx_queue.get()
        if item is None:
            raise ClosedError(f"{self._name} closed")
        return item

    def ingest(self, frame: Frame) -> None:
        if self.closed:
            return
        if self.filters and not match_filters(self.filters, frame.id):
            return
        self._rx_queue.put_nowait(TimestampedFrame(id=frame.id, data=frame.data, timestamp=Instant.now()))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.bus.detach(self)
        self._rx_queue.put_nowait(None)

    def __repr__(self) -> str:
        return f"MockCANInterface(name={self._name!r}, fd={self._fd}, closed={self.closed})"

    def _emit(self, frame: Frame, deadline: Instant) -> None:
        self.tx_history.append(frame)
        self.bus.deliver(self, frame, deadline)


async def wait_for(predicate: Callable[[], bool], timeout: float = 1.0, interval: float = 0.005) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("predicate did not become true within timeout")
