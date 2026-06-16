"""Browser-oriented SLCAN backend for WebSerial/Pyodide."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Iterable
import logging

from .._api import ClosedError, Instant
from ._interface import Filter, Interface, TimestampedFrame
from ._slcan import SLCANParser, encode_frame

_logger = logging.getLogger(__name__)


class AsyncSerialPort(ABC):
    """Minimal async byte stream expected from a WebSerial adapter."""

    @abstractmethod
    async def read(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    async def write(self, data: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class WebSerialSLCANInterface(Interface):
    """
    SLCAN CAN interface over an application-provided async serial byte stream.

    The port is expected to be already opened and configured by browser/Pyodide glue code.
    Only Classic CAN extended-ID data frames are supported.
    """

    def __init__(self, port: AsyncSerialPort, *, name: str = "webserial") -> None:
        self._port = port
        self._name = str(name)
        self._closed = False
        self._failure: BaseException | None = None
        self._parser = SLCANParser()
        self._tx_seq = 0
        self._tx_queue: asyncio.PriorityQueue[tuple[int, int, int, bytes]] = asyncio.PriorityQueue()
        self._rx_queue: asyncio.Queue[TimestampedFrame | BaseException] = asyncio.Queue()
        self._tx_task: asyncio.Task[None] | None = None
        self._rx_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        _logger.info("WebSerial SLCAN init iface=%s", self._name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def fd(self) -> bool:
        return False

    def filter(self, filters: Iterable[Filter]) -> None:
        del filters
        self._raise_if_closed()
        # No-op: WebSerial adapters do not provide hardware acceptance filtering.

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> None:
        self._raise_if_closed()
        chunks = tuple(bytes(item) for item in data)
        for chunk in chunks:
            encode_frame(id, chunk)  # Validate before mutating the queue.
        if self._tx_task is None:
            self._tx_task = asyncio.get_running_loop().create_task(self._tx_loop())
        for chunk in chunks:
            self._tx_seq += 1
            self._tx_queue.put_nowait((id, self._tx_seq, deadline.ns, chunk))

    def purge(self) -> None:
        if self._closed:
            return
        dropped = 0
        try:
            while True:
                self._tx_queue.get_nowait()
                dropped += 1
        except asyncio.QueueEmpty:
            pass
        if dropped > 0:
            _logger.debug("WebSerial SLCAN purge iface=%s dropped=%d", self._name, dropped)

    async def receive(self) -> TimestampedFrame:
        self._raise_if_closed()
        if self._rx_task is None:
            self._rx_task = asyncio.get_running_loop().create_task(self._rx_loop())
        item = await self._rx_queue.get()
        if isinstance(item, BaseException):
            if isinstance(item, ClosedError):
                raise item
            raise ClosedError(f"WebSerial SLCAN interface {self._name} receive failed") from item
        return item

    def close(self) -> None:
        self._close(ClosedError(f"WebSerial SLCAN interface {self._name} closed"))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name!r}, fd={self.fd})"

    async def _tx_loop(self) -> None:
        while not self._closed:
            try:
                identifier, seq, deadline_ns, payload = await self._tx_queue.get()
            except asyncio.CancelledError:
                raise
            if self._closed:
                return
            if Instant.now().ns >= deadline_ns:
                _logger.debug("WebSerial SLCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            timeout = max(0.0, (deadline_ns - Instant.now().ns) * 1e-9)
            if timeout <= 0.0:
                _logger.debug("WebSerial SLCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            try:
                await asyncio.wait_for(self._port.write(encode_frame(identifier, payload)), timeout=timeout)
            except asyncio.TimeoutError:
                self._tx_queue.put_nowait((identifier, seq, deadline_ns, payload))
                await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self._fail(ex)
                return

    async def _rx_loop(self) -> None:
        while not self._closed:
            try:
                chunk = await self._port.read()
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self._fail(ex)
                return
            if not chunk:
                self._fail(EOFError(f"WebSerial SLCAN interface {self._name} ended"))
                return
            for frame in self._parser.feed(chunk):
                self._rx_queue.put_nowait(TimestampedFrame(id=frame.id, data=frame.data, timestamp=Instant.now()))

    def _fail(self, ex: BaseException) -> None:
        if self._failure is None:
            self._failure = ex
            _logger.error("WebSerial SLCAN interface %s failed: %s", self._name, ex)
        self._close(ex)

    def _close(self, unblock: BaseException) -> None:
        if self._closed:
            return
        self._closed = True
        self._cancel_worker_tasks()
        self._drain_rx_queue()
        self._rx_queue.put_nowait(unblock)
        self._close_port()

    def _cancel_worker_tasks(self) -> None:
        current: asyncio.Task[object] | None
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in (self._tx_task, self._rx_task):
            if task is not None and task is not current:
                task.cancel()
        self._tx_task = None
        self._rx_task = None

    def _close_port(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(self._close_port_async())
            except Exception as ex:
                _logger.debug("WebSerial SLCAN port close error on %s: %s", self._name, ex)
            return
        self._close_task = loop.create_task(self._close_port_async())

    async def _close_port_async(self) -> None:
        try:
            await self._port.close()
        except Exception as ex:
            _logger.debug("WebSerial SLCAN port close error on %s: %s", self._name, ex)

    def _raise_if_closed(self) -> None:
        if self._closed:
            if self._failure is not None:
                raise ClosedError(f"WebSerial SLCAN interface {self._name} failed") from self._failure
            raise ClosedError(f"WebSerial SLCAN interface {self._name} closed")

    def _drain_rx_queue(self) -> None:
        try:
            while True:
                self._rx_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
