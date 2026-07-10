"""
Browser-oriented SLCAN backend for WebSerial/Pyodide.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable

from .._api import ClosedError, Instant, SendError
from ._interface import Filter, Interface, TimestampedFrame, closed_error
from ._tx import TxQueue
from ._media_slcan import (
    SLCANParser,
    classify_init_response,
    encode_deinit,
    encode_frame,
    encode_init_sequence,
)

_logger = logging.getLogger(__name__)

_ACK_TIMEOUT = 3.0
_DEINIT_SETTLE = 0.1
_PURGE_DRAIN_TIMEOUT = 0.05


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

    The port is expected to be already opened by browser/Pyodide glue code.
    On startup the adapter is reset: closed, left to settle, its pending input purged (so stale frames
    from a previous configuration are dropped), then configured for the selected bitrate and reopened.
    If no bitrate is given, the bitrate is left unconfigured (old/default).
    """

    def __init__(self, port: AsyncSerialPort, *, name: str = "webserial", bitrate: int | None = None) -> None:
        self._port = port
        self._name = str(name)
        self._bitrate = None if bitrate is None else int(bitrate)
        self._closed = False
        self._failure: BaseException | None = None
        self._parser = SLCANParser()
        self._tx = TxQueue()
        self._rx_queue: asyncio.Queue[TimestampedFrame | BaseException] = asyncio.Queue()
        self._init_task: asyncio.Task[None] | None = None
        self._tx_task: asyncio.Task[None] | None = None
        self._rx_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._start_init()  # Requires a running loop; this is an async interface, always built in one.
        _logger.info("WebSerial SLCAN init iface=%s bitrate=%s", self._name, self._bitrate)

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

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> asyncio.Future[None]:
        self._raise_if_closed()
        chunks = tuple(bytes(item) for item in data)
        for chunk in chunks:
            encode_frame(id, chunk)  # Validate before creating the transfer.
        if self._tx_task is None:
            self._tx_task = asyncio.get_running_loop().create_task(self._tx_loop())
            self._tx_task.add_done_callback(self._on_tx_done)
        return self._tx.push(id, chunks, deadline)

    def purge(self) -> None:
        if self._closed:
            return
        dropped = self._tx.abort_all(lambda: SendError(f"WebSerial SLCAN interface {self._name} tx purged"))
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
        try:
            await self._ensure_initialized()
        except Exception as ex:
            self._fail(ex)
            return
        while not self._closed:
            entry = await self._tx.pop()
            if self._closed:
                return  # _close() already failed every pending transfer.
            job = entry.job
            if job.settled:
                continue  # Tail of an aborted transfer.
            timeout = (job.deadline.ns - Instant.now().ns) * 1e-9
            if timeout <= 0.0:
                _logger.debug("WebSerial SLCAN tx drop expired iface=%s id=%08x", self._name, entry.id)
                job.abort(SendError(f"WebSerial SLCAN interface {self._name} tx deadline expired"))
                continue
            try:
                await asyncio.wait_for(self._port.write(encode_frame(entry.id, entry.payload)), timeout=timeout)
            except asyncio.TimeoutError:
                self._tx.requeue(entry)  # Still owed; the deadline is rechecked on the next pop.
                await asyncio.sleep(0.001)
            except Exception as ex:
                self._fail(ex)
                return
            else:
                job.complete_one()

    async def _rx_loop(self) -> None:
        try:
            await self._ensure_initialized()
        except Exception as ex:
            self._fail(ex)
            return
        while not self._closed:
            try:
                chunk = await self._port.read()
            except Exception as ex:
                self._fail(ex)
                return
            if not chunk:
                self._fail(EOFError(f"WebSerial SLCAN interface {self._name} ended"))
                return
            for frame in self._parser.feed(chunk):
                self._rx_queue.put_nowait(TimestampedFrame(id=frame.id, data=frame.data, timestamp=Instant.now()))

    def _start_init(self) -> None:
        if self._init_task is None:
            self._init_task = asyncio.get_running_loop().create_task(self._init_adapter())
            self._init_task.add_done_callback(self._on_init_done)

    def _on_tx_done(self, task: asyncio.Task[None]) -> None:
        # A crashed TX loop would otherwise leave its pending transfers unresolved forever.
        if task.cancelled() or self._closed:
            return
        ex = task.exception()
        if ex is not None:
            self._fail(ex)

    def _on_init_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as ex:
            if not self._closed:
                self._fail(ex)

    async def _ensure_initialized(self) -> None:
        self._raise_if_closed()
        self._start_init()
        assert self._init_task is not None
        await asyncio.shield(self._init_task)
        self._raise_if_closed()

    async def _init_adapter(self) -> None:
        _logger.info("WebSerial SLCAN setup iface=%s bitrate=%s", self._name, self._bitrate)
        # Reset an adapter that may be in an unknown state: close, settle, discard whatever it was
        # forwarding under the old config, then configure and open.
        await self._port.write(encode_deinit())
        await asyncio.sleep(_DEINIT_SETTLE)
        await self._purge_input()
        for command in encode_init_sequence(self._bitrate):
            _logger.debug("WebSerial SLCAN setup cmd iface=%s cmd=%r", self._name, command)
            await self._port.write(command)
            await self._wait_for_init_ack()
        _logger.info("WebSerial SLCAN setup done iface=%s", self._name)

    async def _purge_input(self) -> None:
        dropped = 0
        while True:
            try:
                chunk = await asyncio.wait_for(self._port.read(), timeout=_PURGE_DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                break
            if not chunk:
                raise EOFError("SLCAN channel ended while purging input")
            dropped += len(chunk)
        if dropped > 0:
            _logger.debug("WebSerial SLCAN purge stale input iface=%s dropped=%d", self._name, dropped)

    async def _wait_for_init_ack(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _ACK_TIMEOUT
        while True:
            timeout = deadline - loop.time()
            if timeout <= 0.0:
                raise TimeoutError("SLCAN ACK timeout")
            chunk = await asyncio.wait_for(self._port.read(), timeout=timeout)
            if not chunk:
                raise EOFError("SLCAN channel ended while waiting for ACK")
            response = classify_init_response(chunk)
            if response is True:
                return
            if response is False:
                raise OSError("SLCAN NACK in response")
            _logger.debug("WebSerial SLCAN setup ignored bytes iface=%s len=%d", self._name, len(chunk))

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
        self._tx.abort_all(self._closed_error)
        self._drain_rx_queue()
        self._rx_queue.put_nowait(unblock)
        self._close_port()

    def _cancel_worker_tasks(self) -> None:
        current: asyncio.Task[object] | None
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in (self._init_task, self._tx_task, self._rx_task):
            if task is not None and task is not current:
                task.cancel()
        self._init_task = None
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
            raise self._closed_error()

    def _closed_error(self) -> ClosedError:
        return closed_error(f"WebSerial SLCAN interface {self._name}", self._failure)

    def _drain_rx_queue(self) -> None:
        try:
            while True:
                self._rx_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
