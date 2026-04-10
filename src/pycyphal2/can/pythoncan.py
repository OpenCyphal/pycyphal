"""
Cross-platform CAN backend using `python-can <https://python-can.readthedocs.io/>`_.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
import logging
import threading
import time

from .._api import ClosedError, Instant
from ._interface import Filter, Interface, TimestampedFrame

try:
    import can
except ImportError:
    raise ImportError("PythonCAN backend requires python-can: pip install 'pycyphal2[pythoncan]'") from None

_logger = logging.getLogger(__name__)

_RX_POLL_TIMEOUT = 0.1
_RX_PAUSE_SLEEP_INTERVAL = 0.001
_RX_PAUSE_TIMEOUT_MULTIPLIER = 10.0
_CAN_EXT_ID_MASK = (1 << 29) - 1


class PythonCANInterface(Interface):
    """
    Wraps a `python-can <https://python-can.readthedocs.io/>`_ bus as a :class:`pycyphal2.can.Interface`.

    The caller is responsible for constructing and configuring the :class:`can.BusABC` instance
    (bitrate, interface type, channel, FD mode, etc.) and passing it in.
    Use :class:`can.ThreadSafeBus` for safe concurrent access from the RX thread and TX executor.

    The ``fd`` flag may be left as ``None``; in that case, FD capability is detected
    from ``bus.protocol`` (see :class:`can.CanProtocol`), defaulting to Classic CAN
    if the bus does not report FD support.
    """

    def __init__(self, bus: can.BusABC, *, fd: bool | None = None) -> None:
        self._bus = bus
        self._name = getattr(bus, "channel_info", repr(bus))
        if fd is None:
            fd = bus.protocol in (can.CanProtocol.CAN_FD, can.CanProtocol.CAN_FD_NON_ISO)
        self._fd = fd
        self._closed = False
        self._failure: BaseException | None = None
        self._tx_seq = 0
        self._tx_queue: asyncio.PriorityQueue[tuple[int, int, int, bytes]] = asyncio.PriorityQueue()
        self._tx_task: asyncio.Task[None] | None = None
        self._rx_queue: asyncio.Queue[TimestampedFrame | BaseException] = asyncio.Queue()
        self._loop = asyncio.get_running_loop()
        self._rx_pause_request = threading.Event()
        self._rx_pause_ack = threading.Event()
        self._rx_thread = threading.Thread(target=self._rx_thread_func, daemon=True, name=f"pythoncan-rx-{self._name}")
        self._rx_thread.start()
        _logger.info("PythonCAN init iface=%s fd=%s", self._name, self._fd)

    @property
    def name(self) -> str:
        return self._name

    @property
    def fd(self) -> bool:
        return self._fd

    def filter(self, filters: Iterable[Filter]) -> None:
        self._raise_if_closed()
        can_filters: list[can.typechecking.CanFilter] = []
        for item in filters:
            can_filters.append(can.typechecking.CanFilter(can_id=item.id, can_mask=item.mask, extended=True))
        try:
            self._pause_rx_thread()
            try:
                self._bus.set_filters(can_filters)
            finally:
                self._resume_rx_thread()
        except can.CanError as ex:
            raise OSError(f"PythonCAN filter configuration failed on {self._name}: {ex}") from ex
        _logger.debug("PythonCAN filters set iface=%s n=%d", self._name, len(can_filters))

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> None:
        self._raise_if_closed()
        if self._tx_task is None:
            self._tx_task = self._loop.create_task(self._tx_loop())
        for chunk in data:
            self._tx_seq += 1
            self._tx_queue.put_nowait((id, self._tx_seq, deadline.ns, bytes(chunk)))

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
            _logger.debug("PythonCAN purge iface=%s dropped=%d", self._name, dropped)

    async def receive(self) -> TimestampedFrame:
        self._raise_if_closed()
        while True:
            item = await self._rx_queue.get()
            if isinstance(item, BaseException):
                self._fail(item)
                raise ClosedError(f"PythonCAN interface {self._name} receive failed") from item
            return item

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._tx_task is not None:
            self._tx_task.cancel()
            self._tx_task = None
        try:
            self._rx_queue.put_nowait(ClosedError(f"PythonCAN interface {self._name} closed"))
        except Exception:
            pass
        try:
            self._pause_rx_thread()
            try:
                self._bus.shutdown()
            finally:
                self._resume_rx_thread()
        except Exception as ex:
            _logger.debug("PythonCAN bus shutdown error on %s: %s", self._name, ex)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name!r}, fd={self._fd})"

    async def _tx_loop(self) -> None:
        # Deadlines are enforced when popping from the queue. Once a frame is handed to bus.send(),
        # the deadline is passed as the blocking timeout but cannot be enforced further by us.
        loop = asyncio.get_running_loop()
        while not self._closed:
            try:
                identifier, _seq, deadline_ns, payload = await self._tx_queue.get()
            except asyncio.CancelledError:
                raise
            if self._closed:
                return
            if Instant.now().ns >= deadline_ns:
                _logger.debug("PythonCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            timeout = max(0.0, (deadline_ns - Instant.now().ns) * 1e-9)
            if timeout <= 0.0:
                _logger.debug("PythonCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            msg = can.Message(
                arbitration_id=identifier,
                is_extended_id=True,
                data=payload,
                is_fd=self._fd and len(payload) > 8,
                bitrate_switch=self._fd and len(payload) > 8,
            )
            try:
                await asyncio.wait_for(loop.run_in_executor(None, self._bus.send, msg, timeout), timeout=timeout)
            except asyncio.TimeoutError:
                self._tx_queue.put_nowait((identifier, self._tx_seq, deadline_ns, payload))
                self._tx_seq += 1
                await asyncio.sleep(0.001)
            except can.CanError as ex:
                _logger.debug("PythonCAN tx retry iface=%s err=%s", self._name, ex)
                self._tx_queue.put_nowait((identifier, self._tx_seq, deadline_ns, payload))
                self._tx_seq += 1
                await asyncio.sleep(0.001)
            except OSError as ex:
                self._fail(ex)
                return

    def _rx_thread_func(self) -> None:
        while not self._closed:
            if self._rx_pause_request.is_set():
                self._rx_pause_ack.set()
                while self._rx_pause_request.is_set() and not self._closed:
                    time.sleep(_RX_PAUSE_SLEEP_INTERVAL)
                self._rx_pause_ack.clear()
                continue
            try:
                msg = self._bus.recv(timeout=_RX_POLL_TIMEOUT)
            except Exception as ex:
                if not self._closed:
                    try:
                        self._loop.call_soon_threadsafe(self._rx_queue.put_nowait, ex)
                    except RuntimeError:
                        pass
                return
            if msg is None:
                continue
            try:
                frame = _parse_message(msg)
            except Exception as ex:
                _logger.debug("PythonCAN rx drop malformed: %s", ex)
                continue
            if frame is not None:
                try:
                    self._loop.call_soon_threadsafe(self._rx_queue.put_nowait, frame)
                except RuntimeError:
                    return

    def _fail(self, ex: BaseException) -> None:
        if self._failure is None:
            self._failure = ex
            _logger.error("PythonCAN interface %s failed: %s", self._name, ex)
        self.close()

    def _raise_if_closed(self) -> None:
        if self._closed:
            if self._failure is not None:
                raise ClosedError(f"PythonCAN interface {self._name} failed") from self._failure
            raise ClosedError(f"PythonCAN interface {self._name} closed")

    def _pause_rx_thread(self) -> None:
        # Defensive future-proofing: avoid self-deadlock if this helper ever gets reused from the RX thread.
        if not self._rx_thread.is_alive() or threading.current_thread() is self._rx_thread:
            return
        self._rx_pause_request.set()
        if not self._rx_pause_ack.wait(timeout=_RX_POLL_TIMEOUT * _RX_PAUSE_TIMEOUT_MULTIPLIER):
            _logger.warning("PythonCAN rx pause timeout iface=%s", self._name)

    def _resume_rx_thread(self) -> None:
        self._rx_pause_request.clear()
        self._rx_pause_ack.clear()


def _parse_message(msg: can.Message) -> TimestampedFrame | None:
    if msg.is_error_frame:
        _logger.debug("PythonCAN drop error frame id=%08x", msg.arbitration_id)
        return None
    if not msg.is_extended_id:
        _logger.debug("PythonCAN drop non-extended id=%08x", msg.arbitration_id)
        return None
    if msg.is_remote_frame:
        _logger.debug("PythonCAN drop remote frame id=%08x", msg.arbitration_id)
        return None
    return TimestampedFrame(id=msg.arbitration_id & _CAN_EXT_ID_MASK, data=bytes(msg.data), timestamp=Instant.now())
