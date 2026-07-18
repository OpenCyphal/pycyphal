"""Linux SocketCAN backend for :mod:`pycyphal2.can`."""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Iterable
import logging
from pathlib import Path
import socket
import struct
import sys

from .._api import ClosedError, Instant, SendError
from ._interface import Filter, Interface, TimestampedFrame, closed_error
from ._tx import TxQueue

if sys.platform != "linux" or not hasattr(socket, "AF_CAN"):
    raise ImportError("SocketCAN is available only on Linux with AF_CAN support")

_logger = logging.getLogger(__name__)

_CAN_FILTER_CAPACITY = 64
_CAN_INTERFACE_TYPE = 280
_CANFD_FDF = getattr(socket, "CANFD_FDF", 0)
_CAN_FRAME_STRUCT = struct.Struct("=IB3x8s")
_CANFD_FRAME_STRUCT = struct.Struct("=IBBBB64s")
_CAN_FILTER_STRUCT = struct.Struct("=II")
# SocketCAN frame sizes — sizeof(struct can_frame)=16, sizeof(struct canfd_frame)=72 (NOT the 8/64 payload MTU).
# The kernel also reports these as the CAN netdev MTU, so they double as the FD-capability threshold.
_CLASSIC_FRAME_SIZE = _CAN_FRAME_STRUCT.size
_FD_FRAME_SIZE = _CANFD_FRAME_STRUCT.size
_TRANSIENT_TX_ERRNO = {errno.EAGAIN, errno.EWOULDBLOCK, errno.ENOBUFS, errno.ENOMEM, errno.EBUSY}


class SocketCANInterface(Interface):
    def __init__(self, name: str) -> None:
        self._name = str(name)
        self._sock = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        self._sock.setblocking(False)
        self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_LOOPBACK, 1)
        self._sock.bind((self._name,))
        self._fd = self._read_iface_mtu() >= _FD_FRAME_SIZE
        if self._fd:
            self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_FD_FRAMES, 1)
        self._closed = False
        self._failure: BaseException | None = None
        self._tx = TxQueue()
        self._tx_task: asyncio.Task[None] | None = None
        # RX runs in its own task feeding a queue, so close()/fail() can wake a parked reader with a
        # sentinel instead of leaving it hung in sock_recv (which the selector loop never wakes on a
        # bare socket.close()). This mirrors the python-can and webserial backends.
        self._rx_queue: asyncio.Queue[TimestampedFrame | BaseException] = asyncio.Queue()
        self._rx_task: asyncio.Task[None] | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def fd(self) -> bool:
        return self._fd

    def filter(self, filters: Iterable[Filter]) -> None:
        self._raise_if_closed()
        flt = list(filters)
        if len(flt) > _CAN_FILTER_CAPACITY:
            flt = Filter.coalesce(flt, _CAN_FILTER_CAPACITY)
        packed = bytearray()
        for item in flt:
            packed.extend(
                _CAN_FILTER_STRUCT.pack(
                    socket.CAN_EFF_FLAG | (item.id & socket.CAN_EFF_MASK),
                    # Keep CAN_RTR_FLAG in the mask so the kernel rejects RTR frames at the filter layer.
                    socket.CAN_EFF_FLAG | socket.CAN_RTR_FLAG | (item.mask & socket.CAN_EFF_MASK),
                )
            )
        self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_FILTER, bytes(packed))

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> asyncio.Future[None]:
        self._raise_if_closed()
        if self._tx_task is None:
            self._tx_task = asyncio.get_running_loop().create_task(self._tx_loop())
            self._tx_task.add_done_callback(self._on_task_done)
        return self._tx.push(id, (bytes(chunk) for chunk in data), deadline)

    def purge(self) -> None:
        if self._closed:
            return
        dropped = self._tx.abort_all(lambda: SendError(f"SocketCAN interface {self._name} tx purged"))
        if dropped > 0:
            _logger.debug("SocketCAN purge iface=%s dropped=%d", self._name, dropped)

    async def receive(self) -> TimestampedFrame:
        self._raise_if_closed()
        if self._rx_task is None:
            self._rx_task = asyncio.get_running_loop().create_task(self._rx_loop())
            self._rx_task.add_done_callback(self._on_task_done)
        item = await self._rx_queue.get()
        if isinstance(item, BaseException):
            self._fail(item)
            raise ClosedError(f"SocketCAN interface {self._name} receive failed") from item
        return item

    async def _rx_loop(self) -> None:
        loop = asyncio.get_running_loop()
        recv_size = _FD_FRAME_SIZE if self._fd else _CLASSIC_FRAME_SIZE
        while not self._closed:
            try:
                raw = await loop.sock_recv(self._sock, recv_size)
            except asyncio.CancelledError:
                raise
            except OSError as ex:
                if not self._closed:
                    self._rx_queue.put_nowait(ex)
                return
            frame = self._decode(raw)
            if frame is not None:
                self._rx_queue.put_nowait(frame)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._rx_task is not None and self._rx_task is not asyncio.current_task():
            self._rx_task.cancel()
        self._rx_task = None
        # Wake a reader parked on the queue; the socket is closed last so the cancelled reader task
        # deregisters cleanly before the fd goes away.
        self._rx_queue.put_nowait(self._closed_error())
        if self._tx_task is not None:
            self._tx_task.cancel()
            self._tx_task = None
        self._tx.abort_all(self._closed_error)
        self._sock.close()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name!r}, fd={self._fd})"

    async def _tx_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed:
            entry = await self._tx.pop()
            if self._closed:
                return  # close() already failed every pending transfer.
            job = entry.job
            if job.settled:
                continue  # Tail of an aborted transfer.
            timeout = (job.deadline.ns - Instant.now().ns) * 1e-9
            if timeout <= 0.0:
                _logger.debug("SocketCAN tx drop expired iface=%s id=%08x", self._name, entry.id)
                job.abort(SendError(f"SocketCAN interface {self._name} tx deadline expired"))
                continue
            try:
                frame = self._encode(entry.id, entry.payload)
            except ValueError as ex:
                # An unencodable frame (e.g. oversized on Classic CAN) dooms its transfer, not the interface.
                _logger.warning("SocketCAN tx drop unencodable iface=%s id=%08x: %s", self._name, entry.id, ex)
                job.abort(SendError(str(ex)))
                continue
            try:
                await asyncio.wait_for(loop.sock_sendall(self._sock, frame), timeout=timeout)
            except asyncio.TimeoutError:
                self._tx.requeue(entry)  # Still owed; the deadline is rechecked on the next pop.
                await asyncio.sleep(0.001)
            except OSError as ex:
                if self._is_transient_tx_error(ex):
                    _logger.debug("SocketCAN tx retry iface=%s err=%s", self._name, ex)
                    self._tx.requeue(entry)
                    await asyncio.sleep(0.001)
                    continue
                self._fail(ex)
                return
            else:
                job.complete_one()

    def _read_iface_mtu(self) -> int:
        return int(Path(f"/sys/class/net/{self._name}/mtu").read_text().strip())

    def _fail(self, ex: BaseException) -> None:
        if self._failure is None:
            self._failure = ex
            _logger.error("SocketCAN interface %s failed: %s", self._name, ex)
        self.close()

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        # Surface an unexpected TX-task crash as an interface failure instead of swallowing it.
        if task.cancelled() or self._closed:
            return
        ex = task.exception()
        if ex is not None:
            self._fail(ex)

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise self._closed_error()

    def _closed_error(self) -> ClosedError:
        return closed_error(f"SocketCAN interface {self._name}", self._failure)

    @staticmethod
    def _is_transient_tx_error(ex: OSError) -> bool:
        return ex.errno in _TRANSIENT_TX_ERRNO

    def _encode(self, identifier: int, data: bytes) -> bytes:
        # The frame format is a property of the interface, fixed at construction, not of the payload
        # length: every frame on an FD interface is an FD frame (FDF set, BRS never), as in the
        # reference (cy_can_socketcan selects the FD/Classic vtable once from the netdev MTU).
        if self._fd:
            return _CANFD_FRAME_STRUCT.pack(
                socket.CAN_EFF_FLAG | (identifier & socket.CAN_EFF_MASK),
                len(data),
                _CANFD_FDF,
                0,
                0,
                data.ljust(64, b"\x00"),
            )
        if len(data) > 8:
            raise ValueError(f"SocketCAN interface {self._name} cannot send a {len(data)}-byte frame on Classic CAN")
        return _CAN_FRAME_STRUCT.pack(
            socket.CAN_EFF_FLAG | (identifier & socket.CAN_EFF_MASK),
            len(data),
            data.ljust(8, b"\x00"),
        )

    @staticmethod
    def _decode(raw: bytes) -> TimestampedFrame | None:
        if len(raw) < _CLASSIC_FRAME_SIZE:
            _logger.debug("SocketCAN drop short len=%d", len(raw))
            return None
        if len(raw) >= _FD_FRAME_SIZE:
            can_id, length, _flags, _reserved0, _reserved1, data = _CANFD_FRAME_STRUCT.unpack(raw[:_FD_FRAME_SIZE])
            payload = data[: min(length, 64)]
        else:
            can_id, length, data = _CAN_FRAME_STRUCT.unpack(raw[:_CLASSIC_FRAME_SIZE])
            payload = data[: min(length, 8)]
        if (can_id & socket.CAN_EFF_FLAG) == 0 or (can_id & (socket.CAN_RTR_FLAG | socket.CAN_ERR_FLAG)) != 0:
            _logger.debug("SocketCAN drop non-extended or non-data id=%08x", can_id)
            return None
        return TimestampedFrame(
            id=can_id & socket.CAN_EFF_MASK,
            data=payload,
            timestamp=Instant.now(),
        )

    @staticmethod
    def list_interfaces() -> list[str]:
        out: list[str] = []
        base = Path("/sys/class/net")
        try:
            for item in sorted(base.iterdir()):
                try:
                    if int((item / "type").read_text().strip()) == _CAN_INTERFACE_TYPE:
                        out.append(item.name)
                except OSError:
                    continue
                except ValueError:
                    continue
        except OSError:
            pass
        return out
