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

from .._api import ClosedError, Instant
from ._interface import Filter, Interface, TimestampedFrame

if sys.platform != "linux" or not hasattr(socket, "AF_CAN"):
    raise ImportError("SocketCAN is available only on Linux with AF_CAN support")

_logger = logging.getLogger(__name__)

_CAN_FILTER_CAPACITY = 64
_CAN_INTERFACE_TYPE = 280
_CAN_CLASSIC_MTU = 16
_CAN_FD_MTU = 72
_CANFD_FDF = getattr(socket, "CANFD_FDF", 0)
_CAN_FRAME_STRUCT = struct.Struct("=IB3x8s")
_CANFD_FRAME_STRUCT = struct.Struct("=IBBBB64s")
_CAN_FILTER_STRUCT = struct.Struct("=II")
_TRANSIENT_TX_ERRNO = {errno.EAGAIN, errno.EWOULDBLOCK, errno.ENOBUFS, errno.ENOMEM, errno.EBUSY}


class SocketCANInterface(Interface):
    def __init__(self, name: str) -> None:
        self._name = str(name)
        self._sock = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        self._sock.setblocking(False)
        self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_LOOPBACK, 1)
        self._sock.bind((self._name,))
        self._fd = self._read_iface_mtu() >= _CAN_FD_MTU
        if self._fd:
            self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_FD_FRAMES, 1)
        self._closed = False
        self._failure: BaseException | None = None
        self._tx_seq = 0
        self._tx_queue: asyncio.PriorityQueue[tuple[int, int, int, bytes]] = asyncio.PriorityQueue()
        self._tx_task: asyncio.Task[None] | None = None

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

    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> None:
        self._raise_if_closed()
        if self._tx_task is None:
            self._tx_task = asyncio.get_running_loop().create_task(self._tx_loop())
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
            _logger.debug("SocketCAN purge iface=%s dropped=%d", self._name, dropped)

    async def receive(self) -> TimestampedFrame:
        self._raise_if_closed()
        loop = asyncio.get_running_loop()
        recv_size = _CAN_FD_MTU if self._fd else _CAN_CLASSIC_MTU
        while True:
            try:
                raw = await loop.sock_recv(self._sock, recv_size)
            except asyncio.CancelledError:
                raise
            except OSError as ex:
                self._fail(ex)
                raise ClosedError(f"SocketCAN interface {self._name} receive failed") from ex
            frame = self._decode(raw)
            if frame is not None:
                return frame

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._tx_task is not None:
            self._tx_task.cancel()
            self._tx_task = None
        self._sock.close()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name!r}, fd={self._fd})"

    async def _tx_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed:
            try:
                identifier, seq, deadline_ns, payload = await self._tx_queue.get()
            except asyncio.CancelledError:
                raise
            if self._closed:
                return
            if Instant.now().ns >= deadline_ns:
                _logger.debug("SocketCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            frame = self._encode(identifier, payload)
            timeout = max(0.0, (deadline_ns - Instant.now().ns) * 1e-9)
            if timeout <= 0.0:
                _logger.debug("SocketCAN tx drop expired iface=%s id=%08x", self._name, identifier)
                continue
            try:
                await asyncio.wait_for(loop.sock_sendall(self._sock, frame), timeout=timeout)
            except asyncio.TimeoutError:
                self._tx_queue.put_nowait((identifier, seq, deadline_ns, payload))
                await asyncio.sleep(0.001)
            except OSError as ex:
                if self._is_transient_tx_error(ex):
                    _logger.debug("SocketCAN tx retry iface=%s err=%s", self._name, ex)
                    self._tx_queue.put_nowait((identifier, seq, deadline_ns, payload))
                    await asyncio.sleep(0.001)
                    continue
                self._fail(ex)
                return

    def _read_iface_mtu(self) -> int:
        return int(Path(f"/sys/class/net/{self._name}/mtu").read_text().strip())

    def _fail(self, ex: BaseException) -> None:
        if self._failure is None:
            self._failure = ex
            _logger.error("SocketCAN interface %s failed: %s", self._name, ex)
        self.close()

    def _raise_if_closed(self) -> None:
        if self._closed:
            if self._failure is not None:
                raise ClosedError(f"SocketCAN interface {self._name} failed") from self._failure
            raise ClosedError(f"SocketCAN interface {self._name} closed")

    @staticmethod
    def _is_transient_tx_error(ex: OSError) -> bool:
        return ex.errno in _TRANSIENT_TX_ERRNO

    def _encode(self, identifier: int, data: bytes) -> bytes:
        if len(data) > 8:
            if not self._fd:
                raise ClosedError(f"SocketCAN interface {self._name} is not CAN FD-capable")
            return _CANFD_FRAME_STRUCT.pack(
                socket.CAN_EFF_FLAG | (identifier & socket.CAN_EFF_MASK),
                len(data),
                _CANFD_FDF,
                0,
                0,
                data.ljust(64, b"\x00"),
            )
        return _CAN_FRAME_STRUCT.pack(
            socket.CAN_EFF_FLAG | (identifier & socket.CAN_EFF_MASK),
            len(data),
            data.ljust(8, b"\x00"),
        )

    @staticmethod
    def _decode(raw: bytes) -> TimestampedFrame | None:
        if len(raw) < _CAN_CLASSIC_MTU:
            _logger.debug("SocketCAN drop short len=%d", len(raw))
            return None
        if len(raw) >= _CAN_FD_MTU:
            can_id, length, _flags, _reserved0, _reserved1, data = _CANFD_FRAME_STRUCT.unpack(raw[:_CAN_FD_MTU])
            payload = data[: min(length, 64)]
        else:
            can_id, length, data = _CAN_FRAME_STRUCT.unpack(raw[:_CAN_CLASSIC_MTU])
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
