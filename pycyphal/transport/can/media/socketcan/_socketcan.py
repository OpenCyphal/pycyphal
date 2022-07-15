# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import enum
import time
import errno
import typing
import socket
import struct
import select
import asyncio
import logging
import warnings
import threading
import contextlib
import pathlib
import pycyphal.transport
from pycyphal.transport import Timestamp
from pycyphal.transport.can.media import Media, Envelope, FilterConfiguration, FrameFormat
from pycyphal.transport.can.media import DataFrame

# Disable unused ignore warning for this file only because there appears to be no other way to make MyPy
# accept this file both on Windows and GNU/Linux.
# mypy: warn_unused_ignores=False


_logger = logging.getLogger(__name__)


class SocketCANMedia(Media):
    """
    This media implementation provides a simple interface for the standard Linux SocketCAN media layer.
    If you are testing with a virtual CAN bus and you need CAN FD, you may need to enable it manually
    (https://stackoverflow.com/questions/36568167/can-fd-support-for-virtual-can-vcan-on-socketcan);
    otherwise, you may observe errno 90 "Message too long". Configuration example::

        ip link set vcan0 mtu 72

    SocketCAN documentation: https://www.kernel.org/doc/Documentation/networking/can.txt
    """

    def __init__(self, iface_name: str, mtu: int, loop: typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        CAN Classic/FD is selected automatically based on the MTU. It is not possible to use CAN FD with MTU of 8 bytes.

        :param iface_name: E.g., ``can0``.

        :param mtu: The maximum data field size in bytes. CAN FD is used if this value > 8, Classic CAN otherwise.
            This value must belong to Media.VALID_MTU_SET.

        :param loop: Deprecated.
        """
        # This can't be made a class attribute because these errnos are only available on GNU/Linux.
        self._errno_unrecoverable = {
            errno.ENODEV,  # type: ignore
            errno.ENXIO,  # type: ignore
            errno.EBADF,  # type: ignore
            errno.EBADFD,  # type: ignore
            errno.ENAVAIL,  # type: ignore
            errno.ENETDOWN,  # type: ignore
            errno.ENETRESET,  # type: ignore
            errno.ENETUNREACH,  # type: ignore
            errno.ENOLINK,  # type: ignore
        }

        self._mtu = int(mtu)
        if self._mtu not in self.VALID_MTU_SET:
            raise ValueError(f"Invalid MTU: {self._mtu} not in {self.VALID_MTU_SET}")
        if loop:
            warnings.warn("The loop argument is deprecated", DeprecationWarning)

        self._iface_name = str(iface_name)
        self._is_fd = self._mtu > _NativeFrameDataCapacity.CAN_CLASSIC
        self._native_frame_data_capacity = int(
            {
                False: _NativeFrameDataCapacity.CAN_CLASSIC,
                True: _NativeFrameDataCapacity.CAN_FD,
            }[self._is_fd]
        )
        self._native_frame_size = _FRAME_HEADER_STRUCT.size + self._native_frame_data_capacity

        self._sock = _make_socket(iface_name, can_fd=self._is_fd, native_frame_size=self._native_frame_size)
        self._ctl_main, self._ctl_worker = socket.socketpair()  # This is used for controlling the worker thread.
        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._loopback_enabled = False

        self._ancillary_data_buffer_size = socket.CMSG_SPACE(_TIMEVAL_STRUCT.size)  # type: ignore

        super().__init__()

    @property
    def interface_name(self) -> str:
        return self._iface_name

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def number_of_acceptance_filters(self) -> int:
        """
        512 for SocketCAN.

        - https://github.com/torvalds/linux/blob/9c7db5004280767566e91a33445bf93aa479ef02/net/can/af_can.c#L327-L348
        - https://github.com/torvalds/linux/blob/54dee406374ce8adb352c48e175176247cb8db7c/include/uapi/linux/can.h#L200
        """
        return 512

    def start(self, handler: Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        if self._maybe_thread is None:
            self._maybe_thread = threading.Thread(
                target=self._thread_function, name=str(self), args=(handler, asyncio.get_event_loop()), daemon=True
            )
            self._maybe_thread.start()
            if no_automatic_retransmission:
                _logger.info("%s non-automatic retransmission is not supported", self)
        else:
            raise RuntimeError("The RX frame handler is already set up")

    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(repr(self))
        _logger.info(
            "%s FIXME: acceptance filter configuration is not yet implemented; please submit patches! "
            "Requested configuration: %s",
            self,
            ", ".join(map(str, configuration)),
        )

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        num_sent = 0
        for f in frames:
            if self._closed:
                raise pycyphal.transport.ResourceClosedError(repr(self))
            self._set_loopback_enabled(f.loopback)
            try:
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.sock_sendall(self._sock, self._compile_native_frame(f.frame)),
                    timeout=monotonic_deadline - loop.time(),
                )
            except OSError as err:
                if self._closed:  # https://github.com/OpenCyphal/pycyphal/issues/204
                    break
                if err.errno == errno.EINVAL and self._is_fd:
                    raise pycyphal.transport.InvalidMediaConfigurationError(
                        "Invalid socketcan configuration: "
                        "the device probably doesn't support CAN-FD. "
                        "Try setting MTU to 8 (Classic CAN)"
                    ) from err
                self._closed = self._closed or err.errno in self._errno_unrecoverable
                raise err
            except asyncio.TimeoutError:
                break
            else:
                num_sent += 1
        return num_sent

    def close(self) -> None:
        try:
            self._closed = True
            if self._ctl_main.fileno() >= 0:  # Ignore if already closed.
                self._ctl_main.send(b"stop")  # The actual data is irrelevant, we just need it to unblock the select().
            if self._maybe_thread:
                self._maybe_thread.join(timeout=_SELECT_TIMEOUT)
                self._maybe_thread = None
        finally:
            self._sock.close()  # These are expected to be idempotent.
            self._ctl_worker.close()
            self._ctl_main.close()

    def _thread_function(self, handler: Media.ReceivedFramesHandler, loop: asyncio.AbstractEventLoop) -> None:
        def handler_wrapper(frs: typing.Sequence[typing.Tuple[Timestamp, Envelope]]) -> None:
            try:
                if not self._closed:  # Don't call after closure to prevent race conditions and use-after-close.
                    handler(frs)
            except Exception as exc:
                _logger.exception("%s: Unhandled exception in the receive handler: %s; lost frames: %s", self, exc, frs)

        while not self._closed:
            try:
                (
                    read_ready,
                    _,
                    _,
                ) = select.select((self._sock, self._ctl_worker), (), (), _SELECT_TIMEOUT)
                ts_mono_ns = time.monotonic_ns()

                if self._sock in read_ready:
                    frames: typing.List[typing.Tuple[Timestamp, Envelope]] = []
                    try:
                        while True:
                            frames.append(self._read_frame(ts_mono_ns))
                    except OSError as ex:
                        if ex.errno != errno.EAGAIN:
                            raise
                    try:
                        loop.call_soon_threadsafe(handler_wrapper, frames)
                    except RuntimeError as ex:
                        _logger.debug("%s: Event loop is closed, exiting: %r", self, ex)
                        break
                if self._ctl_worker in read_ready:
                    if self._ctl_worker.recv(1):  # pragma: no branch
                        break
            except Exception as ex:  # pragma: no cover
                if (
                    self._sock.fileno() < 0
                    or self._ctl_worker.fileno() < 0
                    or self._ctl_main.fileno() < 0
                    or (isinstance(ex, OSError) and ex.errno in self._errno_unrecoverable)
                ):
                    self._closed = True
                _logger.exception("%s thread failure: %s", self, ex)
                time.sleep(1)  # Is this an adequate failure management strategy?

        self._closed = True
        _logger.debug("%s thread is about to exit", self)

    def _read_frame(self, ts_mono_ns: int) -> typing.Tuple[Timestamp, Envelope]:
        while True:
            data, ancdata, msg_flags, _addr = self._sock.recvmsg(  # type: ignore
                self._native_frame_size, self._ancillary_data_buffer_size
            )
            assert msg_flags & socket.MSG_TRUNC == 0, "The data buffer is not large enough"
            assert msg_flags & socket.MSG_CTRUNC == 0, "The ancillary data buffer is not large enough"

            loopback = bool(msg_flags & socket.MSG_CONFIRM)
            ts_system_ns = 0
            for cmsg_level, cmsg_type, cmsg_data in ancdata:
                if cmsg_level == socket.SOL_SOCKET and cmsg_type == _SO_TIMESTAMP:
                    sec, usec = _TIMEVAL_STRUCT.unpack(cmsg_data)
                    ts_system_ns = (sec * 1_000_000 + usec) * 1000
                else:
                    assert False, f"Unexpected ancillary data: {cmsg_level}, {cmsg_type}, {cmsg_data!r}"

            assert ts_system_ns > 0, "Missing the timestamp; does the driver support timestamping?"
            timestamp = Timestamp(system_ns=ts_system_ns, monotonic_ns=ts_mono_ns)
            out = SocketCANMedia._parse_native_frame(data)
            if out is not None:
                return timestamp, Envelope(out, loopback=loopback)

    def _compile_native_frame(self, source: DataFrame) -> bytes:
        flags = _CANFD_BRS if self._is_fd else 0
        ident = source.identifier | (_CAN_EFF_FLAG if source.format == FrameFormat.EXTENDED else 0)
        header = _FRAME_HEADER_STRUCT.pack(ident, len(source.data), flags)
        out = header + source.data.ljust(self._native_frame_data_capacity, b"\x00")
        assert len(out) == self._native_frame_size
        return out

    @staticmethod
    def _parse_native_frame(source: bytes) -> typing.Optional[DataFrame]:
        header_size = _FRAME_HEADER_STRUCT.size
        ident_raw, data_length, _flags = _FRAME_HEADER_STRUCT.unpack(source[:header_size])
        if (ident_raw & _CAN_RTR_FLAG) or (ident_raw & _CAN_ERR_FLAG):  # Unsupported format, ignore silently
            _logger.debug("Unsupported CAN frame dropped; raw SocketCAN ID is %08x", ident_raw)
            return None
        frame_format = FrameFormat.EXTENDED if ident_raw & _CAN_EFF_FLAG else FrameFormat.BASE
        data = source[header_size : header_size + data_length]
        assert len(data) == data_length
        ident = ident_raw & _CAN_EFF_MASK
        return DataFrame(frame_format, ident, bytearray(data))

    def _set_loopback_enabled(self, enable: bool) -> None:
        if enable != self._loopback_enabled:
            self._sock.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_RECV_OWN_MSGS, int(enable))  # type: ignore
            self._loopback_enabled = enable

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        import re
        import subprocess

        try:
            proc = subprocess.run("ip link show", check=True, timeout=1, text=True, shell=True, capture_output=True)
            return re.findall(r"\d+?: ([a-z0-9]+?): <[^>]*UP[^>]*>.*\n *link/can", proc.stdout)
        except Exception as ex:
            _logger.debug(
                "Could not scrape the output of `ip link show`, using the fallback method: %s", ex, exc_info=True
            )
            with open("/proc/net/dev") as f:  # pylint: disable=unspecified-encoding
                out = [line.split(":")[0].strip() for line in f if ":" in line and "can" in line]
            return sorted(out, key=lambda x: "can" in x, reverse=True)


class _NativeFrameDataCapacity(enum.IntEnum):
    CAN_CLASSIC = 8
    CAN_FD = 64


_SELECT_TIMEOUT = 1.0


# struct can_frame {
#     canid_t can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
#     __u8    can_dlc; /* data length code: 0 .. 8 */
#     __u8    data[8] __attribute__((aligned(8)));
# };
# struct canfd_frame {
#     canid_t can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
#     __u8    len;     /* frame payload length in byte */
#     __u8    flags;   /* additional flags for CAN FD */
#     __u8    __res0;  /* reserved / padding */
#     __u8    __res1;  /* reserved / padding */
#     __u8    data[CANFD_MAX_DLEN] __attribute__((aligned(8)));
# };
_FRAME_HEADER_STRUCT = struct.Struct("=IBB2x")  # Using standard size because the native definition relies on stdint.h
_TIMEVAL_STRUCT = struct.Struct("@Ll")  # Using native size because the native definition uses plain integers

# From the Linux kernel; not exposed via the Python's socket module
_SO_TIMESTAMP = 29
_SO_SNDBUF = 7

_CANFD_BRS = 1

_CAN_EFF_FLAG = 0x80000000
_CAN_RTR_FLAG = 0x40000000
_CAN_ERR_FLAG = 0x20000000

_CAN_EFF_MASK = 0x1FFFFFFF

# approximate sk_buffer kernel struct overhead.
# A lower estimate over higher estimate is preferred since _SO_SNDBUF will enforce
# a minimum value, and blocking behavior will not work if this is too high.
_SKB_OVERHEAD = 444


def _get_tx_queue_len(iface_name: str) -> int:
    try:
        sysfs_net = pathlib.Path("/sys/class/net/")
        sysfs_tx_queue_len = sysfs_net / iface_name / "tx_queue_len"
        return int(sysfs_tx_queue_len.read_text())
    except FileNotFoundError as e:
        raise FileNotFoundError("tx_queue_len sysfs location not found") from e


def _make_socket(iface_name: str, can_fd: bool, native_frame_size: int) -> socket.socket:
    s = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)  # type: ignore
    try:
        s.bind((iface_name,))
        s.setsockopt(socket.SOL_SOCKET, _SO_TIMESTAMP, 1)  # timestamping
        default_sndbuf_size = s.getsockopt(socket.SOL_SOCKET, _SO_SNDBUF)
        blocking_sndbuf_size = (native_frame_size + _SKB_OVERHEAD) * _get_tx_queue_len(iface_name)

        # Allow CAN sockets to block when full similar to how Ethernet sockets do.
        # Avoids ENOBUFS errors on TX when queues are full in most cases.
        # More info:
        #   - https://github.com/OpenCyphal/pycyphal/issues/233
        #   - "SocketCAN and queueing disciplines: Final Report", Sojka et al, 2012
        s.setsockopt(socket.SOL_SOCKET, _SO_SNDBUF, min(blocking_sndbuf_size, default_sndbuf_size) // 2)
        if can_fd:
            s.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_FD_FRAMES, 1)  # type: ignore

        s.setblocking(False)

        if 0 != s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            raise OSError("Could not configure the socket: getsockopt(SOL_SOCKET, SO_ERROR) != 0")
    except BaseException:
        with contextlib.suppress(Exception):
            s.close()
        raise

    return s
