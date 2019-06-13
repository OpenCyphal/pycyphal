#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import enum
import typing
import socket
import struct
import asyncio
import logging
import contextlib
import pyuavcan.transport
import pyuavcan.transport.can.media as _media


_logger = logging.getLogger(__name__)


class SocketCANMedia(_media.Media):
    """
    This media implementation provides a simple interface for the standard Linux SocketCAN media layer.
    If you are testing with a virtual CAN bus and you need CAN FD, you may need to enable it manually
    (https://stackoverflow.com/questions/36568167/can-fd-support-for-virtual-can-vcan-on-socketcan);
    otherwise, you may observe errno 90 "Message too long". Configuration example:
        ip link set vcan0 mtu 72
    """

    def __init__(self,
                 iface_name:            str,
                 max_data_field_length: int,
                 loop:                  typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        if sys.platform != 'linux':
            raise RuntimeError('SocketCAN is available only on Linux-based OS')

        max_data_field_length = int(max_data_field_length)
        if max_data_field_length not in self.VALID_MAX_DATA_FIELD_LENGTH_SET:
            raise ValueError(f'Invalid MTU: {max_data_field_length} not in {self.VALID_MAX_DATA_FIELD_LENGTH_SET}')

        self._iface_name = str(iface_name)
        self._max_data_field_length = int(max_data_field_length)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        self._is_fd = self._max_data_field_length > _NativeFrameDataCapacity.CAN_20
        self._native_frame_data_capacity = int({
            False: _NativeFrameDataCapacity.CAN_20,
            True:  _NativeFrameDataCapacity.CAN_FD,
        }[self._is_fd])
        self._native_frame_size = _FRAME_HEADER_STRUCT.size + self._native_frame_data_capacity

        self._sock = _make_socket(iface_name, can_fd=self._is_fd)
        self._closed = False
        self._maybe_task: typing.Optional[asyncio.Task[None]] = None

        super(SocketCANMedia, self).__init__()

    @property
    def interface_name(self) -> str:
        return self._iface_name

    @property
    def max_data_field_length(self) -> int:
        return self._max_data_field_length

    @property
    def number_of_acceptance_filters(self) -> int:
        """
        https://www.kernel.org/doc/Documentation/networking/can.txt
        https://github.com/torvalds/linux/blob/9c7db5004280767566e91a33445bf93aa479ef02/net/can/af_can.c#L327-L348
        https://github.com/torvalds/linux/blob/54dee406374ce8adb352c48e175176247cb8db7c/include/uapi/linux/can.h#L200
        """
        return 512

    def set_received_frames_handler(self, handler: _media.Media.ReceivedFramesHandler) -> None:
        if self._maybe_task is not None:
            self._maybe_task = self._loop.create_task(self._task_function(handler))
        else:
            raise RuntimeError('The RX frame handler is already set up')

    def configure_acceptance_filters(self, configuration: typing.Sequence[_media.FilterConfiguration]) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

        _logger.warning('%s FIXME: acceptance filter configuration is not yet implemented; please submit patches! '
                        'Requested configuration: %s', ', '.join(map(str, configuration)))

    def enable_automatic_retransmission(self) -> None:
        """
        This is currently a no-op for SocketCAN. This may change later.
        """
        pass

    async def send(self, frames: typing.Iterable[_media.DataFrame]) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

        raise NotImplementedError

    def close(self) -> None:
        self._closed = True
        self._sock.close()
        if self._maybe_task is not None:
            self._maybe_task.cancel()
            self._maybe_task = None

    async def _task_function(self, handler: _media.Media.ReceivedFramesHandler) -> None:
        while not self._closed:
            try:
                pass
                # TODO call recvmsg(); record error if MSG_CTRUNC is set (not supposed to be)
                # https://stackoverflow.com/questions/38235997/how-to-implement-recvmsg-with-asyncio
            except asyncio.CancelledError:
                break
            except OSError as ex:
                _logger.exception('%s task input/output error; stopping: %s', self, ex)
                break
            except Exception as ex:
                _logger.exception('%s task failure: %s', self, ex)
                await asyncio.sleep(1)      # Is this an adequate failure management strategy?

        self._closed = True
        with contextlib.suppress(Exception):
            self._sock.close()

    def _compile_native_frame(self, source: _media.DataFrame) -> bytes:
        flags = _CANFD_BRS if self._is_fd else 0
        ident = source.identifier | (_CAN_EFF_FLAG if source.format == _media.FrameFormat.EXTENDED else 0)
        header = _FRAME_HEADER_STRUCT.pack(ident, len(source.data), flags)
        out = header + source.data.ljust(self._native_frame_data_capacity, b'\x00')
        assert len(out) == self._native_frame_size
        return out

    @staticmethod
    def _parse_native_frame(source: bytes,
                            loopback: bool,
                            timestamp: pyuavcan.transport.Timestamp) -> typing.Optional[_media.TimestampedDataFrame]:
        header_size = _FRAME_HEADER_STRUCT.size
        ident_raw, data_length, _flags = _FRAME_HEADER_STRUCT.unpack(source[:header_size])
        if (ident_raw & _CAN_RTR_FLAG) or (ident_raw & _CAN_ERR_FLAG):  # Unsupported format, ignore silently
            return None
        frame_format = _media.FrameFormat.EXTENDED if ident_raw & _CAN_EFF_FLAG else _media.FrameFormat.BASE
        ident = ident_raw & _CAN_EFF_MASK
        data = source[header_size:header_size + data_length]
        assert len(data) == data_length
        return _media.TimestampedDataFrame(identifier=ident,
                                           data=bytearray(data),
                                           format=frame_format,
                                           loopback=loopback,
                                           timestamp=timestamp)


class _NativeFrameDataCapacity(enum.IntEnum):
    CAN_20 = 8
    CAN_FD = 64


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
_FRAME_HEADER_STRUCT = struct.Struct('=IBB2x')


# From the Linux kernel; not exposed via the Python's socket module
_SO_TIMESTAMP = 29

_CANFD_BRS = 1

_CAN_EFF_FLAG = 0x80000000
_CAN_RTR_FLAG = 0x40000000
_CAN_ERR_FLAG = 0x20000000

_CAN_EFF_MASK = 0x1FFFFFFF


def _make_socket(iface_name: str, can_fd: bool) -> socket.SocketType:
    s = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
    try:
        s.bind((iface_name,))
        s.setsockopt(socket.SOL_SOCKET, _SO_TIMESTAMP, 1)  # timestamping
        s.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_RECV_OWN_MSGS, 1)  # loopback all frames
        if can_fd:
            s.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_FD_FRAMES, 1)

        s.setblocking(False)

        if 0 != s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            raise OSError('Could not configure the socket: getsockopt(SOL_SOCKET, SO_ERROR) != 0')
    except BaseException:
        with contextlib.suppress(Exception):
            s.close()
        raise

    return s
