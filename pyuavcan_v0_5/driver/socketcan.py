#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import os
import re
import errno
import fcntl
import socket
import struct
import select
import subprocess
import time
import threading
from fcntl import ioctl
from logging import getLogger

from .common import DriverError, TxQueueFullError, CANFrame, CANFrameFd, AbstractDriver
from .timestamp_estimator import TimestampEstimator
from uavcan import CAN_2_DATAFIELD_LEN, CAN_FD_DATAFIELD_LEN

try:
    import queue
except ImportError:
    # noinspection PyPep8Naming,PyUnresolvedReferences
    import Queue as queue

logger = getLogger(__name__)

# CAN-FD flags for tx frames
CANFD_FLAG_BRS = 1

# Python 3.3+'s socket module has support for SocketCAN when running on Linux. Use that if possible.
# noinspection PyBroadException
try:
    # noinspection PyStatementEffect
    socket.CAN_RAW

    def get_socket(ifname, is_canfd):
        # using native sockets we don't need to do anything special here
        # for CAN-FD. We will have to set a socket option for it later.
        s = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        s.bind((ifname, ))
        return s

    NATIVE_SOCKETCAN = True

    from socket import SOL_CAN_RAW
    from socket import CAN_RAW_FILTER, CAN_RAW_ERR_FILTER, CAN_RAW_LOOPBACK, CAN_RAW_RECV_OWN_MSGS, CAN_RAW_FD_FRAMES

except Exception:
    NATIVE_SOCKETCAN = False

    import ctypes
    import ctypes.util

    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

    # from linux/can.h
    CAN_RAW = 1

    # from linux/socket.h
    AF_CAN = 29

    from socket import SOL_SOCKET

    SOL_CAN_BASE = 100
    SOL_CAN_RAW = SOL_CAN_BASE + CAN_RAW
    CAN_RAW_FILTER = 1                      # set 0 .. n can_filter(s)
    CAN_RAW_ERR_FILTER = 2                  # set filter for error frames
    CAN_RAW_LOOPBACK = 3                    # local loopback (default:on)
    CAN_RAW_RECV_OWN_MSGS = 4               # receive my own msgs (default:off)
    CAN_RAW_FD_FRAMES = 5                   # allow CAN FD frames (default:off)

    # noinspection PyPep8Naming
    class sockaddr_can(ctypes.Structure):
        """
        typedef __u32 canid_t;
        struct sockaddr_can {
            sa_family_t can_family;
            int         can_ifindex;
            union {
                struct { canid_t rx_id, tx_id; } tp;
            } can_addr;
        };
        """
        _fields_ = [
            ("can_family", ctypes.c_uint16),
            ("can_ifindex", ctypes.c_int),
            ("can_addr_tp_rx_id", ctypes.c_uint32),
            ("can_addr_tp_tx_id", ctypes.c_uint32)
        ]

    # noinspection PyPep8Naming
    class can_frame(ctypes.Structure):
        """
        typedef __u32 canid_t;
        struct can_frame {
            canid_t can_id;
            __u8    can_dlc;
            __u8    data[8] __attribute__((aligned(8)));
        };
        """
        _fields_ = [
            ("can_id", ctypes.c_uint32),
            ("can_dlc", ctypes.c_uint8),
            ("_pad", ctypes.c_ubyte * 3),
            ("data", ctypes.c_uint8 * 8)
        ]

    # noinspection PyPep8Naming
    class can_frame_fd(ctypes.Structure):
        """
        struct canfd_frame {
            canid_t can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
            __u8    len;     /* frame payload length in byte (0 .. 64) */
            __u8    flags;   /* additional flags for CAN FD */
            __u8    __res0;  /* reserved / padding */
            __u8    __res1;  /* reserved / padding */
            __u8    data[64] __attribute__((aligned(8)));
        };
        """
        _fields_ = [
            ("can_id", ctypes.c_uint32),
            ("can_dlc", ctypes.c_uint8),
            ("flags", ctypes.c_uint8),
            ("_pad", ctypes.c_ubyte * 2),
            ("data", ctypes.c_uint8 * 64)
        ]

    class CANSocket(object):
        def __init__(self, fd, is_canfd):
            if fd < 0:
                raise DriverError('Invalid socket fd')
            self.fd = fd
            self.is_canfd = is_canfd

        def recv(self, bufsize):
            buf = ctypes.create_string_buffer(bufsize)
            nbytes = libc.read(self.fd, ctypes.byref(buf), bufsize)
            return buf[0:nbytes]

        def send(self, data):
            if self.is_canfd:
                frame = can_frame_fd()
            else:
                frame = can_frame()
            ctypes.memmove(ctypes.byref(frame), data, ctypes.sizeof(frame))
            return libc.write(self.fd, ctypes.byref(frame), ctypes.sizeof(frame))

        def fileno(self):
            return self.fd

        def close(self):
            libc.close(self.fd)

    def get_socket(ifname, is_canfd):
        socket_fd = libc.socket(AF_CAN, socket.SOCK_RAW, CAN_RAW)
        if socket_fd < 0:
            raise DriverError('Could not open socket')

        libc.fcntl(socket_fd, fcntl.F_SETFL, os.O_NONBLOCK)

        ifidx = libc.if_nametoindex(ifname)
        if ctypes.get_errno() != 0:
            raise DriverError('Could not determine iface index [errno %s]' % ctypes.get_errno())

        addr = sockaddr_can(AF_CAN, ifidx)
        error = libc.bind(socket_fd, ctypes.byref(addr), ctypes.sizeof(addr))
        if error != 0:
            raise DriverError('Could not bind socket [errno %s]' % ctypes.get_errno())

        return CANSocket(socket_fd, is_canfd)


# from linux/can.h
CAN_EFF_FLAG = 0x80000000
CAN_EFF_MASK = 0x1FFFFFFF

SO_TIMESTAMP = 29

# from linux/sockios.h
SIOCGIFMTU = 0x8921
SIOCSIFMTU = 0x8922

def get_ifname_mtu(ifname, sock):
    '''
    Gets the Maximum Transmission Unit (MTU) using ioctl
    Note: this should be set to '72' for CAN-FD
    :raises: Exception if ioctl call fails
    :returns: Int MTU value
    '''

    ifr = ifname + '\x00'*(32-len(ifname))
    try:
        ifs = ioctl(sock, SIOCGIFMTU, ifr)
        mtu = struct.unpack('<H',ifs[16:18])[0]
    except Exception as e:
        logger.exception("Could not call ioctl on socket")
        raise

    return mtu

class SocketCAN(AbstractDriver):
    FRAME_FORMAT = '=IB3x8s'
    FRAME_FORMAT_FD = '=I2B2x64s'
    FRAME_FORMAT_FD_20 = '=I2B2x8s'
    FRAME_SIZE = 16
    FRAME_SIZE_FD = 72
    TIMEVAL_FORMAT = '@LL'
    TX_QUEUE_SIZE = 1000

    def __init__(self, interface, **_extras):
        super(SocketCAN, self).__init__()

        try:
            self._canfd = _extras["fd"]
            self._frame = CANFrameFd
        except KeyError:
            self._canfd = False
            self._frame = CANFrame

        self.socket = get_socket(interface, self._canfd)

        self._poll_rx = select.poll()
        self._poll_rx.register(self.socket.fileno(), select.POLLIN | select.POLLPRI | select.POLLERR)

        self._writer_thread_should_stop = False
        self._write_queue = queue.Queue(self.TX_QUEUE_SIZE)
        self._write_feedback_queue = queue.Queue()

        self._writer_thread = threading.Thread(target=self._writer_thread_loop, name='socketcan_writer')
        self._writer_thread.daemon = True
        self._writer_thread.start()

        # Timestamping
        if NATIVE_SOCKETCAN:
            self.socket.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMP, 1)

        if self._canfd:
            ## enabling CAN-FD for this driver
            try:
                iface_mtu = get_ifname_mtu(interface, self.socket)

                if iface_mtu < 72:
                    logger.error(("Selected interface ({iface}) MTU is set to less\n" + \
                                 "than 72 (MTU: {mtu}), but CAN-FD socket has been requested. Please change the MTU setting.").format(
                                 iface=interface, 
                                 mtu=iface_mtu)
                        )
            except Exception as e:
                logger.error("Unable to obtain MTU value.")

            self.socket.setsockopt(SOL_CAN_RAW, CAN_RAW_FD_FRAMES, 1)
            self._max_datalen = CAN_FD_DATAFIELD_LEN
            self._frame_size = self.FRAME_SIZE_FD
        else:
            self._max_datalen = CAN_2_DATAFIELD_LEN
            self._frame_size = self.FRAME_SIZE

        ppm = lambda x: x / 1e6
        milliseconds = lambda x: x * 1e-3

        # We're using this thing to estimate the difference between monotonic and real clocks
        # See http://stackoverflow.com/questions/35426864 (at the time of writing the question was unanswered)
        self._mono_to_real_estimator = TimestampEstimator(max_rate_error=ppm(100),
                                                          fixed_delay=milliseconds(0.001),
                                                          max_phase_error_to_resync=milliseconds(50))

    def _convert_real_to_monotonic(self, value):
        mono = time.monotonic()  # est_real is the best guess about real timestamp here
        real = time.time()
        est_real = self._mono_to_real_estimator.update(mono, real)
        mono_to_real_offset = est_real - mono
        return value - mono_to_real_offset

    def _writer_thread_loop(self):
        while not self._writer_thread_should_stop:
            try:
                frame = self._write_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                while not self._writer_thread_should_stop:
                    try:
                        message_id = frame.id | (CAN_EFF_FLAG if frame.extended else 0)
                        message_pad = bytes(frame.data) + b'\x00' * (self._max_datalen - len(frame.data))
                        if not self._canfd:
                            ## CAN 2.0
                            raw_message = struct.pack(self.FRAME_FORMAT, message_id, len(frame.data), message_pad)
                        else:
                            ## CAN FD
                            raw_message = struct.pack(self.FRAME_FORMAT_FD, message_id, len(frame.data), CANFD_FLAG_BRS, message_pad)

                        self.socket.send(raw_message)

                        frame.ts_monotonic = time.monotonic()
                        frame.ts_real = time.time()
                        self._write_feedback_queue.put(frame)
                    except OSError as ex:
                        if ex.errno == errno.ENOBUFS:
                            time.sleep(0.0002)
                        else:
                            raise
                    else:
                        break
            except Exception as ex:
                self._write_feedback_queue.put(ex)

    def _check_write_feedback(self):
        try:
            item = self._write_feedback_queue.get_nowait()
        except queue.Empty:
            return

        if isinstance(item, Exception):
            raise item

        if isinstance(item, CANFrame):
            self._tx_hook(item)
        else:
            raise DriverError('Unexpected item in write feedback queue: %r' % item)

    def close(self):
        self._writer_thread_should_stop = True
        self._writer_thread.join()
        self.socket.close()

    def receive(self, timeout=None):
        self._check_write_feedback()

        timeout = -1 if timeout is None else (timeout * 1000)

        if self._poll_rx.poll(timeout):
            ts_real = None
            ts_mono = None

            if NATIVE_SOCKETCAN:
                # Reading the frame together with timestamps in the ancillary data structures
                ancillary_len = 64   # Arbitrary value, must be large enough to accommodate all ancillary data
                packet_raw, ancdata, _flags, _addr = self.socket.recvmsg(self._frame_size,
                                                                         socket.CMSG_SPACE(ancillary_len))

                # Parsing the timestamps
                for cmsg_level, cmsg_type, cmsg_data in ancdata:
                        if cmsg_level == socket.SOL_SOCKET and cmsg_type == SO_TIMESTAMP:
                            sec, usec = struct.unpack(self.TIMEVAL_FORMAT, cmsg_data)
                            ts_real = sec + usec * 1e-6
            else:
                packet_raw = self.socket.recv(self._frame_size)

            # Parsing the frame
            if not self._canfd:
                can_id, can_dlc, can_data = struct.unpack(self.FRAME_FORMAT, packet_raw)
            else:
                # If FD mode was enabled on the socket, all frames come in as struct canfd_frame
                # the size of the MTU is checked to identify an FD frame or a non-FD frame
                if len(packet_raw) == self.FRAME_SIZE:
                    can_id, can_dlc, can_flags, can_data = struct.unpack(self.FRAME_FORMAT_FD_20, packet_raw)
                else:
                    # for socketcan "dlc" is actually the data-payload length.
                    can_id, can_dlc, can_flags, can_data = struct.unpack(self.FRAME_FORMAT_FD, packet_raw)

            # TODO: receive timestamps directly from hardware
            # TODO: ...or at least obtain timestamps from the socket layer in local monotonic domain
            if ts_real and not ts_mono:
                ts_mono = self._convert_real_to_monotonic(ts_real)

            frame = self._frame(can_id & CAN_EFF_MASK, can_data[0:can_dlc], bool(can_id & CAN_EFF_FLAG),
                             ts_monotonic=ts_mono, ts_real=ts_real)

            self._rx_hook(frame)
            return frame

    def send(self, message_id, message, extended=False):
        self._check_write_feedback()
        try:
            self._write_queue.put_nowait(self._frame(message_id, message, extended))
        except queue.Full:
            raise TxQueueFullError()
