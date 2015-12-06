#
# Copyright (C) 2014-2015  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import os
import sys
import time
import fcntl
import socket
import struct
import binascii
import select
from logging import getLogger

import uavcan

logger = getLogger(__name__)

__all__ = ['make_driver']


# If PySerial isn't available, we can't support SLCAN
try:
    import serial
except ImportError:
    serial = None
    logger.info("uavcan.driver cannot import PySerial; SLCAN will not be available.")


class DriverError(uavcan.UAVCANException):
    pass


# Python 3.3+'s socket module has support for SocketCAN when running on Linux. Use that if possible.
# noinspection PyBroadException
try:
    socket.CAN_RAW

    def get_socket(ifname):
        s = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        s.bind((ifname, ))
        return s

except Exception:
    import ctypes  # @UnusedImport
    import ctypes.util
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

    # from linux/can.h
    CAN_RAW = 1

    # from linux/socket.h
    AF_CAN = 29
    SO_TIMESTAMP = 29

    from socket import SOL_SOCKET

    SOL_CAN_BASE = 100
    SOL_CAN_RAW = SOL_CAN_BASE + CAN_RAW
    CAN_RAW_FILTER = 1                      # set 0 .. n can_filter(s)
    CAN_RAW_ERR_FILTER = 2                  # set filter for error frames
    CAN_RAW_LOOPBACK = 3                    # local loopback (default:on)
    CAN_RAW_RECV_OWN_MSGS = 4               # receive my own msgs (default:off)
    CAN_RAW_FD_FRAMES = 5                   # allow CAN FD frames (default:off)

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

    class CANSocket(object):
        def __init__(self, fd):
            if fd < 0:
                raise DriverError('Invalid socket fd')
            self.fd = fd

        def recv(self, bufsize, flags=None):
            frame = can_frame()
            nbytes = libc.read(self.fd, ctypes.byref(frame),
                               sys.getsizeof(frame))
            return ctypes.string_at(ctypes.byref(frame),
                                    ctypes.sizeof(frame))[0:nbytes]

        def send(self, data, flags=None):
            frame = can_frame()
            ctypes.memmove(ctypes.byref(frame), data,
                           ctypes.sizeof(frame))
            return libc.write(self.fd, ctypes.byref(frame),
                              ctypes.sizeof(frame))

        def fileno(self):
            return self.fd

        def close(self):
            libc.close(self.fd)

    def get_socket(ifname):
        on = ctypes.c_int(1)

        socket_fd = libc.socket(AF_CAN, socket.SOCK_RAW, CAN_RAW)
        if socket_fd < 0:
            raise DriverError('Could not open socket')

        libc.fcntl(socket_fd, fcntl.F_SETFL, os.O_NONBLOCK)

        error = libc.setsockopt(socket_fd, SOL_SOCKET, SO_TIMESTAMP, ctypes.byref(on), ctypes.sizeof(on))
        if error != 0:
            raise DriverError('Could not enable timestamping on socket [errno %s]' % ctypes.get_errno())

        ifidx = libc.if_nametoindex(ifname)
        if ctypes.get_errno() != 0:
            raise DriverError('Could not determine iface index [errno %s]' % ctypes.get_errno())

        addr = sockaddr_can(AF_CAN, ifidx)
        error = libc.bind(socket_fd, ctypes.byref(addr), ctypes.sizeof(addr))
        if error != 0:
            raise DriverError('Could not bind socket [errno %s]' % ctypes.get_errno())

        return CANSocket(socket_fd)


# from linux/can.h
CAN_EFF_FLAG = 0x80000000
CAN_EFF_MASK = 0x1FFFFFFF


class RxFrame:
    def __init__(self, can_id, data, extended, ts_monotonic=None, ts_real=None):
        self.id = can_id
        self.data = data
        self.extended = extended
        self.ts_monotonic = ts_monotonic or time.monotonic()
        self.ts_real = ts_real or time.monotonic()


class SocketCAN(object):
    FRAME_FORMAT = '=IB3x8s'

    def __init__(self, interface, **_extras):
        self.socket = get_socket(interface)
        self.poll = select.poll()
        self.poll.register(self.socket.fileno())

    def close(self, callback=None):
        self.socket.close()

    def receive(self, timeout=None):
        timeout = -1 if timeout is None else (timeout * 1000)

        self.poll.modify(self.socket.fileno(), select.POLLIN | select.POLLPRI)
        if self.poll.poll(timeout):
            packet = self.socket.recv(16)
            assert len(packet) == 16
            can_id, can_dlc, can_data = struct.unpack(self.FRAME_FORMAT, packet)
            # TODO: Socket-level timestamping
            return RxFrame(can_id & CAN_EFF_MASK, can_data[0:can_dlc], bool(can_id & CAN_EFF_FLAG))

    def send(self, message_id, message, extended=False):
        if extended:
            message_id |= CAN_EFF_FLAG

        message_pad = bytes(message) + b'\x00' * (8 - len(message))
        self.socket.send(struct.pack(self.FRAME_FORMAT, message_id, len(message), message_pad))


class SLCAN(object):
    DEFAULT_BAUDRATE = 3000000
    ACK_TIMEOUT = 0.5
    ACK = b'\r'
    NACK = b'\x07'

    def __init__(self, device, bitrate, baudrate=None, **_extras):
        if not serial:
            raise RuntimeError("PySerial not imported; SLCAN is not available. Please install PySerial.")

        baudrate = baudrate or self.DEFAULT_BAUDRATE

        self.conn = serial.Serial(device, baudrate)
        self._read_buffer = bytes()

        speed_code = {
            1000000: 8,
            500000: 6,
            250000: 5,
            125000: 4,
            100000: 3
        }[bitrate]

        # Discarding all input
        self.conn.flushInput()

        # Setting speed code
        self.conn.write('S{0:d}\r'.format(speed_code).encode())
        self.conn.flush()
        self._wait_for_ack()

        # Opening the channel
        self.conn.write(b'O\r')
        self.conn.flush()
        self._wait_for_ack()

        # Doing something I don't know what. Ben, what are we doing here?
        time.sleep(0.1)

        self.conn.flushInput()

    def close(self):
        self.conn.write(b'C\r')
        self.conn.flush()
        time.sleep(0.1)     # TODO: Ben, why?
        self.conn.close()

    def _wait_for_ack(self):
        self.conn.timeout = self.ACK_TIMEOUT
        while True:
            b = self.conn.read(1)
            if not b:
                raise DriverError('SLCAN ACK timeout')
            if b == self.NACK:
                raise DriverError('SLCAN NACK in response')
            if b == self.ACK:
                break

    def _parse(self, message):
        try:
            id_len = 8 if message[0] == b'T' else 3

            # Parse the message into a (message ID, data) tuple.
            packet_id = int(message[1:1 + id_len].decode(), 16)
            packet_len = int(message.decode()[1 + id_len])      # Why .decode()? Try int(b'123'[0]) in Py3 and Py2.
            packet_data = binascii.a2b_hex(message[2 + id_len:2 + id_len + packet_len * 2])

            # ID, data, extended
            # TODO: SLCAN timestamping support
            return RxFrame(packet_id, packet_data, (id_len == 8))
        except Exception:
            logger.error('Could not parse SLCAN frame [%r]', message, exc_info=True)
            return

    def receive(self, timeout=None):
        self.conn.timeout = timeout
        deadline = (time.monotonic() + timeout) if timeout is not None else None

        while True:
            if self._read_buffer and self._read_buffer.endswith(b'\r'):
                break

            if deadline is not None:
                to = deadline - time.monotonic()
                if to <= 0:
                    return
                self.conn.timeout = to

            self._read_buffer += self.conn.read(1)

        assert self._read_buffer.endswith(b'\r')

        msg = self._read_buffer
        self._read_buffer = bytes()
        return self._parse(msg)

    def send(self, message_id, message, extended=False):
        start = ('T{0:08X}' if extended else 't{0:03X}').format(message_id).encode()
        line = '{0:s}{1:1d}{2:s}\r'.format(start, len(message), binascii.b2a_hex(message)).encode()
        self.conn.write(line)
        self.conn.flush()


def make_driver(device_name, **kwargs):
    """Creates an instance of CAN driver.
    The right driver class will be selected automatically based on the device_name.
    :param device_name: This parameter is used to select driver class. E.g. "/dev/ttyACM0", "COM9", "can0".
    :param args: Passed directly to the constructor.
    :param kwargs: Passed directly to the constructor.
    """
    windows_com_port = device_name.replace('\\', '').replace('.', '').lower().startswith('com')
    unix_tty = device_name.startswith('/dev/')

    if windows_com_port or unix_tty:
        return SLCAN(device_name, **kwargs)
    else:
        return SocketCAN(device_name, **kwargs)


if __name__ == "__main__":
    if sys.version_info[0] < 3:
        import monotonic  # @UnresolvedImport
        time.monotonic = monotonic.monotonic

    if len(sys.argv) < 2:
        print("Usage: driver.py <can-device> [param=value ...]")
        sys.exit(1)

    kw = {}
    for a in sys.argv[2:]:
        k, v = a.split('=')
        kw[k] = int(v)

    can = make_driver(sys.argv[1], **kw)
    while True:
        print(can.receive(2))
