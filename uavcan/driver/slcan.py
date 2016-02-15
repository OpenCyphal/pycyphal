#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import sys
import time
import binascii
import select
import threading
from logging import getLogger
from .common import DriverError, RxFrame

try:
    import queue
except ImportError:
    # noinspection PyPep8Naming,PyUnresolvedReferences
    import Queue as queue

logger = getLogger(__name__)

# If PySerial isn't available, we can't support SLCAN
try:
    import serial
except ImportError:
    serial = None
    logger.info("Cannot import PySerial; SLCAN will not be available.")


class SLCAN(object):
    DEFAULT_BAUDRATE = 3000000
    ACK_TIMEOUT = 0.5
    ACK = b'\r'
    NACK = b'\x07'

    def __init__(self, device, bitrate, baudrate=None, rx_buffer_size=None, **_extras):
        if not serial:
            raise RuntimeError("PySerial not imported; SLCAN is not available. Please install PySerial.")

        baudrate = baudrate or self.DEFAULT_BAUDRATE

        self.conn = serial.Serial(device, baudrate)
        self._received_messages = queue.Queue(maxsize=rx_buffer_size or 10000)

        speed_code = {
            1000000: 8,
            8000000: 7,
            500000: 6,
            250000: 5,
            125000: 4,
            100000: 3,
            50000: 2,
            20000: 1,
            10000: 0
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

        self._thread_should_exit = False
        self._thread = threading.Thread(target=self._rx_thread, name='slcan_rx')
        self._thread.daemon = True
        self._thread.start()

    def _rx_thread(self):
        logger.debug('SLCAN RX thread started (%r)', self._thread)
        py2_compat = sys.version_info[0] < 3
        buf = bytes()
        while not self._thread_should_exit:
            try:
                # Read as much data as possible in order to avoid RX overrun
                select.select([self.conn.fileno()], [], [], 0.1)
                self.conn.timeout = 0
                buf += self.conn.read(1024 * 1024)  # Arbitrary large number

                # The parsing logic below is heavily optimized for speed
                pos = 0
                buf_len = len(buf)
                while True:
                    # Looking for start of the next message, break if not found
                    while pos < buf_len and buf[pos] not in b'Tt':
                        pos += 1
                    if pos >= buf_len:
                        break

                    # Now, pos points to the beginning of the next message - parse it
                    try:
                        id_len = 8 if buf[pos] == b'T'[0] else 3

                        available_length = buf_len - pos
                        if available_length < id_len + 2:  # Shortest message is 't<ID>0'
                            break

                        # Parse the header
                        packet_id = int(buf[pos + 1:pos + 1 + id_len].decode(), 16)
                        if py2_compat:
                            packet_len = int(buf[pos + 1 + id_len])             # This version is horribly slow
                        else:
                            packet_len = buf[pos + 1 + id_len] - 48             # Py3 version is faster

                        if packet_len > 8:
                            raise DriverError('Invalid packet length')

                        # <type> <id> <dlc> <data>
                        # 1      3|8  1     packet_len * 2
                        total_length = 2 + id_len + packet_len * 2
                        if available_length < total_length:
                            break

                        # TODO: parse timestamps as well
                        packet_data = binascii.a2b_hex(buf[pos + 2 + id_len:pos + 2 + id_len + packet_len * 2])
                        pos += total_length
                    except Exception:   # Message is malformed
                        logger.warning('Could not parse SLCAN stream [%r]', buf[pos:], exc_info=True)
                        pos += 1        # Continue from the next position
                    else:
                        self._received_messages.put_nowait(RxFrame(packet_id, packet_data, (id_len == 8)))

                # All data that could be parsed is already parsed - discard everything up to the current pos
                buf = buf[pos:]
            except Exception:
                buf = bytes()
                logger.error('SLCAN RX thread error (%r), buffer discarded', self._thread, exc_info=True)

        logger.debug('SLCAN RX thread is exiting (%r)', self._thread)

    def close(self):
        self._thread_should_exit = True
        self._thread.join()
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

    def receive(self, timeout=None):
        try:
            return self._received_messages.get(block=True, timeout=timeout)
        except queue.Empty:
            return

    def send(self, message_id, message, extended=False):
        start = ('T{0:08X}' if extended else 't{0:03X}').format(message_id)
        line = '{0:s}{1:1d}{2:s}\r'.format(start, len(message), binascii.b2a_hex(message).decode()).encode()
        self.conn.write(line)
        self.conn.flush()
