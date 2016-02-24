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
import copy
from logging import getLogger
from .common import DriverError, CANFrame
from.timestamp_estimator import TimestampEstimator

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
    DEFAULT_BITRATE = 1000000
    DEFAULT_BAUDRATE = 3000000
    ACK_TIMEOUT = 0.5
    ACK = b'\r'
    NACK = b'\x07'

    TIMESTAMP_OVERFLOW_PERIOD = 60          # Defined by SLCAN protocol

    DEFAULT_MAX_ADAPTER_CLOCK_RATE_ERROR_PPM = 200      # Suits virtually all adapters
    DEFAULT_FIXED_RX_DELAY = 0.0001                     # Good for USB, could be higher for UART
    DEFAULT_MAX_ESTIMATED_RX_DELAY_TO_RESYNC = 0.02     # When clock divergence exceeds this value, resync

    def __init__(self, device,
                 bitrate=None,
                 baudrate=None,
                 rx_buffer_size=None,
                 max_adapter_clock_rate_error_ppm=None,
                 fixed_rx_delay=None,
                 max_estimated_rx_delay_to_resync=None,
                 **_extras):
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
        }[bitrate if bitrate is not None else self.DEFAULT_BITRATE]

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

        # This is needed to convert timestamps from hardware clock to local clocks
        if max_adapter_clock_rate_error_ppm is None:
            max_adapter_clock_rate_error = self.DEFAULT_MAX_ADAPTER_CLOCK_RATE_ERROR_PPM / 1e6
        else:
            max_adapter_clock_rate_error = max_adapter_clock_rate_error_ppm / 1e6

        fixed_rx_delay = fixed_rx_delay if fixed_rx_delay is not None else self.DEFAULT_FIXED_RX_DELAY

        max_estimated_rx_delay_to_resync = \
            max_estimated_rx_delay_to_resync or self.DEFAULT_MAX_ESTIMATED_RX_DELAY_TO_RESYNC

        self._ts_estimator_mono = TimestampEstimator(max_rate_error=max_adapter_clock_rate_error,
                                                     source_clock_overflow_period=self.TIMESTAMP_OVERFLOW_PERIOD,
                                                     fixed_delay=fixed_rx_delay,
                                                     max_phase_error_to_resync=max_estimated_rx_delay_to_resync)

        self._ts_estimator_real = copy.deepcopy(self._ts_estimator_mono)

        # Starting the RX thread
        self._thread_should_exit = False
        self._thread = threading.Thread(target=self._rx_thread, name='slcan_rx')
        self._thread.daemon = True
        self._thread.start()

    # noinspection PyBroadException
    def _rx_thread(self):
        logger.debug('SLCAN RX thread started (%r)', self._thread)
        py2_compat = sys.version_info[0] < 3
        buf = bytes()
        while not self._thread_should_exit:
            try:
                select.select([self.conn.fileno()], [], [], 0.1)

                # Timestamping as soon as possible after unblocking
                local_ts_mono = time.monotonic()
                local_ts_real = time.time()

                # Read as much data as possible in order to avoid RX overrun
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

                        # All kinds of weird and wonderful stuff
                        # <type> <id> <dlc> <data>         [timestamp] \r
                        # 1      3|8  1     packet_len * 2 [4]         1
                        total_length = 2 + id_len + packet_len * 2 + 1
                        if available_length < total_length:
                            break
                        with_timestamp = buf[pos + total_length - 1] in b'0123456789ABCDEF'
                        if with_timestamp:
                            total_length += 3                                   # 3 not 4 because we don't need \r
                            if available_length < total_length:
                                break

                        packet_data = binascii.a2b_hex(buf[pos + 2 + id_len:pos + 2 + id_len + packet_len * 2])
                        pos += total_length

                        if with_timestamp:
                            ts_hardware = int(buf[pos - 4:pos], 16) * 1e-3
                        else:
                            ts_hardware = None
                    except Exception:   # Message is malformed
                        logger.warning('Could not parse SLCAN stream [%r]', buf[pos:], exc_info=True)
                        pos += 1        # Continue from the next position
                    else:
                        # Converting the hardware timestamp into the local clock domains
                        if ts_hardware is not None:
                            ts_mono = self._ts_estimator_mono.update(ts_hardware, local_ts_mono)
                            ts_real = self._ts_estimator_real.update(ts_hardware, local_ts_real)
                        else:
                            ts_mono = local_ts_mono
                            ts_real = local_ts_real

                        self._received_messages.put_nowait(CANFrame(packet_id, packet_data, (id_len == 8),
                                                                    ts_monotonic=ts_mono, ts_real=ts_real))

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
