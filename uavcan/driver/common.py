#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import sys
from logging import getLogger
from .. import UAVCANException


logger = getLogger(__name__)


class DriverError(UAVCANException):
    pass


class TxQueueFullError(DriverError):
    pass


class CANFrame:
    MAX_DATA_LENGTH = 8

    def __init__(self, can_id, data, extended, ts_monotonic=None, ts_real=None):
        self.id = can_id
        self.data = data
        self.extended = extended
        self.ts_monotonic = ts_monotonic or time.monotonic()
        self.ts_real = ts_real or time.time()

    def __str__(self):
        if sys.version_info[0] > 2:
            b2int = lambda x: x
        else:
            b2int = ord

        id_str = ('%0*x' % (8 if self.extended else 3, self.id)).rjust(8)
        hex_data = ' '.join(['%02x' % b2int(x) for x in self.data]).ljust(3 * self.MAX_DATA_LENGTH)
        ascii_data = ''.join([(chr(x) if 32 <= x <= 126 else '.') for x in self.data])

        return "%12.6f %12.6f  %s  %s  '%s'" % \
               (self.ts_monotonic, self.ts_real, id_str, hex_data, ascii_data)

    __repr__ = __str__


class AbstractDriver(object):
    FRAME_DIRECTION_INCOMING = 'rx'
    FRAME_DIRECTION_OUTGOING = 'tx'

    class HookRemover:
        def __init__(self, remover):
            self.remove = remover

    def __init__(self):
        self._io_hooks = []

    def add_io_hook(self, hook):
        """
        Args:
            hook:   This hook will be invoked for every incoming and outgoing CAN frame.
                    Hook arguments: (direction, frame)
                    See FRAME_DIRECTION_*, CANFrame.
        """
        def proxy(*args):
            hook(*args)

        self._io_hooks.append(proxy)

        return self.HookRemover(lambda: self._io_hooks.remove(proxy))

    def _call_io_hooks(self, direction, frame):
        for h in self._io_hooks:
            try:
                h(direction, frame)
            except Exception as ex:
                logger.error('Uncaught exception from CAN IO hook: %r', ex, exc_info=True)

    def _tx_hook(self, frame):
        self._call_io_hooks(self.FRAME_DIRECTION_OUTGOING, frame)

    def _rx_hook(self, frame):
        self._call_io_hooks(self.FRAME_DIRECTION_INCOMING, frame)
