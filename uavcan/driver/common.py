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
from .. import UAVCANException


class DriverError(UAVCANException):
    pass


class RxFrame:
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
