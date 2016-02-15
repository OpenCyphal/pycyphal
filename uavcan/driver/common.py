#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import binascii
from .. import UAVCANException


class DriverError(UAVCANException):
    pass


class RxFrame:
    def __init__(self, can_id, data, extended, ts_monotonic=None, ts_real=None):
        self.id = can_id
        self.data = data
        self.extended = extended
        self.ts_monotonic = ts_monotonic or time.monotonic()
        self.ts_real = ts_real or time.monotonic()

    def __str__(self):
        return '%0*x %s' % (8 if self.extended else 3, self.id, binascii.hexlify(self.data).decode())

    __repr__ = __str__
