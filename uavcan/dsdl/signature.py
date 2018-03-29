#
# Copyright (C) 2014-2015  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#         Ben Dyer <ben_dyer@mac.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals

import sys

import crcmod.predefined

#
# CRC-64-WE
# Description: http://reveng.sourceforge.net/crc-catalogue/17plus.htm#crc.cat-bits.64
# Initial value: 0xFFFFFFFFFFFFFFFF
# Poly: 0x42F0E1EBA9EA3693
# Reverse: no
# Output xor: 0xFFFFFFFFFFFFFFFF
# Check: 0x62EC59E3F1A4F00A
#

crcfun = crcmod.predefined.mkPredefinedCrcFun('crc-64-we')

class Signature:
    '''
    This class implements the UAVCAN DSDL signature hash function. Please refer to the specification for details.
    '''
    MASK64 = 0xFFFFFFFFFFFFFFFF
    POLY = 0x42F0E1EBA9EA3693

    def __init__(self, extend_from=None):
        '''
        extend_from    Initial value (optional)
        '''
        if extend_from is not None:
            self._crc = int(extend_from)
        else:
            self._crc = 0

    def add(self, data_bytes):
        '''Feed ASCII string or bytes to the signature function'''
        global crcfun

        if sys.version_info < (3, 0):
            if isinstance(data_bytes, basestring):
                data_bytes = bytes(data_bytes)
        else:
            if isinstance(data_bytes, str):
                data_bytes = bytes(data_bytes, 'utf8')

        self._crc = crcfun(data_bytes, self._crc)

    def get_value(self):
        '''Returns integer signature value'''
        return self._crc


def compute_signature(data):
    '''
    One-shot signature computation for ASCII string or bytes.
    Returns integer signture value.
    '''
    s = Signature()
    s.add(data)
    return s.get_value()


# if __name__ == '__main__':
if 1:
    s = Signature()
    s.add(b'123')
    s.add('456789')
    assert s.get_value() == 0x62EC59E3F1A4F00A
