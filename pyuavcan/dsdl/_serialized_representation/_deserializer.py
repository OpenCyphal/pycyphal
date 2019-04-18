#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import numpy
import typing
import struct


class DeserializerBase:
    def __init__(self, source_bytes: numpy.ndarray):
        if not isinstance(source_bytes, numpy.ndarray) or source_bytes.dtype != numpy.ubyte:
            raise ValueError(f'Unsupported buffer: {type(source_bytes)}')

        self._buf = source_bytes
        self._bit_offset = 0


class LittleEndianDeserializer(DeserializerBase):
    pass


class BigEndianDeserializer(DeserializerBase):
    pass
