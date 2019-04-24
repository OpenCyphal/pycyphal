#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import numpy
import typing
import struct

# We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
_Byte = numpy.uint8


class Deserializer:
    def __init__(self, source_bytes: numpy.ndarray):
        if issubclass(Deserializer, type(self)):
            raise TypeError('Deserializer cannot be instantiated directly; use the new() factory instead')

        if not isinstance(source_bytes, numpy.ndarray) or source_bytes.dtype != _Byte:
            raise ValueError(f'Unsupported buffer: {type(source_bytes)}')

        self._buf = source_bytes
        self._bit_offset = 0

    @staticmethod
    def new(source_bytes: numpy.ndarray) -> 'Deserializer':
        return {
            'little': _LittleEndianDeserializer,
            'big':       _BigEndianDeserializer,
        }[sys.byteorder](source_bytes)


class _LittleEndianDeserializer(Deserializer):
    pass


class _BigEndianDeserializer(Deserializer):
    pass
