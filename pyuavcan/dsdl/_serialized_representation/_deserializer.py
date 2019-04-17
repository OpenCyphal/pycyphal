#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import numpy
import typing
import struct


class DeserializerBase:
    def __init__(self, serialized_representation: numpy.ndarray):
        if not isinstance(serialized_representation, numpy.ndarray) or serialized_representation.dtype != numpy.ubyte:
            raise ValueError(f'Unsupported buffer: {type(serialized_representation)}')

        self._buf = serialized_representation
        self._bit_offset = 0
