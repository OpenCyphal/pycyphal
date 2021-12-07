# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import numpy

Byte = numpy.uint8
"""
We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
"""

StdPrimitive = typing.TypeVar(
    "StdPrimitive",
    numpy.float64,
    numpy.float32,
    numpy.float16,
    numpy.uint8,
    numpy.uint16,
    numpy.uint32,
    numpy.uint64,
    numpy.int8,
    numpy.int16,
    numpy.int32,
    numpy.int64,
)
