#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging

import numpy
import pytest

import pyuavcan.dsdl


_logger = logging.getLogger(__name__)


# noinspection PyUnusedLocal
def _unittest_slow_manual_del(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    import test_dsdl_namespace.if_

    # Implicit zero extension
    ize = pyuavcan.dsdl.deserialize(test_dsdl_namespace.if_.del_1_0, [memoryview(b'')])
    assert ize is not None
    assert repr(ize) == repr(test_dsdl_namespace.if_.del_1_0())

    obj = pyuavcan.dsdl.deserialize(
        test_dsdl_namespace.if_.del_1_0,
        _compile_serialized_representation(
            # Second element C.1.0
            '1'  # x = 1
            '0'  # first field selected uint1 x
            # First element C.1.0
            '1'  # y = 1
            '1'  # second field selected uint1 y
            # B union, second field C.1.0[<=2] y
            '10'  # length 2 elements
            '1'
            # void1
            '0'
            # ^^^ THE FIRST FIELD STARTS HERE AND THE REST GOES IN THE REVERSE ORDER UPWARDS ^^^

            # Padding to byte.
            '0'
            # empty B.1.0[<=2] y
            '00'
            # Second element C.1.0
            '1'  # y = 1
            '1'  # second field selected uint1 y
            # First element C.1.0
            '0'  # x = 0
            '0'  # first field selected uint1 x
            # B union, first field C.1.0[2] x
            '0'
            # ^^^ FIELDS OF THE SECOND BYTE OF THE SERIALIZED REPRESENTATION BEGIN HERE ^^^
        )
    )
    assert obj is not None
    assert obj.else_[0].x is None
    assert obj.else_[0].y is not None
    assert len(obj.else_[0].y) == 2
    assert obj.else_[0].y[0].x is None
    assert obj.else_[0].y[0].y == 1
    assert obj.else_[0].y[1].x == 1
    assert obj.else_[0].y[1].y is None
    assert obj.else_[1].x is not None
    assert obj.else_[1].y is None
    assert obj.else_[1].x[0].x == 0
    assert obj.else_[1].x[0].y is None
    assert obj.else_[1].x[1].x is None
    assert obj.else_[1].x[1].y == 1
    assert len(obj.raise_) == 0

    with pytest.raises(AttributeError, match='nonexistent'):
        pyuavcan.dsdl.get_attribute(obj, 'nonexistent')

    with pytest.raises(AttributeError, match='nonexistent'):
        pyuavcan.dsdl.set_attribute(obj, 'nonexistent', 123)


# noinspection PyUnusedLocal
def _unittest_slow_manual_heartbeat(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    import uavcan.node

    # Implicit zero extension
    ize = pyuavcan.dsdl.deserialize(uavcan.node.Heartbeat_1_0, [memoryview(b'')])
    assert ize is not None
    assert repr(ize) == repr(uavcan.node.Heartbeat_1_0())
    assert ize.uptime == 0
    assert ize.vendor_specific_status_code == 0

    obj = pyuavcan.dsdl.deserialize(
        uavcan.node.Heartbeat_1_0,
        _compile_serialized_representation(
            _bin(0xefbe_adde, 32),              # uptime dead beef in little-endian byte order
            '111',                              # vendor-specific, fragment
            '010',                              # mode maintenance
            '10',                               # health caution
            '11111111''11111111'                # vendor-specific, the remaining 16 bits
        )
    )
    assert obj is not None
    assert obj.uptime == 0xdeadbeef
    assert obj.health == uavcan.node.Heartbeat_1_0.HEALTH_CAUTION
    assert obj.mode == uavcan.node.Heartbeat_1_0.MODE_MAINTENANCE
    assert obj.vendor_specific_status_code == 0x7FFFF

    with pytest.raises(AttributeError, match='nonexistent'):
        pyuavcan.dsdl.get_attribute(obj, 'nonexistent')

    with pytest.raises(AttributeError, match='nonexistent'):
        pyuavcan.dsdl.set_attribute(obj, 'nonexistent', 123)


def _compile_serialized_representation(*binary_chunks: str) -> typing.Sequence[memoryview]:
    s = ''.join(binary_chunks)
    s = s.ljust(len(s) + 8 - len(s) % 8, '0')
    assert len(s) % 8 == 0
    byte_sized_chunks = [s[i:i + 8] for i in range(0, len(s), 8)]
    byte_list = list(map(lambda x: int(x, 2), byte_sized_chunks))
    out = numpy.array(byte_list, dtype=numpy.uint8)
    _logger.debug('Constructed serialized representation: %r --> %s', binary_chunks, out)
    return [out.data]


def _bin(value: int, width: int) -> str:
    out = bin(value)[2:].zfill(width)
    assert len(out) == width, f'Value is too wide: {bin(value)} is more than {width} bits wide'
    return out
