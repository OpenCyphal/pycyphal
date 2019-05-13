#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import numpy
import typing
import pydsdl
import random
import struct
from itertools import starmap
from functools import partial

import pyuavcan.dsdl


def make_random_object(data_type: pydsdl.SerializableType) -> typing.Any:
    """
    Returns an object of the specified DSDL type populated with random data.
    """
    def fifty_fifty() -> bool:
        return random.random() >= 0.5

    if isinstance(data_type, pydsdl.BooleanType):
        return fifty_fifty()

    elif isinstance(data_type, pydsdl.IntegerType):  # noinspection PyTypeChecker
        return random.randint(int(data_type.inclusive_value_range.min),
                              int(data_type.inclusive_value_range.max))

    elif isinstance(data_type, pydsdl.FloatType):   # We want inf/nan as well, so we generate int and then reinterpret
        int_value = random.randrange(0, 2 ** data_type.bit_length)
        unpack_fmt, pack_fmt = {
            16: ('e', 'H'),
            32: ('f', 'I'),
            64: ('d', 'Q'),
        }[data_type.bit_length]
        fmt_prefix = '<'
        out, = struct.unpack(fmt_prefix + unpack_fmt, struct.pack(fmt_prefix + pack_fmt, int_value))
        return out

    elif isinstance(data_type, pydsdl.FixedLengthArrayType):
        out = [make_random_object(data_type.element_type) for _ in range(data_type.capacity)]
        et = data_type.element_type
        if isinstance(et, pydsdl.UnsignedIntegerType) and et.bit_length <= 8 and fifty_fifty():
            out = bytes(out)
        return out

    elif isinstance(data_type, pydsdl.VariableLengthArrayType):
        length = random.randint(0, data_type.capacity)
        out = [make_random_object(data_type.element_type) for _ in range(length)]
        et = data_type.element_type
        if isinstance(et, pydsdl.UnsignedIntegerType) and et.bit_length <= 8 and fifty_fifty():
            out = bytes(out)
        if data_type.string_like and fifty_fifty():
            try:
                out = bytes(out).decode()
            except ValueError:
                pass
        return out

    elif isinstance(data_type, pydsdl.StructureType):
        o = pyuavcan.dsdl.get_generated_class(data_type)()
        for f in data_type.fields_except_padding:
            v = make_random_object(f.data_type)
            pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    elif isinstance(data_type, pydsdl.UnionType):
        f = random.choice(data_type.fields)
        v = make_random_object(f.data_type)
        o = pyuavcan.dsdl.get_generated_class(data_type)()
        pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    else:   # pragma: no cover
        raise TypeError(f'Unsupported type: {type(data_type)}')


def are_close(data_type: pydsdl.SerializableType, a: typing.Any, b: typing.Any) -> bool:
    """
    If you ever decided to copy-paste this test function into a production application,
    beware that it evaluates (NaN == NaN) as True. This is what we want when testing,
    but this is not what most real systems expect.
    """
    if a is None or b is None:  # These occur, for example, in unions
        return (a is None) == (b is None)

    elif isinstance(data_type, pydsdl.CompositeType):
        if type(a) != type(b):  # pragma: no cover
            return False
        for f in pyuavcan.dsdl.get_type(a).fields_except_padding:  # pragma: no cover
            if not are_close(f.data_type,
                             pyuavcan.dsdl.get_attribute(a, f.name),
                             pyuavcan.dsdl.get_attribute(b, f.name)):
                return False
        return True                 # Empty objects of same type compare equal

    elif isinstance(data_type, pydsdl.ArrayType):
        return all(starmap(partial(are_close, data_type.element_type), zip(a, b))) \
            if len(a) == len(b) and a.dtype == b.dtype else False

    elif isinstance(data_type, pydsdl.FloatType):
        t = {
            16: numpy.float16,
            32: numpy.float32,
            64: numpy.float64,
        }[data_type.bit_length]
        return numpy.allclose(t(a), t(b), equal_nan=True)

    else:
        return numpy.allclose(a, b)
