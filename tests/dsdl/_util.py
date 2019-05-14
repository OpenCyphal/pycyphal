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
import itertools
import functools
import pyuavcan.dsdl


def expand_service_types(models: typing.Iterable[pydsdl.CompositeType], keep_services: bool = False) \
        -> typing.Iterator[pydsdl.CompositeType]:
    """
    Iterates all types in the provided list, expanding each ServiceType into a pair of CompositeType: one for
    request, one for response.
    """
    for m in models:
        if isinstance(m, pydsdl.ServiceType):
            yield m.request_type
            yield m.response_type
            if keep_services:
                yield m
        else:
            yield m


def make_random_object(model: pydsdl.SerializableType) -> typing.Any:
    """
    Returns an object of the specified DSDL type populated with random data.
    """
    def fifty_fifty() -> bool:
        return random.random() >= 0.5

    if isinstance(model, pydsdl.BooleanType):
        return fifty_fifty()

    elif isinstance(model, pydsdl.IntegerType):  # noinspection PyTypeChecker
        return random.randint(int(model.inclusive_value_range.min),
                              int(model.inclusive_value_range.max))

    elif isinstance(model, pydsdl.FloatType):   # We want inf/nan as well, so we generate int and then reinterpret
        int_value = random.randrange(0, 2 ** model.bit_length)
        unpack_fmt, pack_fmt = {
            16: ('e', 'H'),
            32: ('f', 'I'),
            64: ('d', 'Q'),
        }[model.bit_length]
        fmt_prefix = '<'
        out, = struct.unpack(fmt_prefix + unpack_fmt, struct.pack(fmt_prefix + pack_fmt, int_value))
        return out

    elif isinstance(model, pydsdl.FixedLengthArrayType):
        out = [make_random_object(model.element_type) for _ in range(model.capacity)]
        et = model.element_type
        if isinstance(et, pydsdl.UnsignedIntegerType) and et.bit_length <= 8 and fifty_fifty():
            out = bytes(out)
        return out

    elif isinstance(model, pydsdl.VariableLengthArrayType):
        length = random.randint(0, model.capacity)
        out = [make_random_object(model.element_type) for _ in range(length)]
        et = model.element_type
        if isinstance(et, pydsdl.UnsignedIntegerType) and et.bit_length <= 8 and fifty_fifty():
            out = bytes(out)
        if model.string_like and fifty_fifty():
            try:
                out = bytes(out).decode()
            except ValueError:
                pass
        return out

    elif isinstance(model, pydsdl.StructureType):
        o = pyuavcan.dsdl.get_class(model)()
        for f in model.fields_except_padding:
            v = make_random_object(f.data_type)
            pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    elif isinstance(model, pydsdl.UnionType):
        f = random.choice(model.fields)
        v = make_random_object(f.data_type)
        o = pyuavcan.dsdl.get_class(model)()
        pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    else:   # pragma: no cover
        raise TypeError(f'Unsupported type: {type(model)}')


def are_close(model: pydsdl.SerializableType, a: typing.Any, b: typing.Any) -> bool:
    """
    If you ever decided to copy-paste this test function into a production application,
    beware that it evaluates (NaN == NaN) as True. This is what we want when testing,
    but this is not what most real systems expect.
    """
    if a is None or b is None:  # These occur, for example, in unions
        return (a is None) == (b is None)

    elif isinstance(model, pydsdl.CompositeType):
        if type(a) != type(b):  # pragma: no cover
            return False
        for f in pyuavcan.dsdl.get_model(a).fields_except_padding:  # pragma: no cover
            if not are_close(f.data_type,
                             pyuavcan.dsdl.get_attribute(a, f.name),
                             pyuavcan.dsdl.get_attribute(b, f.name)):
                return False
        return True                 # Empty objects of same type compare equal

    elif isinstance(model, pydsdl.ArrayType):
        return all(itertools.starmap(functools.partial(are_close, model.element_type), zip(a, b))) \
            if len(a) == len(b) and a.dtype == b.dtype else False

    elif isinstance(model, pydsdl.FloatType):
        t = {
            16: numpy.float16,
            32: numpy.float32,
            64: numpy.float64,
        }[model.bit_length]
        return numpy.allclose(t(a), t(b), equal_nan=True)

    else:
        return numpy.allclose(a, b)
