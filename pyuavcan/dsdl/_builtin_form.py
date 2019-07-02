#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import numpy
import typing
import pydsdl
from ._composite_object import CompositeObject, CompositeObjectTypeVar, get_model, get_attribute


def to_builtin(obj: CompositeObject) -> typing.Dict[str, typing.Any]:
    """
    Accepts a DSDL object (an instance of a Python class auto-generated from a DSDL definition),
    returns its value represented using only native built-in types: dict, list, bool, int, float, str.
    This is intended for use with JSON, YAML, and other serialization formats.
    """
    out = _to_builtin_impl(obj, get_model(obj))
    assert isinstance(out, dict)
    return out


def update_from_builtin(destination: CompositeObjectTypeVar,
                        source: typing.Dict[str, typing.Any]) -> CompositeObjectTypeVar:
    raise NotImplementedError


def _to_builtin_impl(obj:   typing.Union[CompositeObject, numpy.ndarray, str, bool, int, float],
                     model: pydsdl.SerializableType) \
        -> typing.Union[typing.Dict[str, typing.Any], typing.List[typing.Any], str, bool, int, float]:
    if isinstance(model, pydsdl.CompositeType):
        assert isinstance(obj, CompositeObject)
        return {
            f.name: _to_builtin_impl(get_attribute(obj, f.name), f.data_type)
            for f in model.fields_except_padding
            if get_attribute(obj, f.name) is not None  # The check is to hide inactive union variants.
        }

    elif isinstance(model, pydsdl.ArrayType):
        assert isinstance(obj, numpy.ndarray)
        if model.string_like:  # TODO: drop this special case when strings are natively supported in DSDL.
            return bytes(e for e in obj).decode('unicode_escape')
        else:
            return [_to_builtin_impl(e, model.element_type) for e in obj]

    elif isinstance(model, pydsdl.PrimitiveType):
        # The explicit conversions are needed to get rid of NumPy scalar types.
        if isinstance(model, pydsdl.IntegerType):
            return int(obj)
        elif isinstance(model, pydsdl.FloatType):
            return float(obj)
        elif isinstance(model, pydsdl.BooleanType):
            return bool(obj)
        else:
            assert isinstance(obj, str)
            return obj

    else:
        assert False, 'Unexpected inputs'
