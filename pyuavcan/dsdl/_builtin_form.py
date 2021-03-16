# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing

import numpy
import pydsdl

from ._composite_object import CompositeObject, get_model, get_attribute, set_attribute, get_class
from ._composite_object import CompositeObjectTypeVar


def to_builtin(obj: CompositeObject) -> typing.Dict[str, typing.Any]:
    """
    Accepts a DSDL object (an instance of a Python class auto-generated from a DSDL definition),
    returns its value represented using only native built-in types: dict, list, bool, int, float, str.
    Ordering of dict elements is guaranteed to match the field ordering of the source definition.
    Keys of dicts representing DSDL objects use the original unstropped names from the source DSDL definition;
    e.g., ``if``, not ``if_``.

    This is intended for use with JSON, YAML, and other serialization formats.

    ..  doctest::
        :hide:

        >>> import tests
        >>> _ = tests.dsdl.compile()

    >>> import json
    >>> import uavcan.primitive.array
    >>> json.dumps(to_builtin(uavcan.primitive.array.Integer32_1_0([-123, 456, 0])))
    '{"value": [-123, 456, 0]}'
    >>> import uavcan.register
    >>> request = uavcan.register.Access_1_0.Request(
    ...     uavcan.register.Name_1_0('my.register'),
    ...     uavcan.register.Value_1_0(integer16=uavcan.primitive.array.Integer16_1_0([1, 2, +42, -10_000]))
    ... )
    >>> to_builtin(request)  # doctest: +NORMALIZE_WHITESPACE
    {'name':  {'name': 'my.register'},
     'value': {'integer16': {'value': [1, 2, 42, -10000]}}}
    """
    model = get_model(obj)
    _raise_if_service_type(model)
    out = _to_builtin_impl(obj, model)
    assert isinstance(out, dict)
    return out


def _to_builtin_impl(
    obj: typing.Union[CompositeObject, numpy.ndarray, str, bool, int, float], model: pydsdl.SerializableType
) -> typing.Union[typing.Dict[str, typing.Any], typing.List[typing.Any], str, bool, int, float]:
    if isinstance(model, pydsdl.CompositeType):
        assert isinstance(obj, CompositeObject)
        return {
            f.name: _to_builtin_impl(get_attribute(obj, f.name), f.data_type)
            for f in model.fields_except_padding
            if get_attribute(obj, f.name) is not None  # The check is to hide inactive union variants.
        }

    if isinstance(model, pydsdl.ArrayType):
        assert isinstance(obj, numpy.ndarray)
        if model.string_like:  # TODO: drop this special case when strings are natively supported in DSDL.
            try:
                return bytes(e for e in obj).decode()
            except UnicodeError:
                return list(map(int, obj))
        return [_to_builtin_impl(e, model.element_type) for e in obj]

    if isinstance(model, pydsdl.PrimitiveType):
        # The explicit conversions are needed to get rid of NumPy scalar types.
        if isinstance(model, pydsdl.IntegerType):
            return int(obj)  # type: ignore
        if isinstance(model, pydsdl.FloatType):
            return float(obj)  # type: ignore
        if isinstance(model, pydsdl.BooleanType):
            return bool(obj)
        assert isinstance(obj, str)
        return obj

    assert False, "Unexpected inputs"


def update_from_builtin(destination: CompositeObjectTypeVar, source: typing.Any) -> CompositeObjectTypeVar:
    """
    Updates the provided DSDL object (an instance of a Python class auto-generated from a DSDL definition)
    with the values from a native representation, where DSDL objects are represented as dicts, arrays
    are lists, and primitives are represented as int/float/bool. This is the reverse of :func:`to_builtin`.
    Values that are not specified in the source are not updated (left at their original values),
    so an empty source will leave the input object unchanged.

    Source field names shall match the original unstropped names provided in the DSDL definition;
    e.g., `if`, not `if_`. If there is more than one variant specified for a union type, the last
    specified variant takes precedence.

    :param destination: The object to update. The update will be done in-place. If you don't want the source
        object modified, clone it beforehand.

    :param source: The :class:`dict` instance containing the values to update the destination object with.

    :return: A reference to destination (not a copy).

    :raises: :class:`ValueError` if the provided source values cannot be applied to the destination object,
        or if the source contains fields that are not present in the destination object.
        :class:`TypeError` if an entity of the source cannot be converted into the type expected by the destination.

    >>> import tests; tests.dsdl.compile()  # DSDL package generation not shown in this example.
    [...]
    >>> import json
    >>> import uavcan.primitive.array
    >>> import uavcan.register
    >>> request = uavcan.register.Access_1_0.Request(
    ...     uavcan.register.Name_1_0('my.register'),
    ...     uavcan.register.Value_1_0(string=uavcan.primitive.String_1_0('Hello world!'))
    ... )
    >>> request
    uavcan.register.Access.Request...name='my.register'...value='Hello world!'...
    >>> update_from_builtin(request, {  # Switch the Value union from string to int16; keep the name unchanged.
    ...     'value': {
    ...         'integer16': {
    ...             'value': [1, 2, 42, -10000]
    ...         }
    ...     }
    ... })  # doctest: +NORMALIZE_WHITESPACE
    uavcan.register.Access.Request...name='my.register'...value=[ 1, 2, 42,-10000]...
    """
    # UX improvement; see https://github.com/UAVCAN/pyuavcan/issues/116
    if not isinstance(source, dict):
        raise TypeError(
            f"Invalid format: cannot update an instance of type {type(destination).__name__!r} "
            f"from value of type {type(source).__name__!r}"
        )

    source = dict(source)  # Create copy to prevent mutation of the original

    if not isinstance(destination, CompositeObject):  # pragma: no cover
        raise TypeError(f"Bad destination: expected a CompositeObject, got {type(destination).__name__}")

    model = get_model(destination)
    _raise_if_service_type(model)

    for f in model.fields_except_padding:
        field_type = f.data_type
        try:
            value = source.pop(f.name)
        except LookupError:
            continue  # No value specified, keep original value

        if isinstance(field_type, pydsdl.CompositeType):
            field_obj = get_attribute(destination, f.name)
            if field_obj is None:  # Oh, this is a union
                field_obj = get_class(field_type)()  # The variant was not selected, construct a default
                set_attribute(destination, f.name, field_obj)  # Switch the union to the new variant
            update_from_builtin(field_obj, value)

        elif isinstance(field_type, pydsdl.ArrayType):
            element_type = field_type.element_type
            if isinstance(element_type, pydsdl.PrimitiveType):
                set_attribute(destination, f.name, value)
            elif isinstance(element_type, pydsdl.CompositeType):
                dtype = get_class(element_type)
                set_attribute(destination, f.name, [update_from_builtin(dtype(), s) for s in value])
            else:
                assert False, f"Unexpected array element type: {element_type!r}"

        elif isinstance(field_type, pydsdl.PrimitiveType):
            set_attribute(destination, f.name, value)

        else:
            assert False, f"Unexpected field type: {field_type!r}"

    if source:
        raise ValueError(f"No such fields in {model}: {list(source.keys())}")

    return destination


def _raise_if_service_type(model: pydsdl.SerializableType) -> None:
    if isinstance(model, pydsdl.ServiceType):  # pragma: no cover
        raise TypeError(
            f"Built-in form is not defined for service types. "
            f"Did you mean to use Request or Response? Input type: {model}"
        )
