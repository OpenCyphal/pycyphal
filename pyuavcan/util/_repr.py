#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#


def repr_object(obj: object, *anonymous_elements: object, **named_elements: object) -> str:
    """
    A simple helper function that constructs a repr() form of an object. Used widely across the library.
    >>> class Aa: pass
    >>> assert repr_object(Aa()) == 'Aa()'
    >>> assert repr_object(Aa(), 123) == 'Aa(123)'
    >>> assert repr_object(Aa(), foo=123) == 'Aa(foo=123)'
    >>> assert repr_object(Aa(), 456, foo=123, bar='abc') == "Aa(456, foo=123, bar='abc')"
    """
    fld = list(map(repr, anonymous_elements)) + list(f'{name}={value!r}' for name, value in named_elements.items())
    return f'{type(obj).__name__}(' + ', '.join(fld) + ')'
