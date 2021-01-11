# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>


def repr_attributes(obj: object, *anonymous_elements: object, **named_elements: object) -> str:
    """
    A simple helper function that constructs a :func:`repr` form of an object. Used widely across the library.
    String representations will be obtained by invoking :func:`str` on each value.

    >>> class Aa: pass
    >>> assert repr_attributes(Aa()) == 'Aa()'
    >>> assert repr_attributes(Aa(), 123) == 'Aa(123)'
    >>> assert repr_attributes(Aa(), foo=123) == 'Aa(foo=123)'
    >>> assert repr_attributes(Aa(), 456, foo=123, bar=repr('abc')) == "Aa(456, foo=123, bar='abc')"
    """
    fld = list(map(str, anonymous_elements)) + list(f"{name}={value}" for name, value in named_elements.items())
    return f"{type(obj).__name__}(" + ", ".join(fld) + ")"


def repr_attributes_noexcept(obj: object, *anonymous_elements: object, **named_elements: object) -> str:
    """
    A robust version of :meth:`repr_attributes` that never raises exceptions.

    >>> class Aa: pass
    >>> repr_attributes_noexcept(Aa(), 456, foo=123, bar=repr('abc'))
    "Aa(456, foo=123, bar='abc')"
    >>> class Bb:
    ...     def __repr__(self) -> str:
    ...         raise Exception('Ford, you are turning into a penguin')
    >>> repr_attributes_noexcept(Aa(), foo=Bb())
    "<REPR FAILED: Exception('Ford, you are turning into a penguin')>"
    >>> class Cc(Exception):
    ...     def __str__(self) -> str:  raise Cc()  # Infinite recursion
    ...     def __repr__(self) -> str: raise Cc()  # Infinite recursion
    >>> repr_attributes_noexcept(Aa(), foo=Cc())
    '<REPR FAILED: UNKNOWN ERROR>'
    """
    try:
        return repr_attributes(obj, *anonymous_elements, **named_elements)
    except Exception as ex:
        # noinspection PyBroadException
        try:
            return f"<REPR FAILED: {ex!r}>"
        except Exception:
            return "<REPR FAILED: UNKNOWN ERROR>"
