#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#


def repr_attributes(obj: object, *anonymous_elements: object, **named_elements: object) -> str:
    """
    A simple helper function that constructs a :func:`repr` form of an object. Used widely across the library.

    >>> class Aa: pass
    >>> assert repr_attributes(Aa()) == 'Aa()'
    >>> assert repr_attributes(Aa(), 123) == 'Aa(123)'
    >>> assert repr_attributes(Aa(), foo=123) == 'Aa(foo=123)'
    >>> assert repr_attributes(Aa(), 456, foo=123, bar='abc') == "Aa(456, foo=123, bar='abc')"
    """
    fld = list(map(repr, anonymous_elements)) + list(f'{name}={value!r}' for name, value in named_elements.items())
    return f'{type(obj).__name__}(' + ', '.join(fld) + ')'


def repr_attributes_noexcept(obj: object, *anonymous_elements: object, **named_elements: object) -> str:
    """
    A robust version of :meth:`repr_attributes` that never raises exceptions.
    """
    try:
        return repr_attributes(obj, *anonymous_elements, **named_elements)
    except Exception as ex:
        # noinspection PyBroadException
        try:
            return f'<REPR FAILED: {ex!r}>'
        except Exception:
            return '<REPR FAILED: UNKNOWN ERROR>'


def _unittest_repr_attributes_noexcept() -> None:
    class Aa:
        pass

    class Ee(Exception):
        def __repr__(self) -> str:
            raise Ee()

    assert repr_attributes_noexcept(Aa(), 456, foo=123, bar='abc') == "Aa(456, foo=123, bar='abc')"
    assert repr_attributes_noexcept(Aa(), 456, foo=123, bar='abc', baz=Ee()) == "<REPR FAILED: UNKNOWN ERROR>"
