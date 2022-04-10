# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import logging

R = typing.TypeVar("R")

_logger = logging.getLogger(__name__)


def broadcast(
    functions: typing.Iterable[typing.Callable[..., R]]
) -> typing.Callable[..., typing.List[typing.Union[R, Exception]]]:
    """
    Returns a function that invokes each supplied function in series with the specified arguments
    following the specified order.
    If a function is executed successfully, its result is added to the output list.
    If it raises an exception, the exception is suppressed, logged, and added to the output list instead of the result.

    This function is mostly intended for invoking various handlers.

    ..  doctest::
        :hide:

        >>> _logger.setLevel(100)  # Suppress error reports from the following doctest.

    >>> def add(a, b):
    ...     return a + b
    >>> def fail(a, b):
    ...     raise ValueError(f'Arguments: {a}, {b}')
    >>> broadcast([add, fail])(4, b=5)
    [9, ValueError('Arguments: 4, 5')]
    >>> broadcast([print])('Hello', 'world!')
    Hello world!
    [None]
    >>> broadcast([])()
    []
    """

    def delegate(*args: typing.Any, **kwargs: typing.Any) -> typing.List[typing.Union[R, Exception]]:
        out: typing.List[typing.Union[R, Exception]] = []
        for fn in functions:
            try:
                r: typing.Union[R, Exception] = fn(*args, **kwargs)
            except Exception as ex:
                r = ex
                _logger.exception("Unhandled exception in %s: %s", fn, ex)
            out.append(r)
        return out

    return delegate
