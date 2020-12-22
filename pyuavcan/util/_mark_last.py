# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing


T = typing.TypeVar("T")


def mark_last(it: typing.Iterable[T]) -> typing.Iterable[typing.Tuple[bool, T]]:
    """
    This is an iteration helper like :func:`enumerate`. It amends every item with a boolean flag which is False
    for all items except the last one. If the input iterable is empty, yields nothing.

    >>> list(mark_last([]))
    []
    >>> list(mark_last([123]))
    [(True, 123)]
    >>> list(mark_last([123, 456]))
    [(False, 123), (True, 456)]
    >>> list(mark_last([123, 456, 789]))
    [(False, 123), (False, 456), (True, 789)]
    """
    it = iter(it)
    try:
        last = next(it)
    except StopIteration:
        pass
    else:
        for val in it:
            yield False, last
            last = val
        yield True, last
