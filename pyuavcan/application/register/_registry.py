# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
from fnmatch import fnmatchcase
from typing import List, Optional, Iterator, Iterable
import logging
import pyuavcan
from . import backend
from ._value import RelaxedValue, ValueProxy, Value

if sys.version_info >= (3, 9):
    from collections.abc import Mapping
else:  # pragma: no cover
    from typing import Mapping  # pylint: disable=ungrouped-imports


class MissingRegisterError(KeyError):
    """
    Raised when the user attempts to access a register that is not defined.
    """


class ValueProxyWithFlags(ValueProxy):
    """
    This is like :class:`ValueProxy` but extended with register flags.
    """

    def __init__(self, msg: Value, mutable: bool, persistent: bool) -> None:
        super().__init__(msg)
        self._mutable = mutable
        self._persistent = persistent

    @property
    def mutable(self) -> bool:
        return self._mutable

    @property
    def persistent(self) -> bool:
        return self._persistent

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, repr(self.value), mutable=self.mutable, persistent=self.persistent)


class Registry(Mapping[str, ValueProxyWithFlags]):
    """
    The registry (register repository) is the main access point for the application to its registers.
    It is a facade that provides user-friendly API on top of multiple underlying register backends
    (see :class:`backend.Backend`).
    Observe that it implements :class:`Mapping`.

    Here's how to use it. First, we need backends to set up the registry on top of:

    >>> from pyuavcan.application.register.backend.sqlite import SQLiteBackend
    >>> from pyuavcan.application.register.backend.dynamic import DynamicBackend
    >>> b0 = SQLiteBackend()
    >>> b0["c"] = Value()           # Create registry with empty value stored in the SQLite backend.
    >>> b0["a"] = Value()
    >>> b1 = DynamicBackend()
    >>> b1["b"] = lambda: Value()   # Create immutable (read-only) non-persistent dynamic register.

    We can modify backends and add additional ones after the registry is set up --
    it doesn't keep any internal state and is fully transparent.
    Moving on:

    >>> r = Registry([b0, b1])
    >>> list(r.keys())                  # Sorted lexicographically per backend.
    ['a', 'c', 'b']
    >>> list(Registry([b1, b0]).keys()) # Notice how the order is affected.
    ['b', 'a', 'c']
    >>> r.index(0), r.index(1), r.index(2), r.index(3)
    ('a', 'c', 'b', None)
    >>> list(r)                         # The registry keys are iterable.
    ['a', 'c', 'b']
    >>> len(r)                          # The number of registers.
    3

    Get/set behaviors:

    >>> from pyuavcan.application.register import Bit
    >>> r.get("foo") is None            # No such register --> None.
    True
    >>> r["foo"]                        # This is an alternative to get(). # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    MissingRegisterError: 'baz'
    >>> r["foo"] = True                 # No such register --> exception. # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    MissingRegisterError: 'foo'
    >>> b0["foo"] = Value(bit=Bit([True, False]))       # Create register "foo" in the SQLite backend.
    >>> e = r["foo"]                                    # Now it is gettable.
    >>> e.bools    # Use the proxy properties to automatically convert the register value to a native type.
    [True, False]
    >>> e.ints
    [1, 0]
    >>> e.floats
    [1.0, 0.0]
    >>> e.value.bit.value[0], e.value.bit.value[1]  # Or just access the underlying DSDL value directly.
    (True, False)
    >>> r["foo"].ints                               # The alternative way that mimics dict.
    [1, 0]
    >>> r["foo"] = True, False, False               # Dimensionality mismatch. # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueConversionError: ...
    >>> val = Value(bit=Bit([True, False, False]))
    >>> def set_val(v: Value):
    ...     global val
    ...     val = v
    >>> b1["bar"]   = (lambda: val), set_val        # Create mutable dynamic register.
    >>> b1["bar.ro"] = lambda: val                  # Create immutable (read-only) dynamic register.
    >>> r["bar"].bools
    [True, False, False]
    >>> r["bar"] = [0, 1.5, -5]                     # The value type is converted automatically.
    >>> r["bar"].floats
    [0.0, 1.0, 1.0]

    Deleting registers using wildcard matching (every backend where matching names are found is affected):

    >>> list(r.keys())
    ['a', 'c', 'foo', 'b', 'bar', 'bar.ro']
    >>> del r["bar*"]
    >>> list(r.keys())
    ['a', 'c', 'foo', 'b']
    >>> del r["*a*"]
    >>> list(r.keys())
    ['c', 'foo', 'b']

    TODO: Add modification callbacks to allow applications implement hot reloading.
    """

    def __init__(self, backends: Iterable[backend.Backend]) -> None:
        """
        :param backends: Providing backend instances here is equivalent to invoking :meth:`bind` afterwards.
        """
        self._backends: List[backend.Backend] = []
        for b in backends:
            self.bind(b)

    def bind(self, b: backend.Backend) -> None:
        """
        Connect a new backend to this registry after the existing backends.
        Count, keys, and ordering will be invalidated.
        If a register exists in more than one registry, only the first copy will be used;
        however, the count will include all redundant registers.
        """
        self._backends.append(b)

    def close(self) -> None:
        """
        Closes all storage backends.
        """
        for b in self._backends:
            b.close()
        self._backends.clear()

    def index(self, index: int) -> Optional[str]:
        """
        This is mostly intended for implementing ``uavcan.register.List``.
        Returns None if index is out of range.
        The ordering is like :meth:`__iter__` and :meth:`keys` (invalidated by :meth:`bind` and :meth:`delete`).
        """
        for i, key in enumerate(self):
            if i == index:
                return key
        return None

    def __getitem__(self, key: str) -> ValueProxyWithFlags:
        """
        :returns: :class:`ValueProxyWithFlags` (:class:`ValueProxy`) if exists.
        :raises: :class:`MissingRegisterError` (:class:`KeyError`) if no such register.
        """
        _ensure_name(key)
        for b in self._backends:
            ent = b.get(key)
            if ent is not None:
                return ValueProxyWithFlags(ent.value, mutable=ent.mutable, persistent=b.persistent)
        raise MissingRegisterError(key)

    def __setitem__(self, name: str, value: RelaxedValue) -> None:
        """
        Set if the register exists and the type of the value is matching or can be converted to the register's type.
        The mutability flag may be ignored depending on which backend the register is stored at.
        The conversion is implemented by :meth:`ValueProxy.assign`.

        :raises:
            :class:`MissingRegisterError` (subclass of :class:`KeyError`) if the register does not exist.
            :class:`ValueConversionError` if the register exists but the value cannot be converted to its type.
        """
        _ensure_name(name)
        for b in self._backends:
            e = b.get(name)
            if e is not None:
                c = ValueProxy(e.value)
                c.assign(value)
                b[name] = c.value
                break
        else:
            raise MissingRegisterError(name)

    def __delitem__(self, wildcard: str) -> None:
        """
        Remove registers that match the specified wildcard from all backends. Matching is case-sensitive.
        Count and keys are invalidated. **If no matching keys are found, no exception is raised.**
        """
        _ensure_name(wildcard)
        for b in self._backends:
            names = [n for n in b.keys() if fnmatchcase(n, wildcard)]
            _logger.debug("%r: Deleting %d registers matching %r from %r: %r", self, len(names), wildcard, b, names)
            for n in names:
                del b[n]

    def __iter__(self) -> Iterator[str]:
        """
        Iterator over register names. They may not be unique if different backends redefine the same register!
        The ordering is defined by backend ordering, then lexicographically.
        """
        return iter(n for b in self._backends for n in b.keys())

    def __len__(self) -> int:
        """
        Number of registers in all backends.
        """
        return sum(map(len, self._backends))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self._backends)


def _ensure_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"Register names are strings, not {type(name)}")


_logger = logging.getLogger(__name__)
