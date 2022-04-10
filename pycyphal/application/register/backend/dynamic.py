# Copyright (C) 2021  OpenCyphal  <opencyphal.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Tuple, Optional, Callable, Dict, Iterator, Union
import logging
from . import Entry, BackendError, Backend, Value


__all__ = ["DynamicBackend"]


class DynamicBackend(Backend):
    """
    Register backend where register access is delegated to external getters and setters.
    It does not store values internally.
    Exceptions raised by getters/setters are wrapped into :class:`BackendError`.

    Create new registers and change value of existing ones using :meth:`__setitem__`.

    >>> from pycyphal.application.register import Bit
    >>> b = DynamicBackend()
    >>> b.persistent
    False
    >>> b.get("foo") is None
    True
    >>> b.index(0) is None
    True
    >>> foo = Value(bit=Bit([True, False, True]))
    >>> def set_foo(v: Value):
    ...     global foo
    ...     foo = v
    >>> b["foo"] = (lambda: foo), set_foo   # Create new mutable register.
    >>> b["foo"].mutable
    True
    >>> list(b["foo"].value.bit.value)
    [True, False, True]
    >>> b["foo"] = Value(bit=Bit([False, True, True]))  # Set new value.
    >>> list(b["foo"].value.bit.value)
    [False, True, True]
    >>> b["foo"] = lambda: foo  # Replace register with a new one that is now immutable.
    >>> b["foo"] = Value(bit=Bit([False, False, False]))    # Value cannot be changed.
    >>> list(b["foo"].value.bit.value)
    [False, True, True]
    >>> list(b)
    ['foo']
    >>> del b["foo"]
    >>> list(b)
    []
    """

    def __init__(self) -> None:
        self._reg: Dict[str, GetSetPair] = {}  # This dict is always sorted lexicographically by key!
        super().__init__()

    @property
    def location(self) -> str:
        """This is a stub."""
        return ""

    @property
    def persistent(self) -> bool:
        """Always false."""
        return False

    def close(self) -> None:
        """Clears all registered registers."""
        self._reg.clear()

    def index(self, index: int) -> Optional[str]:
        try:
            return list(self)[index]
        except LookupError:
            return None

    def __getitem__(self, key: str) -> Entry:
        getter, setter = self._reg[key]
        try:
            value = getter()
        except Exception as ex:
            raise BackendError(f"Unhandled exception in getter for {key!r}: {ex}") from ex
        e = Entry(value, mutable=setter is not None)
        _logger.debug("%r: Get %r -> %r", self, key, e)
        return e

    def __setitem__(
        self,
        key: str,
        value: Union[
            Entry,
            Value,
            Callable[[], Value],
            Tuple[Callable[[], Value], Callable[[Value], None]],
        ],
    ) -> None:
        """
        :param key: The register name.

        :param value:
            - If this is an instance of :class:`Entry` or :class:`Value`, and the referenced register is mutable,
              its setter is invoked with the supplied instance of :class:`Value`
              (if :class:`Entry` is given, the value is extracted from there and the mutability flag is ignored).
              If the register is immutable, nothing is done.
              The caller is required to ensure that the type is acceptable.

            - If this is a single callable, a new immutable register is defined (existing registers overwritten).

            - If this is a tuple of two callables, a new mutable register is defined (existing registers overwritten).
        """
        if isinstance(value, Entry):
            value = value.value

        if isinstance(value, Value):
            try:
                _, setter = self._reg[key]
            except LookupError:
                setter = None
            if setter is not None:
                _logger.debug("%r: Set %r <- %r", self, key, value)
                try:
                    setter(value)
                except Exception as ex:
                    raise BackendError(f"Unhandled exception in setter for {key!r}: {ex}") from ex
            else:
                _logger.debug("%r: Set %r not supported", self, key)
        else:
            if callable(value):
                getter, setter = value, None
            elif isinstance(value, tuple) and len(value) == 2 and all(map(callable, value)):
                getter, setter = value
            else:  # pragma: no cover
                raise TypeError(f"Invalid argument: {value!r}")
            items = list(self._reg.items())
            items.append((key, (getter, setter)))
            self._reg = dict(sorted(items, key=lambda x: x[0]))

    def __delitem__(self, key: str) -> None:
        _logger.debug("%r: Delete %r", self, key)
        del self._reg[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._reg)

    def __len__(self) -> int:
        return len(self._reg)


GetSetPair = Tuple[
    Callable[[], Value],
    Optional[Callable[[Value], None]],
]

_logger = logging.getLogger(__name__)


def _unittest_dyn() -> None:
    from uavcan.primitive import String_1 as String

    b = DynamicBackend()
    assert not b.persistent
    assert len(b) == 0
    assert list(b.keys()) == []
    assert b.get("foo") is None
    assert b.index(0) is None

    bar = Value(string=String())

    def set_bar(v: Value) -> None:
        nonlocal bar
        bar = v

    b["foo"] = lambda: Value(string=String("Hello"))
    b["bar"] = lambda: bar, set_bar
    assert len(b) == 2
    assert list(b.keys()) == ["bar", "foo"]
    assert b.index(0) == "bar"
    assert b.index(1) == "foo"
    assert b.index(2) is None

    e = b.get("foo")
    assert e
    assert not e.mutable
    assert e.value.string
    assert e.value.string.value.tobytes().decode() == "Hello"

    e = b.get("bar")
    assert e
    assert e.mutable
    assert e.value.string
    assert e.value.string.value.tobytes().decode() == ""

    b["foo"] = Value(string=String("world"))
    b["bar"] = Entry(Value(string=String("world")), mutable=False)  # Flag ignored

    e = b.get("foo")
    assert e
    assert not e.mutable
    assert e.value.string
    assert e.value.string.value.tobytes().decode() == "Hello"

    e = b.get("bar")
    assert e
    assert e.mutable
    assert e.value.string
    assert e.value.string.value.tobytes().decode() == "world"

    del b["foo"]
    assert len(b) == 1
    assert list(b.keys()) == ["bar"]

    b.close()
    assert len(b) == 0
