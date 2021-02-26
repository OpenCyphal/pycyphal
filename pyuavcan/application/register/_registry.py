# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import abc
from fnmatch import fnmatchcase
from typing import Optional, Iterator, Union, Callable, Tuple, Sequence
import logging
import pyuavcan
from . import backend
from ._value import RelaxedValue, ValueProxy, Value

if sys.version_info >= (3, 9):
    from collections.abc import MutableMapping
else:  # pragma: no cover
    from typing import MutableMapping  # pylint: disable=ungrouped-imports


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


class Registry(MutableMapping[str, ValueProxy]):
    """
    The registry (register repository) is the main access point for the application to its registers (configuration).
    It is a facade that provides user-friendly API on top of multiple underlying register backends
    (see :class:`backend.Backend`).
    Observe that it implements :class:`MutableMapping`.

    The user is not expected to instantiate this class manually;
    instead, it is provided as a member of :class:`pyuavcan.application.Node`.

    ..  doctest::
        :hide:

        >>> from pyuavcan.application.register.backend.sqlite import SQLiteBackend
        >>> from pyuavcan.application.register.backend.dynamic import DynamicBackend
        >>> import tempfile
        >>> class DocTestRegistry(Registry):
        ...     def __init__(self) -> None:
        ...         self._sqlite = SQLiteBackend(tempfile.mktemp(".db", "pyuavcan_register_test"))
        ...         self._dynamic = DynamicBackend()
        ...         super().__init__()
        ...     @property
        ...     def backends(self):
        ...         return [self._sqlite, self._dynamic]
        ...     def _create_persistent(self, name: str, value: Value) -> None:
        ...         self._sqlite[name] = value
        ...     def _create_dynamic(self, name: str, get: Callable[[], Value], set: Optional[Callable[[Value], None]]):
        ...         self._dynamic[name] = get if set is None else (get, set)
        >>> registry = DocTestRegistry()

    Create persistent registers (stored in the register file):

    >>> from pyuavcan.application.register import Natural16, Real32, Bit, String
    >>> registry["p.a"] = Value(natural16=Natural16([1234]))        # Assign or create.
    >>> registry.setdefault("p.b", Value(real32=Real32([12.34])))   # Update or create. # doctest: +NORMALIZE_WHITESPACE
    ValueProxyWithFlags(uavcan.register.Value...(real32=uavcan.primitive.array.Real32...(value=[12.34])),
                        mutable=True,
                        persistent=True)

    Create dynamic registers (getter/setter invoked at every access; existing entries overwritten automatically):

    >>> registry["d.a"] = lambda: Value(real32=Real32([1, 2, 3]))   # Immutable (read-only).
    >>> d_b = [True, False, True]
    >>> def set_d_b(v: Value):
    ...     global d_b
    ...     d_b = ValueProxy(v).bools
    >>> registry["d.b"] = (lambda: Value(bit=Bit(d_b))), set_d_b     # Mutable.

    Only a small set of types can be used to create new registers (listed in :attr:`CreationArgument`),
    otherwise you get a :class:`MissingRegisterError`:

    >>> registry["n.a"] = "Cannot create register from argument of this type"   # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    MissingRegisterError: 'n.a'

    Read/write/delete using the same dict-like API:

    >>> list(registry)  # Sorted lexicographically per backend. Altering backends affects register ordering.
    ['p.a', 'p.b', 'd.a', 'd.b']
    >>> len(registry)
    4
    >>> int(registry["p.a"])
    1234
    >>> registry["p.a"] = 88
    >>> int(registry["p.a"])
    88
    >>> registry["d.b"].bools
    [True, False, True]
    >>> registry["d.b"] = [-1, 5, 0.0]      # Automatic type conversion.
    >>> registry["d.b"].bools
    [True, True, False]
    >>> del registry["*.a"]                 # Use wildcards to remove multiple at the same time.
    >>> list(registry)
    ['p.b', 'd.b']
    >>> registry["d.b"].ints                # Type conversion by ValueProxy.
    [1, 1, 0]
    >>> registry["d.b"].floats
    [1.0, 1.0, 0.0]
    >>> registry["d.b"].value.bit           # doctest: +NORMALIZE_WHITESPACE
    uavcan.primitive.array.Bit...(value=[ True, True,False])

    By default, registers created by :meth:`setdefault` are initialized from environment variables:

    >>> import os
    >>> os.environ["P__C"] = "999 +888.3"
    >>> os.environ["D__C"] = "Hello world!"
    >>> registry.setdefault("p.c", Value(natural16=Natural16([111, 222]))).ints  # Value from env is used!
    [999, 888]
    >>> registry.setdefault("p.d", Value(natural16=Natural16([111, 222]))).ints  # No environment variable for this one.
    [111, 222]
    >>> d_c = 'Coffee'
    >>> def set_d_c(v: Value):
    ...     global d_c
    ...     d_c = str(ValueProxy(v))
    >>> str(registry.setdefault("d.c", (lambda: Value(string=String(d_c)), set_d_c)))   # Setter is invoked immediately.
    'Hello world!'
    >>> registry["d.c"] = "New text"                                        # Change the value again.
    >>> d_c                                                                 # Yup, changed.
    'New text'
    >>> str(registry.setdefault("d.c", lambda: Value(string=String(d_c))))  # Environment var ignored because no setter.
    'New text'

    In rare circumstances this behavior may be undesirable, so it can be disabled:

    >>> registry.use_defaults_from_environment = False
    >>> del registry["*.c"]                                                 # Drop the registers we created above.
    >>> registry.setdefault("p.c", Value(natural16=Natural16([111, 222]))).ints  # Environment variables ignored now!
    [111, 222]

    Notice that variables created by direct assignment are never overridden from environment variables:

    >>> registry.use_defaults_from_environment = True
    >>> registry["p.c"] = Value(natural16=Natural16([111, 222]))            # Direct assignment instead of setdefault().
    >>> registry["p.c"].ints                                                # Environment variables ignored!
    [111, 222]

    ..  doctest::
        :hide:

        >>> for k in os.environ:
        ...     if "__" in k:
        ...         del os.environ[k]
        >>> registry.close()

    TODO: Add modification callbacks to allow applications implement hot reloading.
    """

    CreationArgument = Union[
        Value,
        ValueProxy,
        Callable[[], Union[Value, ValueProxy]],
        Tuple[
            Callable[[], Union[Value, ValueProxy]],
            Callable[[Value], None],
        ],
    ]
    """
    - If :class:`Value` or :class:`ValueProxy`,
      a persistent register will be created and stored in the registry file.

    - If a single callable, it will be invoked whenever this register is read.
      Such register will be reported as immutable.
      The registry file is not affected and therefore this change is not persistent.
      :attr:`use_environment_variables` is ignored in this case since the register cannot be written.

    - If a tuple of two callables, then the first one is a getter that is invoked on read (see above),
      and the second is a setter that is invoked on write with a single argument of type :class:`Value`.
      It is guaranteed that the type of the value passed into the setter is always the same as that which
      is returned by the getter.
      The type conversion is performed automatically by polling the getter beforehand to discover the type.
      The registry file is not affected and therefore this change is not persistent.

    Callables overwrite existing entries unconditionally.
    """

    def __init__(self) -> None:
        self._use_environment_variables = True

    @property
    def use_defaults_from_environment(self) -> bool:
        """
        See :meth:`update_from_environment`, :meth:`setdefault`.
        """
        return self._use_environment_variables

    @use_defaults_from_environment.setter
    def use_defaults_from_environment(self, value: bool) -> None:
        self._use_environment_variables = bool(value)

    @property
    @abc.abstractmethod
    def backends(self) -> Sequence[backend.Backend]:
        """
        If a register exists in more than one registry, only the first copy will be used;
        however, the count will include all redundant registers.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _create_persistent(self, name: str, value: Value) -> None:
        """This is an abstract method because only the implementation knows which backend should be used."""
        raise NotImplementedError

    @abc.abstractmethod
    def _create_dynamic(self, name: str, get: Callable[[], Value], set: Optional[Callable[[Value], None]]) -> None:
        """This is an abstract method because only the implementation knows which backend should be used."""
        raise NotImplementedError

    def close(self) -> None:
        """
        Closes all storage backends.
        """
        for b in self.backends:
            b.close()

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

    def update_from_environment(self, register_name: str) -> None:
        """
        Invoke :func:`pyuavcan.application.register.update_from_environment` for the specified register.
        This should be done for all registers shortly after initialization to accept values passed to this node
        by the launcher.
        Registers that are created later will pick up their environment variables by virtue of
        :attr:`use_defaults_from_environment`.

        :raises:
            :class:`ValueError` if the value of an environment variable is invalid.
            :class:`MissingRegisterError` if there is no register under this name.
        """
        from . import update_from_environment as upd

        v = self[register_name]
        if upd(v, register_name):
            _logger.debug("%r: Updated %r from env: %r", self, register_name, v)
            self[register_name] = v

    def setdefault(self, name: str, default: Optional[CreationArgument] = None) -> ValueProxyWithFlags:
        """
        **This is the preferred method for creating new registers.**

        If the register exists, its value will be returned an no further action will be taken.

        If the register doesn't exist, it will be created; and, if :attr:`use_defaults_from_environment` is True,
        :meth:`update_from_environment` will be automatically invoked.

        :param name:    Register name.
        :param default: If exists, this value is ignored; otherwise created as described in :attr:`CreationArgument`.
        :return:        Resulting value.
        """
        try:
            return self[name]
        except KeyError as ex:
            if default is None:
                raise TypeError from ex  # pragma: no cover
            _logger.debug("%r: Create %r default %r", self, name, default)
            self._set(name, default, create_only=True)
            if self.use_defaults_from_environment:
                self.update_from_environment(name)
            return self[name]

    def __getitem__(self, name: str) -> ValueProxyWithFlags:
        """
        :returns: :class:`ValueProxyWithFlags` (:class:`ValueProxy`) if exists.
        :raises: :class:`MissingRegisterError` (:class:`KeyError`) if no such register.
        """
        _ensure_name(name)
        for b in self.backends:
            ent = b.get(name)
            if ent is not None:
                return ValueProxyWithFlags(ent.value, mutable=ent.mutable, persistent=b.persistent)
        raise MissingRegisterError(name)

    def __setitem__(self, name: str, value: Union[RelaxedValue, CreationArgument]) -> None:
        """
        Assign a new value to the register if it exists and the type of the value is matching or can be
        converted to the register's type.
        The mutability flag may be ignored depending on which backend the register is stored at.
        The conversion is implemented by :meth:`ValueProxy.assign`.

        If the register does not exist, and the value is of type :attr:`CreationArgument`,
        a new register will be created.
        However, :meth:`update_from_environment` is NOT invoked in this case, as one might guess.

        Otherwise, :class:`MissingRegisterError` is raised.

        :raises:
            :class:`MissingRegisterError` (subclass of :class:`KeyError`) if the register does not exist
            and cannot be created.
            :class:`ValueConversionError` if the register exists but the value cannot be converted to its type.
        """
        self._set(name, value)

    def __delitem__(self, wildcard: str) -> None:
        """
        Remove registers that match the specified wildcard from all backends. Matching is case-sensitive.
        Count and keys are invalidated. **If no matching keys are found, no exception is raised.**
        """
        _ensure_name(wildcard)
        for b in self.backends:
            names = [n for n in b if fnmatchcase(n, wildcard)]
            _logger.debug("%r: Deleting %d registers matching %r from %r: %r", self, len(names), wildcard, b, names)
            for n in names:
                del b[n]

    def __iter__(self) -> Iterator[str]:
        """
        Iterator over register names. They may not be unique if different backends redefine the same register!
        The ordering is defined by backend ordering, then lexicographically.
        """
        return iter(n for b in self.backends for n in b.keys())

    def __len__(self) -> int:
        """
        Number of registers in all backends.
        """
        return sum(map(len, self.backends))

    def _set(self, name: str, value: Union[RelaxedValue, CreationArgument], *, create_only: bool = False) -> None:
        _ensure_name(name)

        def strictify(x: Union[Value, ValueProxy]) -> Value:
            if isinstance(x, ValueProxy):
                return x.value
            return x

        if callable(value):
            self._create_dynamic(name, lambda: strictify(value()), None)  # type: ignore
            return
        if isinstance(value, tuple) and len(value) == 2 and all(map(callable, value)):
            g, s = value
            self._create_dynamic(name, (lambda: strictify(g())), s)
            return

        if not create_only:
            for b in self.backends:
                e = b.get(name)
                if e is not None:
                    c = ValueProxy(e.value)
                    c.assign(value)
                    b[name] = c.value
                    return

        if isinstance(value, (Value, ValueProxy)):
            self._create_persistent(name, ValueProxy(value).value)
            return

        raise MissingRegisterError(
            name,
            f"Cannot create register from argument of type {type(value).__name__}. "
            f"New registers can only be constructed from: {Registry.CreationArgument}",
        )

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self.backends)


def _ensure_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"Register names are strings, not {type(name)}")


_logger = logging.getLogger(__name__)
