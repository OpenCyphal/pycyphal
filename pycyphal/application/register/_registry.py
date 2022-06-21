# Copyright (C) 2021  OpenCyphal  <opencyphal.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import sys
import abc
from fnmatch import fnmatchcase
from typing import Optional, Iterator, Union, Callable, Tuple, Sequence, Dict
import logging
import pycyphal
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
        return pycyphal.util.repr_attributes(self, repr(self.value), mutable=self.mutable, persistent=self.persistent)


class Registry(MutableMapping[str, ValueProxy]):
    """
    The registry (register repository) is the main access point for the application to its registers (configuration).
    It is a facade that provides user-friendly API on top of multiple underlying register backends
    (see :class:`backend.Backend`).
    Observe that it implements :class:`MutableMapping`.

    The user is not expected to instantiate this class manually;
    instead, it is provided as a member of :class:`pycyphal.application.Node`,
    or via :func:`pycyphal.application.make_node`.

    >>> import pycyphal.application
    >>> registry = pycyphal.application.make_registry(environment_variables={})

    Create static registers (stored in the register file):

    >>> from pycyphal.application.register import Natural16, Real32
    >>> registry["p.a"] = Natural16([1234])                 # Assign or create.
    >>> registry.setdefault("p.b", Real32([12.34]))         # Update or create. # doctest: +NORMALIZE_WHITESPACE
    ValueProxyWithFlags(uavcan.register.Value...(real32=uavcan.primitive.array.Real32...(value=[12.34])),
                        mutable=True,
                        persistent=False)

    Create dynamic registers (getter/setter invoked at every access; existing entries overwritten automatically):

    >>> registry["d.a"] = lambda: [1.0, 2.0, 3.0]           # Immutable (read-only), deduced type: real64[3].
    >>> list(registry["d.a"].value.real64.value)            # Yup, deduced as expected, real64.
    [1.0, 2.0, 3.0]
    >>> registry["d.a"] = lambda: Real32([1.0, 2.0, 3.0])   # Like above, but now it is "real32[3]".
    >>> list(registry["d.a"].value.real32.value)
    [1.0, 2.0, 3.0]
    >>> d_b = [True, False, True]                   # Suppose we have some internal object.
    >>> def set_d_b(v: Value):                      # Define a setter for it.
    ...     global d_b
    ...     d_b = ValueProxy(v).bools
    >>> registry["d.b"] = (lambda: d_b), set_d_b    # Expose the object via mutable register with deduced type "bit[3]".

    Read/write/delete using the same dict-like API:

    >>> list(registry)  # Sorted lexicographically per backend. Altering backends affects register ordering.
    ['p.a', 'p.b', 'd.a', 'd.b']
    >>> len(registry)
    4
    >>> int(registry["p.a"])
    1234
    >>> registry["p.a"] = 88                        # Automatic type conversion to "natural16[1]" (defined above).
    >>> int(registry["p.a"])
    88
    >>> registry["d.b"].bools
    [True, False, True]
    >>> registry["d.b"] = [-1, 5, 0.0]              # Automatic type conversion to "bit[3]".
    >>> registry["d.b"].bools
    [True, True, False]
    >>> del registry["*.a"]                         # Use wildcards to remove multiple at the same time.
    >>> list(registry)
    ['p.b', 'd.b']
    >>> registry["d.b"].ints                        # Type conversion by ValueProxy.
    [1, 1, 0]
    >>> registry["d.b"].floats
    [1.0, 1.0, 0.0]
    >>> registry["d.b"].value.bit                   # doctest: +NORMALIZE_WHITESPACE
    uavcan.primitive.array.Bit...(value=[ True, True,False])

    Registers created by :meth:`setdefault` are always initialized from environment variables:

    >>> registry.environment_variables["P__C"] = b"999 +888.3"
    >>> registry.environment_variables["D__C"] = b"Hello world!"
    >>> registry.setdefault("p.c", Natural16([111, 222])).ints  # Value from environment is used here!
    [999, 888]
    >>> registry.setdefault("p.d", Natural16([111, 222])).ints  # No environment variable for this one.
    [111, 222]
    >>> d_c = 'Coffee'
    >>> def set_d_c(v: Value):
    ...     global d_c
    ...     d_c = str(ValueProxy(v))
    >>> str(registry.setdefault("d.c", (lambda: d_c, set_d_c))) # Setter is invoked immediately.
    'Hello world!'
    >>> registry["d.c"] = "New text"                            # Change the value again.
    >>> d_c                                                     # Yup, changed.
    'New text'
    >>> str(registry.setdefault("d.c", lambda: d_c))            # Environment var ignored because no setter.
    'New text'

    If such behavior is undesirable, one can either clear the environment variable dict or remove specific entries.
    See also: :func:`pycyphal.application.make_node`.

    Variables created by direct assignment are (obviously) not affected by environment variables:

    >>> registry["p.c"] = [111, 222]                            # Direct assignment instead of setdefault().
    >>> registry["p.c"].ints                                    # Environment variables ignored!
    [111, 222]

    Closing the registry will close all underlying backends.

    >>> registry.close()

    TODO: Add modification notification callbacks to allow applications implement hot reloading.
    """

    Assignable = Union[
        RelaxedValue,
        Callable[[], RelaxedValue],
        Tuple[
            Callable[[], RelaxedValue],
            Callable[[Value], None],
        ],
    ]
    """
    An instance of any type from this union can be used to assign or create a register.
    Creation is handled depending on the type:

    - If a single callable, it will be invoked whenever this register is read; such register is called "dynamic".
      Such register will be reported as immutable.
      The registry file is not affected and therefore this change is not persistent.
      :attr:`environment_variables` are always ignored in this case since the register cannot be written.
      The result of the callable is converted to the register value using :class:`ValueProxy`.

    - If a tuple of two callables, then the first one is a getter that is invoked on read (see above),
      and the second is a setter that is invoked on write with a single argument of type :class:`Value`.
      It is guaranteed that the type of the value passed into the setter is always the same as that which
      is returned by the getter.
      The type conversion is performed automatically by polling the getter beforehand to discover the type.
      The registry file is not affected and therefore this change is not persistent.

    - Any other type (e.g., :class:`Value`, ``Natural16``, native, etc.):
      a static register will be created and stored in the registry file.
      Conversion logic is implemented by :class:`ValueProxy`.

    Dynamic registers (callables) overwrite existing entries unconditionally.
    It is not recommended to create dynamic registers with same names as existing static registers,
    as it may cause erratic behaviors.
    """

    @property
    @abc.abstractmethod
    def backends(self) -> Sequence[backend.Backend]:
        """
        If a register exists in more than one registry, only the first copy will be used;
        however, the count will include all redundant registers.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def environment_variables(self) -> Dict[str, bytes]:
        """
        When a new register is created using :meth:`setdefault`, its default value will be overridden from this dict.
        This is done to let the registry use values passed over to this node via environment variables or a similar
        mechanism.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _create_static(self, name: str, value: Value) -> None:
        """This is an abstract method because only the implementation knows which backend should be used."""
        raise NotImplementedError

    @abc.abstractmethod
    def _create_dynamic(
        self,
        name: str,
        getter: Callable[[], Value],
        setter: Optional[Callable[[Value], None]],
    ) -> None:
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
        Get register name by index. The ordering is like :meth:`__iter__`. Returns None if index is out of range.
        """
        for i, key in enumerate(self):
            if i == index:
                return key
        return None

    def setdefault(self, key: str, default: Optional[Assignable] = None) -> ValueProxyWithFlags:
        """
        **This is the preferred method for creating new registers.**

        If the register exists, its value will be returned an no further action will be taken.
        If the register doesn't exist, it will be created and immediately updated from :attr:`environment_variables`
        (using :meth:`ValueProxy.assign_environment_variable`).
        The register value instance is created using :class:`ValueProxy`.

        :param key:     Register name.
        :param default: If exists, this value is ignored; otherwise created as described in :attr:`Assignable`.
        :return:        Resulting value.
        :raises:        See :meth:`ValueProxy.assign_environment_variable` and :meth:`ValueProxy`.
        """
        try:
            return self[key]
        except KeyError:
            pass
        if default is None:
            raise TypeError  # pragma: no cover
        from . import get_environment_variable_name

        _logger.debug("%r: Create %r <- %r", self, key, default)
        self._set(key, default, create_only=True)
        env_val = self.environment_variables.get(get_environment_variable_name(key))
        if env_val is not None:
            _logger.debug("%r: Update from env: %r <- %r", self, key, env_val)
            reg = self[key]
            reg.assign_environment_variable(env_val)
            self[key] = reg

        return self[key]

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

    def __setitem__(self, name: str, value: Assignable) -> None:
        """
        Assign a new value to the register if it exists and the type of the value is matching or can be
        converted to the register's type.
        The mutability flag may be ignored depending on which backend the register is stored at.
        The conversion is implemented by :meth:`ValueProxy.assign`.

        If the register does not exist, a new one will be created.
        However, unlike :meth:`setdefault`, :meth:`ValueProxy.assign_environment_variable` is not invoked.
        The register value instance is created using :class:`ValueProxy`.

        :raises:
            :class:`ValueConversionError` if the register exists but the value cannot be converted to its type
            or (in case of creation) the environment variable contains an invalid value.
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

    def _set(self, name: str, value: Assignable, *, create_only: bool = False) -> None:
        _ensure_name(name)

        if callable(value):
            self._create_dynamic(name, lambda: ValueProxy(value()).value, None)  # type: ignore
            return
        if isinstance(value, tuple) and len(value) == 2 and all(map(callable, value)):
            g, s = value
            self._create_dynamic(name, (lambda: ValueProxy(g()).value), s)
            return

        if not create_only:
            for b in self.backends:
                e = b.get(name)
                if e is not None:
                    c = ValueProxy(e.value)
                    c.assign(value)  # type: ignore
                    b[name] = c.value
                    return

        self._create_static(name, ValueProxy(value).value)  # type: ignore

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self.backends)


def _ensure_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"Register names are strings, not {type(name).__name__}")


_logger = logging.getLogger(__name__)
