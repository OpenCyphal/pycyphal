# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import abc
from typing import Optional, Union
import dataclasses
import pyuavcan
from uavcan.register import Value_1_0 as Value

if sys.version_info >= (3, 9):
    from collections.abc import MutableMapping
else:  # pragma: no cover
    from typing import MutableMapping  # pylint: disable=ungrouped-imports


__all__ = ["Value", "Backend", "Entry", "BackendError"]


class BackendError(RuntimeError):
    """
    Unsuccessful storage transaction. This is a very low-level error representing a system configuration issue.
    """


@dataclasses.dataclass(frozen=True)
class Entry:
    value: Value
    mutable: bool


class Backend(MutableMapping[str, Entry]):
    """
    Register backend interface implementing the :class:`MutableMapping` interface.
    The registers are ordered lexicographically by name.
    """

    @property
    @abc.abstractmethod
    def location(self) -> str:
        """
        The physical storage location for the data (e.g., file name).
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def persistent(self) -> bool:
        """
        An in-memory DB is reported as non-persistent.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def index(self, index: int) -> Optional[str]:
        """
        :returns: Name of the register at the specified index or None if the index is out of range.
            See ordering requirements in the class docs.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __setitem__(self, key: str, value: Union[Entry, Value]) -> None:
        """
        If the register does not exist, it is either created or nothing is done, depending on the implementation.
        If exists, it will be overwritten unconditionally with the specified value.
        Observe that the method accepts either :class:`Entry` or :class:`Value`.

        The value shall be of the same type as the register, the caller is responsible to ensure that
        (implementations may lift this restriction if the type can be changed).

        The mutability flag is ignored (it is intended mostly for the UAVCAN Register Interface, not for local use).
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, repr(self.location), persistent=self.persistent)
