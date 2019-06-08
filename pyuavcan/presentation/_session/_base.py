#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.transport


DataTypeClass = typing.TypeVar('DataTypeClass', bound=pyuavcan.dsdl.CompositeObject)


TypedSessionFinalizer = typing.Callable[[], typing.Awaitable[None]]


class OutgoingTransferIDCounter:
    def __init__(self) -> None:
        self._value: int = 0

    def get_then_increment(self) -> int:
        out = self._value
        self._value += 1
        return out

    def override(self, value: int) -> None:
        self._value = int(value)


class TypedSession(abc.ABC, typing.Generic[DataTypeClass]):
    @property
    @abc.abstractmethod
    def dtype(self) -> typing.Type[DataTypeClass]:
        """
        The generated Python class modeling the corresponding DSDL data type.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def port_id(self) -> int:
        """
        The subject/service ID of the underlying transport session instance.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        """
        Invalidates the object and closes the underlying transport session instance.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError
