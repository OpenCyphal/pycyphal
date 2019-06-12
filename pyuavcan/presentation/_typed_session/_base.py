#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport


# This value is not mandated by the Specification.
DEFAULT_PRIORITY = pyuavcan.transport.Priority.NOMINAL


TypedSessionFinalizer = typing.Callable[[typing.Iterable[pyuavcan.transport.Session]], typing.Awaitable[None]]


TypeClass = typing.TypeVar('TypeClass', bound=pyuavcan.dsdl.CompositeObject)
MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)


class OutgoingTransferIDCounter:
    def __init__(self) -> None:
        self._value: int = 0

    def get_then_increment(self) -> int:
        out = self._value
        self._value += 1
        return out

    def override(self, value: int) -> None:
        self._value = int(value)


class TypedSession(abc.ABC, typing.Generic[TypeClass]):
    @property
    @abc.abstractmethod
    def dtype(self) -> typing.Type[TypeClass]:
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
        Invalidates the object and closes the underlying resources if necessary.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


class MessageTypedSession(TypedSession[MessageClass]):
    @property
    @abc.abstractmethod
    def transport_session(self) -> pyuavcan.transport.Session:
        """
        The underlying transport session instance.
        """
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        return ds.subject_id

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self,
                                             dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                             transport_session=self.transport_session)


class ServiceTypedSession(TypedSession[ServiceClass]):
    @property
    @abc.abstractmethod
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        """
        The underlying transport session instance used for the input transfers (requests for servers, responses
        for clients).
        """
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.input_transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.ServiceDataSpecifier)
        return ds.service_id

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self,
                                             dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                             input_transport_session=self.input_transport_session)
