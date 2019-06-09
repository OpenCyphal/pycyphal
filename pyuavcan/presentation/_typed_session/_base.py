#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.dsdl
import pyuavcan.transport


DataTypeClass = typing.TypeVar('DataTypeClass', bound=pyuavcan.dsdl.CompositeObject)
MessageTypeClass = typing.TypeVar('MessageTypeClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceTypeClass = typing.TypeVar('ServiceTypeClass', bound=pyuavcan.dsdl.ServiceObject)


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


class MessageTypedSession(TypedSession[MessageTypeClass]):
    def __init__(self, dtype: typing.Type[MessageTypeClass]):
        self._dtype = dtype

    @property
    def dtype(self) -> typing.Type[MessageTypeClass]:
        return self._dtype

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
        return f'{type(self).__name__}(' \
            f'dtype={pyuavcan.dsdl.get_model(self.dtype)}, ' \
            f'transport_session={self.transport_session})'


class ServiceTypedSession(TypedSession[ServiceTypeClass]):
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
        return f'{type(self).__name__}(' \
            f'dtype={pyuavcan.dsdl.get_model(self.dtype)}, ' \
            f'input_transport_session={self.input_transport_session})'
