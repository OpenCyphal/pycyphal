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


DataTypeClass    = typing.TypeVar('DataTypeClass', bound=pyuavcan.dsdl.CompositeObject)
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
    def data_type_class(self) -> typing.Type[DataTypeClass]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def port_id(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


class MessageTypedSession(TypedSession[MessageTypeClass]):
    def __init__(self, dtype: typing.Type[MessageTypeClass]):
        self._dtype = dtype

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._dtype

    @property
    @abc.abstractmethod
    def transport_session(self) -> pyuavcan.transport.Session:
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        return ds.subject_id

    def __repr__(self) -> str:
        return f'{type(self).__name__}(' \
            f'dsdl_type={pyuavcan.dsdl.get_model(self.data_type_class)}, ' \
            f'transport_session={self.transport_session})'


class ServiceTypedSession(TypedSession[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.input_transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.ServiceDataSpecifier)
        return ds.service_id

    def __repr__(self) -> str:
        return f'{type(self).__name__}(' \
            f'dsdl_type={pyuavcan.dsdl.get_model(self.data_type_class)}, ' \
            f'input_transport_session={self.input_transport_session})'


class Publisher(MessageTypedSession[MessageTypeClass]):
    def __init__(self,
                 dtype:               typing.Type[MessageTypeClass],
                 transport_session:   pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 finalizer:           TypedSessionFinalizer):
        self._transport_session = transport_session
        self._transfer_id_counter = transfer_id_counter
        self._finalizer = finalizer
        super(Publisher, self).__init__(dtype=dtype)

    @property
    def transport_session(self) -> pyuavcan.transport.OutputSession:
        return self._transport_session

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        return self._transfer_id_counter

    async def publish(self, message:  MessageTypeClass, priority: pyuavcan.transport.Priority) -> None:
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(message))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=priority,
                                               transfer_id=self._transfer_id_counter.get_then_increment(),
                                               fragmented_payload=fragmented_payload)
        await self._transport_session.send(transfer)

    async def close(self) -> None:
        try:
            await self._transport_session.close()
        finally:
            await self._finalizer()


class Subscriber(MessageTypedSession[MessageTypeClass]):
    def __init__(self,
                 dtype:             typing.Type[MessageTypeClass],
                 transport_session: pyuavcan.transport.InputSession,
                 finalizer:         TypedSessionFinalizer):
        self._transport_session = transport_session
        self._finalizer = finalizer
        self._deserialization_failure_count = 0
        super(Subscriber, self).__init__(dtype=dtype)

    @property
    def transport_session(self) -> pyuavcan.transport.InputSession:
        return self._transport_session

    async def receive_with_metadata(self) \
            -> typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]:
        transfer: typing.Optional[pyuavcan.transport.TransferFrom] = None
        message: typing.Optional[MessageTypeClass] = None
        while message is None or transfer is None:
            transfer = await self._transport_session.receive()
            message = pyuavcan.dsdl.try_deserialize(self._dtype, transfer.fragmented_payload)
            if message is None:
                self._deserialization_failure_count += 1
        return message, transfer

    async def try_receive_with_metadata(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]]:
        transfer = await self._transport_session.try_receive(monotonic_deadline)
        if transfer is not None:
            message = pyuavcan.dsdl.try_deserialize(self._dtype, transfer.fragmented_payload)
            if message is not None:
                return message, transfer
            else:
                self._deserialization_failure_count += 1
        return None

    async def receive(self) -> MessageTypeClass:
        return (await self.receive_with_metadata())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageTypeClass]:
        out = await self.try_receive_with_metadata(monotonic_deadline)
        return out[0] if out else None

    async def close(self) -> None:
        try:
            await self._transport_session.close()
        finally:
            await self._finalizer()

    @property
    def deserialization_failure_count(self) -> int:
        return self._deserialization_failure_count
