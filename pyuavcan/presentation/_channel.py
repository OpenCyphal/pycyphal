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


Finalizer = typing.Callable[[], typing.Awaitable[None]]


class OutgoingTransferIDCounter:
    def __init__(self) -> None:
        self._value: int = 0

    def get_then_increment(self) -> int:
        out = self._value
        self._value += 1
        return out

    def override(self, value: int) -> None:
        self._value = int(value)


class Channel(abc.ABC, typing.Generic[DataTypeClass]):
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
    def __str__(self) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__str__()


class MessageChannel(Channel[MessageTypeClass]):
    @property
    @abc.abstractmethod
    def session(self) -> pyuavcan.transport.Session:
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        return ds.subject_id


class ServiceChannel(Channel[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def input_session(self) -> pyuavcan.transport.InputSession:
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.input_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.ServiceDataSpecifier)
        return ds.service_id


class Publisher(MessageChannel[MessageTypeClass]):
    def __init__(self,
                 cls:                 typing.Type[MessageTypeClass],
                 session:             pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 priority:            pyuavcan.transport.Priority,
                 finalizer:           Finalizer):
        self._cls = cls
        self._session = session
        self._priority = pyuavcan.transport.Priority(priority)
        self._transfer_id_counter = transfer_id_counter
        self._finalizer = finalizer

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def session(self) -> pyuavcan.transport.OutputSession:
        return self._session

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        return self._transfer_id_counter

    async def publish(self, message: MessageTypeClass) -> None:
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(message))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=self._priority,
                                               transfer_id=self._transfer_id_counter.get_then_increment(),
                                               fragmented_payload=fragmented_payload)
        await self._session.send(transfer)

    async def close(self) -> None:
        try:
            await self._session.close()
        finally:
            await self._finalizer()


class Subscriber(MessageChannel[MessageTypeClass]):
    def __init__(self,
                 cls:       typing.Type[MessageTypeClass],
                 session:   pyuavcan.transport.InputSession,
                 finalizer: Finalizer):
        self._cls = cls
        self._session = session
        self._finalizer = finalizer

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def session(self) -> pyuavcan.transport.InputSession:
        return self._session

    async def receive_with_metadata(self) \
            -> typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]:
        transfer: typing.Optional[pyuavcan.transport.TransferFrom] = None
        message: typing.Optional[MessageTypeClass] = None
        while message is None or transfer is None:
            transfer = await self._session.receive()
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            # TODO: if message is None, record deserialization error
        return message, transfer

    async def try_receive_with_metadata(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]]:
        transfer = await self._session.try_receive(monotonic_deadline)
        if transfer is not None:
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            if message is not None:
                return message, transfer
        return None

    async def receive(self) -> MessageTypeClass:
        return (await self.receive_with_metadata())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageTypeClass]:
        out = await self.try_receive_with_metadata(monotonic_deadline)
        return out[0] if out else None

    async def close(self) -> None:
        try:
            await self._session.close()
        finally:
            await self._finalizer()
