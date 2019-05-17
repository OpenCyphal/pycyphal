#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import time
import typing
import dataclasses
import pyuavcan.dsdl
import pyuavcan.transport
import pyuavcan.aggregate_transport


DataTypeClass    = typing.TypeVar('DataTypeClass', bound=pyuavcan.dsdl.CompositeObject)
MessageTypeClass = typing.TypeVar('MessageTypeClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceTypeClass = typing.TypeVar('ServiceTypeClass', bound=pyuavcan.dsdl.ServiceObject)


class OutgoingTransferIDCounter:
    def __init__(self) -> None:
        self._value: int = 0

    def get_value_then_increment(self) -> int:
        out = self._value
        self._value += 1
        return out

    def override_value(self, value: int) -> None:
        self._value = int(value)


@dataclasses.dataclass(frozen=True)
class ReceivedTransferMetadata:
    timestamp:      pyuavcan.transport.Timestamp
    priority:       pyuavcan.transport.Priority
    transfer_id:    int
    source_node_id: typing.Optional[int]        # Not set for anonymous transfers
    loopback:       bool

    @staticmethod
    def from_received_transfer(transfer: pyuavcan.transport.ReceivedTransfer) -> ReceivedTransferMetadata:
        return ReceivedTransferMetadata(timestamp=transfer.timestamp,
                                        priority=transfer.priority,
                                        transfer_id=transfer.transfer_id,
                                        source_node_id=transfer.source_node_id,
                                        loopback=transfer.loopback)


class TypedPort(abc.ABC, typing.Generic[DataTypeClass]):
    @property
    @abc.abstractmethod
    def data_type_class(self) -> typing.Type[DataTypeClass]:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class MessagePort(TypedPort[MessageTypeClass]):
    @property
    @abc.abstractmethod
    def subject_id(self) -> int:
        raise NotImplementedError


class ServicePort(TypedPort[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def service_id(self) -> int:
        raise NotImplementedError


class Publisher(MessagePort[MessageTypeClass]):
    def __init__(self,
                 cls:                 typing.Type[MessageTypeClass],
                 data_specifier:      pyuavcan.transport.MessageDataSpecifier,
                 port:                pyuavcan.aggregate_transport.AggregateOutputPort,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 priority:            pyuavcan.transport.Priority,
                 loopback:            bool):
        self._cls = cls
        self._ds = data_specifier
        self._port = port
        self._priority = priority
        self._loopback = loopback
        self._transfer_id_counter = transfer_id_counter

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def subject_id(self) -> int:
        return self._ds.subject_id

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def loopback(self) -> bool:
        return self._loopback

    @loopback.setter
    def loopback(self, value: bool) -> None:
        self._loopback = bool(value)

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        return self._transfer_id_counter

    async def publish(self, message: MessageTypeClass) -> None:
        fragmented_payload = list(pyuavcan.dsdl.serialize(message))
        transfer = pyuavcan.transport.OutgoingTransfer(priority=self._priority,
                                                       transfer_id=self._transfer_id_counter.get_value_then_increment(),
                                                       fragmented_payload=fragmented_payload,
                                                       loopback=self._loopback,
                                                       destination_node_id=None)
        await self._port.send(transfer)

    async def close(self) -> None:
        raise NotImplementedError


class Subscriber(MessagePort[MessageTypeClass]):
    def __init__(self,
                 cls:            typing.Type[MessageTypeClass],
                 data_specifier: pyuavcan.transport.MessageDataSpecifier,
                 port:           pyuavcan.aggregate_transport.AggregateInputPort):
        self._cls = cls
        self._ds = data_specifier
        self._port = port

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def subject_id(self) -> int:
        return self._ds.subject_id

    async def receive(self) -> typing.Tuple[MessageTypeClass, ReceivedTransferMetadata]:
        transfer: typing.Optional[pyuavcan.transport.ReceivedTransfer] = None
        message: typing.Optional[MessageTypeClass] = None
        while message is None or transfer is None:
            transfer = await self._port.receive()
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
        return message, ReceivedTransferMetadata.from_received_transfer(transfer)

    async def try_receive(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, ReceivedTransferMetadata]]:
        transfer = await self._port.try_receive(monotonic_deadline)
        if transfer is not None:
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            if message is not None:
                return message, ReceivedTransferMetadata.from_received_transfer(transfer)
        return None

    async def close(self) -> None:
        raise NotImplementedError


class Client(ServicePort[ServiceTypeClass]):
    def __init__(self,
                 cls:                 typing.Type[ServiceTypeClass],
                 data_specifier:      pyuavcan.transport.ServiceDataSpecifier,
                 input_port:          pyuavcan.aggregate_transport.AggregateInputPort,
                 output_port:         pyuavcan.aggregate_transport.AggregateOutputPort,
                 transfer_id_counter: OutgoingTransferIDCounter,   # TODO Must be a map!
                 priority:            pyuavcan.transport.Priority):
        self._cls: typing.Type[pyuavcan.dsdl.ServiceObject] = cls
        self._ds = data_specifier
        self._input_port = input_port
        self._output_port = output_port
        self._priority = priority
        self._transfer_id_counter = transfer_id_counter

    @property
    def data_type_class(self) -> typing.Type[ServiceTypeClass]:
        return self._cls  # type: ignore

    @property
    def service_id(self) -> int:
        return self._ds.service_id

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        return self._transfer_id_counter

    async def try_call(self,
                       server_node_id: int,
                       request: DataTypeClass,
                       response_monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedTransferMetadata]]:  # TODO: use proper types
        await self._do_send(server_node_id, request)
        return await self._try_receive(response_monotonic_deadline)

    async def _do_send(self, server_node_id: int, request: DataTypeClass) -> None:  # TODO: use proper types
        if not isinstance(request, self._cls.Request):
            raise ValueError(f'Expected an instance of {self._cls.Request}, found {type(request)} instead')
        fragmented_payload = list(pyuavcan.dsdl.serialize(request))
        transfer = pyuavcan.transport.OutgoingTransfer(priority=self._priority,
                                                       transfer_id=self._transfer_id_counter.get_value_then_increment(),
                                                       fragmented_payload=fragmented_payload,
                                                       loopback=False,
                                                       destination_node_id=server_node_id)
        await self._output_port.send(transfer)

    async def _try_receive(self, response_monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedTransferMetadata]]:  # TODO: use proper types
        # TODO: matching by server node ID and transfer ID with backlog
        while time.monotonic() <= response_monotonic_deadline:
            transfer = await self._input_port.try_receive(response_monotonic_deadline)
            if transfer is not None:
                response = pyuavcan.dsdl.try_deserialize(self._cls.Response, transfer.fragmented_payload)
                if response is not None:
                    return response, ReceivedTransferMetadata.from_received_transfer(transfer)  # type: ignore
        return None     # Timed out

    async def close(self) -> None:
        raise NotImplementedError


class Server(ServicePort[ServiceTypeClass]):
    def __init__(self,
                 cls:            typing.Type[ServiceTypeClass],
                 data_specifier: pyuavcan.transport.ServiceDataSpecifier,
                 input_port:     pyuavcan.aggregate_transport.AggregateInputPort,
                 output_port:    pyuavcan.aggregate_transport.AggregateOutputPort):
        self._cls = cls
        self._ds = data_specifier
        self._input_port = input_port
        self._output_port = output_port

    @property
    def data_type_class(self) -> typing.Type[ServiceTypeClass]:
        return self._cls

    @property
    def service_id(self) -> int:
        return self._ds.service_id

    # TODO: use proper types
    async def listen(self) -> typing.Tuple[DataTypeClass, ReceivedTransferMetadata]:
        raise NotImplementedError

    # TODO: use proper types
    async def try_listen(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedTransferMetadata]]:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
