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
from .. import _aggregate_transport


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
class ReceivedMetadata:
    # We don't use TransferFrom for the sake of genericity - we want to support both promiscuous and selective
    # input sessions, which implies that the transfer may not have the source node ID. However, if we're using
    # a selective session, the source node ID will be possible to obtain from the session instance. Therefore,
    # the information we're looking for is always available but not from the same source.
    transfer:       pyuavcan.transport.Transfer     # The transfer which delivered the data.
    transport:      pyuavcan.transport.Transport    # The interface this transfer was received from.
    source_node_id: typing.Optional[int]            # None for anonymous transfers.


class TypedSession(abc.ABC, typing.Generic[DataTypeClass]):
    @property
    @abc.abstractmethod
    def data_type_class(self) -> typing.Type[DataTypeClass]:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class MessageSession(TypedSession[MessageTypeClass]):
    @property
    @abc.abstractmethod
    def aggregate_session(self) -> _aggregate_transport.AggregateSession:
        raise NotImplementedError


class ServiceSession(TypedSession[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def aggregate_input_session(self) -> _aggregate_transport.InputAggregateSession:
        raise NotImplementedError


class Publisher(MessageSession[MessageTypeClass]):
    def __init__(self,
                 cls:                 typing.Type[MessageTypeClass],
                 session:             _aggregate_transport.OutputAggregateSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 priority:            pyuavcan.transport.Priority,
                 loopback:            bool):
        self._cls = cls
        self._session = session
        self._priority = priority
        self._loopback = loopback
        self._transfer_id_counter = transfer_id_counter

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def aggregate_session(self) -> _aggregate_transport.OutputAggregateSession:
        return self._session

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
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(message))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=self._priority,
                                               transfer_id=self._transfer_id_counter.get_value_then_increment(),
                                               fragmented_payload=fragmented_payload,
                                               loopback=self._loopback)
        await self._session.send(transfer)

    async def close(self) -> None:
        raise NotImplementedError


class Subscriber(MessageSession[MessageTypeClass]):
    def __init__(self,
                 cls:            typing.Type[MessageTypeClass],
                 data_specifier: pyuavcan.transport.MessageDataSpecifier,
                 session:        _aggregate_transport.InputAggregateSession):
        self._cls = cls
        self._ds = data_specifier
        self._session = session

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def aggregate_session(self) -> _aggregate_transport.InputAggregateSession:
        return self._session

    async def receive_with_metadata(self) -> typing.Tuple[MessageTypeClass, ReceivedMetadata]:
        transfer: typing.Optional[pyuavcan.transport.Transfer] = None
        transport: typing.Optional[pyuavcan.transport.Transport] = None
        message: typing.Optional[MessageTypeClass] = None
        while message is None or transfer is None or transport is None:
            transfer, transport = await self._session.receive()
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            # TODO: if message is None, record deserialization error
        return message, self._construct_metadata(transfer, transport)

    async def try_receive_with_metadata(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, ReceivedMetadata]]:
        result = await self._session.try_receive(monotonic_deadline)
        if result is not None:
            transfer, transport = result
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            if message is not None:
                return message, self._construct_metadata(transfer, transport)
        return None

    async def receive(self) -> MessageTypeClass:
        return (await self.receive_with_metadata())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageTypeClass]:
        out = await self.try_receive_with_metadata(monotonic_deadline)
        return out[0] if out else None

    async def close(self) -> None:
        raise NotImplementedError

    def _construct_metadata(self,
                            transfer: pyuavcan.transport.Transfer,
                            transport: pyuavcan.transport.Transport) -> ReceivedMetadata:
        if isinstance(transfer, pyuavcan.transport.TransferFrom):
            source_node_id = transfer.source_node_id
        elif isinstance(self._session, _aggregate_transport.SelectiveInputAggregateSession):
            source_node_id = self._session.source_node_id
        else:
            raise RuntimeError('Impossible configuration: source node ID is not obtainable')
        return ReceivedMetadata(transfer=transfer,
                                transport=transport,
                                source_node_id=source_node_id)


class Client(ServiceSession[ServiceTypeClass]):
    def __init__(self,                # Not making assumptions about promiscuity or broadcasting. Shall be generic.
                 cls:                 typing.Type[ServiceTypeClass],
                 input_session:       _aggregate_transport.InputAggregateSession,
                 output_session:      _aggregate_transport.OutputAggregateSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 priority:            pyuavcan.transport.Priority):
        self._cls: typing.Type[pyuavcan.dsdl.ServiceObject] = cls
        self._input_session = input_session
        self._output_session = output_session
        self._priority = priority
        self._transfer_id_counter = transfer_id_counter

    @property
    def data_type_class(self) -> typing.Type[ServiceTypeClass]:
        return self._cls  # type: ignore

    @property
    def aggregate_input_session(self) -> _aggregate_transport.InputAggregateSession:
        return self._input_session

    @property
    def aggregate_output_session(self) -> _aggregate_transport.OutputAggregateSession:
        return self._output_session

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        return self._transfer_id_counter

    async def try_call_with_metadata(self,
                                     request: DataTypeClass,
                                     response_monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedMetadata]]:  # TODO: use proper types
        # TODO: THIS IS WRONG; MATCH THE RESPONSE BY TRANSFER ID; WE'RE GOING TO NEED A WORKER TASK!
        transfer_id = self._transfer_id_counter.get_value_then_increment()
        await self._do_send(request, transfer_id)
        return await self._try_receive(transfer_id, response_monotonic_deadline)

    async def try_call(self,
                       request: DataTypeClass,
                       response_monotonic_deadline: float) -> typing.Optional[DataTypeClass]:  # TODO: use proper types
        out = await self.try_call_with_metadata(request, response_monotonic_deadline)
        return out[0] if out else None

    async def _do_send(self, request: DataTypeClass, transfer_id: int) -> None:  # TODO: use proper types
        if not isinstance(request, self._cls.Request):
            raise ValueError(f'Expected an instance of {self._cls.Request}, found {type(request)} instead')
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(request))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=self._priority,
                                               transfer_id=transfer_id,
                                               fragmented_payload=fragmented_payload,
                                               loopback=False)
        await self._output_session.send(transfer)

    async def _try_receive(self, transfer_id: int, response_monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedMetadata]]:  # TODO: use proper types
        while time.monotonic() <= response_monotonic_deadline:
            # TODO: THIS IS WRONG; MATCH THE RESPONSE BY TRANSFER ID; WE'RE GOING TO NEED A WORKER TASK!
            result = await self._input_session.try_receive(response_monotonic_deadline)
            if result is not None:
                transfer, transport = result
                response = pyuavcan.dsdl.try_deserialize(self._cls.Response, transfer.fragmented_payload)
                if response is not None:
                    return response, self._construct_metadata(transfer, transport)
        return None     # Timed out

    async def close(self) -> None:
        raise NotImplementedError

    def _construct_metadata(self,
                            transfer: pyuavcan.transport.Transfer,
                            transport: pyuavcan.transport.Transport) -> ReceivedMetadata:
        if isinstance(transfer, pyuavcan.transport.TransferFrom):
            source_node_id = transfer.source_node_id
        elif isinstance(self._input_session, _aggregate_transport.SelectiveInputAggregateSession):
            source_node_id = self._input_session.source_node_id
        elif isinstance(self._output_session, _aggregate_transport.UnicastOutputAggregateSession):
            source_node_id = self._output_session.destination_node_id
        else:
            raise RuntimeError('Impossible configuration: server node ID is not obtainable')
        return ReceivedMetadata(transfer=transfer,
                                transport=transport,
                                source_node_id=source_node_id)


class Server(ServiceSession[ServiceTypeClass]):
    _OutputSessionFactory = typing.Callable[[int], typing.Awaitable[_aggregate_transport.OutputAggregateSession]]

    # TODO: use proper types!
    Handler = typing.Callable[[DataTypeClass, ReceivedMetadata], typing.Awaitable[typing.Optional[DataTypeClass]]]

    def __init__(self,
                 cls:                    typing.Type[ServiceTypeClass],
                 input_session:          _aggregate_transport.InputAggregateSession,
                 output_session_factory: _OutputSessionFactory):
        self._cls = cls
        self._input_session = input_session
        self._output_session_factory = output_session_factory
        self._output_session_cache: typing.Dict[int, _aggregate_transport.OutputAggregateSession] = {}

    @property
    def data_type_class(self) -> typing.Type[ServiceTypeClass]:
        return self._cls

    @property
    def aggregate_input_session(self) -> _aggregate_transport.InputAggregateSession:
        return self._input_session

    async def listen_forever(self, handler: Handler) -> None:
        while True:
            await self.listen_until(handler, time.monotonic() + 10.0 ** 10)

    async def listen_until(self, handler: Handler, monotonic_deadline: float) -> None:
        while time.monotonic() <= monotonic_deadline:
            # TODO: WHEN WE ARE AGGREGATING TRANSFERS WITH DIFFERENT TRANSFER ID MODULO SETTINGS, THE TRANSFER ID
            # TODO: VALUE OBTAINED FROM THE REQUEST TRANSFER MAY BE INCORRECT FOR SOME OF THE INTERFACES WHEN WE ARE
            # TODO: TRANSMITTING THE RESPONSE!
            result = await self._try_receive(monotonic_deadline)
            if result:
                request, meta = result
                response = await handler(request, meta)
                if response is not None:
                    await self._do_send(response,
                                        priority=meta.transfer.priority,
                                        transfer_id=meta.transfer.transfer_id,
                                        client_node_id=meta.source_node_id)

    async def close(self) -> None:
        raise NotImplementedError

    async def _try_receive(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedMetadata]]:  # TODO: use proper types
        while time.monotonic() <= monotonic_deadline:
            result = await self._input_session.try_receive(monotonic_deadline)
            if result is not None:
                transfer, transport = result
                response = pyuavcan.dsdl.try_deserialize(self._cls.Response, transfer.fragmented_payload)
                if response is not None:
                    return response, self._construct_metadata(transfer, transport)
        return None     # Timed out

    async def _do_send(self,
                       response: DataTypeClass,
                       priority: pyuavcan.transport.Priority,
                       transfer_id: int,
                       client_node_id: int) -> None:  # TODO: use proper types
        if not isinstance(response, self._cls.Response):
            raise ValueError(f'Expected an instance of {self._cls.Response}, found {type(response)} instead')
        output_session = await self._get_output_session(client_node_id)
        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(response))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=priority,
                                               transfer_id=transfer_id,
                                               fragmented_payload=fragmented_payload,
                                               loopback=False)
        await output_session.send(transfer)

    async def _get_output_session(self, client_node_id: int) -> _aggregate_transport.OutputAggregateSession:
        client_node_id = int(client_node_id)
        try:
            return self._output_session_cache[client_node_id]
        except KeyError:
            out = await self._output_session_factory(client_node_id)
            self._output_session_cache[client_node_id] = out
            return out

    def _construct_metadata(self,
                            transfer: pyuavcan.transport.Transfer,
                            transport: pyuavcan.transport.Transport) -> ReceivedMetadata:
        if isinstance(transfer, pyuavcan.transport.TransferFrom):
            source_node_id = transfer.source_node_id
        elif isinstance(self._input_session, _aggregate_transport.SelectiveInputAggregateSession):
            source_node_id = self._input_session.source_node_id
        else:
            raise RuntimeError('Impossible configuration: client node ID is not obtainable')
        return ReceivedMetadata(transfer=transfer,
                                transport=transport,
                                source_node_id=source_node_id)
