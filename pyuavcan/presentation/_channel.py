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
    transfer: pyuavcan.transport.Transfer   # The transfer which delivered the data. Specialized per transport.


@dataclasses.dataclass(frozen=True)
class ReceivedMessageMetadata(ReceivedMetadata):
    source_node_id: typing.Optional[int]            # None for anonymous transfers.


@dataclasses.dataclass(frozen=True)
class ReceivedServiceMetadata(ReceivedMetadata):
    source_node_id: int                             # Always populated.


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


class MessageChannel(Channel[MessageTypeClass]):
    @property
    @abc.abstractmethod
    def session(self) -> pyuavcan.transport.Session:
        raise NotImplementedError


class ServiceChannel(Channel[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def input_session(self) -> pyuavcan.transport.InputSession:
        raise NotImplementedError


class Publisher(MessageChannel[MessageTypeClass]):
    def __init__(self,
                 cls:                 typing.Type[MessageTypeClass],
                 session:             pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 priority:            pyuavcan.transport.Priority):
        self._cls = cls
        self._session = session
        self._priority = priority
        self._transfer_id_counter = transfer_id_counter

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
                                               transfer_id=self._transfer_id_counter.get_value_then_increment(),
                                               fragmented_payload=fragmented_payload)
        await self._session.send(transfer)

    async def close(self) -> None:
        raise NotImplementedError


class Subscriber(MessageChannel[MessageTypeClass]):
    def __init__(self,
                 cls:            typing.Type[MessageTypeClass],
                 data_specifier: pyuavcan.transport.MessageDataSpecifier,
                 session:        pyuavcan.transport.InputSession):
        self._cls = cls
        self._ds = data_specifier
        self._session = session

    @property
    def data_type_class(self) -> typing.Type[MessageTypeClass]:
        return self._cls

    @property
    def session(self) -> pyuavcan.transport.InputSession:
        return self._session

    async def receive_with_metadata(self) -> typing.Tuple[MessageTypeClass, ReceivedMessageMetadata]:
        transfer: typing.Optional[pyuavcan.transport.Transfer] = None
        message: typing.Optional[MessageTypeClass] = None
        while message is None or transfer is None:
            transfer = await self._session.receive()
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            # TODO: if message is None, record deserialization error
        return message, self._construct_metadata(transfer)

    async def try_receive_with_metadata(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, ReceivedMessageMetadata]]:
        transfer = await self._session.try_receive(monotonic_deadline)
        if transfer is not None:
            message = pyuavcan.dsdl.try_deserialize(self._cls, transfer.fragmented_payload)
            if message is not None:
                return message, self._construct_metadata(transfer)
        return None

    async def receive(self) -> MessageTypeClass:
        return (await self.receive_with_metadata())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageTypeClass]:
        out = await self.try_receive_with_metadata(monotonic_deadline)
        return out[0] if out else None

    async def close(self) -> None:
        raise NotImplementedError

    def _construct_metadata(self, transfer: pyuavcan.transport.Transfer) -> ReceivedMessageMetadata:
        if isinstance(transfer, pyuavcan.transport.TransferFrom):
            source_node_id = transfer.source_node_id
        elif isinstance(self._session, pyuavcan.transport.SelectiveInput):
            source_node_id = self._session.source_node_id
        else:
            raise RuntimeError('Impossible configuration: source node ID is not obtainable')
        return ReceivedMessageMetadata(transfer=transfer, source_node_id=source_node_id)


class Client(ServiceChannel[ServiceTypeClass]):
    def __init__(self,                # Not making assumptions about promiscuity or broadcasting. Shall be generic.
                 cls:                 typing.Type[ServiceTypeClass],
                 input_session:       pyuavcan.transport.InputSession,
                 output_session:      pyuavcan.transport.OutputSession,
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
    def input_session(self) -> pyuavcan.transport.InputSession:
        return self._input_session

    @property
    def output_session(self) -> pyuavcan.transport.OutputSession:
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
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedServiceMetadata]]:  # TODO: use proper types
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
                                               fragmented_payload=fragmented_payload)
        await self._output_session.send(transfer)

    async def _try_receive(self, transfer_id: int, response_monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedServiceMetadata]]:  # TODO: use proper types
        while time.monotonic() <= response_monotonic_deadline:
            # TODO: THIS IS WRONG; MATCH THE RESPONSE BY TRANSFER ID; WE'RE GOING TO NEED A WORKER TASK!
            transfer = await self._input_session.try_receive(response_monotonic_deadline)
            if transfer is not None:
                response = pyuavcan.dsdl.try_deserialize(self._cls.Response, transfer.fragmented_payload)
                if response is not None:
                    return response, self._construct_metadata(transfer)  # type: ignore
        return None     # Timed out

    async def close(self) -> None:
        raise NotImplementedError

    def _construct_metadata(self, transfer: pyuavcan.transport.Transfer) -> ReceivedServiceMetadata:
        if isinstance(transfer, pyuavcan.transport.TransferFrom) and transfer.source_node_id is not None:
            source_node_id = transfer.source_node_id
        elif isinstance(self._input_session, pyuavcan.transport.SelectiveInput):
            source_node_id = self._input_session.source_node_id
        elif isinstance(self._output_session, pyuavcan.transport.UnicastOutput):
            source_node_id = self._output_session.destination_node_id
        else:
            raise RuntimeError('Impossible configuration: server node ID is not obtainable')
        assert source_node_id is not None
        return ReceivedServiceMetadata(transfer=transfer, source_node_id=source_node_id)


class Server(ServiceChannel[ServiceTypeClass]):
    _OutputSessionFactory = typing.Callable[[int], typing.Awaitable[pyuavcan.transport.OutputSession]]

    # TODO: use proper types!
    Handler = typing.Callable[[DataTypeClass, ReceivedServiceMetadata],
                              typing.Awaitable[typing.Optional[DataTypeClass]]]

    def __init__(self,
                 cls:                    typing.Type[ServiceTypeClass],
                 input_session:          pyuavcan.transport.InputSession,
                 output_session_factory: _OutputSessionFactory):
        self._cls = cls
        self._input_session = input_session
        self._output_session_factory = output_session_factory
        self._output_session_cache: typing.Dict[int, pyuavcan.transport.OutputSession] = {}

    @property
    def data_type_class(self) -> typing.Type[ServiceTypeClass]:
        return self._cls

    @property
    def input_session(self) -> pyuavcan.transport.InputSession:
        return self._input_session

    async def listen_forever(self, handler: Handler) -> None:  # type: ignore
        while True:
            await self.listen_until(handler, time.monotonic() + 10.0 ** 10)

    async def listen_until(self, handler: Handler, monotonic_deadline: float) -> None:  # type: ignore
        while time.monotonic() <= monotonic_deadline:
            # TODO: WHEN WE ARE AGGREGATING TRANSFERS WITH DIFFERENT TRANSFER ID MODULO SETTINGS, THE TRANSFER ID
            # TODO: VALUE OBTAINED FROM THE REQUEST TRANSFER MAY BE INCORRECT FOR SOME OF THE INTERFACES WHEN WE ARE
            # TODO: TRANSMITTING THE RESPONSE!
            result = await self._try_receive(monotonic_deadline)
            if result:
                request, meta = result  # type: ignore
                response = await handler(request, meta)
                if response is not None:
                    await self._do_send(response,
                                        priority=meta.transfer.priority,
                                        transfer_id=meta.transfer.transfer_id,
                                        client_node_id=meta.source_node_id)

    async def close(self) -> None:
        raise NotImplementedError

    async def _try_receive(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[DataTypeClass, ReceivedServiceMetadata]]:  # TODO: use proper types
        while time.monotonic() <= monotonic_deadline:
            transfer = await self._input_session.try_receive(monotonic_deadline)
            if transfer is not None:
                response = pyuavcan.dsdl.try_deserialize(self._cls.Response, transfer.fragmented_payload)
                if response is not None:
                    meta = self._try_construct_metadata(transfer)       # TODO: log error if failed
                    if meta is not None:
                        return response, meta  # type: ignore
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
                                               fragmented_payload=fragmented_payload)
        await output_session.send(transfer)

    async def _get_output_session(self, client_node_id: int) -> pyuavcan.transport.OutputSession:
        client_node_id = int(client_node_id)
        try:
            return self._output_session_cache[client_node_id]
        except KeyError:
            out = await self._output_session_factory(client_node_id)
            self._output_session_cache[client_node_id] = out
            return out

    def _try_construct_metadata(self, transfer: pyuavcan.transport.Transfer) \
            -> typing.Optional[ReceivedServiceMetadata]:
        if isinstance(transfer, pyuavcan.transport.TransferFrom) and transfer.source_node_id is not None:
            source_node_id = transfer.source_node_id
        elif isinstance(self._input_session, pyuavcan.transport.SelectiveInput):
            source_node_id = self._input_session.source_node_id
        else:
            return None
        assert isinstance(source_node_id, int)
        return ReceivedServiceMetadata(transfer=transfer, source_node_id=source_node_id)
