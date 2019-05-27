#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import asyncio
import pyuavcan.transport
from . import _frame, _can_id, _transfer_serializer


Finalizer = typing.Callable[[], None]


class BaseSession:
    def __init__(self,
                 transport:      pyuavcan.transport.can.CANTransport,
                 finalizer:      Finalizer):
        self._transport = transport
        self._finalizer = finalizer
        self._media = transport.media


class BaseOutputSession(BaseSession):
    def __init__(self,
                 transport:      pyuavcan.transport.can.CANTransport,
                 finalizer:      Finalizer,
                 send_lock:      asyncio.Lock):
        self._send_lock = send_lock
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        super(BaseOutputSession, self).__init__(transport=transport,
                                                finalizer=finalizer)

    async def _do_send(self, can_identifier: int, transfer: pyuavcan.transport.Transfer) -> None:
        async with self._send_lock:
            await self._media.send(_transfer_serializer.serialize_transfer(
                can_identifier=can_identifier,
                transfer_id=transfer.transfer_id,
                fragmented_payload=transfer.fragmented_payload,
                max_data_field_length=self._media.max_data_field_length,
                loopback=self._feedback_handler is not None
            ))


class PromiscuousInputSession(BaseSession, pyuavcan.transport.PromiscuousInputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.TransferFrom:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        raise NotImplementedError


class SelectiveInputSession(BaseSession, pyuavcan.transport.SelectiveInputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.Transfer:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.Transfer]:
        raise NotImplementedError

    @property
    def source_node_id(self) -> int:
        raise NotImplementedError


class BroadcastOutputSession(BaseOutputSession, pyuavcan.transport.BroadcastOutputSession):
    def __init__(self,
                 transport:      pyuavcan.transport.can.CANTransport,
                 data_specifier: pyuavcan.transport.DataSpecifier,
                 finalizer:      Finalizer,
                 send_lock:      asyncio.Lock):
        if not isinstance(data_specifier, pyuavcan.transport.MessageDataSpecifier):
            raise ValueError(f'This transport does not support broadcast outputs for {data_specifier}')

        self._data_specifier: pyuavcan.transport.MessageDataSpecifier = data_specifier

        super(BroadcastOutputSession, self).__init__(transport=transport,
                                                     data_specifier=data_specifier,
                                                     finalizer=finalizer,
                                                     send_lock=send_lock)

    @property
    def data_specifier(self) -> pyuavcan.transport.MessageDataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        can_id = _can_id.MessageCANID(
            priority=transfer.priority,
            subject_id=self.data_specifier.subject_id,
            source_node_id=self._transport.local_node_id  # May be anonymous
        ).compile()
        await self._do_send(can_id, transfer)


class UnicastOutputSession(BaseOutputSession, pyuavcan.transport.UnicastOutputSession):
    def __init__(self,
                 transport:           pyuavcan.transport.can.CANTransport,
                 data_specifier:      pyuavcan.transport.DataSpecifier,
                 finalizer:           Finalizer,
                 send_lock:           asyncio.Lock,
                 destination_node_id: int):
        if not isinstance(data_specifier, pyuavcan.transport.ServiceDataSpecifier):
            raise ValueError(f'This transport does not support unicast outputs for {data_specifier}')

        self._data_specifier: pyuavcan.transport.ServiceDataSpecifier = data_specifier
        self._destination_node_id = int(destination_node_id)

        super(UnicastOutputSession, self).__init__(transport=transport,
                                                   data_specifier=data_specifier,
                                                   finalizer=finalizer,
                                                   send_lock=send_lock)

    @property
    def data_specifier(self) -> pyuavcan.transport.ServiceDataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        source_node_id = self._transport.local_node_id
        if source_node_id is None:
            raise pyuavcan.transport.InvalidTransportConfigurationError(
                'Cannot emit a service transfer because the local node is anonymous (does not have a node ID)')
        can_id = _can_id.ServiceCANID(
            priority=transfer.priority,
            service_id=self.data_specifier.service_id,
            request_not_response=self.data_specifier.role == pyuavcan.transport.ServiceDataSpecifier.Role.CLIENT,
            source_node_id=source_node_id,
            destination_node_id=self._destination_node_id
        ).compile()
        await self._do_send(can_id, transfer)

    @property
    def destination_node_id(self) -> int:
        return self._destination_node_id
