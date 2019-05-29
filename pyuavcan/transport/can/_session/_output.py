#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import asyncio
import logging
import dataclasses
import pyuavcan.transport
from .. import _frame, _can_id
from . import _base, _transfer_sender


_logger = logging.getLogger(__name__)


class Feedback(pyuavcan.transport.Feedback):
    def __init__(self,
                 original_transfer_timestamp: pyuavcan.transport.Timestamp,
                 start_of_transfer:           _frame.TimestampedUAVCANFrame):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._start_of_transfer = start_of_transfer
        assert self._start_of_transfer.start_of_transfer

    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._start_of_transfer.timestamp


@dataclasses.dataclass(frozen=True)
class _PendingFeedbackKey:
    can_identifier:      int
    transfer_id_modulus: int


class OutputSession(_base.Session):
    def __init__(self,
                 transport:  pyuavcan.transport.can.CANTransport,
                 media_lock: asyncio.Lock,
                 finalizer:  _base.Finalizer):
        self._transport = transport
        self._media = transport.media
        self._media_lock = media_lock
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None

        self._pending_feedback: typing.Dict[_PendingFeedbackKey, pyuavcan.transport.Timestamp] = {}

        super(OutputSession, self).__init__(finalizer=finalizer)

    def handle_loopback_frame(self, can_identifier: int, frame: _frame.TimestampedUAVCANFrame) -> None:
        if frame.start_of_transfer:
            key = _PendingFeedbackKey(can_identifier=can_identifier,
                                      transfer_id_modulus=frame.transfer_id)
            try:
                original_timestamp = self._pending_feedback.pop(key)
            except KeyError:
                _logger.debug('No pending feedback entry for ID 0x%08x frame %s', can_identifier, frame)
            else:
                if self._feedback_handler is not None:
                    feedback = Feedback(original_timestamp, frame)
                    try:
                        self._feedback_handler(feedback)
                    except Exception as ex:
                        _logger.exception(f'Unhandled exception in the output session feedback handler '
                                          f'{self._feedback_handler}: {ex}')

    async def _do_send(self, can_identifier: int, transfer: pyuavcan.transport.Transfer) -> None:
        async with self._media_lock:
            needs_feedback = self._feedback_handler is not None
            if needs_feedback:
                key = _PendingFeedbackKey(can_identifier=can_identifier,
                                          transfer_id_modulus=transfer.transfer_id % _frame.TRANSFER_ID_MODULO)
                try:
                    old = self._pending_feedback[key]
                except KeyError:
                    pass
                else:
                    _logger.warning('Overriding old feedback entry %s at key %s', old, key)

                self._pending_feedback[key] = transfer.timestamp

            await self._media.send(_transfer_sender.serialize_transfer(
                can_identifier=can_identifier,
                transfer_id=transfer.transfer_id,
                fragmented_payload=transfer.fragmented_payload,
                max_data_field_length=self._media.max_data_field_length,
                loopback=needs_feedback
            ))

    def _do_enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def _do_disable_feedback(self) -> None:
        self._feedback_handler = None
        self._pending_feedback.clear()


class BroadcastOutputSession(OutputSession, pyuavcan.transport.BroadcastOutputSession):
    def __init__(self,
                 data_specifier: pyuavcan.transport.DataSpecifier,
                 transport:      pyuavcan.transport.can.CANTransport,
                 media_lock:     asyncio.Lock,
                 finalizer:      _base.Finalizer):
        if not isinstance(data_specifier, pyuavcan.transport.MessageDataSpecifier):
            raise ValueError(f'This transport does not support broadcast outputs for {data_specifier}')
        self._data_specifier: pyuavcan.transport.MessageDataSpecifier = data_specifier

        super(BroadcastOutputSession, self).__init__(transport=transport,
                                                     media_lock=media_lock,
                                                     finalizer=finalizer)

    @property
    def data_specifier(self) -> pyuavcan.transport.MessageDataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._do_enable_feedback(handler)

    def disable_feedback(self) -> None:
        self._do_disable_feedback()

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        can_id = _can_id.MessageCANID(
            priority=transfer.priority,
            subject_id=self.data_specifier.subject_id,
            source_node_id=self._transport.local_node_id  # May be anonymous
        ).compile()

        await self._do_send(can_id, transfer)


class UnicastOutputSession(OutputSession, pyuavcan.transport.UnicastOutputSession):
    def __init__(self,
                 destination_node_id: int,
                 data_specifier:      pyuavcan.transport.DataSpecifier,
                 transport:           pyuavcan.transport.can.CANTransport,
                 media_lock:          asyncio.Lock,
                 finalizer:           _base.Finalizer):
        self._destination_node_id = int(destination_node_id)

        if not isinstance(data_specifier, pyuavcan.transport.ServiceDataSpecifier):
            raise ValueError(f'This transport does not support unicast outputs for {data_specifier}')
        self._data_specifier: pyuavcan.transport.ServiceDataSpecifier = data_specifier

        super(UnicastOutputSession, self).__init__(transport=transport,
                                                   media_lock=media_lock,
                                                   finalizer=finalizer)

    @property
    def data_specifier(self) -> pyuavcan.transport.ServiceDataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._do_enable_feedback(handler)

    def disable_feedback(self) -> None:
        self._do_disable_feedback()

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
