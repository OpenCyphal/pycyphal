#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import copy
import typing
import logging
import itertools
import dataclasses
import pyuavcan.transport
from .. import _frame, _identifier
from . import _base, _transfer_sender


SendHandler = typing.Callable[[typing.Iterable[_frame.UAVCANFrame]], typing.Awaitable[None]]

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
    compiled_identifier: int
    transfer_id_modulus: int


class CANOutputSession(_base.CANSession, pyuavcan.transport.OutputSession):
    def __init__(self,
                 transport:        pyuavcan.transport.can.CANTransport,
                 send_handler:     SendHandler,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 finalizer:        _base.SessionFinalizer):
        self._transport = transport
        self._send_handler = send_handler
        self._specifier = specifier
        self._payload_metadata = payload_metadata

        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        self._pending_feedback: typing.Dict[_PendingFeedbackKey, pyuavcan.transport.Timestamp] = {}

        self._statistics = pyuavcan.transport.Statistics()

        super(CANOutputSession, self).__init__(finalizer=finalizer)

    def handle_loopback_frame(self, frame: _frame.TimestampedUAVCANFrame) -> None:
        assert frame.loopback, 'Internal API misuse'
        if frame.start_of_transfer:
            key = _PendingFeedbackKey(compiled_identifier=frame.identifier, transfer_id_modulus=frame.transfer_id)
            try:
                original_timestamp = self._pending_feedback.pop(key)
            except KeyError:
                _logger.debug('No pending feedback entry for frame: %s', frame)
            else:
                if self._feedback_handler is not None:  # pragma: no cover
                    feedback = Feedback(original_timestamp, frame)
                    try:
                        self._feedback_handler(feedback)
                    except Exception as ex:  # pragma: no cover
                        _logger.exception(f'Unhandled exception in the output session feedback handler '
                                          f'{self._feedback_handler}: {ex}')

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None
        self._pending_feedback.clear()

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        return copy.copy(self._statistics)

    def close(self) -> None:
        super(CANOutputSession, self).close()

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def _do_send(self, can_id: _identifier.CANID, transfer: pyuavcan.transport.Transfer) -> None:
        self._raise_if_closed()

        # Decompose the outgoing transfer into individual CAN frames
        frames, auxiliary_iter = itertools.tee(_transfer_sender.serialize_transfer(
            compiled_identifier=can_id.compile(transfer.fragmented_payload),
            transfer_id=transfer.transfer_id,
            fragmented_payload=transfer.fragmented_payload,
            max_frame_payload_bytes=self._transport.frame_payload_capacity,
            loopback_first_frame=self._feedback_handler is not None
        ))
        first_frame = next(auxiliary_iter)
        num_frames = 1 + sum(1 for _ in auxiliary_iter)
        assert num_frames > 0
        del auxiliary_iter

        # Ensure we're not trying to emit a multi-frame anonymous transfer - that's illegal
        if can_id.source_node_id is None and num_frames > 1:
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                f'Anonymous nodes cannot emit multi-frame transfers. CANID: {can_id}, transfer: {transfer}')

        # If a loopback was requested, register it in the pending loopback registry
        if first_frame.loopback:
            key = _PendingFeedbackKey(compiled_identifier=first_frame.identifier,
                                      transfer_id_modulus=first_frame.transfer_id)
            try:
                old = self._pending_feedback[key]
            except KeyError:
                pass
            else:
                self._statistics.errors += 1
                _logger.warning('Overriding old feedback entry %s at key %s', old, key)

            self._pending_feedback[key] = transfer.timestamp

        # Emit the frames and update the statistical counters
        try:
            await self._send_handler(frames)
        except Exception:
            self._statistics.errors += 1
            raise
        else:
            self._statistics.transfers += 1
            self._statistics.frames += num_frames
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))  # Session level, not transport


class BroadcastCANOutputSession(CANOutputSession):
    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 transport:        pyuavcan.transport.can.CANTransport,
                 send_handler:     SendHandler,
                 finalizer:        _base.SessionFinalizer):
        assert specifier.remote_node_id is None, 'Internal protocol violation: expected broadcast'
        if not isinstance(specifier.data_specifier, pyuavcan.transport.MessageDataSpecifier):
            raise pyuavcan.transport.UnsupportedSessionConfigurationError(
                f'This transport does not support broadcast outputs for {specifier.data_specifier}')
        self._subject_id = specifier.data_specifier.subject_id

        super(BroadcastCANOutputSession, self).__init__(transport=transport,
                                                        send_handler=send_handler,
                                                        specifier=specifier,
                                                        payload_metadata=payload_metadata,
                                                        finalizer=finalizer)

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        can_id = _identifier.MessageCANID(
            priority=transfer.priority,
            subject_id=self._subject_id,
            source_node_id=self._transport.local_node_id  # May be anonymous
        )

        await self._do_send(can_id, transfer)


class UnicastCANOutputSession(CANOutputSession):
    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 transport:        pyuavcan.transport.can.CANTransport,
                 send_handler:     SendHandler,
                 finalizer:        _base.SessionFinalizer):
        assert isinstance(specifier.remote_node_id, int), 'Internal protocol violation: expected unicast'
        self._destination_node_id = int(specifier.remote_node_id)
        if not isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier):
            raise pyuavcan.transport.UnsupportedSessionConfigurationError(
                f'This transport does not support unicast outputs for {specifier.data_specifier}')
        self._service_id = specifier.data_specifier.service_id
        self._request_not_response = \
            specifier.data_specifier.role == pyuavcan.transport.ServiceDataSpecifier.Role.CLIENT

        super(UnicastCANOutputSession, self).__init__(transport=transport,
                                                      send_handler=send_handler,
                                                      specifier=specifier,
                                                      payload_metadata=payload_metadata,
                                                      finalizer=finalizer)

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        source_node_id = self._transport.local_node_id
        if source_node_id is None:
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                'Cannot emit a service transfer because the local node is anonymous (does not have a node ID)')

        can_id = _identifier.ServiceCANID(
            priority=transfer.priority,
            service_id=self._service_id,
            request_not_response=self._request_not_response,
            source_node_id=source_node_id,
            destination_node_id=self._destination_node_id
        )
        await self._do_send(can_id, transfer)
