#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import copy
import typing
import logging
import pyuavcan
from .._frame import Frame
from ._base import SerialSession
from ._transfer_serializer import serialize_transfer


#: Returns the transmission timestamp.
SendHandler = typing.Callable[[typing.Iterable[Frame], float],
                              typing.Awaitable[typing.Optional[pyuavcan.transport.Timestamp]]]

_logger = logging.getLogger(__name__)


class SerialFeedback(pyuavcan.transport.Feedback):
    def __init__(self,
                 original_transfer_timestamp:        pyuavcan.transport.Timestamp,
                 first_frame_transmission_timestamp: pyuavcan.transport.Timestamp):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._first_frame_transmission_timestamp


class SerialOutputSession(SerialSession, pyuavcan.transport.OutputSession):
    """
    .. todo::
        We currently permit the following unconventional usages:
        1. Broadcast service request transfers (not responses though).
        2. Unicast message transfers.
        Decide whether we want to keep that later. Those can't be implemented on CAN bus, for example.
    """
    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 transport:        pyuavcan.transport.serial.SerialTransport,
                 send_handler:     SendHandler,
                 finalizer:        typing.Callable[[], None]):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_output_session`.
        """
        self._transport = transport
        self._send_handler = send_handler
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        self._statistics = pyuavcan.transport.Statistics()

        if isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier):
            is_response = specifier.data_specifier.role == pyuavcan.transport.ServiceDataSpecifier.Role.RESPONSE
            if is_response and specifier.remote_node_id is None:
                raise pyuavcan.transport.UnsupportedSessionConfigurationError(
                    f'Cannot broadcast a service response. Session specifier: {specifier}')

        super(SerialOutputSession, self).__init__(finalizer)

    async def send_until(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        self._raise_if_closed()

        frames = list(serialize_transfer(
            priority=transfer.priority,
            local_node_id=self._transport.local_node_id,
            session_specifier=self._specifier,
            data_type_hash=self._payload_metadata.data_type_hash,
            transfer_id=transfer.transfer_id,
            fragmented_payload=transfer.fragmented_payload,
            max_frame_payload_bytes=self._transport.single_frame_transfer_payload_capacity_bytes
        ))

        try:
            tx_timestamp = await self._send_handler(frames, monotonic_deadline)
        except Exception:
            self._statistics.errors += 1
            raise

        if tx_timestamp is not None:
            self._statistics.transfers += 1
            self._statistics.frames += len(frames)
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            if self._feedback_handler is not None:
                try:
                    self._feedback_handler(SerialFeedback(transfer.timestamp, tx_timestamp))
                except Exception as ex:  # pragma: no cover
                    _logger.exception(f'Unhandled exception in the output session feedback handler '
                                      f'{self._feedback_handler}: {ex}')
            return True
        else:
            self._statistics.drops += len(frames)
            return False

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        return copy.copy(self._statistics)

    def close(self) -> None:
        super(SerialOutputSession, self).close()
