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
from ._base import SerialSession
from .._frame import Frame


SendHandler = typing.Callable[[typing.Iterable[Frame], float], typing.Awaitable[bool]]

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
        super(SerialOutputSession, self).__init__(finalizer)

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    async def send_until(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        self._raise_if_closed()
        pass

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
