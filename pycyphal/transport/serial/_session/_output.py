# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import copy
import typing
import logging
import pycyphal
from pycyphal.transport import ServiceDataSpecifier
from .._frame import SerialFrame
from ._base import SerialSession


#: Returns the transmission timestamp.
SendHandler = typing.Callable[
    [typing.List[SerialFrame], float], typing.Awaitable[typing.Optional[pycyphal.transport.Timestamp]]
]

_logger = logging.getLogger(__name__)


class SerialFeedback(pycyphal.transport.Feedback):
    def __init__(
        self,
        original_transfer_timestamp: pycyphal.transport.Timestamp,
        first_frame_transmission_timestamp: pycyphal.transport.Timestamp,
    ):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> pycyphal.transport.Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pycyphal.transport.Timestamp:
        return self._first_frame_transmission_timestamp


class SerialOutputSession(SerialSession, pycyphal.transport.OutputSession):
    def __init__(
        self,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        mtu: int,
        local_node_id: typing.Optional[int],
        send_handler: SendHandler,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pycyphal.transport.serial.SerialTransport.get_output_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._mtu = int(mtu)
        self._local_node_id = local_node_id
        self._send_handler = send_handler
        self._feedback_handler: typing.Optional[typing.Callable[[pycyphal.transport.Feedback], None]] = None
        self._statistics = pycyphal.transport.SessionStatistics()
        if self._local_node_id is None and isinstance(self._specifier.data_specifier, ServiceDataSpecifier):
            raise pycyphal.transport.OperationNotDefinedForAnonymousNodeError(
                f"Anonymous nodes cannot emit service transfers. Session specifier: {self._specifier}"
            )
        assert isinstance(self._local_node_id, int) or self._local_node_id is None
        assert callable(send_handler)
        assert (
            specifier.remote_node_id is not None if isinstance(specifier.data_specifier, ServiceDataSpecifier) else True
        ), "Internal protocol violation: cannot broadcast a service transfer"

        super().__init__(finalizer)

    async def send(self, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        self._raise_if_closed()

        def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> SerialFrame:
            if not end_of_transfer and self._local_node_id is None:
                raise pycyphal.transport.OperationNotDefinedForAnonymousNodeError(
                    f"Anonymous nodes cannot emit multi-frame transfers. Session specifier: {self._specifier}"
                )
            return SerialFrame(
                priority=transfer.priority,
                transfer_id=transfer.transfer_id,
                index=index,
                end_of_transfer=end_of_transfer,
                payload=payload,
                source_node_id=self._local_node_id,
                destination_node_id=self._specifier.remote_node_id,
                data_specifier=self._specifier.data_specifier,
            )

        frames = list(
            pycyphal.transport.commons.high_overhead_transport.serialize_transfer(
                transfer.fragmented_payload, self._mtu, construct_frame
            )
        )
        _logger.debug("%s: Sending transfer: %s; current stats: %s", self, transfer, self._statistics)
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
                    _logger.exception(
                        "Unhandled exception in the output session feedback handler %s: %s", self._feedback_handler, ex
                    )
            return True
        self._statistics.drops += len(frames)
        return False

    def enable_feedback(self, handler: typing.Callable[[pycyphal.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    @property
    def specifier(self) -> pycyphal.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pycyphal.transport.SessionStatistics:
        return copy.copy(self._statistics)

    def close(self) -> None:  # pylint: disable=useless-super-delegation
        super().close()
