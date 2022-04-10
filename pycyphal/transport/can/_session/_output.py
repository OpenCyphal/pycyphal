# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import copy
import typing
import logging
import dataclasses
import pycyphal.transport
from pycyphal.transport import Timestamp
from .._frame import CyphalFrame, TRANSFER_ID_MODULO
from .._identifier import CANID, MessageCANID, ServiceCANID
from ._base import CANSession, SessionFinalizer
from ._transfer_sender import serialize_transfer


@dataclasses.dataclass(frozen=True)
class SendTransaction:
    frames: typing.List[CyphalFrame]
    loopback_first: bool
    monotonic_deadline: float


SendHandler = typing.Callable[[SendTransaction], typing.Awaitable[bool]]

_logger = logging.getLogger(__name__)


class CANFeedback(pycyphal.transport.Feedback):
    def __init__(self, original_transfer_timestamp: Timestamp, first_frame_transmission_timestamp: Timestamp):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> Timestamp:
        return self._first_frame_transmission_timestamp


@dataclasses.dataclass(frozen=True)
class _PendingFeedbackKey:
    compiled_identifier: int
    transfer_id_modulus: int


# noinspection PyAbstractClass
class CANOutputSession(CANSession, pycyphal.transport.OutputSession):
    """
    This is actually an abstract class, but its concrete inheritors are hidden from the API.
    The implementation is chosen according to the type of the session requested: broadcast or unicast.
    """

    def __init__(
        self,
        transport: pycyphal.transport.can.CANTransport,
        send_handler: SendHandler,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        finalizer: SessionFinalizer,
    ):
        """Use the factory method."""
        self._transport = transport
        self._send_handler = send_handler
        self._specifier = specifier
        self._payload_metadata = payload_metadata

        self._feedback_handler: typing.Optional[typing.Callable[[pycyphal.transport.Feedback], None]] = None
        self._pending_feedback: typing.Dict[_PendingFeedbackKey, Timestamp] = {}

        self._statistics = pycyphal.transport.SessionStatistics()

        super().__init__(finalizer=finalizer)

    def _handle_loopback_frame(self, timestamp: Timestamp, frame: CyphalFrame) -> None:
        """
        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        if frame.start_of_transfer:
            key = _PendingFeedbackKey(compiled_identifier=frame.identifier, transfer_id_modulus=frame.transfer_id)
            try:
                original_timestamp = self._pending_feedback.pop(key)
            except KeyError:
                pass  # Do not log this because packet capture mode generates a lot of unattended loopback frames.
            else:
                if self._feedback_handler is not None:
                    feedback = CANFeedback(original_timestamp, timestamp)
                    try:
                        self._feedback_handler(feedback)
                    except Exception as ex:  # pragma: no cover
                        _logger.exception(
                            "%s: Unhandled exception in the output session feedback handler %s: %s",
                            self,
                            self._feedback_handler,
                            ex,
                        )

    @property
    def specifier(self) -> pycyphal.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def enable_feedback(self, handler: typing.Callable[[pycyphal.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None
        self._pending_feedback.clear()

    def sample_statistics(self) -> pycyphal.transport.SessionStatistics:
        return copy.copy(self._statistics)

    def close(self) -> None:  # pylint: disable=useless-super-delegation
        super().close()

    async def _do_send(self, can_id: CANID, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        self._raise_if_closed()

        # Decompose the outgoing transfer into individual CAN frames.
        compiled_identifier = can_id.compile(transfer.fragmented_payload)
        tid_mod = transfer.transfer_id % TRANSFER_ID_MODULO  # https://github.com/OpenCyphal/pycyphal/issues/120
        frames = list(
            serialize_transfer(
                compiled_identifier=compiled_identifier,
                transfer_id=tid_mod,
                fragmented_payload=transfer.fragmented_payload,
                max_frame_payload_bytes=self._transport.protocol_parameters.mtu,
            )
        )

        # Ensure we're not trying to emit a multi-frame anonymous transfer - that's illegal.
        if can_id.source_node_id is None and len(frames) > 1:
            raise pycyphal.transport.OperationNotDefinedForAnonymousNodeError(
                f"Anonymous nodes cannot emit multi-frame transfers. CANID: {can_id}, transfer: {transfer}"
            )

        # If a loopback was requested, register it in the pending loopback registry.
        loopback_first_frame = self._feedback_handler is not None
        if loopback_first_frame:
            key = _PendingFeedbackKey(compiled_identifier=compiled_identifier, transfer_id_modulus=tid_mod)
            try:
                old = self._pending_feedback[key]
            except KeyError:
                pass
            else:
                self._statistics.errors += 1
                _logger.warning("%s: Overriding old feedback entry %s at key %s", self, old, key)
            self._pending_feedback[key] = transfer.timestamp

        # Emit the frames and update the statistical counters.
        try:
            transaction = SendTransaction(
                frames=frames, loopback_first=loopback_first_frame, monotonic_deadline=monotonic_deadline
            )
            if await self._send_handler(transaction):
                self._statistics.transfers += 1
                self._statistics.frames += len(frames)
                self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))  # Session level
                return True
            self._statistics.drops += len(frames)
            return False
        except Exception:
            self._statistics.errors += 1
            raise


class BroadcastCANOutputSession(CANOutputSession):
    def __init__(
        self,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        transport: pycyphal.transport.can.CANTransport,
        send_handler: SendHandler,
        finalizer: SessionFinalizer,
    ):
        """Use the factory method."""
        assert specifier.remote_node_id is None, "Internal protocol violation: expected broadcast"
        if not isinstance(specifier.data_specifier, pycyphal.transport.MessageDataSpecifier):
            raise pycyphal.transport.UnsupportedSessionConfigurationError(
                f"This transport does not support broadcast outputs for {specifier.data_specifier}"
            )
        self._subject_id = specifier.data_specifier.subject_id

        super().__init__(
            transport=transport,
            send_handler=send_handler,
            specifier=specifier,
            payload_metadata=payload_metadata,
            finalizer=finalizer,
        )

    async def send(self, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        can_id = MessageCANID(
            priority=transfer.priority,
            subject_id=self._subject_id,
            source_node_id=self._transport.local_node_id,  # May be anonymous
        )
        return await self._do_send(can_id, transfer, monotonic_deadline)


class UnicastCANOutputSession(CANOutputSession):
    def __init__(
        self,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        transport: pycyphal.transport.can.CANTransport,
        send_handler: SendHandler,
        finalizer: SessionFinalizer,
    ):
        """Use the factory method."""
        assert isinstance(specifier.remote_node_id, int), "Internal protocol violation: expected unicast"
        self._destination_node_id = int(specifier.remote_node_id)
        if not isinstance(specifier.data_specifier, pycyphal.transport.ServiceDataSpecifier):
            raise pycyphal.transport.UnsupportedSessionConfigurationError(
                f"This transport does not support unicast outputs for {specifier.data_specifier}"
            )
        if transport.local_node_id is None:
            raise pycyphal.transport.OperationNotDefinedForAnonymousNodeError(
                "Cannot emit service transfers because the local node is anonymous (does not have a node-ID)"
            )
        self._service_id = specifier.data_specifier.service_id
        self._request_not_response = (
            specifier.data_specifier.role == pycyphal.transport.ServiceDataSpecifier.Role.REQUEST
        )

        super().__init__(
            transport=transport,
            send_handler=send_handler,
            specifier=specifier,
            payload_metadata=payload_metadata,
            finalizer=finalizer,
        )

    async def send(self, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        source_node_id = self._transport.local_node_id
        assert source_node_id is not None, "Internal logic error"
        can_id = ServiceCANID(
            priority=transfer.priority,
            service_id=self._service_id,
            request_not_response=self._request_not_response,
            source_node_id=source_node_id,
            destination_node_id=self._destination_node_id,
        )
        return await self._do_send(can_id, transfer, monotonic_deadline)
