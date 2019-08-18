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
                 specifier:                  pyuavcan.transport.SessionSpecifier,
                 payload_metadata:           pyuavcan.transport.PayloadMetadata,
                 sft_payload_capacity_bytes: int,
                 local_node_id_accessor:     typing.Callable[[], typing.Optional[int]],
                 send_handler:               SendHandler,
                 finalizer:                  typing.Callable[[], None]):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_output_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._sft_payload_capacity_bytes = int(sft_payload_capacity_bytes)
        self._local_node_id_accessor = local_node_id_accessor
        self._send_handler = send_handler
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
            local_node_id=self._local_node_id_accessor(),
            session_specifier=self._specifier,
            data_type_hash=self._payload_metadata.data_type_hash,
            transfer_id=transfer.transfer_id,
            fragmented_payload=transfer.fragmented_payload,
            max_frame_payload_bytes=self._sft_payload_capacity_bytes
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


def _unittest_output_session() -> None:
    import asyncio
    from pytest import raises, approx
    from pyuavcan.transport import SessionSpecifier, MessageDataSpecifier, ServiceDataSpecifier, Priority, Transfer
    from pyuavcan.transport import PayloadMetadata, Statistics, Timestamp, Feedback

    run_until_complete = asyncio.get_event_loop().run_until_complete

    tx_timestamp: typing.Optional[Timestamp] = Timestamp.now()
    tx_exception: typing.Optional[Exception] = None
    last_sent_frames: typing.List[Frame] = []
    last_monotonic_deadline = 0.0
    finalized = False

    async def do_send(frames: typing.Iterable[Frame], monotonic_deadline: float) -> typing.Optional[Timestamp]:
        nonlocal last_sent_frames
        nonlocal last_monotonic_deadline
        last_sent_frames = list(frames)
        last_monotonic_deadline = monotonic_deadline
        if tx_exception:
            raise tx_exception
        return tx_timestamp

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

    with raises(pyuavcan.transport.UnsupportedSessionConfigurationError):
        _ = SerialOutputSession(
            specifier=SessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.RESPONSE), None),
            payload_metadata=PayloadMetadata(0xdeadbeefbadc0ffe, 1024),
            sft_payload_capacity_bytes=10,
            local_node_id_accessor=lambda: 1234,  # pragma: no cover
            send_handler=do_send,
            finalizer=do_finalize,
        )

    sos = SerialOutputSession(
        specifier=SessionSpecifier(MessageDataSpecifier(3210), None),
        payload_metadata=PayloadMetadata(0xdead_beef_badc0ffe, 1024),
        sft_payload_capacity_bytes=11,
        local_node_id_accessor=lambda: None,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    assert sos.specifier == SessionSpecifier(MessageDataSpecifier(3210), None)
    assert sos.destination_node_id is None
    assert sos.payload_metadata == PayloadMetadata(0xdead_beef_badc0ffe, 1024)
    assert sos.sample_statistics() == Statistics()

    ts = Timestamp.now()

    assert run_until_complete(sos.send_until(
        Transfer(timestamp=ts,
                 priority=Priority.NOMINAL,
                 transfer_id=12340,
                 fragmented_payload=[memoryview(b'one'), memoryview(b'two'), memoryview(b'three')]),
        123456.789
    ))
    assert last_monotonic_deadline == approx(123456.789)
    assert len(last_sent_frames) == 1

    last_feedback: typing.Optional[Feedback] = None

    def feedback_handler(feedback: Feedback) -> None:
        nonlocal last_feedback
        last_feedback = feedback

    sos.enable_feedback(feedback_handler)

    assert last_feedback is None
    assert run_until_complete(sos.send_until(
        Transfer(timestamp=ts,
                 priority=Priority.NOMINAL,
                 transfer_id=12340,
                 fragmented_payload=[]),
        123456.789
    ))
    assert last_monotonic_deadline == approx(123456.789)
    assert len(last_sent_frames) == 1
    assert last_feedback is not None
    assert last_feedback.original_transfer_timestamp == ts
    assert last_feedback.first_frame_transmission_timestamp == tx_timestamp

    sos.disable_feedback()
    sos.disable_feedback()  # Idempotency check

    assert sos.sample_statistics() == Statistics(
        transfers=2,
        frames=2,
        payload_bytes=11,
        errors=0,
        drops=0
    )

    assert not finalized
    sos.close()
    assert finalized
    finalized = False

    sos = SerialOutputSession(
        specifier=SessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(0xdead_beef_badc0ffe, 1024),
        sft_payload_capacity_bytes=10,
        local_node_id_accessor=lambda: 1234,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    # Induced failure
    tx_timestamp = None
    assert not run_until_complete(sos.send_until(
        Transfer(timestamp=ts,
                 priority=Priority.NOMINAL,
                 transfer_id=12340,
                 fragmented_payload=[memoryview(b'one'), memoryview(b'two'), memoryview(b'three')]),
        123456.789
    ))
    assert last_monotonic_deadline == approx(123456.789)
    assert len(last_sent_frames) == 2

    assert sos.sample_statistics() == Statistics(
        transfers=0,
        frames=0,
        payload_bytes=0,
        errors=0,
        drops=2
    )

    tx_exception = RuntimeError()
    with raises(RuntimeError):
        _ = run_until_complete(sos.send_until(
            Transfer(timestamp=ts,
                     priority=Priority.NOMINAL,
                     transfer_id=12340,
                     fragmented_payload=[memoryview(b'one'), memoryview(b'two'), memoryview(b'three')]),
            123456.789
        ))

    assert sos.sample_statistics() == Statistics(
        transfers=0,
        frames=0,
        payload_bytes=0,
        errors=1,
        drops=2
    )

    assert not finalized
    sos.close()
    assert finalized
