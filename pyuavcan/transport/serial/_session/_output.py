# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import copy
import typing
import logging
import pyuavcan
from pyuavcan.transport import ServiceDataSpecifier
from .._frame import SerialFrame
from ._base import SerialSession


#: Returns the transmission timestamp.
SendHandler = typing.Callable[
    [typing.List[SerialFrame], float], typing.Awaitable[typing.Optional[pyuavcan.transport.Timestamp]]
]

_logger = logging.getLogger(__name__)


class SerialFeedback(pyuavcan.transport.Feedback):
    def __init__(
        self,
        original_transfer_timestamp: pyuavcan.transport.Timestamp,
        first_frame_transmission_timestamp: pyuavcan.transport.Timestamp,
    ):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._first_frame_transmission_timestamp


class SerialOutputSession(SerialSession, pyuavcan.transport.OutputSession):
    def __init__(
        self,
        specifier: pyuavcan.transport.OutputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        mtu: int,
        local_node_id: typing.Optional[int],
        send_handler: SendHandler,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_output_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._mtu = int(mtu)
        self._local_node_id = local_node_id
        self._send_handler = send_handler
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        self._statistics = pyuavcan.transport.SessionStatistics()
        if self._local_node_id is None and isinstance(self._specifier.data_specifier, ServiceDataSpecifier):
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                f"Anonymous nodes cannot emit service transfers. Session specifier: {self._specifier}"
            )
        assert isinstance(self._local_node_id, int) or self._local_node_id is None
        assert callable(send_handler)
        assert (
            specifier.remote_node_id is not None if isinstance(specifier.data_specifier, ServiceDataSpecifier) else True
        ), "Internal protocol violation: cannot broadcast a service transfer"

        super().__init__(finalizer)

    async def send(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        self._raise_if_closed()

        def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> SerialFrame:
            if not end_of_transfer and self._local_node_id is None:
                raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
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
            pyuavcan.transport.commons.high_overhead_transport.serialize_transfer(
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

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    @property
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        return copy.copy(self._statistics)

    def close(self) -> None:  # pylint: disable=useless-super-delegation
        super().close()


def _unittest_output_session() -> None:
    import asyncio
    from pytest import raises, approx
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority
    from pyuavcan.transport import PayloadMetadata, SessionStatistics, Timestamp, Feedback, Transfer

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete

    tx_timestamp: typing.Optional[Timestamp] = Timestamp.now()
    tx_exception: typing.Optional[Exception] = None
    last_sent_frames: typing.List[SerialFrame] = []
    last_monotonic_deadline = 0.0
    finalized = False

    async def do_send(frames: typing.Sequence[SerialFrame], monotonic_deadline: float) -> typing.Optional[Timestamp]:
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

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        SerialOutputSession(
            specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 1111),
            payload_metadata=PayloadMetadata(1024),
            mtu=10,
            local_node_id=None,
            send_handler=do_send,
            finalizer=do_finalize,
        )

    sos = SerialOutputSession(
        specifier=OutputSessionSpecifier(MessageDataSpecifier(3210), None),
        payload_metadata=PayloadMetadata(1024),
        mtu=11,
        local_node_id=None,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    assert sos.specifier == OutputSessionSpecifier(MessageDataSpecifier(3210), None)
    assert sos.destination_node_id is None
    assert sos.payload_metadata == PayloadMetadata(1024)
    assert sos.sample_statistics() == SessionStatistics()

    assert run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            999999999.999,
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 1

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        run_until_complete(
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three four five")],
                ),
                loop.time() + 10.0,
            )
        )

    last_feedback: typing.Optional[Feedback] = None

    def feedback_handler(feedback: Feedback) -> None:
        nonlocal last_feedback
        last_feedback = feedback

    sos.enable_feedback(feedback_handler)

    assert last_feedback is None
    assert run_until_complete(
        sos.send(
            Transfer(timestamp=ts, priority=Priority.NOMINAL, transfer_id=12340, fragmented_payload=[]), 999999999.999
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 1
    assert last_feedback is not None
    assert last_feedback.original_transfer_timestamp == ts
    assert last_feedback.first_frame_transmission_timestamp == tx_timestamp

    sos.disable_feedback()
    sos.disable_feedback()  # Idempotency check

    assert sos.sample_statistics() == SessionStatistics(transfers=2, frames=2, payload_bytes=11, errors=0, drops=0)

    assert not finalized
    sos.close()
    assert finalized
    finalized = False

    sos = SerialOutputSession(
        specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(1024),
        mtu=10,
        local_node_id=1234,
        send_handler=do_send,
        finalizer=do_finalize,
    )

    # Induced failure
    tx_timestamp = None
    assert not run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            999999999.999,
        )
    )
    assert last_monotonic_deadline == approx(999999999.999)
    assert len(last_sent_frames) == 2

    assert sos.sample_statistics() == SessionStatistics(transfers=0, frames=0, payload_bytes=0, errors=0, drops=2)

    tx_exception = RuntimeError()
    with raises(RuntimeError):
        _ = run_until_complete(
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )

    assert sos.sample_statistics() == SessionStatistics(transfers=0, frames=0, payload_bytes=0, errors=1, drops=2)

    assert not finalized
    sos.close()
    assert finalized
    sos.close()  # Idempotency

    with raises(pyuavcan.transport.ResourceClosedError):
        run_until_complete(
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )
