# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import dataclasses
import pyuavcan
from pyuavcan.transport import Timestamp
from pyuavcan.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import SerialFrame
from ._base import SerialSession


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SerialInputSessionStatistics(pyuavcan.transport.SessionStatistics):
    reassembly_errors_per_source_node_id: typing.Dict[
        int, typing.Dict[TransferReassembler.Error, int]
    ] = dataclasses.field(default_factory=dict)
    """
    Keys are source node-IDs; values are dicts where keys are error enum members and values are counts.
    """


class SerialInputSession(SerialSession, pyuavcan.transport.InputSession):
    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0
    """
    Units are seconds. Can be overridden after instantiation if needed.
    """

    def __init__(
        self,
        specifier: pyuavcan.transport.InputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        loop: asyncio.AbstractEventLoop,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_input_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        assert self._loop is not None

        self._statistics = SerialInputSessionStatistics()
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pyuavcan.transport.TransferFrom] = asyncio.Queue()
        self._reassemblers: typing.Dict[int, TransferReassembler] = {}

        super().__init__(finalizer)

    def _process_frame(self, timestamp: Timestamp, frame: SerialFrame) -> None:
        """
        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        assert frame.data_specifier == self._specifier.data_specifier, "Internal protocol violation"
        self._statistics.frames += 1

        transfer: typing.Optional[pyuavcan.transport.TransferFrom]
        if frame.source_node_id is None:
            transfer = TransferReassembler.construct_anonymous_transfer(timestamp, frame)
            if transfer is None:
                self._statistics.errors += 1
                _logger.debug("%s: Invalid anonymous frame: %s", self, frame)
        else:
            transfer = self._get_reassembler(frame.source_node_id).process_frame(
                timestamp, frame, self._transfer_id_timeout
            )
        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug("%s: Received transfer: %s; current stats: %s", self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:  # pragma: no cover
                # TODO: make the queue capacity configurable
                self._statistics.drops += len(transfer.fragmented_payload)

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        try:
            timeout = monotonic_deadline - self._loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
            self._raise_if_closed()
            return None
        else:
            assert isinstance(transfer, pyuavcan.transport.TransferFrom), "Internal protocol violation"
            assert transfer.source_node_id == self._specifier.remote_node_id or self._specifier.remote_node_id is None
            return transfer

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        if value > 0:
            self._transfer_id_timeout = float(value)
        else:
            raise ValueError(f"Invalid value for transfer-ID timeout [second]: {value}")

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> SerialInputSessionStatistics:
        return copy.copy(self._statistics)

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        try:
            return self._reassemblers[source_node_id]
        except LookupError:

            def on_reassembly_error(error: TransferReassembler.Error) -> None:
                self._statistics.errors += 1
                d = self._statistics.reassembly_errors_per_source_node_id[source_node_id]
                try:
                    d[error] += 1
                except LookupError:
                    d[error] = 1

            self._statistics.reassembly_errors_per_source_node_id.setdefault(source_node_id, {})
            reasm = TransferReassembler(
                source_node_id=source_node_id,
                extent_bytes=self._payload_metadata.extent_bytes,
                on_error_callback=on_reassembly_error,
            )
            self._reassemblers[source_node_id] = reasm
            _logger.debug("%s: New %s (%d total)", self, reasm, len(self._reassemblers))
            return reasm


def _unittest_input_session() -> None:
    from pytest import raises, approx
    from pyuavcan.transport import InputSessionSpecifier, MessageDataSpecifier, Priority, TransferFrom
    from pyuavcan.transport import PayloadMetadata
    from pyuavcan.transport.commons.high_overhead_transport import TransferCRC

    ts = Timestamp.now()
    prio = Priority.SLOW
    dst_nid = 1234

    run_until_complete = asyncio.get_event_loop().run_until_complete
    get_monotonic = asyncio.get_event_loop().time

    nihil_supernum = b"nihil supernum"

    finalized = False

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

    session_spec = InputSessionSpecifier(MessageDataSpecifier(2345), None)
    payload_meta = PayloadMetadata(100)

    sis = SerialInputSession(
        specifier=session_spec, payload_metadata=payload_meta, loop=asyncio.get_event_loop(), finalizer=do_finalize
    )
    assert sis.specifier == session_spec
    assert sis.payload_metadata == payload_meta
    assert sis.sample_statistics() == SerialInputSessionStatistics()

    assert sis.transfer_id_timeout == approx(SerialInputSession.DEFAULT_TRANSFER_ID_TIMEOUT)
    sis.transfer_id_timeout = 1.0
    with raises(ValueError):
        sis.transfer_id_timeout = 0.0
    assert sis.transfer_id_timeout == approx(1.0)

    assert run_until_complete(sis.receive(get_monotonic() + 0.1)) is None
    assert run_until_complete(sis.receive(0.0)) is None

    def mk_frame(
        transfer_id: int,
        index: int,
        end_of_transfer: bool,
        payload: typing.Union[bytes, memoryview],
        source_node_id: typing.Optional[int],
    ) -> SerialFrame:
        return SerialFrame(
            priority=prio,
            transfer_id=transfer_id,
            index=index,
            end_of_transfer=end_of_transfer,
            payload=memoryview(payload),
            source_node_id=source_node_id,
            destination_node_id=dst_nid,
            data_specifier=session_spec.data_specifier,
        )

    # ANONYMOUS TRANSFERS.
    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        frames=1,
        errors=1,
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=1, end_of_transfer=True, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        frames=2,
        errors=2,
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=True, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=1,
        frames=3,
        payload_bytes=len(nihil_supernum),
        errors=2,
    )
    assert run_until_complete(sis.receive(0)) == TransferFrom(
        timestamp=ts, priority=prio, transfer_id=0, fragmented_payload=[memoryview(nihil_supernum)], source_node_id=None
    )
    assert run_until_complete(sis.receive(get_monotonic() + 0.1)) is None
    assert run_until_complete(sis.receive(0.0)) is None

    # VALID TRANSFERS. Notice that they are unordered on purpose. The reassembler can deal with that.
    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=1, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=True, payload=nihil_supernum, source_node_id=2222)
    )  # COMPLETED FIRST

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=2,
        frames=5,
        payload_bytes=len(nihil_supernum) * 2,
        errors=2,
        reassembly_errors_per_source_node_id={
            1111: {},
            2222: {},
        },
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=0,
            index=3,
            end_of_transfer=True,
            payload=TransferCRC.new(nihil_supernum * 3).value_as_bytes,
            source_node_id=1111,
        ),
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=2, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )  # COMPLETED SECOND

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=3,
        frames=8,
        payload_bytes=len(nihil_supernum) * 5,
        errors=2,
        reassembly_errors_per_source_node_id={
            1111: {},
            2222: {},
        },
    )

    assert run_until_complete(sis.receive(0)) == TransferFrom(
        timestamp=ts, priority=prio, transfer_id=0, fragmented_payload=[memoryview(nihil_supernum)], source_node_id=2222
    )
    assert run_until_complete(sis.receive(0)) == TransferFrom(
        timestamp=ts,
        priority=prio,
        transfer_id=0,
        fragmented_payload=[memoryview(nihil_supernum)] * 3,
        source_node_id=1111,
    )
    assert run_until_complete(sis.receive(get_monotonic() + 0.1)) is None
    assert run_until_complete(sis.receive(0.0)) is None

    # TRANSFERS WITH REASSEMBLY ERRORS.
    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=1, index=0, end_of_transfer=False, payload=b"", source_node_id=1111  # EMPTY IN MULTIFRAME
        ),
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=2, index=0, end_of_transfer=False, payload=b"", source_node_id=1111  # EMPTY IN MULTIFRAME
        ),
    )

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=3,
        frames=10,
        payload_bytes=len(nihil_supernum) * 5,
        errors=4,
        reassembly_errors_per_source_node_id={
            1111: {
                TransferReassembler.Error.MULTIFRAME_EMPTY_FRAME: 2,
            },
            2222: {},
        },
    )

    assert not finalized
    sis.close()
    assert finalized
    sis.close()  # Idempotency check
