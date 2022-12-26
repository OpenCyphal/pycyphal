# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import copy
import time
import socket as socket_
import typing
import select
import asyncio
import logging
import dataclasses
import pycyphal
from pycyphal.transport import Timestamp
from pycyphal.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import UDPFrame

_READ_SIZE = 0xFFFF  # Per libpcap documentation, this is to be sufficient always.
NODE_ID_MASK = UDPFrame.NODE_ID_MASK
# _READ_TIMEOUT = 1.0

_logger = logging.getLogger(__name__)


class UDPInputSessionStatistics(pycyphal.transport.SessionStatistics):
    pass


class UDPInputSession(pycyphal.transport.InputSession):
    """
    The input session logic is simple because most of the work is handled by the UDP/IP
    stack of the operating system.

    Here we just wait for the frames to arrive (from the socket), reassemble them,
    and pass the resulting transfer.

        [Socket] 1 ---> 1 [Input session] 1 ---> 1 [API]

    *(The plurality notation is supposed to resemble UML: 1 - one, * - many.)*

    A UDP datagram is an atomic unit of workload for the stack.
    Unlike, say, the serial transport, the operating system does the low-level work of framing and
    CRC checking for us (thank you), so we get our stuff sorted up to the OSI layer 4 inclusive.
    The processing pipeline per datagram is as follows:

    - The socket obtains the datagram from the socket using ``recvfrom()``.
      The contents of the Cyphal UDP frame instance is parsed which, among others, contains the source node-ID.
      If anything goes wrong here (like if the source IP address belongs to a wrong subnet or the datagram
      does not contain a valid Cyphal frame or whatever), the datagram is dropped and the appropriate statistical
      counters are updated.

    - Upon reception of the frame, the input session (one of many) updates its reassembler state machine
      and runs all that meticulous bookkeeping you can't get away from if you need to receive multi-frame transfers.

    - If the received frame happened to complete a transfer, the input session passes it up to the higher layer.

    The input session logic is extremely simple because most of the work is handled by the UDP/IP
    stack of the operating system.
    Here we just need to reconstruct the transfer from the frames and pass it up to the higher layer.
    """

    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0
    """
    Units are seconds. Can be overridden after instantiation if needed.
    """

    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        sock: socket_.socket,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly.
        """
        self._closed = False
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._sock = sock
        self._finalizer = finalizer
        assert isinstance(self._specifier, pycyphal.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pycyphal.transport.PayloadMetadata)
        assert callable(self._finalizer)
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pycyphal.transport.TransferFrom] = asyncio.Queue()

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        """
        This method is used to retrieve the transfers from the queue. (Put there by the ``_process_frame()`` method.)
        The method will block until a transfer is available or the deadline is reached.
        If the deadline is reached, the method will return ``None``.
        If the session is closed, the method will raise ``ResourceClosedError``.
        """
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(f"{self} is closed")

        consume_success = await self._consume(monotonic_deadline=monotonic_deadline)

        if consume_success:
            _logger.debug("%s: Consumed a datagram", self)
            loop = asyncio.get_running_loop()
            try:
                timeout = monotonic_deadline - loop.time()
                if timeout > 0:
                    transfer = await asyncio.wait_for(self._queue.get(), timeout)
                else:
                    transfer = self._queue.get_nowait()
            except (asyncio.TimeoutError, asyncio.QueueEmpty):
                # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
                if self._finalizer is None:
                    raise pycyphal.transport.ResourceClosedError(f"{self} is closed") from None
                return None
            else:
                assert isinstance(transfer, pycyphal.transport.TransferFrom), "Internal protocol violation"
                assert (
                    transfer.source_node_id == self._specifier.remote_node_id or self._specifier.remote_node_id is None
                )
                return transfer
        else:  # No datagrams were consumed
            return None

    async def _consume(self, monotonic_deadline: float) -> bool:
        """
        This method is used to consume the datagrams from the socket.
        The method will block until a datagram is available or the deadline is reached.
        If a datagram is read, it will call the underlying ``_process_frame()`` method
        of the input session (PromiscuousInputSession or SelectiveInputSession).
        If no datagram is read and the deadline is reached, the method will return ``False``.
        """
        loop = asyncio.get_running_loop()

        while self._sock.fileno() >= 0:
            try:
                read_ready, _, _ = select.select([self._sock], [], [], monotonic_deadline - loop.time())
                if self._sock in read_ready:
                    # TODO: use socket timestamping when running on GNU/Linux (Windows does not support timestamping).
                    ts = pycyphal.transport.Timestamp.now()

                    # Notice that we MUST create a new buffer for each received datagram to avoid race conditions.
                    # Buffer memory cannot be shared because the rest of the stack is completely zero-copy;
                    # meaning that the data we allocate here, at the very bottom of the protocol stack,
                    # is likely to be carried all the way up to the application layer without being copied.
                    # await asyncio.wait_for(
                    #     loop.sock_recv_into(self._sock, _READ_SIZE), timeout=monotonic_deadline - loop.time()
                    # )
                    data, endpoint = self._sock.recvfrom(_READ_SIZE)
                    assert len(data) < _READ_SIZE, "Datagram might have been truncated"
                    frame = UDPFrame.parse(memoryview(data))
                    _logger.debug(
                        "%r: Received UDP packet of %d bytes from %s containing frame: %s",
                        self,
                        len(data),
                        endpoint,
                        frame,
                    )
                    loop.call_soon_threadsafe(self._process_frame, ts, frame)
                    return True
            except (asyncio.TimeoutError):
                return False
            except Exception as ex:
                _logger.exception("%s: Exception while consuming UDP frames: %s", self, ex)
                time.sleep(1)
                return False

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
    def specifier(self) -> pycyphal.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def close(self) -> None:
        if self._finalizer is not None:
            self._finalizer()
            self._finalizer = None

    @property
    def socket(self) -> socket_.socket:
        """
        Provides access to the underlying UDP socket.
        """
        return self._sock

    @property
    @abc.abstractmethod
    def _statistics(self) -> UDPInputSessionStatistics:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        raise NotImplementedError

    @abc.abstractmethod
    def _process_frame(self, timestamp: Timestamp, frame: typing.Optional[UDPFrame]) -> None:
        raise NotImplementedError


@dataclasses.dataclass
class PromiscuousUDPInputSessionStatistics(UDPInputSessionStatistics):
    reassembly_errors_per_source_node_id: typing.Dict[
        int, typing.Dict[TransferReassembler.Error, int]
    ] = dataclasses.field(default_factory=dict)
    """
    Keys are source node-IDs; values are dicts where keys are error enum members and values are counts.
    """


class PromiscuousUDPInputSession(UDPInputSession):
    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        sock: socket_.socket,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly, use the factory method instead.
        """
        self._statistics_impl = PromiscuousUDPInputSessionStatistics()
        self._reassemblers: typing.Dict[typing.Optional[int], TransferReassembler] = {}
        super().__init__(specifier=specifier, payload_metadata=payload_metadata, sock=sock, finalizer=finalizer)

    def sample_statistics(self) -> PromiscuousUDPInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def _statistics(self) -> PromiscuousUDPInputSessionStatistics:
        return self._statistics_impl

    def _process_frame(self, timestamp: Timestamp, frame: typing.Optional[UDPFrame]) -> None:
        if frame is None:
            self._statistics.errors += 1
            return
        self._statistics.frames += 1

        source_node_id = frame.source_node_id
        assert isinstance(source_node_id, int) and 0 <= source_node_id <= NODE_ID_MASK, "Internal protocol violation"

        transfer = self._get_reassembler(source_node_id).process_frame(timestamp, frame, self._transfer_id_timeout)
        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug("%s: Received transfer: %s; current stats: %s", self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:
                self._statistics.drops += len(transfer.fragmented_payload)  # queue_overflows

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        assert isinstance(source_node_id, int) and source_node_id >= 0, "Internal protocol violation"
        # QUESTION: THIS IS A MESSY SOLUTION DUE TO TransferReassembler NOT SUPPORTING SOURCE NODE-ID = None
        if source_node_id == NODE_ID_MASK:
            source_node_id_None = None
        else:
            source_node_id_None = source_node_id
        try:
            return self._reassemblers[source_node_id]
        except LookupError:

            def on_reassembly_error(error: TransferReassembler.Error) -> None:
                self._statistics.errors += 1
                d = self._statistics.reassembly_errors_per_source_node_id[source_node_id_None]
                try:
                    d[error] += 1
                except LookupError:
                    d[error] = 1

            self._statistics.reassembly_errors_per_source_node_id.setdefault(source_node_id_None, {})
            reasm = TransferReassembler(
                source_node_id=source_node_id,  # <- THIS IS THE PROBLEM
                extent_bytes=self._payload_metadata.extent_bytes,
                on_error_callback=on_reassembly_error,
            )
            self._reassemblers[source_node_id_None] = reasm
            _logger.debug("%s: New %s (%d total)", self, reasm, len(self._reassemblers))
            return reasm


@dataclasses.dataclass
class SelectiveUDPInputSessionStatistics(UDPInputSessionStatistics):
    reassembly_errors: typing.Dict[TransferReassembler.Error, int] = dataclasses.field(default_factory=dict)
    """
    Keys are error enum members and values are counts.
    """


class SelectiveUDPInputSession(UDPInputSession):
    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        sock: socket_.socket,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly, use the factory method instead.
        """
        self._statistics_impl = SelectiveUDPInputSessionStatistics()

        source_node_id = specifier.remote_node_id
        assert source_node_id is not None, "Internal protocol violation"

        def on_reassembly_error(error: TransferReassembler.Error) -> None:
            self._statistics.errors += 1
            try:
                self._statistics.reassembly_errors[error] += 1
            except LookupError:
                self._statistics.reassembly_errors[error] = 1

        self._reassembler = TransferReassembler(
            source_node_id=source_node_id,
            extent_bytes=payload_metadata.extent_bytes,
            on_error_callback=on_reassembly_error,
        )

        super().__init__(specifier=specifier, payload_metadata=payload_metadata, sock=sock, finalizer=finalizer)

    def sample_statistics(self) -> SelectiveUDPInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def _statistics(self) -> SelectiveUDPInputSessionStatistics:
        return self._statistics_impl

    def _process_frame(self, timestamp: Timestamp, frame: typing.Optional[UDPFrame]) -> None:
        if frame is None:
            self._statistics.errors += 1
            return
        if frame.source_node_id != self._specifier.remote_node_id:
            return
        self._statistics.frames += 1

        source_node_id = frame.source_node_id
        assert isinstance(source_node_id, int) and 0 <= source_node_id <= 0xFFFF, "Internal protocol violation"

        transfer = self._get_reassembler(source_node_id).process_frame(timestamp, frame, self._transfer_id_timeout)
        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug("%s: Received transfer: %s; current stats: %s", self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:
                self._statistics.drops += len(transfer.fragmented_payload)  # queue_overflows

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        # THIS SHOULD BE CHANGED?
        assert source_node_id == self._reassembler.source_node_id, "Internal protocol violation"
        return self._reassembler
