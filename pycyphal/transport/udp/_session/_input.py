# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import copy
import socket as socket_
import typing
import select
import asyncio
import logging
import threading
import dataclasses
import pycyphal
from pycyphal.transport import Timestamp
from pycyphal.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import UDPFrame

_READ_SIZE = 0xFFFF  # Per libpcap documentation, this is to be sufficient always.
NODE_ID_MASK = UDPFrame.NODE_ID_MASK

_logger = logging.getLogger(__name__)


class UDPInputSessionStatistics(pycyphal.transport.SessionStatistics):
    pass


class UDPInputSession(pycyphal.transport.InputSession):
    """
    The input session logic is simple because most of the work is handled by the UDP/IP
    stack of the operating system.

    Here we just wait for the frames to arrive (from the socket), reassemble them,
    and pass the resulting transfer.

        [Socket] ---> [Input session] ---> [UDP API]

    *(The plurality notation is supposed to resemble UML: 1 - one, * - many.)*

    A UDP datagram is an atomic unit of workload for the stack.
    Unlike, say, the serial transport, the operating system does the low-level work of framing and
    CRC checking for us (thank you), so we get our stuff sorted up to the OSI layer 4 inclusive.
    The processing pipeline per datagram is as follows:

    - The socket obtains the datagram from the socket using ``recvfrom()``.
      The contents of the Cyphal UDP frame instance is parsed which, among others, contains the source node-ID.
      If anything goes wrong here (like if the datagram
      does not contain a valid Cyphal frame or whatever), the datagram is dropped and the appropriate statistical
      counters are updated.

    - Upon reception of the frame, the input session updates its reassembler state machine(s)
      (many in case of PromiscuousInputSession)
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
        socket: socket_.socket,
        finalizer: typing.Union[typing.Callable[[], None], None],
        local_node_id: typing.Optional[int],
    ):
        """
        Parent class of PromiscuousInputSession and SelectiveInputSession.
        """
        self._closed = False
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._socket = socket
        self._finalizer = finalizer
        self._local_node_id = local_node_id
        assert isinstance(self._specifier, pycyphal.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pycyphal.transport.PayloadMetadata)
        assert callable(self._finalizer)
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._frame_queue: asyncio.Queue[typing.Tuple[Timestamp, UDPFrame | None]] = asyncio.Queue()
        self._thread = threading.Thread(
            target=self._reader_thread, name=str(self), args=(asyncio.get_running_loop(),), daemon=True
        )
        self._thread.start()

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        """
        This method will wait for self._reader_thread to put a frame in the queue.
        If a frame is available, it will retrieved and used to construct a transfer.
        Once a complete transfer can be constructed from the frames, it will be returned.

        The method will block until a transfer is available or the deadline is reached.

        If the deadline is reached, the method will return ``None``.
        If the session is closed, the method will raise ``ResourceClosedError``.
        """
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(f"{self} is closed")
        loop = asyncio.get_running_loop()
        while True:
            timeout = monotonic_deadline - loop.time()
            try:
                if timeout > 0:
                    ts, frame = await asyncio.wait_for(self._frame_queue.get(), timeout=timeout)
                else:
                    ts, frame = self._frame_queue.get_nowait()
            except (asyncio.TimeoutError, asyncio.QueueEmpty):
                # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
                if self._finalizer is None:
                    raise pycyphal.transport.ResourceClosedError(f"{self} is closed") from None
                return None
            if frame is None:
                self._statistics.errors += 1
                continue
            # это проблема но мы это потом починим
            if frame.data_specifier != self._specifier.data_specifier:
                continue
            if frame.source_node_id == self._local_node_id:
                continue
            if not self.specifier.is_promiscuous:
                if frame.source_node_id != self.specifier.remote_node_id:
                    continue
            self._statistics.frames += 1
            source_node_id = frame.source_node_id
            assert (
                isinstance(source_node_id, int) and 0 <= source_node_id <= NODE_ID_MASK
            ), "Internal protocol violation"
            # Anonymous - no reconstruction needed
            if source_node_id == NODE_ID_MASK:
                transfer = TransferReassembler.construct_anonymous_transfer(ts, frame)
            else:
                _logger.debug("%s: Processing frame %s", self, frame)
                transfer = self._get_reassembler(source_node_id).process_frame(ts, frame, self._transfer_id_timeout)
            if transfer is not None:
                self._statistics.transfers += 1
                self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
                _logger.debug("%s: Received transfer %s; current stats: %s", self, transfer, self._statistics)
                return transfer

    def _put_into_queue(self, ts: pycyphal.transport.Timestamp, frame: typing.Optional[UDPFrame]) -> None:
        self._frame_queue.put_nowait((ts, frame))

    def _reader_thread(self, loop: asyncio.AbstractEventLoop) -> None:
        while not self._closed and self._socket.fileno() >= 0:
            try:
                # TODO: add a dedicated socket for aborting the select call
                # when self.close() is invoked to avoid blocking on
                # self._thread.join() in self.close().
                read_ready, _, _ = select.select([self._socket], [], [], 0.1)
                if self._socket in read_ready:
                    # TODO: use socket timestamping when running on GNU/Linux (Windows does not support timestamping).
                    ts = pycyphal.transport.Timestamp.now()

                    # Notice that we MUST create a new buffer for each received datagram to avoid race conditions.
                    # Buffer memory cannot be shared because the rest of the stack is completely zero-copy;
                    # meaning that the data we allocate here, at the very bottom of the protocol stack,
                    # is likely to be carried all the way up to the application layer without being copied.
                    data, endpoint = self._socket.recvfrom(_READ_SIZE)
                    assert len(data) < _READ_SIZE, "Datagram might have been truncated"
                    frame = UDPFrame.parse(memoryview(data))
                    _logger.debug(
                        "%r: Received UDP packet of %d bytes from %s containing frame: %s",
                        self,
                        len(data),
                        endpoint,
                        frame,
                    )
                    try:
                        loop.call_soon_threadsafe(self._put_into_queue, ts, frame)
                    except asyncio.QueueFull:
                        # TODO: make the queue capacity configurable
                        _logger.error("%s: Frame queue is full", self)
            except Exception as ex:
                _logger.exception("%s: Exception while consuming UDP frames: %s", self, ex)

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
        """
        Closes the instance and its socket, waits for the thread to terminate (which should happen instantly).

        Once closed, new listeners can no longer be added.
        Raises :class:`RuntimeError` instead of closing if there is at least one active listener.
        """

        # This method is guaranteed to not return until the socket is closed and all calls that might have been
        # blocked on it have been completed (particularly, the calls made by the worker thread).
        # THIS IS EXTREMELY IMPORTANT because if the worker thread is left on a blocking read from a closed socket,
        # the next created socket is likely to receive the same file descriptor and the worker thread would then
        # inadvertently consume the data destined for another reader.
        # Worse yet, this error may occur spuriously depending on the timing of the worker thread's access to the
        # blocking read function, causing the problem to appear and disappear at random.
        # I literally spent the whole day sifting through logs and Wireshark dumps trying to understand why the test
        # (specifically, the node tracker test, which is an application-layer entity)
        # sometimes fails to see a service response that is actually present on the wire.
        # This case is now covered by a dedicated unit test.

        # The lesson is to never close a file descriptor while there is a system call blocked on it. Never again.

        self._closed = True
        if self._finalizer is not None:
            self._finalizer()
            self._finalizer = None

        # Before closing the socket we need to terminate the reader thread. (See note above)
        # self._thread_stop.set()
        self._thread.join()

        self._socket.close()
        _logger.debug("%s: Closed", self)

    @property
    @abc.abstractmethod
    def _statistics(self) -> UDPInputSessionStatistics:
        raise NotImplementedError

    @abc.abstractmethod
    def sample_statistics(self) -> UDPInputSessionStatistics:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
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
        socket: socket_.socket,
        finalizer: typing.Callable[[], None],
        local_node_id: typing.Optional[int],
        statistics: PromiscuousUDPInputSessionStatistics,
    ):
        """
        Do not call this directly, use the factory method instead.
        """
        self._statistics_impl = statistics
        self._reassemblers: typing.Dict[typing.Optional[int], TransferReassembler] = {}
        assert specifier.is_promiscuous
        super().__init__(
            specifier=specifier,
            payload_metadata=payload_metadata,
            socket=socket,
            finalizer=finalizer,
            local_node_id=local_node_id,
        )

    def sample_statistics(self) -> PromiscuousUDPInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def _statistics(self) -> PromiscuousUDPInputSessionStatistics:
        return self._statistics_impl

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        assert isinstance(source_node_id, int) and source_node_id >= 0, "Internal protocol violation"
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
        socket: socket_.socket,
        finalizer: typing.Callable[[], None],
        local_node_id: typing.Optional[int],
        statistics: SelectiveUDPInputSessionStatistics,
    ):
        """
        Do not call this directly, use the factory method instead.
        """
        self._statistics_impl = statistics

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

        super().__init__(
            specifier=specifier,
            payload_metadata=payload_metadata,
            socket=socket,
            finalizer=finalizer,
            local_node_id=local_node_id,
        )

    def sample_statistics(self) -> SelectiveUDPInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def _statistics(self) -> SelectiveUDPInputSessionStatistics:
        return self._statistics_impl

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        assert source_node_id == self._reassembler.source_node_id, "Internal protocol violation"
        return self._reassembler
