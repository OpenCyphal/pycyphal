# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import copy
import typing
import asyncio
import logging
import dataclasses
import pycyphal
from pycyphal.transport import Timestamp
from pycyphal.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import UDPFrame


_logger = logging.getLogger(__name__)


class UDPInputSessionStatistics(pycyphal.transport.SessionStatistics):
    pass


class UDPInputSession(pycyphal.transport.InputSession):
    """
    As you already know, the UDP port number is a function of the data specifier.
    Hence, the input flow demultiplexing is mostly done by the UDP/IP stack implemented in the operating system
    itself, we just need to put a few basic abstractions on top.
    One of those abstractions is the internal socket reader class, which is not part of the API
    but its function is important if one needs to understand how the data flow is organized inside the library::

        [Socket] 1   --->   1 [Demultiplexer] 1   --->   * [Input session] 1   --->   1 [API]

    *(The plurality notation is supposed to resemble UML: 1 - one, * - many.)*

    A UDP datagram is an atomic unit of workload for the stack.
    Unlike, say, the serial transport, the operating system does the low-level work of framing and
    CRC checking for us (thank you), so we get our stuff sorted up to the OSI layer 4 inclusive.
    The processing pipeline per datagram is as follows:

    - The socket reader obtains the datagram from the socket using ``recvfrom()``.
      The source IP address is mapped to a node-ID and the contents are parsed into a Cyphal UDP frame instance.
      If anything goes wrong here (like if the source IP address belongs to a wrong subnet or the datagram
      does not contain a valid Cyphal frame or whatever), the datagram is dropped and the appropriate statistical
      counters are updated.

    - The socket reader looks up the input session instances that have subscribed for the datagram from the
      current source node-ID (derived from the IP address) and passes the frame to them.
      By the way, remember that this is a zero-copy stack, so every subscribed input session gets a reference
      to the same instance of the frame, although it is beside the point right now.

    - Upon reception of the frame, the input session (one of many) updates its reassembler state machine
      and runs all that meticulous bookkeeping you can't get away from if you need to receive multi-frame transfers.

    - If the received frame happened to complete a transfer, the input session passes it up to the higher layer.

    Now, an attentive reader might exclaim:

        But look! If there is more than one input session instance per source node-ID,
        we'd be running multiple transfer reassemblers with the same input data,
        which is inefficient!
        Why can't we extract the task of transfer reassembly into the socket reader,
        before the pipeline is forked, to avoid the extra workload?

    That is a good question, and here's why:

    - The most important reason is that the proposal would only work if the state of
      a transfer reassembler was a function of the input frame flow only.
      This is not the case.
      The state of a transfer reassembler is also defined by its configuration parameters
      which are defined per-instance, which in turn are defined per input session instance.
      In particular, the transfer-ID timeout is configured separately per input session.

    - The case where there is more than one input session per remote node-ID is uncommon.
      In fact, it may only occur if the higher layers requested a promiscuous and a selective session
      at the same time, which normally does not happen with Cyphal.
      We support this use case nevertheless because this library is supposed to offer a generic and
      flexible API due to its intended usages (read the library design goals).

    - The computing load of updating the state machine of a transfer reassembler is minuscule.
      The most intensive computation happening there is the CRC update, which is not intense at all.

    The architecture of the data processing pipeline in PyCyphal is complex, but that is due to the
    high-level requirements for the library: it has to support *all transport protocols*, a lot of
    media layers, and be portable, so trade-offs had to be made.
    It should be understood that actual safety-critical implementations used in production systems
    can be far simpler because generally they do not have to be multi-transport and multi-platform.
    """

    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0
    """
    Units are seconds. Can be overridden after instantiation if needed.
    """

    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        finalizer: typing.Callable[[], None],
    ):
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._maybe_finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pycyphal.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pycyphal.transport.PayloadMetadata)
        assert callable(self._maybe_finalizer)
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pycyphal.transport.TransferFrom] = asyncio.Queue()

    def _process_frame(self, timestamp: Timestamp, source_node_id: int, frame: typing.Optional[UDPFrame]) -> None:
        """
        The source node-ID is always valid because anonymous transfers are not defined for the UDP transport.
        The frame argument may be None to indicate that the underlying transport has received a datagram
        which is valid but does not contain a Cyphal UDP frame inside. This is needed for error stats tracking.

        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        assert isinstance(source_node_id, int) and source_node_id >= 0, "Internal protocol violation"
        if frame is None:  # Malformed frame.
            self._statistics.errors += 1
            return
        self._statistics.frames += 1

        transfer = self._get_reassembler(source_node_id).process_frame(timestamp, frame, self._transfer_id_timeout)
        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug("%s: Received transfer: %s; current stats: %s", self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:  # pragma: no cover
                # TODO: make the queue capacity configurable
                self._statistics.drops += len(transfer.fragmented_payload)

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        loop = asyncio.get_running_loop()
        try:
            timeout = monotonic_deadline - loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
            if self._maybe_finalizer is None:
                raise pycyphal.transport.ResourceClosedError(f"{self} is closed") from None
            return None
        else:
            assert isinstance(transfer, pycyphal.transport.TransferFrom), "Internal protocol violation"
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
    def specifier(self) -> pycyphal.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def close(self) -> None:
        if self._maybe_finalizer is not None:
            self._maybe_finalizer()
            self._maybe_finalizer = None

    @property
    @abc.abstractmethod
    def _statistics(self) -> UDPInputSessionStatistics:
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
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly, use the factory method instead.
        """
        self._statistics_impl = PromiscuousUDPInputSessionStatistics()
        self._reassemblers: typing.Dict[int, TransferReassembler] = {}
        super().__init__(specifier=specifier, payload_metadata=payload_metadata, finalizer=finalizer)

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

        super().__init__(specifier=specifier, payload_metadata=payload_metadata, finalizer=finalizer)

    def sample_statistics(self) -> SelectiveUDPInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def _statistics(self) -> SelectiveUDPInputSessionStatistics:
        return self._statistics_impl

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        assert source_node_id == self._reassembler.source_node_id, "Internal protocol violation"
        return self._reassembler
