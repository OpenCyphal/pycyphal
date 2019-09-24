#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import dataclasses
import pyuavcan
from pyuavcan.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import UDPFrame


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPInputSessionStatistics(pyuavcan.transport.SessionStatistics):
    #: Keys are data type hash values collected from received frames that did not match the local type configuration.
    #: Values are the number of times each hash value has been encountered.
    mismatched_data_type_hashes: typing.Dict[int, int] = dataclasses.field(default_factory=dict)

    #: Keys are source node-IDs; values are dicts where keys are error enum members and values are counts.
    reassembly_errors_per_source_node_id: typing.Dict[int, typing.Dict[TransferReassembler.Error, int]] = \
        dataclasses.field(default_factory=dict)


class UDPInputSession(pyuavcan.transport.InputSession):
    #: Units are seconds. Can be overridden after instantiation if needed.
    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0

    def __init__(self,
                 specifier:        pyuavcan.transport.InputSessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 loop:             asyncio.AbstractEventLoop,
                 finalizer:        typing.Callable[[], None]):
        """
        Do not call this directly, use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        self._maybe_finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert isinstance(self._loop, asyncio.AbstractEventLoop)
        assert callable(self._maybe_finalizer)

        self._statistics = UDPInputSessionStatistics()
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pyuavcan.transport.TransferFrom] = asyncio.Queue()
        self._reassemblers: typing.Dict[int, TransferReassembler] = {}

    def _process_frame(self, source_node_id: int, frame: typing.Optional[UDPFrame]) -> None:
        """
        The source node-ID is always valid because anonymous nodes are not defined for the UDP transport.
        The frame argument may be None to indicate that the underlying transport has received a datagram
        which is valid but does not contain a UAVCAN UDP frame inside. This is needed for error stats tracking.

        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        assert isinstance(source_node_id, int) and source_node_id >= 0, 'Internal protocol violation'
        if frame is None:   # Malformed frame.
            self._statistics.errors += 1
            return
        self._statistics.frames += 1

        if frame.data_type_hash != self._payload_metadata.data_type_hash:
            self._statistics.errors += 1
            try:
                self._statistics.mismatched_data_type_hashes[frame.data_type_hash] += 1
            except LookupError:
                self._statistics.mismatched_data_type_hashes[frame.data_type_hash] = 1
            return

        transfer = self._get_reassembler(source_node_id).process_frame(frame, self._transfer_id_timeout)
        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug('%s: Received transfer: %s; current stats: %s', self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:  # pragma: no cover
                # TODO: make the queue capacity configurable
                self._statistics.drops += len(transfer.fragmented_payload)

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        try:
            timeout = monotonic_deadline - self._loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout, loop=self._loop)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
            if self._maybe_finalizer is None:
                raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')
            return None
        else:
            assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
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
            raise ValueError(f'Invalid value for transfer-ID timeout [second]: {value}')

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> UDPInputSessionStatistics:
        return copy.copy(self._statistics)

    def close(self) -> None:
        if self._maybe_finalizer is not None:
            self._maybe_finalizer()
            self._maybe_finalizer = None

    def _get_reassembler(self, source_node_id: int) -> TransferReassembler:
        assert isinstance(source_node_id, int) and source_node_id >= 0, 'Internal protocol violation'
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
            reasm = TransferReassembler(source_node_id=source_node_id,
                                        max_payload_size_bytes=self._payload_metadata.max_size_bytes,
                                        on_error_callback=on_reassembly_error)
            self._reassemblers[source_node_id] = reasm
            _logger.debug('%s: New %s (%d total)', self, reasm, len(self._reassemblers))
            return reasm
