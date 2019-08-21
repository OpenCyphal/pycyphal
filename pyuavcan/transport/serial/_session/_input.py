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
import collections
import dataclasses
import pyuavcan
from .._frame import Frame
from ._base import SerialSession
from pyuavcan.transport.commons.high_overhead_transport import TransferReassembler


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SerialInputStatistics(pyuavcan.transport.Statistics):
    #: Keys are data type hash values collected from received frames that did not match the local type configuration.
    #: Values are the number of times each hash value has been encountered.
    mismatched_data_type_hashes: typing.DefaultDict[int, int] = \
        dataclasses.field(default_factory=lambda: collections.defaultdict(int))


class SerialInputSession(SerialSession, pyuavcan.transport.InputSession):
    #: Units are seconds. Can be overridden after instantiation if needed.
    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0

    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 loop:             asyncio.AbstractEventLoop,
                 finalizer:        typing.Callable[[], None]):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_input_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        assert self._loop is not None

        self._statistics = SerialInputStatistics()
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pyuavcan.transport.TransferFrom] = asyncio.Queue()
        self._reassemblers: typing.Dict[int, TransferReassembler] = {}

        super(SerialInputSession, self).__init__(finalizer)

    def _process_frame(self, frame: Frame) -> None:
        """
        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        assert frame.data_specifier == self._specifier.data_specifier, 'Internal protocol violation'
        if frame.data_type_hash != self._payload_metadata.data_type_hash:
            self._statistics.errors += 1
            self._statistics.mismatched_data_type_hashes[frame.data_type_hash] += 1
            return

        self._statistics.frames += 1

        transfer: typing.Optional[pyuavcan.transport.TransferFrom]
        if frame.source_node_id is None:
            transfer = pyuavcan.transport.TransferFrom(timestamp=frame.timestamp,
                                                       priority=frame.priority,
                                                       transfer_id=frame.transfer_id,
                                                       fragmented_payload=[frame.payload],
                                                       source_node_id=None)
        else:
            try:
                reasm = self._reassemblers[frame.source_node_id]
            except LookupError:
                reasm = TransferReassembler(frame.source_node_id, self._payload_metadata.max_size_bytes)
                self._reassemblers[frame.source_node_id] = reasm
                _logger.info('%s: New %s (%d total)', self, reasm, len(self._reassemblers))

            transfer = reasm.process_frame(frame, self._transfer_id_timeout)

        if transfer is not None:
            self._statistics.transfers += 1
            self._statistics.payload_bytes += sum(map(len, transfer.fragmented_payload))
            _logger.debug('%s: Received transfer: %s; current stats: %s', self, transfer, self._statistics)
            try:
                self._queue.put_nowait(transfer)
            except asyncio.QueueFull:
                self._statistics.drops += len(transfer.fragmented_payload)  # TODO This is a hack?

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        try:
            timeout = monotonic_deadline - self._loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout, loop=self._loop)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed messages, allow the caller to read them even if the instance is closed.
            self._raise_if_closed()
            return None
        else:
            assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
            assert transfer.source_node_id in (None, self._specifier.remote_node_id), 'Internal protocol violation'
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
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> SerialInputStatistics:
        out = copy.copy(self._statistics)
        out.errors += sum(sum(tr.error_counters.values()) for tr in self._reassemblers.values())
        return out
