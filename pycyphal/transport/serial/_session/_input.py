# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import dataclasses
import pycyphal
from pycyphal.transport import Timestamp
from pycyphal.transport.commons.high_overhead_transport import TransferReassembler
from .._frame import SerialFrame
from ._base import SerialSession


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SerialInputSessionStatistics(pycyphal.transport.SessionStatistics):
    reassembly_errors_per_source_node_id: typing.Dict[
        int, typing.Dict[TransferReassembler.Error, int]
    ] = dataclasses.field(default_factory=dict)
    """
    Keys are source node-IDs; values are dicts where keys are error enum members and values are counts.
    """


class SerialInputSession(SerialSession, pycyphal.transport.InputSession):
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
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pycyphal.transport.serial.SerialTransport.get_input_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._statistics = SerialInputSessionStatistics()
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pycyphal.transport.TransferFrom] = asyncio.Queue()
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

        transfer: typing.Optional[pycyphal.transport.TransferFrom]
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

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        try:
            loop = asyncio.get_running_loop()
            timeout = monotonic_deadline - loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
            self._raise_if_closed()
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
