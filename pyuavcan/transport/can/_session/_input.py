#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import copy
import typing
import asyncio
import logging
import dataclasses
import pyuavcan.util
import pyuavcan.transport
from .. import _frame, _identifier
from . import _base, _transfer_receiver


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ExtendedStatistics(pyuavcan.transport.Statistics):
    reception_error_counters: typing.Dict[_transfer_receiver.TransferReceptionError, int] = \
        dataclasses.field(default_factory=lambda: {e: 0 for e in _transfer_receiver.TransferReceptionError})


# noinspection PyAbstractClass
class CANInputSession(_base.CANSession, pyuavcan.transport.InputSession):
    DEFAULT_TRANSFER_ID_TIMEOUT = 2     # [second] Per the Specification.

    _QueueItem = typing.Tuple[_identifier.CANID, _frame.TimestampedUAVCANFrame]

    def __init__(self,
                 metadata:  pyuavcan.transport.SessionMetadata,
                 loop:      typing.Optional[asyncio.AbstractEventLoop],
                 finalizer: _base.Finalizer):
        self._metadata = metadata
        self._queue: asyncio.Queue[CANInputSession._QueueItem] = asyncio.Queue()
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._transfer_id_timeout_ns = int(CANInputSession.DEFAULT_TRANSFER_ID_TIMEOUT / _NANO)

        self._receivers = [_transfer_receiver.TransferReceiver(metadata.payload_metadata.max_size_bytes)
                           for _ in _node_id_range()]

        self._statistics = ExtendedStatistics()         # We could easily support per-source-node statistics if needed

        super(CANInputSession, self).__init__(finalizer=finalizer)

    def push_frame(self, can_id: _identifier.CANID, frame: _frame.TimestampedUAVCANFrame) -> None:
        """
        Pushes a newly received frame for later processing.
        This method must be non-blocking and non-yielding (hence it's not async).
        """
        try:
            self._queue.put_nowait((can_id, frame))
        except asyncio.QueueFull:
            self._statistics.overruns += 1
            _logger.info('Input session %s: input queue overflow; frame %s (CAN ID fields: %s) is dropped',
                         self, frame, can_id)

    @property
    def queue_capacity(self) -> typing.Optional[int]:
        """
        Returns the capacity of the input frame queue. None means that the capacity is unlimited, which is the default.
        """
        return self._queue.maxsize if self._queue.maxsize > 0 else None

    @queue_capacity.setter
    def queue_capacity(self, value: typing.Optional[int]) -> None:
        """
        Changes the input frame queue capacity. If the argument is None, the new capacity will be unlimited.
        If the new capacity is smaller than the number of frames currently in the queue, the newest frames will
        be discarded and the number of queue overruns will be incremented accordingly.
        The complexity may be up to linear on the number of frames currently in the queue.
        """
        old_queue = self._queue
        self._queue = asyncio.Queue(value if value is not None else 0, loop=self._loop)
        try:
            while True:
                self.push_frame(*old_queue.get_nowait())
        except asyncio.QueueEmpty:
            pass

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        return self._metadata.data_specifier

    async def _do_try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        while True:
            self._raise_if_closed()

            timeout = monotonic_deadline - time.monotonic()
            if timeout <= 0:
                break

            try:
                canid, frame = await asyncio.wait_for(self._queue.get(), timeout, loop=self._loop)
                assert isinstance(canid, _identifier.CANID)
                assert isinstance(frame, _frame.TimestampedUAVCANFrame)
            except asyncio.TimeoutError:
                continue

            self._statistics.frames += 1

            if isinstance(canid, _identifier.MessageCANID):
                assert isinstance(self._metadata.data_specifier, pyuavcan.transport.MessageDataSpecifier)
                assert self._metadata.data_specifier.subject_id == canid.subject_id
                source_node_id = canid.source_node_id
                if source_node_id is None:
                    # Anonymous transfer - no reconstruction needed
                    self._statistics.transfers += 1
                    self._statistics.payload_bytes += len(frame.padded_payload)
                    return pyuavcan.transport.TransferFrom(timestamp=frame.timestamp,
                                                           priority=canid.priority,
                                                           transfer_id=frame.transfer_id,
                                                           fragmented_payload=[frame.padded_payload],
                                                           source_node_id=None)

            elif isinstance(canid, _identifier.ServiceCANID):
                assert isinstance(self._metadata.data_specifier, pyuavcan.transport.ServiceDataSpecifier)
                assert self._metadata.data_specifier.service_id == canid.service_id
                assert (self._metadata.data_specifier.role == pyuavcan.transport.ServiceDataSpecifier.Role.SERVER) \
                    == canid.request_not_response
                source_node_id = canid.source_node_id

            else:
                assert False

            receiver = self._receivers[source_node_id]
            result = receiver.process_frame(canid.priority, source_node_id, frame, self._transfer_id_timeout_ns)
            if isinstance(result, _transfer_receiver.TransferReceptionError):
                self._statistics.errors += 1
                self._statistics.reception_error_counters[result] += 1
            elif isinstance(result, pyuavcan.transport.TransferFrom):
                self._statistics.transfers += 1
                self._statistics.payload_bytes += sum(map(len, result.fragmented_payload))
                return result
            elif result is None:
                pass        # Nothing to do - expecting more frames
            else:
                assert False
        return None

    def _do_sample_statistics(self) -> ExtendedStatistics:
        return copy.copy(self._statistics)


class PromiscuousCANInput(CANInputSession, pyuavcan.transport.PromiscuousInput):
    def __init__(self,
                 metadata:  pyuavcan.transport.SessionMetadata,
                 loop:      typing.Optional[asyncio.AbstractEventLoop],
                 finalizer: _base.Finalizer):
        super(PromiscuousCANInput, self).__init__(metadata=metadata,
                                                  loop=loop,
                                                  finalizer=finalizer)

    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        return self._metadata

    def sample_statistics(self) -> ExtendedStatistics:
        return self._do_sample_statistics()

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.TransferFrom:
        out: typing.Optional[pyuavcan.transport.TransferFrom] = None
        while out is None:
            out = await self.try_receive(time.monotonic() + _INFINITE_RECEIVE_RETRY_INTERVAL)
        return out

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        return await self._do_try_receive(monotonic_deadline)

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout_ns * _NANO

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        self._transfer_id_timeout_ns = int(value / _NANO)


class SelectiveCANInput(CANInputSession, pyuavcan.transport.SelectiveInput):
    def __init__(self,
                 source_node_id: int,
                 metadata:       pyuavcan.transport.SessionMetadata,
                 loop:           typing.Optional[asyncio.AbstractEventLoop],
                 finalizer:      _base.Finalizer):
        self._source_node_id = int(source_node_id)
        super(SelectiveCANInput, self).__init__(metadata=metadata,
                                                loop=loop,
                                                finalizer=finalizer)

    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        return self._metadata

    def sample_statistics(self) -> ExtendedStatistics:
        return self._do_sample_statistics()

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.Transfer:
        out: typing.Optional[pyuavcan.transport.Transfer] = None
        while out is None:
            out = await self.try_receive(time.monotonic() + _INFINITE_RECEIVE_RETRY_INTERVAL)
        return out

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.Transfer]:
        out = await self._do_try_receive(monotonic_deadline)
        assert out is None or out.source_node_id == self.source_node_id, 'Mishandled selective input session'
        return out

    @property
    def source_node_id(self) -> int:
        return self._source_node_id

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout_ns * _NANO

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        self._transfer_id_timeout_ns = int(value / _NANO)


def _node_id_range() -> typing.Iterable[int]:
    return range(_identifier.CANID.NODE_ID_MASK + 1)


_NANO = 1e-9

_INFINITE_RECEIVE_RETRY_INTERVAL = 60
