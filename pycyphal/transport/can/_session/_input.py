# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import dataclasses
import pycyphal.util
import pycyphal.transport
from pycyphal.transport import Timestamp
from .._frame import CyphalFrame
from .._identifier import CANID, MessageCANID, ServiceCANID
from ._base import CANSession, SessionFinalizer
from ._transfer_reassembler import TransferReassemblyErrorID, TransferReassembler


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CANInputSessionStatistics(pycyphal.transport.SessionStatistics):
    reception_error_counters: typing.Dict[TransferReassemblyErrorID, int] = dataclasses.field(
        default_factory=lambda: {e: 0 for e in TransferReassemblyErrorID}
    )


class CANInputSession(CANSession, pycyphal.transport.InputSession):
    DEFAULT_TRANSFER_ID_TIMEOUT = 2
    """
    Per the Cyphal specification. Units are seconds. Can be overridden after instantiation if needed.
    """

    _QueueItem = typing.Tuple[Timestamp, CANID, CyphalFrame]

    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        finalizer: SessionFinalizer,
    ):
        """Use the factory method."""
        self._specifier = specifier
        self._payload_metadata = payload_metadata

        self._queue: asyncio.Queue[CANInputSession._QueueItem] = asyncio.Queue()
        self._transfer_id_timeout_ns = int(CANInputSession.DEFAULT_TRANSFER_ID_TIMEOUT / _NANO)

        self._receivers = [TransferReassembler(nid, payload_metadata.extent_bytes) for nid in _node_id_range()]

        self._statistics = CANInputSessionStatistics()  # We could easily support per-source-node statistics if needed

        super().__init__(finalizer=finalizer)

    def _push_frame(self, timestamp: Timestamp, can_id: CANID, frame: CyphalFrame) -> None:
        """
        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        try:
            self._queue.put_nowait((timestamp, can_id, frame))
        except asyncio.QueueFull:
            self._statistics.drops += 1
            _logger.info(
                "%s: Input queue overflow; frame %s (CAN ID fields: %s) received at %s is dropped",
                self,
                frame,
                can_id,
                timestamp,
            )

    @property
    def frame_queue_capacity(self) -> typing.Optional[int]:
        """
        Capacity of the input frame queue. None means that the capacity is unlimited, which is the default.
        This may deplete the heap if input transfers are not consumed quickly enough so beware.

        If the capacity is changed and the new value is smaller than the number of frames currently in the queue,
        the newest frames will be discarded and the number of queue overruns will be incremented accordingly.
        The complexity of a queue capacity change may be up to linear of the number of frames currently in the queue.
        If the value is not None, it must be a positive integer, otherwise you get a :class:`ValueError`.
        """
        return self._queue.maxsize if self._queue.maxsize > 0 else None

    @frame_queue_capacity.setter
    def frame_queue_capacity(self, value: typing.Optional[int]) -> None:
        if value is not None and not value > 0:
            raise ValueError(f"Invalid value for queue capacity: {value}")

        old_queue = self._queue
        self._queue = asyncio.Queue(int(value) if value is not None else 0)
        try:
            while True:
                self._push_frame(*old_queue.get_nowait())
        except asyncio.QueueEmpty:
            pass

    @property
    def specifier(self) -> pycyphal.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> CANInputSessionStatistics:
        return copy.copy(self._statistics)

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout_ns * _NANO

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        if value > 0:
            self._transfer_id_timeout_ns = round(value / _NANO)
        else:
            raise ValueError(f"Invalid value for transfer-ID timeout [second]: {value}")

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        out = await self._do_receive(monotonic_deadline)
        assert (
            out is None or self.specifier.remote_node_id is None or out.source_node_id == self.specifier.remote_node_id
        ), "Internal input session protocol violation"
        return out

    def close(self) -> None:  # pylint: disable=useless-super-delegation
        super().close()

    async def _do_receive(self, monotonic_deadline: float) -> typing.Optional[pycyphal.transport.TransferFrom]:
        loop = asyncio.get_running_loop()
        while True:
            try:
                # Continue reading past the deadline until the queue is empty or a transfer is received.
                timeout = monotonic_deadline - loop.time()
                if timeout > 0:
                    timestamp, canid, frame = await asyncio.wait_for(self._queue.get(), timeout)
                else:
                    timestamp, canid, frame = self._queue.get_nowait()
                assert isinstance(timestamp, Timestamp)
                assert isinstance(canid, CANID)
                assert isinstance(frame, CyphalFrame)
            except (asyncio.TimeoutError, asyncio.QueueEmpty):
                # If there are unprocessed messages, allow the caller to read them even if the instance is closed.
                self._raise_if_closed()
                return None

            self._statistics.frames += 1

            if isinstance(canid, MessageCANID):
                assert isinstance(self._specifier.data_specifier, pycyphal.transport.MessageDataSpecifier)
                assert self._specifier.data_specifier.subject_id == canid.subject_id
                source_node_id = canid.source_node_id
                if source_node_id is None:
                    # Anonymous transfer - no reconstruction needed
                    self._statistics.transfers += 1
                    self._statistics.payload_bytes += len(frame.padded_payload)
                    out = pycyphal.transport.TransferFrom(
                        timestamp=timestamp,
                        priority=canid.priority,
                        transfer_id=frame.transfer_id,
                        fragmented_payload=[frame.padded_payload],
                        source_node_id=None,
                    )
                    _logger.debug("%s: Received anonymous transfer: %s; current stats: %s", self, out, self._statistics)
                    return out

            elif isinstance(canid, ServiceCANID):
                assert isinstance(self._specifier.data_specifier, pycyphal.transport.ServiceDataSpecifier)
                assert self._specifier.data_specifier.service_id == canid.service_id
                assert (
                    self._specifier.data_specifier.role == pycyphal.transport.ServiceDataSpecifier.Role.REQUEST
                ) == canid.request_not_response
                source_node_id = canid.source_node_id

            else:
                assert False

            receiver = self._receivers[source_node_id]
            result = receiver.process_frame(timestamp, canid.priority, frame, self._transfer_id_timeout_ns)
            if isinstance(result, TransferReassemblyErrorID):
                self._statistics.errors += 1
                self._statistics.reception_error_counters[result] += 1
                _logger.debug(
                    "%s: Rejecting CAN frame %s because %s; current stats: %s", self, frame, result, self._statistics
                )
            elif isinstance(result, pycyphal.transport.TransferFrom):
                self._statistics.transfers += 1
                self._statistics.payload_bytes += sum(map(len, result.fragmented_payload))
                _logger.debug("%s: Received transfer: %s; current stats: %s", self, result, self._statistics)
                return result
            elif result is None:
                pass  # Nothing to do - expecting more frames
            else:
                assert False


def _node_id_range() -> typing.Iterable[int]:
    return range(CANID.NODE_ID_MASK + 1)


_NANO = 1e-9
