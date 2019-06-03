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
import itertools
import dataclasses
import pyuavcan.transport
from .media import Media, TimestampedDataFrame, optimize_filter_configurations
from ._session import SessionFinalizer, CANInputSession, CANOutputSession
from ._session import PromiscuousCANInput, SelectiveCANInput, BroadcastCANOutput, UnicastCANOutput
from ._frame import UAVCANFrame, TimestampedUAVCANFrame, TRANSFER_ID_MODULO
from ._identifier import CANID, MessageCANID, ServiceCANID, generate_filter_configurations


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CANFrameStatistics:
    """
    Invariants:
        sent >= loopback_requested
        received >= received_uavcan >= received_uavcan_accepted
        loopback_requested >= loopback_returned
    """

    sent: int = 0                       # Number of frames sent to the media instance

    received:                 int = 0   # Number of genuine frames received from the bus (loopback not included)
    received_uavcan:          int = 0   # Subset of the above that happen to be valid UAVCAN frames
    received_uavcan_accepted: int = 0   # Subset of the above that are useful for the local application

    loopback_requested: int = 0         # Number of sent frames that we requested loopback for
    loopback_returned:  int = 0         # Number of loopback frames received from the media instance (not from the bus)

    errored: int = 0                    # How many frames of any kind could not be successfully processed

    @property
    def media_acceptance_filtering_efficiency(self) -> float:
        """
        An efficiency metric for the acceptance filtering implemented in the media instance.
        The value of 1.0 (100%) indicates perfect filtering, where the media can sort out relevant frames from
        irrelevant ones completely autonomously. The value of 0 indicates that none of the frames passed over
        from the media instance are useful for the application (all ignored).
        """
        return (self.received_uavcan_accepted / self.received) if self.received > 0 else 1.0

    @property
    def lost_loopback(self) -> int:
        """
        The number of loopback frames that have been requested but never returned. Normally the value should be zero.
        The value may transiently increase to small values if the counters happened to be sampled while the loopback
        frames reside in the transmission queue of the CAN controller awaiting being processed. If the value remains
        positive for long periods of time, the media driver is probably misbehaving.
        """
        return self.loopback_requested - self.loopback_returned


class CANTransport(pyuavcan.transport.Transport):
    def __init__(self,
                 media: Media,
                 loop:  typing.Optional[asyncio.AbstractEventLoop] = None):
        self._maybe_media: typing.Optional[Media] = media
        self._local_node_id: typing.Optional[int] = None
        self._media_lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        # Lookup performance for the output registry is not important because it's only used for loopback frames.
        # Hence we don't trade-off memory for speed here.
        # TODO: consider using weakref.WeakValueDictionary?
        # TODO: consider traversing using gc.get_referrers()?
        # https://stackoverflow.com/questions/510406/is-there-a-way-to-get-the-current-ref-count-of-an-object-in-python
        self._output_registry: typing.Dict[typing.Tuple[pyuavcan.transport.DataSpecifier, typing.Optional[int]],
                                           CANOutputSession] = {}  # None for broadcast

        # Input lookup must be fast, so we use constant-complexity static lookup table.
        # TODO: consider using weakref?
        # TODO: consider traversing using gc.get_referrers()?
        self._input_dispatch_table: typing.List[typing.Optional[CANInputSession]] = \
            [None] * (_INPUT_DISPATCH_TABLE_SIZE + 1)   # This method of construction is an order of magnitude faster.

        # This is redundant since it duplicates the state kept in the input dispatch table, but it is necessary
        # since the dispatch table takes almost a second to traverse.
        # TODO: encapsulate the input dispatch table
        self._input_sessions: typing.Set[CANInputSession] = set()

        self._frame_stats = CANFrameStatistics()

        if media.max_data_field_length not in Media.VALID_MAX_DATA_FIELD_LENGTH_SET:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The maximum data field length value {media.max_data_field_length} '
                f'is not a member of {Media.VALID_MAX_DATA_FIELD_LENGTH_SET}')
        self._frame_payload_capacity = media.max_data_field_length - 1
        assert self._frame_payload_capacity > 0

        if media.number_of_acceptance_filters < 1:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The number of acceptance filters is too low: {media.number_of_acceptance_filters}')

        media.set_received_frames_handler(self._on_frames_received)   # Starts the transport.

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=TRANSFER_ID_MODULO,
            node_id_set_cardinality=CANID.NODE_ID_MASK + 1,
            single_frame_transfer_payload_capacity_bytes=self.frame_payload_capacity
        )

    @property
    def frame_payload_capacity(self) -> int:
        return self._frame_payload_capacity

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    async def set_local_node_id(self, node_id: int) -> None:
        async with self._media_lock:
            if self._local_node_id is None:
                if 0 <= node_id <= CANID.NODE_ID_MASK:
                    self._local_node_id = int(node_id)
                    await self._media.enable_automatic_retransmission()
                    await self._reconfigure_acceptance_filters_with_lock_acquired()
                else:
                    raise ValueError(f'Invalid node ID for CAN: {node_id}')
            else:
                raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    @property
    def inputs(self) -> typing.List[pyuavcan.transport.InputSession]:
        return list(self._input_sessions)

    @property
    def outputs(self) -> typing.List[pyuavcan.transport.OutputSession]:
        return list(self._output_registry.values())

    async def close(self) -> None:
        async with self._media_lock:
            await self._media.close()
            self._maybe_media = None

    def sample_frame_statistics(self) -> CANFrameStatistics:
        return copy.copy(self._frame_stats)

    async def get_broadcast_output(self,
                                   data_specifier:   pyuavcan.transport.DataSpecifier,
                                   payload_metadata: pyuavcan.transport.PayloadMetadata) -> BroadcastCANOutput:
        def factory(finalizer: SessionFinalizer) -> BroadcastCANOutput:
            metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
            return BroadcastCANOutput(metadata=metadata,
                                      transport=self,
                                      send_handler=self._do_send,
                                      finalizer=finalizer)
        out = self._get_output(data_specifier, None, factory)
        assert isinstance(out, BroadcastCANOutput)
        return out

    async def get_unicast_output(self,
                                 data_specifier:      pyuavcan.transport.DataSpecifier,
                                 payload_metadata:    pyuavcan.transport.PayloadMetadata,
                                 destination_node_id: int) -> UnicastCANOutput:
        def factory(finalizer: SessionFinalizer) -> UnicastCANOutput:
            metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
            return UnicastCANOutput(destination_node_id=destination_node_id,
                                    metadata=metadata,
                                    transport=self,
                                    send_handler=self._do_send,
                                    finalizer=finalizer)
        out = self._get_output(data_specifier, destination_node_id, factory)
        assert isinstance(out, UnicastCANOutput)
        return out

    async def get_promiscuous_input(self,
                                    data_specifier:   pyuavcan.transport.DataSpecifier,
                                    payload_metadata: pyuavcan.transport.PayloadMetadata) -> PromiscuousCANInput:
        def factory(finalizer: SessionFinalizer) -> PromiscuousCANInput:
            metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
            return PromiscuousCANInput(metadata=metadata, loop=self._loop, finalizer=finalizer)
        out = await self._get_input(data_specifier, None, factory)
        assert isinstance(out, PromiscuousCANInput)
        return out

    async def get_selective_input(self,
                                  data_specifier:   pyuavcan.transport.DataSpecifier,
                                  payload_metadata: pyuavcan.transport.PayloadMetadata,
                                  source_node_id:   int) -> SelectiveCANInput:
        def factory(finalizer: SessionFinalizer) -> SelectiveCANInput:
            metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
            return SelectiveCANInput(source_node_id=source_node_id,
                                     metadata=metadata,
                                     loop=self._loop,
                                     finalizer=finalizer)
        out = await self._get_input(data_specifier, source_node_id, factory)
        assert isinstance(out, SelectiveCANInput)
        return out

    def _get_output(self,
                    data_specifier:      pyuavcan.transport.DataSpecifier,
                    destination_node_id: typing.Optional[int],
                    factory:             typing.Callable[[SessionFinalizer], CANOutputSession]) -> CANOutputSession:
        async def finalizer() -> None:
            self._output_registry.pop(key)

        key = data_specifier, destination_node_id
        try:
            return self._output_registry[key]
        except KeyError:
            session = factory(finalizer)
            self._output_registry[key] = session
            return session

    async def _get_input(self,
                         data_specifier: pyuavcan.transport.DataSpecifier,
                         source_node_id: typing.Optional[int],
                         factory:        typing.Callable[[SessionFinalizer], CANInputSession]) -> CANInputSession:
        async def finalizer() -> None:
            async with self._media_lock:
                self._input_dispatch_table[index] = None
                self._input_sessions.remove(session)
                await self._reconfigure_acceptance_filters_with_lock_acquired()

        async with self._media_lock:
            index = _compute_input_dispatch_table_index(data_specifier, source_node_id)
            session = self._input_dispatch_table[index]
            if session is None:
                session = factory(finalizer)
                self._input_dispatch_table[index] = session
                self._input_sessions.add(session)
                await self._reconfigure_acceptance_filters_with_lock_acquired()
            return session

    async def _do_send(self, frames: typing.Iterable[UAVCANFrame]) -> None:
        async with self._media_lock:
            frames, stat_iter = itertools.tee(frames)
            await self._media.send(x.compile() for x in frames)
            del frames
            for f in stat_iter:
                self._frame_stats.sent += 1
                if f.loopback:
                    self._frame_stats.loopback_requested += 1

    def _on_frames_received(self, frames: typing.Iterable[TimestampedDataFrame]) -> None:
        for raw_frame in frames:
            try:
                if raw_frame.loopback:
                    self._frame_stats.loopback_returned += 1
                else:
                    self._frame_stats.received += 1

                cid = CANID.try_parse(raw_frame.identifier)
                if cid is not None:                                             # Ignore non-UAVCAN CAN frames
                    ufr = TimestampedUAVCANFrame.try_parse(raw_frame)
                    if ufr is not None:                                         # Ignore non-UAVCAN CAN frames
                        if not ufr.loopback:
                            self._frame_stats.received_uavcan += 1
                            if self._handle_received_frame(cid, ufr):
                                self._frame_stats.received_uavcan_accepted += 1
                        else:
                            self._handle_loopback_frame(cid, ufr)
            except Exception as ex:  # pragma: no cover
                self._frame_stats.errored += 1
                _logger.exception(f'Unhandled exception while processing input CAN frame {raw_frame}: {ex}')

    def _handle_received_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> bool:
        assert not frame.loopback
        data_spec = can_id.to_input_data_specifier()
        if isinstance(can_id, ServiceCANID):
            exact_source_node_id: typing.Optional[int] = can_id.source_node_id
        elif isinstance(can_id, MessageCANID):
            exact_source_node_id = can_id.source_node_id
        else:
            assert False

        accepted = False
        for nid in {exact_source_node_id, None}:
            index = _compute_input_dispatch_table_index(data_spec, nid)
            session = self._input_dispatch_table[index]
            if session is not None:                                     # Ignore UAVCAN frames we don't care about
                session.push_frame(can_id, frame)
                accepted = True

        return accepted

    def _handle_loopback_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> None:
        assert frame.loopback
        data_spec = can_id.to_output_data_specifier()
        if isinstance(can_id, ServiceCANID):
            dest_nid: typing.Optional[int] = can_id.destination_node_id
        else:
            assert not hasattr(can_id, 'destination_node_id')
            dest_nid = None

        try:
            session = self._output_registry[(data_spec, dest_nid)]
        except KeyError:
            _logger.info('No matching output session for loopback frame: %s; '
                         'parsed CAN ID: %s; data specifier: %s; destination node ID: %s. '
                         'Either the session has just been closed or the media driver is misbehaving.',
                         frame, can_id, data_spec, dest_nid)
        else:
            session.handle_loopback_frame(frame)

    async def _reconfigure_acceptance_filters_with_lock_acquired(self) -> None:
        assert self._media_lock.locked, 'Internal protocol violation: lock is not acquired'

        subject_ids = set(
            ds.subject_id for ds in (x.data_specifier for x in self._input_sessions)
            if isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        )

        fcs = generate_filter_configurations(subject_ids, self.local_node_id)
        assert len(fcs) > len(subject_ids)
        del subject_ids

        num_filters = self._media.number_of_acceptance_filters
        fcs = optimize_filter_configurations(fcs, num_filters)
        assert len(fcs) <= num_filters
        await self._media.configure_acceptance_filters(fcs)

    @property
    def _media(self) -> Media:
        assert self._media_lock.locked, 'Internal protocol violation: non-synchronized media access'
        out = self._maybe_media
        if out is not None:
            return out
        else:
            raise pyuavcan.transport.ResourceClosedError('The driver is already closed')


# TODO: encapsulate the input dispatch table
def _compute_input_dispatch_table_index(data_specifier: pyuavcan.transport.DataSpecifier,
                                        source_node_id: typing.Optional[int]) -> int:
    """
    Time-memory trade-off: the input dispatch table is tens of megabytes large, but the lookup is very fast and O(1).
    """
    assert source_node_id is None or source_node_id < _NUM_NODE_IDS

    if isinstance(data_specifier, pyuavcan.transport.MessageDataSpecifier):
        dim1 = data_specifier.subject_id
    elif isinstance(data_specifier, pyuavcan.transport.ServiceDataSpecifier):
        if data_specifier.role == data_specifier.Role.CLIENT:
            dim1 = data_specifier.service_id + _NUM_SUBJECTS
        elif data_specifier.role == data_specifier.Role.SERVER:
            dim1 = data_specifier.service_id + _NUM_SUBJECTS + _NUM_SERVICES
        else:
            assert False
    else:
        assert False

    dim2_cardinality = _NUM_NODE_IDS + 1
    dim2 = source_node_id if source_node_id is not None else _NUM_NODE_IDS

    point = dim1 * dim2_cardinality + dim2

    assert 0 <= point < _INPUT_DISPATCH_TABLE_SIZE
    return point


_NUM_SUBJECTS = pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK + 1
_NUM_SERVICES = pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK + 1
_NUM_NODE_IDS = CANID.NODE_ID_MASK + 1

# Services multiplied by two to account for requests and responses.
# One added to nodes to allow promiscuous inputs which don't care about source node ID.
_INPUT_DISPATCH_TABLE_SIZE = (_NUM_SUBJECTS + _NUM_SERVICES * 2) * (_NUM_NODE_IDS + 1)


def _unittest_slow_can_compute_input_dispatch_table_index() -> None:
    values: typing.Set[int] = set()
    for node_id in (*range(_NUM_NODE_IDS), None):
        for subj in range(_NUM_SUBJECTS):
            out = _compute_input_dispatch_table_index(pyuavcan.transport.MessageDataSpecifier(subj), node_id)
            assert out not in values
            values.add(out)
            assert out < _INPUT_DISPATCH_TABLE_SIZE

        for serv in range(_NUM_SERVICES):
            for role in pyuavcan.transport.ServiceDataSpecifier.Role:
                out = _compute_input_dispatch_table_index(pyuavcan.transport.ServiceDataSpecifier(serv, role), node_id)
                assert out not in values
                values.add(out)
                assert out < _INPUT_DISPATCH_TABLE_SIZE

    assert len(values) == _INPUT_DISPATCH_TABLE_SIZE
