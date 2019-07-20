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
from .media import Media, TimestampedDataFrame, optimize_filter_configurations, FilterConfiguration
from ._session import CANInputSession, CANOutputSession
from ._session import BroadcastCANOutputSession, UnicastCANOutputSession
from ._frame import UAVCANFrame, TimestampedUAVCANFrame, TRANSFER_ID_MODULO
from ._identifier import CANID, generate_filter_configurations
from ._input_dispatch_table import InputDispatchTable


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CANFrameStatistics:
    """
    Invariants::

        sent >= loopback_requested
        received >= received_uavcan >= received_uavcan_accepted
        loopback_requested >= loopback_returned
    """

    sent: int = 0                       #: Number of frames sent to the media instance successfully
    unsent: int = 0                     #: Number of frames that were supposed to be sent but timed out

    received:                 int = 0   #: Number of genuine frames received from the bus (loopback not included)
    received_uavcan:          int = 0   #: Subset of the above that happen to be valid UAVCAN frames
    received_uavcan_accepted: int = 0   #: Subset of the above that are useful for the local application

    loopback_requested: int = 0         #: Number of sent frames that we requested loopback for
    loopback_returned:  int = 0         #: Number of loopback frames received from the media instance (not from the bus)

    errored: int = 0                    #: How many frames of any kind could not be successfully processed

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
    """
    CAN 2.0 and CAN FD transport implementation.
    """

    def __init__(self, media: Media, loop: typing.Optional[asyncio.AbstractEventLoop] = None):
        """
        :param media: The media implementation such as :class:`pyuavcan.transport.can.media.socketcan.SocketCAN`.
        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._maybe_media: typing.Optional[Media] = media
        self._local_node_id: typing.Optional[int] = None
        self._media_lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        # Lookup performance for the output registry is not important because it's only used for loopback frames.
        # Hence we don't trade-off memory for speed here.
        # https://stackoverflow.com/questions/510406/is-there-a-way-to-get-the-current-ref-count-of-an-object-in-python
        self._output_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, CANOutputSession] = {}

        # Input lookup must be fast, so we use constant-complexity static lookup table.
        self._input_dispatch_table = InputDispatchTable()

        self._last_filter_configuration_set: typing.Optional[typing.Sequence[FilterConfiguration]] = None

        self._frame_stats = CANFrameStatistics()

        if media.mtu not in Media.VALID_MTU_SET:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The MTU value {media.mtu} is not a member of {Media.VALID_MTU_SET}')
        self._frame_payload_capacity = media.mtu - 1
        assert self._frame_payload_capacity > 0

        if media.number_of_acceptance_filters < 1:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The number of acceptance filters is too low: {media.number_of_acceptance_filters}')

        media.set_received_frames_handler(self._on_frames_received)   # Starts the transport.

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

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

    def set_local_node_id(self, node_id: int) -> None:
        if self._local_node_id is None:
            if 0 <= node_id <= CANID.NODE_ID_MASK:
                self._local_node_id = int(node_id)
                self._media.enable_automatic_retransmission()
                self._reconfigure_acceptance_filters()
            else:
                raise ValueError(f'Invalid node ID for CAN: {node_id}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    @property
    def input_sessions(self) -> typing.List[pyuavcan.transport.InputSession]:
        return list(self._input_dispatch_table.items)

    @property
    def output_sessions(self) -> typing.List[pyuavcan.transport.OutputSession]:
        return list(self._output_registry.values())

    def close(self) -> None:
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:
                _logger.exception('Failed to close session %r: %s', s, ex)

        media, self._maybe_media = self._maybe_media, None
        if media is not None:  # Double-close is NOT an error!
            media.close()

    def sample_frame_statistics(self) -> CANFrameStatistics:
        return copy.copy(self._frame_stats)

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> CANInputSession:
        self._raise_if_closed()

        def finalizer() -> None:
            self._input_dispatch_table.remove(specifier)
            self._reconfigure_acceptance_filters()

        session = self._input_dispatch_table.get(specifier)
        if session is None:
            session = CANInputSession(specifier=specifier,
                                      payload_metadata=payload_metadata,
                                      loop=self._loop,
                                      finalizer=finalizer)
            self._input_dispatch_table.add(session)
            self._reconfigure_acceptance_filters()
        return session

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> CANOutputSession:
        self._raise_if_closed()

        try:
            out = self._output_registry[specifier]
            assert out.specifier == specifier
            assert (specifier.remote_node_id is None) == isinstance(out, BroadcastCANOutputSession)
            return out
        except KeyError:
            pass

        def finalizer() -> None:
            self._output_registry.pop(specifier)

        if specifier.remote_node_id is None:
            session: CANOutputSession = \
                BroadcastCANOutputSession(specifier=specifier,
                                          payload_metadata=payload_metadata,
                                          transport=self,
                                          send_handler=self._do_send_until,
                                          finalizer=finalizer)
        else:
            session = UnicastCANOutputSession(specifier=specifier,
                                              payload_metadata=payload_metadata,
                                              transport=self,
                                              send_handler=self._do_send_until,
                                              finalizer=finalizer)

        self._output_registry[specifier] = session
        return session

    async def _do_send_until(self, frames: typing.Iterable[UAVCANFrame], monotonic_deadline: float) -> bool:
        async with self._media_lock:
            frames, stat = itertools.tee(frames)
            timeout = monotonic_deadline - self._loop.time()
            try:
                await asyncio.wait_for(self._media.send(x.compile() for x in frames), timeout=timeout, loop=self._loop)
            except asyncio.TimeoutError:
                success = False
                stat_list = list(stat)
                stat = iter(stat_list)
                _logger.info('%d frames with the following CAN ID values could not be sent in %.3f seconds: %r',
                             len(stat_list), timeout, ', '.join(set(f'0x{f.identifier:08x}' for f in stat_list)))
            else:
                success = True

            for f in stat:
                if f.loopback:
                    self._frame_stats.loopback_requested += 1
                if success:
                    self._frame_stats.sent += 1
                else:
                    self._frame_stats.unsent += 1
            return success

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
                        self._handle_any_frame(cid, ufr)
            except Exception as ex:  # pragma: no cover
                self._frame_stats.errored += 1
                _logger.exception(f'Unhandled exception while processing input CAN frame {raw_frame}: {ex}')

    def _handle_any_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> None:
        if not frame.loopback:
            self._frame_stats.received_uavcan += 1
            if self._handle_received_frame(can_id, frame):
                self._frame_stats.received_uavcan_accepted += 1
        else:
            self._handle_loopback_frame(can_id, frame)

    def _handle_received_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> bool:
        assert not frame.loopback
        ss = can_id.to_input_session_specifier()
        accepted = False
        dest_nid = can_id.get_destination_node_id()
        if dest_nid is None or dest_nid == self._local_node_id:
            session = self._input_dispatch_table.get(ss)
            if session is not None:
                session.push_frame(can_id, frame)
                accepted = True

            if ss.remote_node_id is not None:
                ss = pyuavcan.transport.SessionSpecifier(ss.data_specifier, None)
                session = self._input_dispatch_table.get(ss)
                if session is not None:
                    session.push_frame(can_id, frame)
                    accepted = True

        return accepted

    def _handle_loopback_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> None:
        assert frame.loopback
        ss = can_id.to_output_session_specifier()
        try:
            session = self._output_registry[ss]
        except KeyError:
            _logger.info('No matching output session for loopback frame: %s; parsed CAN ID: %s; session specifier: %s. '
                         'Either the session has just been closed or the media driver is misbehaving.',
                         frame, can_id, ss)
        else:
            session.handle_loopback_frame(frame)

    def _reconfigure_acceptance_filters(self) -> None:
        subject_ids = set(
            ds.subject_id for ds in (x.specifier.data_specifier for x in self._input_dispatch_table.items)
            if isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        )

        fcs = generate_filter_configurations(subject_ids, self._local_node_id)
        assert len(fcs) > len(subject_ids)
        del subject_ids

        num_filters = self._media.number_of_acceptance_filters
        fcs = optimize_filter_configurations(fcs, num_filters)
        assert len(fcs) <= num_filters
        if self._last_filter_configuration_set != fcs:
            try:
                self._media.configure_acceptance_filters(fcs)
            except Exception:  # pragma: no cover
                self._last_filter_configuration_set = None
                raise
            else:
                self._last_filter_configuration_set = fcs

    @property
    def _media(self) -> Media:
        out = self._maybe_media
        if out is not None:
            return out
        else:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

    def _raise_if_closed(self) -> None:
        if self._maybe_media is None:
            raise pyuavcan.transport.ResourceClosedError(repr(self))
