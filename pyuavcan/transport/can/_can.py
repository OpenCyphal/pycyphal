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
import pyuavcan.transport
from .media import Media, TimestampedDataFrame, optimize_filter_configurations, FilterConfiguration
from ._session import CANInputSession, CANOutputSession
from ._session import BroadcastCANOutputSession, UnicastCANOutputSession
from ._frame import UAVCANFrame, TimestampedUAVCANFrame, TRANSFER_ID_MODULO
from ._identifier import CANID, generate_filter_configurations
from ._input_dispatch_table import InputDispatchTable


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CANTransportStatistics(pyuavcan.transport.TransportStatistics):
    """
    The following invariants apply::

        out_frames >= out_frames_loopback
        in_frames >= in_frames_uavcan >= in_frames_uavcan_accepted
        out_frames_loopback >= in_frames_loopback
    """
    in_frames:                 int = 0  #: Number of genuine frames received from the bus (loopback not included).
    in_frames_uavcan:          int = 0  #: Subset of the above that happen to be valid UAVCAN frames.
    in_frames_uavcan_accepted: int = 0  #: Subset of the above that are useful for the local application.
    in_frames_loopback:        int = 0  #: Number of loopback frames received from the media instance (not bus).
    in_frames_errored:         int = 0  #: How many frames of any kind could not be successfully processed.

    out_frames:          int = 0        #: Number of frames sent to the media instance successfully.
    out_frames_timeout:  int = 0        #: Number of frames that were supposed to be sent but timed out.
    out_frames_loopback: int = 0        #: Number of sent frames that we requested loopback for.

    @property
    def media_acceptance_filtering_efficiency(self) -> float:
        """
        An efficiency metric for the acceptance filtering implemented in the media instance.
        The value of 1.0 (100%) indicates perfect filtering, where the media can sort out relevant frames from
        irrelevant ones completely autonomously. The value of 0 indicates that none of the frames passed over
        from the media instance are useful for the application (all ignored).
        """
        return (self.in_frames_uavcan_accepted / self.in_frames) if self.in_frames > 0 else 1.0

    @property
    def lost_loopback_frames(self) -> int:
        """
        The number of loopback frames that have been requested but never returned. Normally the value should be zero.
        The value may transiently increase to small values if the counters happened to be sampled while the loopback
        frames reside in the transmission queue of the CAN controller awaiting being processed. If the value remains
        positive for long periods of time, the media driver is probably misbehaving.
        A negative value means that the media instance is sending more loopback frames than requested (bad).
        """
        return self.out_frames_loopback - self.in_frames_loopback


class CANTransport(pyuavcan.transport.Transport):
    """
    The standard UAVCAN/CAN transport implementation as defined in the UAVCAN specification.
    Please read the module documentation for details.
    """

    def __init__(self,
                 media:         Media,
                 local_node_id: typing.Optional[int],
                 loop:          typing.Optional[asyncio.AbstractEventLoop] = None):
        """
        :param media:         The media implementation.
        :param local_node_id: The node-ID to use. Can't be changed. None means anonymous (useful for PnP allocation).
        :param loop:          The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._maybe_media: typing.Optional[Media] = media
        self._local_node_id = int(local_node_id) if local_node_id is not None else None
        self._media_lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        # Lookup performance for the output registry is not important because it's only used for loopback frames.
        # Hence we don't trade-off memory for speed here.
        self._output_registry: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, CANOutputSession] = {}

        # Input lookup must be fast, so we use constant-complexity static lookup table.
        self._input_dispatch_table = InputDispatchTable()

        self._last_filter_configuration_set: typing.Optional[typing.Sequence[FilterConfiguration]] = None

        self._frame_stats = CANTransportStatistics()

        if self._local_node_id is not None and not (0 <= self._local_node_id <= CANID.NODE_ID_MASK):
            raise ValueError(f'Invalid node ID for CAN: {self._local_node_id}')

        if media.mtu not in Media.VALID_MTU_SET:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The MTU value {media.mtu} is not a member of {Media.VALID_MTU_SET}')
        self._mtu = media.mtu - 1
        assert self._mtu > 0

        if media.number_of_acceptance_filters < 1:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The number of acceptance filters is too low: {media.number_of_acceptance_filters}')

        if media.loop is not self._loop:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                f'The media instance cannot use a different event loop: {media.loop} is not {self._loop}')

        media_name = type(media).__name__.lower()[:-len('Media')]
        self._descriptor = \
            f'<can><{media_name} mtu="{media.mtu}">{media.interface_name}</{media_name}></can>'

        media.start(self._on_frames_received, no_automatic_retransmission=self._local_node_id is None)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=TRANSFER_ID_MODULO,
            max_nodes=CANID.NODE_ID_MASK + 1,
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        """
        If the local node-ID is not assigned, automatic retransmission in the media implementation is disabled to
        facilitate plug-and-play node-ID allocation.
        """
        return self._local_node_id

    @property
    def input_sessions(self) -> typing.Sequence[CANInputSession]:
        return list(self._input_dispatch_table.items)

    @property
    def output_sessions(self) -> typing.Sequence[CANOutputSession]:
        return list(self._output_registry.values())

    @property
    def descriptor(self) -> str:
        return self._descriptor

    def close(self) -> None:
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:
                _logger.exception('Failed to close session %r: %s', s, ex)

        media, self._maybe_media = self._maybe_media, None
        if media is not None:  # Double-close is NOT an error!
            media.close()

    def sample_statistics(self) -> CANTransportStatistics:
        return copy.copy(self._frame_stats)

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> CANInputSession:
        """
        See the base class docs for background.
        Whenever an input session is created or destroyed, the hardware acceptance filters are reconfigured
        automatically; computation of a new configuration and its deployment on the CAN controller may be slow.
        """
        if self._maybe_media is None:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')

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
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> CANOutputSession:
        if self._maybe_media is None:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')

        try:
            out = self._output_registry[specifier]
            assert out.specifier == specifier
            assert (specifier.remote_node_id is None) == isinstance(out, BroadcastCANOutputSession)
            return out
        except KeyError:
            pass

        def finalizer() -> None:
            self._output_registry.pop(specifier)

        if specifier.is_broadcast:
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
        if not self._last_filter_configuration_set:
            # It is necessary to reconfigure the filters at least once to ensure that we are able to receive
            # loopback frames even if there are no input sessions in use.
            self._reconfigure_acceptance_filters()
        return session

    async def _do_send_until(self, frames: typing.Iterable[UAVCANFrame], monotonic_deadline: float) -> bool:
        """
        All frames shall share the same CAN ID value.
        """
        frames_list = list(frames)
        del frames
        async with self._media_lock:
            if self._maybe_media is None:
                raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')
            else:
                num_sent = await self._maybe_media.send_until((x.compile() for x in frames_list), monotonic_deadline)
            assert 0 <= num_sent <= len(frames_list), 'Media sub-layer API contract violation'
            sent_frames, unsent_frames = frames_list[:num_sent], frames_list[num_sent:]

            self._frame_stats.out_frames += len(sent_frames)
            self._frame_stats.out_frames_timeout += len(unsent_frames)
            self._frame_stats.out_frames_loopback += sum(1 for f in sent_frames if f.loopback)

        if unsent_frames:
            can_id_int_set = set(f.identifier for f in unsent_frames)
            assert len(can_id_int_set) == 1, 'CAN transport layer internal contract violation'
            can_id_int, = can_id_int_set
            _logger.info('% frames of %d total with CAN ID 0x%08x could not be sent before the deadline',
                         len(unsent_frames), num_sent, can_id_int)

        return not unsent_frames

    def _on_frames_received(self, frames: typing.Iterable[TimestampedDataFrame]) -> None:
        for raw_frame in frames:
            try:
                if raw_frame.loopback:
                    self._frame_stats.in_frames_loopback += 1
                else:
                    self._frame_stats.in_frames += 1

                cid = CANID.parse(raw_frame.identifier)
                if cid is not None:                                             # Ignore non-UAVCAN CAN frames
                    ufr = TimestampedUAVCANFrame.parse(raw_frame)
                    if ufr is not None:                                         # Ignore non-UAVCAN CAN frames
                        self._handle_any_frame(cid, ufr)
            except Exception as ex:  # pragma: no cover
                self._frame_stats.in_frames_errored += 1
                _logger.exception(f'Unhandled exception while processing input CAN frame {raw_frame}: {ex}')

    def _handle_any_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> None:
        if not frame.loopback:
            self._frame_stats.in_frames_uavcan += 1
            if self._handle_received_frame(can_id, frame):
                self._frame_stats.in_frames_uavcan_accepted += 1
        else:
            self._handle_loopback_frame(can_id, frame)

    def _handle_received_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> bool:
        assert not frame.loopback
        ss = pyuavcan.transport.InputSessionSpecifier(can_id.data_specifier, can_id.source_node_id)
        accepted = False
        dest_nid = can_id.get_destination_node_id()
        if dest_nid is None or dest_nid == self._local_node_id:
            session = self._input_dispatch_table.get(ss)
            if session is not None:
                # noinspection PyProtectedMember
                session._push_frame(can_id, frame)
                accepted = True

            if ss.remote_node_id is not None:
                ss = pyuavcan.transport.InputSessionSpecifier(ss.data_specifier, None)
                session = self._input_dispatch_table.get(ss)
                if session is not None:
                    # noinspection PyProtectedMember
                    session._push_frame(can_id, frame)
                    accepted = True

        return accepted

    def _handle_loopback_frame(self, can_id: CANID, frame: TimestampedUAVCANFrame) -> None:
        assert frame.loopback
        ss = pyuavcan.transport.OutputSessionSpecifier(can_id.data_specifier, can_id.get_destination_node_id())
        try:
            session = self._output_registry[ss]
        except KeyError:
            _logger.info('No matching output session for loopback frame: %s; parsed CAN ID: %s; session specifier: %s. '
                         'Either the session has just been closed or the media driver is misbehaving.',
                         frame, can_id, ss)
        else:
            # noinspection PyProtectedMember
            session._handle_loopback_frame(frame)

    def _reconfigure_acceptance_filters(self) -> None:
        subject_ids = set(
            ds.subject_id for ds in (x.specifier.data_specifier for x in self._input_dispatch_table.items)
            if isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        )
        fcs = generate_filter_configurations(subject_ids, self._local_node_id)
        assert len(fcs) > len(subject_ids)
        del subject_ids
        if self._maybe_media is not None:
            num_filters = self._maybe_media.number_of_acceptance_filters
            fcs = optimize_filter_configurations(fcs, num_filters)
            assert len(fcs) <= num_filters
            if self._last_filter_configuration_set != fcs:
                try:
                    self._maybe_media.configure_acceptance_filters(fcs)
                except Exception:  # pragma: no cover
                    self._last_filter_configuration_set = None
                    raise
                else:
                    self._last_filter_configuration_set = fcs
