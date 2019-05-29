#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import asyncio
import logging
import pyuavcan.transport
from . import _session, media as _media, _frame, _can_id


_logger = logging.getLogger(__name__)


class CANTransport(pyuavcan.transport.Transport):
    def __init__(self,
                 media: _media.Media,
                 loop:  typing.Optional[asyncio.AbstractEventLoop] = None):
        self._media = media
        self._local_node_id: typing.Optional[int] = None
        self._started = False
        self._media_lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        self._output_registry: typing.Dict[pyuavcan.transport.DataSpecifier, _session.OutputSession] = {}

        self._media.set_received_frames_handler(self._on_frames_received)

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        sft_payload_capacity = self._media.max_data_field_length - 1
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=_frame.TRANSFER_ID_MODULO,
            node_id_set_cardinality=_can_id.CANID.NODE_ID_MASK + 1,
            single_frame_transfer_payload_capacity_bytes=sft_payload_capacity
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    async def set_local_node_id(self, node_id: int) -> None:
        if self._local_node_id is None:
            if 0 <= node_id <= _can_id.CANID.NODE_ID_MASK:
                self._local_node_id = int(node_id)
                await self._media.enable_automatic_retransmission()
                await self._reconfigure_acceptance_filters()
            else:
                raise ValueError(f'Invalid node ID for CAN: {node_id}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    async def close(self) -> None:
        await self._media.close()

    async def get_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    async def get_broadcast_output(self,
                                   data_specifier:   pyuavcan.transport.DataSpecifier,
                                   payload_metadata: pyuavcan.transport.PayloadMetadata) \
            -> _session.BroadcastOutputSession:
        def finalizer() -> None:
            pass        # TODO

        metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
        return _session.BroadcastOutputSession(metadata=metadata,
                                               transport=self,
                                               media_lock=self._media_lock,
                                               finalizer=finalizer)

    async def get_unicast_output(self,
                                 data_specifier:      pyuavcan.transport.DataSpecifier,
                                 payload_metadata:    pyuavcan.transport.PayloadMetadata,
                                 destination_node_id: int) -> _session.UnicastOutputSession:
        def finalizer() -> None:
            pass        # TODO

        metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
        return _session.UnicastOutputSession(destination_node_id=destination_node_id,
                                             metadata=metadata,
                                             transport=self,
                                             media_lock=self._media_lock,
                                             finalizer=finalizer)

    async def get_promiscuous_input(self,
                                    data_specifier:   pyuavcan.transport.DataSpecifier,
                                    payload_metadata: pyuavcan.transport.PayloadMetadata) \
            -> _session.PromiscuousInputSession:
        def finalizer() -> None:
            pass        # TODO

        queue: asyncio.Queue[_session.InputQueueItem] = asyncio.Queue(loop=self._loop)  # TODO

        metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
        return _session.PromiscuousInputSession(metadata=metadata,
                                                loop=self._loop,
                                                queue=queue,
                                                finalizer=finalizer)

    async def get_selective_input(self,
                                  data_specifier:   pyuavcan.transport.DataSpecifier,
                                  payload_metadata: pyuavcan.transport.PayloadMetadata,
                                  source_node_id:   int) -> _session.SelectiveInputSession:
        def finalizer() -> None:
            pass        # TODO

        queue: asyncio.Queue[_session.InputQueueItem] = asyncio.Queue(loop=self._loop)  # TODO

        metadata = pyuavcan.transport.SessionMetadata(data_specifier, payload_metadata)
        return _session.SelectiveInputSession(source_node_id=source_node_id,
                                              metadata=metadata,
                                              loop=self._loop,
                                              queue=queue,
                                              finalizer=finalizer)

    @property
    def media(self) -> _media.Media:
        return self._media

    def _on_frames_received(self, frames: typing.Iterable[_media.TimestampedDataFrame]) -> None:
        for raw_frame in frames:
            try:
                cid = _can_id.CANID.try_parse(raw_frame.identifier)
                if cid is not None:                                             # Ignore non-UAVCAN CAN frames
                    ufr = _frame.TimestampedUAVCANFrame.try_parse(raw_frame)
                    if ufr is not None:                                         # Ignore non-UAVCAN CAN frames
                        if not ufr.loopback:
                            self._handle_received_frame(cid, ufr)
                        else:
                            self._handle_loopback_frame(cid, ufr)
            except Exception as ex:
                _logger.exception(f'Unhandled exception while processing input CAN frame {raw_frame}: {ex}')

    def _handle_received_frame(self, can_id: _can_id.CANID, frame: _frame.TimestampedUAVCANFrame) -> None:
        assert not frame.loopback
        # TODO queue dispatch
        pass

    def _handle_loopback_frame(self, can_id: _can_id.CANID, frame: _frame.TimestampedUAVCANFrame) -> None:
        assert frame.loopback
        key = can_id.to_output_data_specifier()
        try:
            session = self._output_registry[key]
        except KeyError:
            _logger.info('No matching output session for loopback frame: %s; '
                         'parsed CAN ID: %s; reconstructed data specifier: %s. '
                         'Either the session has just been closed or the media driver is misbehaving.',
                         frame, can_id, key, self._media)
        else:
            session.handle_loopback_frame(frame)

    async def _reconfigure_acceptance_filters(self) -> None:
        pass

    def __str__(self) -> str:
        raise NotImplementedError
