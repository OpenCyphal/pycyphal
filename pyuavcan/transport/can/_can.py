#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import asyncio
import pyuavcan.transport
from . import _session, media as _media, _frame, _can_id


_SessionFactory = typing.TypeVar('_SessionFactory')


class CANTransport(pyuavcan.transport.Transport):
    def __init__(self,
                 media: _media.Media,
                 loop:  typing.Optional[asyncio.AbstractEventLoop] = None):
        self._media = media
        self._local_node_id: typing.Optional[int] = None
        self._started = False
        self._media_lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

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

    async def get_broadcast_output(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _session.BroadcastOutputSession:
        def finalizer() -> None:
            pass        # TODO

        return _session.BroadcastOutputSession(data_specifier=data_specifier,
                                               transport=self,
                                               media_lock=self._media_lock,
                                               finalizer=finalizer)

    async def get_unicast_output(self, data_specifier: pyuavcan.transport.DataSpecifier, destination_node_id: int) \
            -> _session.UnicastOutputSession:
        def finalizer() -> None:
            pass        # TODO

        return _session.UnicastOutputSession(destination_node_id=destination_node_id,
                                             data_specifier=data_specifier,
                                             transport=self,
                                             media_lock=self._media_lock,
                                             finalizer=finalizer)

    async def get_promiscuous_input(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _session.PromiscuousInputSession:
        def finalizer() -> None:
            pass        # TODO

        queue: asyncio.Queue[_session.InputQueueItem] = asyncio.Queue(loop=self._loop)  # TODO

        return _session.PromiscuousInputSession(data_specifier=data_specifier,
                                                loop=self._loop,
                                                queue=queue,
                                                finalizer=finalizer)

    async def get_selective_input(self, data_specifier: pyuavcan.transport.DataSpecifier, source_node_id: int) \
            -> _session.SelectiveInputSession:
        def finalizer() -> None:
            pass        # TODO

        queue: asyncio.Queue[_session.InputQueueItem] = asyncio.Queue(loop=self._loop)  # TODO

        return _session.SelectiveInputSession(source_node_id=source_node_id,
                                              data_specifier=data_specifier,
                                              loop=self._loop,
                                              queue=queue,
                                              finalizer=finalizer)

    @property
    def media(self) -> _media.Media:
        return self._media

    async def _on_frames_received(self, frames: typing.Iterable[_media.TimestampedDataFrame]) -> None:
        async with self._media_lock:
            for fr in frames:
                cid = _can_id.CANID.try_parse(fr.identifier)
                # TODO queue dispatch
                # TODO loopback handling

    async def _reconfigure_acceptance_filters(self) -> None:
        pass

    def __str__(self) -> str:
        raise NotImplementedError
