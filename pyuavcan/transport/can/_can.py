#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import pyuavcan.transport
from . import _session, media as _media


_SessionFactory = typing.TypeVar('_SessionFactory')


def _auto_start(fun: _SessionFactory) -> _SessionFactory:
    async def decorator(self: CANTransport, *args, **kwargs) -> pyuavcan.transport.Session:
        if not self._started:
            self._started = True
            await self._start()
        return await fun(self, *args, **kwargs)
    return decorator


class CANTransport(pyuavcan.transport.Transport):
    def __init__(self, media: _media.Media):
        self._media = media
        self._local_node_id: typing.Optional[int] = None
        self._started = False

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        sft_payload_capacity = self._media.mtu - 1
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=32,
            node_id_set_cardinality=128,
            single_frame_transfer_payload_capacity_bytes=sft_payload_capacity
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    async def set_local_node_id(self, node_id: int) -> None:
        self._local_node_id = int(node_id)

    async def close(self) -> None:
        # TODO: STOP THE LOCAL TASK
        await self._media.close()

    async def get_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    @_auto_start
    async def get_broadcast_output(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _session.BroadcastOutputSession:
        raise NotImplementedError

    @_auto_start
    async def get_unicast_output(self, data_specifier: pyuavcan.transport.DataSpecifier, destination_node_id: int) \
            -> _session.UnicastOutputSession:
        raise NotImplementedError

    @_auto_start
    async def get_promiscuous_input(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _session.PromiscuousInputSession:
        raise NotImplementedError

    @_auto_start
    async def get_selective_input(self, data_specifier: pyuavcan.transport.DataSpecifier, source_node_id: int) \
            -> _session.SelectiveInputSession:
        raise NotImplementedError

    async def _start(self) -> None:
        pass

    def __str__(self) -> str:
        raise NotImplementedError
