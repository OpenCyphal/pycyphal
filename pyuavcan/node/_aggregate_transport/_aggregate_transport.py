#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.transport
from . import _aggregate_session


class AggregateTransport:
    def __init__(self) -> None:
        self._transports: typing.List[pyuavcan.transport.Transport] = []

    @property
    def local_node_id(self) -> int:
        raise NotImplementedError

    async def set_local_node_id(self, node_id: int) -> None:
        raise NotImplementedError

    @property
    def transports(self) -> typing.List[pyuavcan.transport.Transport]:
        return self._transports[:]  # Return copy to prevent mutation

    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def get_broadcast_output(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _aggregate_session.BroadcastOutputAggregateSession:
        raise NotImplementedError

    async def get_unicast_output(self, data_specifier: pyuavcan.transport.DataSpecifier, destination_node_id: int) \
            -> _aggregate_session.UnicastOutputAggregateSession:
        raise NotImplementedError

    async def get_promiscuous_input(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _aggregate_session.PromiscuousInputAggregateSession:
        raise NotImplementedError

    async def get_selective_input(self, data_specifier: pyuavcan.transport.DataSpecifier, source_node_id: int) \
            -> _aggregate_session.SelectiveInputAggregateSession:
        raise NotImplementedError
