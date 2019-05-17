#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.transport
from . import _aggregate_port


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

    async def get_output_port(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _aggregate_port.AggregateOutputPort:
        raise NotImplementedError

    async def get_input_port(self, data_specifier: pyuavcan.transport.DataSpecifier) \
            -> _aggregate_port.AggregateInputPort:
        raise NotImplementedError
