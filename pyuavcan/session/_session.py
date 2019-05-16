#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import pyuavcan.transport
import pyuavcan.dsdl


class Session:
    def __init__(self) -> None:
        self._transports: typing.List[pyuavcan.transport.Transport] = []

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    @property
    def local_node_id(self) -> int:
        raise NotImplementedError

    async def set_local_node_id(self, node_id: int) -> None:
        raise NotImplementedError

    @property
    def transports(self) -> typing.List[pyuavcan.transport.Transport]:
        return self._transports[:]      # Return copy to prevent mutation

    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def new_publisher(self,
                            cls:        pyuavcan.dsdl.CompositeObjectTypeVar,
                            subject_id: typing.Optional[int] = None,
                            priority:   pyuavcan.transport.Priority = pyuavcan.transport.Priority.NOMINAL,
                            loopback:   bool = False) -> None:
        raise NotImplementedError

    async def new_subscriber(self,
                             cls:        pyuavcan.dsdl.CompositeObjectTypeVar,
                             subject_id: typing.Optional[int] = None) -> None:
        raise NotImplementedError

    async def new_client(self,
                         cls:            pyuavcan.dsdl.ServiceObjectTypeVar,
                         server_node_id: int,
                         service_id:     typing.Optional[int] = None,
                         priority:       pyuavcan.transport.Priority = pyuavcan.transport.Priority.NOMINAL) -> None:
        raise NotImplementedError

    async def get_server(self,
                         cls:        pyuavcan.dsdl.ServiceObjectTypeVar,
                         service_id: typing.Optional[int] = None) -> None:
        raise NotImplementedError
