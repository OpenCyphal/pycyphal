#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import pyuavcan.transport
import pyuavcan.dsdl
from . import _port


MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


DEFAULT_PRIORITY = pyuavcan.transport.Priority.SLOW


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
                            cls:        typing.Type[MessageClass],
                            subject_id: int,
                            priority:   pyuavcan.transport.Priority = DEFAULT_PRIORITY,
                            loopback:   bool = False) -> _port.Publisher[MessageClass]:
        raise NotImplementedError

    async def new_publisher_with_fixed_subject_id(self,
                                                  cls:      typing.Type[FixedPortMessageClass],
                                                  priority: pyuavcan.transport.Priority = DEFAULT_PRIORITY,
                                                  loopback: bool = False) -> _port.Publisher[FixedPortMessageClass]:
        return await self.new_publisher(cls=cls,
                                        subject_id=pyuavcan.dsdl.get_fixed_port_id(cls),
                                        priority=priority,
                                        loopback=loopback)

    async def new_subscriber(self, cls: typing.Type[MessageClass], subject_id: int) -> None:
        raise NotImplementedError

    async def new_subscriber_with_fixed_subject_id(self, cls: typing.Type[FixedPortMessageClass]) -> None:
        return await self.new_subscriber(cls=cls, subject_id=pyuavcan.dsdl.get_fixed_port_id(cls))

    async def new_client(self,
                         cls:            typing.Type[ServiceClass],
                         service_id:     int,
                         server_node_id: int,
                         priority:       pyuavcan.transport.Priority = DEFAULT_PRIORITY) -> None:
        raise NotImplementedError

    async def new_client_with_fixed_service_id(self,
                                               cls:            typing.Type[FixedPortServiceClass],
                                               server_node_id: int,
                                               priority:       pyuavcan.transport.Priority = DEFAULT_PRIORITY) -> None:
        return await self.new_client(cls=cls,
                                     service_id=pyuavcan.dsdl.get_fixed_port_id(cls),
                                     server_node_id=server_node_id,
                                     priority=priority)

    async def get_server(self, cls: typing.Type[ServiceClass], service_id: int) -> None:
        raise NotImplementedError

    async def get_server_with_fixed_service_id(self, cls: typing.Type[FixedPortServiceClass]) -> None:
        return await self.get_server(cls=cls, service_id=pyuavcan.dsdl.get_fixed_port_id(cls))
