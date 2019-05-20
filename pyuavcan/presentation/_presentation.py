#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import pyuavcan.dsdl
import pyuavcan.transport
from ._channel import Publisher, Subscriber, Client, Server


MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


DEFAULT_PRIORITY = pyuavcan.transport.Priority.SLOW


class Presentation:
    def __init__(self, transport: pyuavcan.transport.Transport) -> None:
        self._transport = transport

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        return self._transport

    async def get_publisher(self,
                            cls:        typing.Type[MessageClass],
                            subject_id: int,
                            priority:   pyuavcan.transport.Priority = DEFAULT_PRIORITY) -> Publisher[MessageClass]:
        raise NotImplementedError

    async def get_publisher_with_fixed_subject_id(self,
                                                  cls:      typing.Type[FixedPortMessageClass],
                                                  priority: pyuavcan.transport.Priority = DEFAULT_PRIORITY) \
            -> Publisher[FixedPortMessageClass]:
        return await self.get_publisher(cls=cls,
                                        subject_id=pyuavcan.dsdl.get_fixed_port_id(cls),
                                        priority=priority)

    async def get_subscriber(self, cls: typing.Type[MessageClass], subject_id: int) -> Subscriber[MessageClass]:
        raise NotImplementedError

    async def get_subscriber_with_fixed_subject_id(self, cls: typing.Type[FixedPortMessageClass]) \
            -> Subscriber[FixedPortMessageClass]:
        return await self.get_subscriber(cls=cls, subject_id=pyuavcan.dsdl.get_fixed_port_id(cls))

    async def get_client(self,
                         cls:            typing.Type[ServiceClass],
                         service_id:     int,
                         server_node_id: int,
                         priority:       pyuavcan.transport.Priority = DEFAULT_PRIORITY) -> Client[ServiceClass]:
        raise NotImplementedError

    async def get_client_with_fixed_service_id(self,
                                               cls:            typing.Type[FixedPortServiceClass],
                                               server_node_id: int,
                                               priority:       pyuavcan.transport.Priority = DEFAULT_PRIORITY) \
            -> Client[FixedPortServiceClass]:
        return await self.get_client(cls=cls,
                                     service_id=pyuavcan.dsdl.get_fixed_port_id(cls),
                                     server_node_id=server_node_id,
                                     priority=priority)

    async def get_server(self, cls: typing.Type[ServiceClass], service_id: int) -> Server[ServiceClass]:
        raise NotImplementedError

    async def get_server_with_fixed_service_id(self, cls: typing.Type[FixedPortServiceClass]) \
            -> Server[FixedPortServiceClass]:
        return await self.get_server(cls=cls, service_id=pyuavcan.dsdl.get_fixed_port_id(cls))
