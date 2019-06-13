#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import pyuavcan
import pyuavcan.application
import uavcan.node


NodeInfo = uavcan.node.GetInfo_0_1.Response

MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


_logger = logging.getLogger(__name__)


class Node:
    def __init__(self,
                 transport: pyuavcan.transport.Transport,
                 info:      NodeInfo):
        self._presentation = pyuavcan.presentation.Presentation(transport)
        self._info = info
        self._heartbeat_publisher = pyuavcan.application.heartbeat_publisher.HeartbeatPublisher(self._presentation)

    @property
    def presentation(self) -> pyuavcan.presentation.Presentation:
        return self._presentation

    @property
    def info(self) -> NodeInfo:
        return self._info

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._presentation.transport.local_node_id

    async def set_local_node_id(self, node_id: int) -> None:
        await self._presentation.transport.set_local_node_id(node_id)

    @property
    def heartbeat_publisher(self) -> pyuavcan.application.heartbeat_publisher.HeartbeatPublisher:
        return self._heartbeat_publisher

    # ---------------------------------------- PUBLISHER FACTORY ----------------------------------------

    async def make_publisher(self, dtype: typing.Type[MessageClass], subject_id: int) \
            -> pyuavcan.presentation.Publisher[MessageClass]:
        """Wrapper for Presentation.make_publisher(..)."""
        return await self._presentation.make_publisher(dtype, subject_id)

    async def make_publisher_with_fixed_subject_id(self, dtype: typing.Type[FixedPortMessageClass]) \
            -> pyuavcan.presentation.Publisher[FixedPortMessageClass]:
        """Wrapper for Presentation.make_publisher_with_fixed_subject_id(..)."""
        return await self._presentation.make_publisher_with_fixed_subject_id(dtype)

    # ---------------------------------------- SUBSCRIBER FACTORY ----------------------------------------

    async def make_subscriber(self,
                              dtype:          typing.Type[MessageClass],
                              subject_id:     int,
                              queue_capacity: typing.Optional[int] = None) -> \
            pyuavcan.presentation.Subscriber[MessageClass]:
        """Wrapper for Presentation.make_subscriber(..)."""
        return await self._presentation.make_subscriber(dtype, subject_id, queue_capacity)

    async def make_subscriber_with_fixed_subject_id(self,
                                                    dtype:          typing.Type[FixedPortMessageClass],
                                                    queue_capacity: typing.Optional[int] = None) \
            -> pyuavcan.presentation.Subscriber[FixedPortMessageClass]:
        """Wrapper for Presentation.make_subscriber_with_fixed_subject_id(..)."""
        return await self._presentation.make_subscriber_with_fixed_subject_id(dtype, queue_capacity)

    # ---------------------------------------- SERVICE CLIENT FACTORY ----------------------------------------

    async def make_client(self,
                          dtype:          typing.Type[ServiceClass],
                          service_id:     int,
                          server_node_id: int) -> pyuavcan.presentation.Client[ServiceClass]:
        """Wrapper for Presentation.make_client(..)."""
        return await self._presentation.make_client(dtype, service_id, server_node_id)

    async def make_client_with_fixed_service_id(self, dtype: typing.Type[FixedPortServiceClass], server_node_id: int) \
            -> pyuavcan.presentation.Client[FixedPortServiceClass]:
        """Wrapper for Presentation.make_client_with_fixed_service_id(..)."""
        return await self._presentation.make_client_with_fixed_service_id(dtype, server_node_id)

    # ---------------------------------------- SERVICE SERVER FACTORY ----------------------------------------

    async def get_server(self, dtype: typing.Type[ServiceClass], service_id: int) \
            -> pyuavcan.presentation.Server[ServiceClass]:
        """Wrapper for Presentation.get_server(..)."""
        return await self._presentation.get_server(dtype, service_id)

    async def get_server_with_fixed_service_id(self, dtype: typing.Type[FixedPortServiceClass]) \
            -> pyuavcan.presentation.Server[FixedPortServiceClass]:
        """Wrapper for Presentation.get_server_with_fixed_service_id(..)."""
        return await self._presentation.get_server_with_fixed_service_id(dtype)

    # ---------------------------------------- AUXILIARY ----------------------------------------

    async def close(self) -> None:
        await self._presentation.close()

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self,
                                             info=self._info,
                                             heartbeat=self._heartbeat_publisher.make_message(),
                                             presentation=self._presentation)
