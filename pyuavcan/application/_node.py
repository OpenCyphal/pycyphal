# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import logging
import uavcan.node
import pyuavcan
import pyuavcan.application
import pyuavcan.application.heartbeat_publisher
import pyuavcan.application.diagnostic


NodeInfo = uavcan.node.GetInfo_1_0.Response


_logger = logging.getLogger(__name__)


class Node:
    """
    This is the top-level abstraction representing a UAVCAN node on the bus.
    This class is just a minor addition on top of the lower-level abstractions of the library
    implementing commonly-used/mandatory functions of the protocol such as heartbeat reporting and responding
    to node info requests ``uavcan.node.GetInfo``.

    Start the instance when initialization is finished by invoking :meth:`start`.

    This class automatically instantiates the following application-level function implementations:

    - :class:`pyuavcan.application.heartbeat_publisher.HeartbeatPublisher` (see :attr:`heartbeat_publisher`).

    Additionally, if enabled via the corresponding constructor arguments, optional application-level function
    implementations are instantiated as described in the constructor documentation.
    """

    def __init__(
        self,
        presentation: pyuavcan.presentation.Presentation,
        info: NodeInfo,
        with_diagnostic_subscriber: bool = False,
    ):
        """
        :param presentation: The node takes ownership of the supplied presentation controller.
            Ownership here means that the controller will be closed (along with all sessions and other resources)
            when the node is closed.

        :param info: The info structure is sent as a response to requests of type ``uavcan.node.GetInfo``;
            the corresponding server instance is established and run by the node class automatically.

        :param with_diagnostic_subscriber: If True, an instance of
            :class:`pyuavcan.application.diagnostic.DiagnosticSubscriber` will be constructed to channel
            standard UAVCAN diagnostic messages into the local Python logging facility.
        """
        self._presentation = presentation
        self._info = info
        self._heartbeat_publisher = pyuavcan.application.heartbeat_publisher.HeartbeatPublisher(self._presentation)
        self._srv_info = self._presentation.get_server_with_fixed_service_id(uavcan.node.GetInfo_1_0)

        self._diagnostic_subscriber = (
            pyuavcan.application.diagnostic.DiagnosticSubscriber(self._presentation)
            if with_diagnostic_subscriber
            else None
        )

        self._started = False

    @property
    def presentation(self) -> pyuavcan.presentation.Presentation:
        """Provides access to the underlying instance of :class:`pyuavcan.presentation.Presentation`."""
        return self._presentation

    @property
    def info(self) -> NodeInfo:
        """Provides access to the local node info structure. See :class:`pyuavcan.application.NodeInfo`."""
        return self._info

    @property
    def heartbeat_publisher(self) -> pyuavcan.application.heartbeat_publisher.HeartbeatPublisher:
        """Provides access to the heartbeat publisher instance of this node."""
        return self._heartbeat_publisher

    def start(self) -> None:
        """
        Starts the GetInfo server in the background, the heartbeat publisher, etc.
        Those will be automatically terminated when the node is closed.
        Does nothing if already started.
        """
        if not self._started:
            self._srv_info.serve_in_background(self._handle_get_info_request)
            self._heartbeat_publisher.start()
            if self._diagnostic_subscriber is not None:
                self._diagnostic_subscriber.start()
            self._started = True

    def close(self) -> None:
        """
        Closes the underlying presentation instance, application-level functions, and all other entities.
        Does nothing if already closed.
        """
        try:
            self._heartbeat_publisher.close()
            self._srv_info.close()
            if self._diagnostic_subscriber is not None:
                self._diagnostic_subscriber.close()
        finally:
            self._presentation.close()

    async def _handle_get_info_request(
        self, _: uavcan.node.GetInfo_1_0.Request, metadata: pyuavcan.presentation.ServiceRequestMetadata
    ) -> NodeInfo:
        _logger.debug("%s got a node info request: %s", self, metadata)
        return self._info

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(
            self, info=self._info, heartbeat=self._heartbeat_publisher.make_message(), presentation=self._presentation
        )
