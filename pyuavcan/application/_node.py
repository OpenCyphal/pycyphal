# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Callable, Type, TypeVar, Optional, List, Any
import abc
import asyncio
import logging
import uavcan.node
import pyuavcan
from pyuavcan.presentation import Presentation, ServiceRequestMetadata, Publisher, Subscriber, Server, Client
from . import heartbeat_publisher
from . import register


NodeInfo = uavcan.node.GetInfo_1_0.Response

MessageClass = TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


class PortNotConfiguredError(register.MissingRegisterError):
    """
    Raised from :meth:`Node.make_publisher`, :meth:`Node.make_subscriber`, :meth:`Node.make_client`,
    :meth:`Node.get_server` if the application requested a port for which there is no configuration register
    and whose data type does not have a fixed port-ID.

    Applications may catch this exception to implement optional ports,
    where the port is not enabled until explicitly configured while other components of the application are functional.
    """


class Node(abc.ABC):
    """
    This is the top-level abstraction representing a UAVCAN node on the bus.
    This is an abstract class; instantiate it using the factory :func:`pyuavcan.application.make_node`
    or (in special cases) create custom implementations.

    This class automatically instantiates the following application-layer function implementations:

    - :class:`heartbeat_publisher.HeartbeatPublisher`
    - Register API server (``uavcan.register.*``)
    - Node info server (``uavcan.node.GetInfo``)
    - Port introspection publisher (``uavcan.port.List``)

    ..  attention::

        If the underlying transport is anonymous, some of these functions may not be available.

    Start the instance when initialization is finished by invoking :meth:`start`.
    This will also automatically start all function implementation instances.
    """

    def __init__(self) -> None:
        self._started = False
        self._on_start: List[Callable[[], None]] = []
        self._on_close: List[Callable[[], None]] = []

        # Instantiate application-layer functions. Please keep the class docstring updated when changing this.
        self._heartbeat_publisher = heartbeat_publisher.HeartbeatPublisher(self)

        from ._port_list_publisher import PortListPublisher
        from ._register_server import RegisterServer

        PortListPublisher(self)

        async def handle_get_info(_req: uavcan.node.GetInfo_1_0.Request, _meta: ServiceRequestMetadata) -> NodeInfo:
            return self.info

        try:
            RegisterServer(self)
            srv_info = self.get_server(uavcan.node.GetInfo_1_0)
        except pyuavcan.transport.OperationNotDefinedForAnonymousNodeError as ex:
            _logger.info("%r: RPC-servers not launched because the transport is anonymous: %s", self, ex)
        else:
            self.add_lifetime_hooks(lambda: srv_info.serve_in_background(handle_get_info), srv_info.close)

    @property
    @abc.abstractmethod
    def presentation(self) -> Presentation:
        """Provides access to the underlying instance of :class:`pyuavcan.presentation.Presentation`."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def info(self) -> NodeInfo:
        """Provides access to the local node info structure. See :class:`pyuavcan.application.NodeInfo`."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def registry(self) -> register.Registry:
        """
        Provides access to the local registry instance (see :class:`pyuavcan.application.register.Registry`).
        The registry manages UAVCAN registers as defined by the standard network service ``uavcan.register``.

        The registers store the configuration parameters of the current application, both standard
        (like subject-IDs, service-IDs, transport configuration, the local node-ID, etc.)
        and application-specific ones.

        See also :meth:`make_publisher`, :meth:`make_subscriber`, :meth:`make_client`, :meth:`get_server`.
        """
        raise NotImplementedError

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Shortcut for ``self.presentation.loop``"""
        return self.presentation.loop

    @property
    def id(self) -> Optional[int]:
        """Shortcut for ``self.presentation.transport.local_node_id``"""
        return self.presentation.transport.local_node_id

    @property
    def heartbeat_publisher(self) -> heartbeat_publisher.HeartbeatPublisher:
        """Provides access to the heartbeat publisher instance of this node."""
        return self._heartbeat_publisher

    def make_publisher(self, dtype: Type[MessageClass], port_name: str = "") -> Publisher[MessageClass]:
        """
        Wrapper over :meth:`pyuavcan.presentation.Presentation.make_publisher`
        that takes the subject-ID from the standard register ``uavcan.pub.PORT_NAME.id``.
        If the register is missing or no name is given, the fixed subject-ID is used unless it is also missing.
        The type information is automatically exposed via ``uavcan.pub.PORT_NAME.type`` based on dtype.
        For details on the standard registers see Specification.

        :raises:
            :class:`PortNotConfiguredError` if the register is missing and no fixed port-ID is defined.
            :class:`TypeError` if no name is given and no fixed port-ID is defined.
        """
        if port_name:
            return self.presentation.make_publisher(dtype, self._resolve_named_port(dtype, "pub", port_name))
        return self.presentation.make_publisher_with_fixed_subject_id(dtype)  # type: ignore

    def make_subscriber(self, dtype: Type[MessageClass], port_name: str = "") -> Subscriber[MessageClass]:
        """
        Wrapper over :meth:`pyuavcan.presentation.Presentation.make_subscriber`
        that takes the subject-ID from the standard register ``uavcan.sub.PORT_NAME.id``.
        If the register is missing or no name is given, the fixed subject-ID is used unless it is also missing.
        The type information is automatically exposed via ``uavcan.sub.PORT_NAME.type`` based on dtype.
        For details on the standard registers see Specification.

        :raises:
            :class:`PortNotConfiguredError` if the register is missing and no fixed port-ID is defined.
            :class:`TypeError` if no name is given and no fixed port-ID is defined.
        """
        if port_name:
            return self.presentation.make_subscriber(dtype, self._resolve_named_port(dtype, "sub", port_name))
        return self.presentation.make_subscriber_with_fixed_subject_id(dtype)  # type: ignore

    def make_client(self, dtype: Type[ServiceClass], server_node_id: int, port_name: str = "") -> Client[ServiceClass]:
        """
        Wrapper over :meth:`pyuavcan.presentation.Presentation.make_client`
        that takes the service-ID from the standard register ``uavcan.cln.PORT_NAME.id``.
        If the register is missing or no name is given, the fixed service-ID is used unless it is also missing.
        The type information is automatically exposed via ``uavcan.cln.PORT_NAME.type`` based on dtype.
        For details on the standard registers see Specification.

        :raises:
            :class:`PortNotConfiguredError` if the register is missing and no fixed port-ID is defined.
            :class:`TypeError` if no name is given and no fixed port-ID is defined.
        """
        if port_name:
            return self.presentation.make_client(
                dtype,
                service_id=self._resolve_named_port(dtype, "cln", port_name),
                server_node_id=server_node_id,
            )
        return self.presentation.make_client_with_fixed_service_id(dtype, server_node_id=server_node_id)  # type: ignore

    def get_server(self, dtype: Type[ServiceClass], port_name: str = "") -> Server[ServiceClass]:
        """
        Wrapper over :meth:`pyuavcan.presentation.Presentation.get_server`
        that takes the service-ID from the standard register ``uavcan.srv.PORT_NAME.id``.
        If the register is missing or no name is given, the fixed service-ID is used unless it is also missing.
        The type information is automatically exposed via ``uavcan.srv.PORT_NAME.type`` based on dtype.
        For details on the standard registers see Specification.

        :raises:
            :class:`PortNotConfiguredError` if the register is missing and no fixed port-ID is defined.
            :class:`TypeError` if no name is given and no fixed port-ID is defined.
        """
        if port_name:
            return self.presentation.get_server(dtype, self._resolve_named_port(dtype, "srv", port_name))
        return self.presentation.get_server_with_fixed_service_id(dtype)  # type: ignore

    def _resolve_named_port(self, dtype: Type[pyuavcan.dsdl.CompositeObject], kind: str, name: str) -> int:
        assert name, "Internal error"
        model = pyuavcan.dsdl.get_model(dtype)

        id_register_name = f"uavcan.{kind}.{name}.id"
        port_id = int(
            self.registry.setdefault(
                id_register_name,
                register.Value(natural16=register.Natural16([0xFFFF])),  # 0xFFFF means unset
            )
        )
        # Expose the type information to other network participants as prescribed by the Specification.
        self.registry[f"uavcan.{kind}.{name}.type"] = lambda: register.Value(string=register.String(str(model)))

        # Check if the value stored in the register is actually valid.
        mask = {
            "pub": pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK,
            "sub": pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK,
            "cln": pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK,
            "srv": pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK,
        }[kind]
        if 0 <= port_id <= mask:
            return port_id

        # Default to the fixed port-ID if the register value is invalid.
        _logger.debug("%r: %r = %r not in [0, %d], assume undefined", self, id_register_name, port_id, mask)
        if model.fixed_port_id is not None:
            assert isinstance(model.fixed_port_id, int)
            return model.fixed_port_id

        raise PortNotConfiguredError(
            id_register_name,
            f"Cannot initialize {kind}-port {name!r} because the register "
            f"does not define a valid port-ID and no fixed port-ID is defined for {model}. "
            f"Check if the environment variables are passed correctly or if the application is using the "
            f"correct register file.",
        )

    def start(self) -> None:
        """
        Starts all application-layer function implementations that are initialized on this node
        (like the heartbeat publisher, diagnostics, and basically anything that takes a node reference
        in its constructor).
        These will be automatically terminated when the node is closed.
        This method is idempotent.
        """
        if not self._started:
            for fun in self._on_start:  # First failure aborts the start.
                fun()
            self._started = True

    def close(self) -> None:
        """
        Closes the :attr:`presentation` (which includes the transport), the registry, the application-layer functions.
        The user does not have to close every port manually as it will be done automatically.
        This method is idempotent.
        Calling :meth:`start` on a closed node may lead to unpredictable results.
        """
        pyuavcan.util.broadcast(self._on_close)()
        self.presentation.close()
        self.registry.close()

    def add_lifetime_hooks(self, start: Optional[Callable[[], None]], close: Optional[Callable[[], None]]) -> None:
        """
        The start hook will be invoked when this node is :meth:`start`-ed.
        If the node is already started when this method is invoked, the start hook is called immediately.

        The close hook is invoked when this node is :meth:`close`-d.
        If the node is already closed, the close hook will never be invoked.
        """
        if start is not None:
            if self._started:
                start()
            else:
                self._on_start.append(start)
        if close is not None:
            self._on_close.append(close)

    def __enter__(self) -> Node:
        """
        Invokes :meth:`start` upon entering the context. Does nothing if already started.
        """
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        """
        Invokes :meth:`close` upon leaving the context. Does nothing if already closed.
        """
        self.close()

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self.info, self.presentation, self.registry)


_logger = logging.getLogger(__name__)
