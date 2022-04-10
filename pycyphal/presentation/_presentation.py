# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import logging
import asyncio
import pycyphal.util
import pycyphal.dsdl
import pycyphal.transport
from ._port import OutgoingTransferIDCounter, PortFinalizer, Closable, Port
from ._port import Publisher, PublisherImpl
from ._port import Subscriber, SubscriberImpl
from ._port import Client, ClientImpl
from ._port import Server


T = typing.TypeVar("T")

_logger = logging.getLogger(__name__)


class Presentation:
    r"""
    This is the presentation layer controller.
    It weaves the fabric of peace and maintains balance even when it looks like the darkest of skies spins above.

    Methods named ``make_*()`` create a new instance upon every invocation. Such instances implement the RAII pattern,
    managing the life cycle of the underlying resource automatically, so the user does not necessarily have to call
    ``close()`` manually, although it is recommended for determinism.

    Methods named ``get_*()`` create a new instance only the first time they are invoked for the
    particular key parameter; the same instance is returned for every subsequent call for the same
    key parameter until it is manually closed by the caller.
    """

    def __init__(self, transport: pycyphal.transport.Transport) -> None:
        """
        The presentation controller takes ownership of the supplied transport.
        When the presentation instance is closed, its transport is also closed (and so are all its sessions).
        """
        self._transport = transport
        self._closed = False
        self._output_transfer_id_map: typing.Dict[
            pycyphal.transport.OutputSessionSpecifier, OutgoingTransferIDCounter
        ] = {}
        # For services, the session is the input session.
        self._registry: typing.Dict[
            typing.Tuple[typing.Type[Port[object]], pycyphal.transport.SessionSpecifier],
            Closable,
        ] = {}

    @property
    def output_transfer_id_map(
        self,
    ) -> typing.Dict[pycyphal.transport.OutputSessionSpecifier, OutgoingTransferIDCounter]:
        """
        This property is designed for very short-lived processes like CLI tools. Most applications will not
        benefit from it and should not use it.

        Access to the output transfer-ID map allows short-running applications
        to store/restore the map to/from a persistent storage that retains data across restarts of the application.
        That may allow applications with very short life cycles (typically under several seconds) to adhere to the
        transfer-ID computation requirements presented in the specification. If the requirement were to be violated,
        then upon restart a process using the same node-ID could be unable to initiate communication using same
        port-ID until the receiving nodes reached the transfer-ID timeout state.

        The typical usage pattern is as follows: Upon launch, check if there is a transfer-ID map stored in a
        predefined location (e.g., a file or a database). If there is, and the storage was last written recently
        (no point restoring a map that is definitely obsolete), load it and commit to this instance by invoking
        :meth:`dict.update` on the object returned by this property. If there isn't, do nothing. When the application
        is finished running (e.g., this could be implemented via :func:`atexit.register`), access the map via this
        property and write it to the predefined storage location atomically. Make sure to shard the location by
        node-ID because nodes that use different node-ID values obviously shall not share their transfer-ID maps.
        Nodes sharing the same node-ID cannot exist on the same transport, but the local system might be running
        nodes under the same node-ID on independent networks concurrently, so this may need to be accounted for.
        """
        return self._output_transfer_id_map

    @property
    def transport(self) -> pycyphal.transport.Transport:
        """
        Direct reference to the underlying transport instance.
        The presentation layer instance owns its transport.
        """
        return self._transport

    @property
    def loop(self) -> asyncio.AbstractEventLoop:  # pragma: no cover
        """
        Deprecated.
        """
        # noinspection PyDeprecation
        return self._transport.loop

    # ----------------------------------------  SESSION FACTORY METHODS  ----------------------------------------

    def make_publisher(self, dtype: typing.Type[T], subject_id: int) -> Publisher[T]:
        """
        Creates a new publisher instance for the specified subject-ID. All publishers created for a specific
        subject share the same underlying implementation object which is hidden from the user;
        the implementation is reference counted and it is destroyed automatically along with its
        underlying transport level session instance when the last publisher is closed.
        The publisher instance will be closed automatically from the finalizer when garbage collected
        if the user did not bother to do that manually. This logic follows the RAII pattern.

        See :class:`Publisher` for further information about publishers.
        """
        if not pycyphal.dsdl.is_message_type(dtype):
            raise TypeError(f"Not a message type: {dtype}")

        self._raise_if_closed()
        _logger.debug("%s: Constructing new publisher for %r at subject-ID %d", self, dtype, subject_id)

        data_specifier = pycyphal.transport.MessageDataSpecifier(subject_id)
        session_specifier = pycyphal.transport.OutputSessionSpecifier(data_specifier, None)
        try:
            impl = self._registry[Publisher, session_specifier]
            assert isinstance(impl, PublisherImpl)
        except LookupError:
            transport_session = self._transport.get_output_session(
                session_specifier, self._make_payload_metadata(dtype)
            )
            transfer_id_counter = self._output_transfer_id_map.setdefault(
                session_specifier, OutgoingTransferIDCounter()
            )
            impl = PublisherImpl(
                dtype=dtype,
                transport_session=transport_session,
                transfer_id_counter=transfer_id_counter,
                finalizer=self._make_finalizer(Publisher, session_specifier),
            )
            self._registry[Publisher, session_specifier] = impl

        assert isinstance(impl, PublisherImpl)
        return Publisher(impl)

    def make_subscriber(
        self, dtype: typing.Type[T], subject_id: int, queue_capacity: typing.Optional[int] = None
    ) -> Subscriber[T]:
        """
        Creates a new subscriber instance for the specified subject-ID. All subscribers created for a specific
        subject share the same underlying implementation object which is hidden from the user; the implementation
        is reference counted and it is destroyed automatically along with its underlying transport level session
        instance when the last subscriber is closed. The subscriber instance will be closed automatically from
        the finalizer when garbage collected if the user did not bother to do that manually.
        This logic follows the RAII pattern.

        By default, the size of the input queue is unlimited; the user may provide a positive integer value to override
        this. If the user is not reading the received messages quickly enough and the size of the queue is limited
        (technically, it is always limited at least by the amount of the available memory),
        the queue may become full in which case newer messages will be dropped and the overrun counter
        will be incremented once per dropped message.

        See :class:`Subscriber` for further information about subscribers.
        """
        if not pycyphal.dsdl.is_message_type(dtype):
            raise TypeError(f"Not a message type: {dtype}")

        self._raise_if_closed()
        _logger.debug(
            "%s: Constructing new subscriber for %r at subject-ID %d with queue limit %s",
            self,
            dtype,
            subject_id,
            queue_capacity,
        )

        data_specifier = pycyphal.transport.MessageDataSpecifier(subject_id)
        session_specifier = pycyphal.transport.InputSessionSpecifier(data_specifier, None)
        try:
            impl = self._registry[Subscriber, session_specifier]
            assert isinstance(impl, SubscriberImpl)
        except LookupError:
            transport_session = self._transport.get_input_session(session_specifier, self._make_payload_metadata(dtype))
            impl = SubscriberImpl(
                dtype=dtype,
                transport_session=transport_session,
                finalizer=self._make_finalizer(Subscriber, session_specifier),
            )
            self._registry[Subscriber, session_specifier] = impl

        assert isinstance(impl, SubscriberImpl)
        return Subscriber(impl=impl, queue_capacity=queue_capacity)

    def make_client(self, dtype: typing.Type[T], service_id: int, server_node_id: int) -> Client[T]:
        """
        Creates a new client instance for the specified service-ID and the remote server node-ID.
        The number of such instances can be arbitrary.
        For example, different tasks may simultaneously create and use client instances
        invoking the same service on the same server node.

        All clients created with a specific combination of service-ID and server node-ID share the same
        underlying implementation object which is hidden from the user.
        The implementation instance is reference counted and it is destroyed automatically along with its
        underlying transport level session instances when its last client is closed.
        The client instance will be closed automatically from its finalizer when garbage
        collected if the user did not bother to do that manually.
        This logic follows the RAII pattern.

        See :class:`Client` for further information about clients.
        """
        if not pycyphal.dsdl.is_service_type(dtype):
            raise TypeError(f"Not a service type: {dtype}")
        # https://github.com/python/mypy/issues/7121
        request_dtype = dtype.Request  # type: ignore
        response_dtype = dtype.Response  # type: ignore

        self._raise_if_closed()
        _logger.debug(
            "%s: Constructing new client for %r at service-ID %d with remote server node-ID %s",
            self,
            dtype,
            service_id,
            server_node_id,
        )

        def transfer_id_modulo_factory() -> int:
            return self._transport.protocol_parameters.transfer_id_modulo

        input_session_specifier = pycyphal.transport.InputSessionSpecifier(
            pycyphal.transport.ServiceDataSpecifier(service_id, pycyphal.transport.ServiceDataSpecifier.Role.RESPONSE),
            server_node_id,
        )
        output_session_specifier = pycyphal.transport.OutputSessionSpecifier(
            pycyphal.transport.ServiceDataSpecifier(service_id, pycyphal.transport.ServiceDataSpecifier.Role.REQUEST),
            server_node_id,
        )
        try:
            impl = self._registry[Client, input_session_specifier]
            assert isinstance(impl, ClientImpl)
        except LookupError:
            output_transport_session = self._transport.get_output_session(
                output_session_specifier, self._make_payload_metadata(request_dtype)
            )
            input_transport_session = self._transport.get_input_session(
                input_session_specifier, self._make_payload_metadata(response_dtype)
            )
            transfer_id_counter = self._output_transfer_id_map.setdefault(
                output_session_specifier, OutgoingTransferIDCounter()
            )
            impl = ClientImpl(
                dtype=dtype,
                input_transport_session=input_transport_session,
                output_transport_session=output_transport_session,
                transfer_id_counter=transfer_id_counter,
                transfer_id_modulo_factory=transfer_id_modulo_factory,
                finalizer=self._make_finalizer(Client, input_session_specifier),
            )
            self._registry[Client, input_session_specifier] = impl

        assert isinstance(impl, ClientImpl)
        return Client(impl=impl)

    def get_server(self, dtype: typing.Type[T], service_id: int) -> Server[T]:
        """
        Returns the server instance for the specified service-ID. If such instance does not exist, it will be
        created. The instance should be used from one task only.

        Observe that unlike other sessions, the server instance is returned as-is without
        any intermediate proxy objects, and this interface does NOT implement the RAII pattern.
        The server instance will not be garbage collected as long as its presentation layer controller exists,
        hence it is the responsibility of the user to close unwanted servers manually.
        However, when the parent presentation layer controller is closed (see :meth:`close`),
        all of its session instances are also closed, servers are no exception, so the application does not
        really have to hunt down every server to terminate a Cyphal stack properly.

        See :class:`Server` for further information about servers.
        """
        if not pycyphal.dsdl.is_service_type(dtype):
            raise TypeError(f"Not a service type: {dtype}")
        # https://github.com/python/mypy/issues/7121
        request_dtype = dtype.Request  # type: ignore
        response_dtype = dtype.Response  # type: ignore

        self._raise_if_closed()
        _logger.debug("%s: Providing server for %r at service-ID %d", self, dtype, service_id)

        def output_transport_session_factory(client_node_id: int) -> pycyphal.transport.OutputSession:
            _logger.debug("%s: %r has requested a new output session to client node %s", self, impl, client_node_id)
            ds = pycyphal.transport.ServiceDataSpecifier(
                service_id, pycyphal.transport.ServiceDataSpecifier.Role.RESPONSE
            )
            return self._transport.get_output_session(
                pycyphal.transport.OutputSessionSpecifier(ds, client_node_id),
                self._make_payload_metadata(response_dtype),
            )

        input_session_specifier = pycyphal.transport.InputSessionSpecifier(
            pycyphal.transport.ServiceDataSpecifier(service_id, pycyphal.transport.ServiceDataSpecifier.Role.REQUEST),
            None,
        )
        try:
            impl = self._registry[Server, input_session_specifier]
            assert isinstance(impl, Server)
        except LookupError:
            input_transport_session = self._transport.get_input_session(
                input_session_specifier, self._make_payload_metadata(request_dtype)
            )
            impl = Server(
                dtype=dtype,
                input_transport_session=input_transport_session,
                output_transport_session_factory=output_transport_session_factory,
                finalizer=self._make_finalizer(Server, input_session_specifier),
            )
            self._registry[Server, input_session_specifier] = impl

        assert isinstance(impl, Server)
        return impl

    # ----------------------------------------  CONVENIENCE FACTORY METHODS  ----------------------------------------

    def make_publisher_with_fixed_subject_id(self, dtype: typing.Type[T]) -> Publisher[T]:
        """
        A wrapper for :meth:`make_publisher` that uses the fixed subject-ID associated with this type.
        Raises a TypeError if the type has no fixed subject-ID.
        """
        return self.make_publisher(dtype=dtype, subject_id=self._get_fixed_port_id(dtype))

    def make_subscriber_with_fixed_subject_id(
        self, dtype: typing.Type[T], queue_capacity: typing.Optional[int] = None
    ) -> Subscriber[T]:
        """
        A wrapper for :meth:`make_subscriber` that uses the fixed subject-ID associated with this type.
        Raises a TypeError if the type has no fixed subject-ID.
        """
        return self.make_subscriber(
            dtype=dtype, subject_id=self._get_fixed_port_id(dtype), queue_capacity=queue_capacity
        )

    def make_client_with_fixed_service_id(self, dtype: typing.Type[T], server_node_id: int) -> Client[T]:
        """
        A wrapper for :meth:`make_client` that uses the fixed service-ID associated with this type.
        Raises a TypeError if the type has no fixed service-ID.
        """
        return self.make_client(dtype=dtype, service_id=self._get_fixed_port_id(dtype), server_node_id=server_node_id)

    def get_server_with_fixed_service_id(self, dtype: typing.Type[T]) -> Server[T]:
        """
        A wrapper for :meth:`get_server` that uses the fixed service-ID associated with this type.
        Raises a TypeError if the type has no fixed service-ID.
        """
        return self.get_server(dtype=dtype, service_id=self._get_fixed_port_id(dtype))

    # ----------------------------------------  AUXILIARY ENTITIES  ----------------------------------------

    def close(self) -> None:
        """
        Closes the underlying transport instance and all existing session instances.
        I.e., the application is not required to close every session instance explicitly.
        """
        for s in list(self._registry.values()):
            try:
                s.close()
            except Exception as ex:
                _logger.exception("%r.close() could not close session %r: %s", self, s, ex)

        self._closed = True
        self._transport.close()

    def _make_finalizer(
        self,
        session_type: typing.Type[Port[object]],
        session_specifier: pycyphal.transport.SessionSpecifier,
    ) -> PortFinalizer:
        done = False

        def finalizer(transport_sessions: typing.Iterable[pycyphal.transport.Session]) -> None:
            # So this is rather messy. Observe that a port instance aggregates two distinct resources that
            # must be allocated and deallocated atomically: the local registry entry in this class and the
            # corresponding transport session instance. I don't want to plaster our session objects with locks and
            # container references, so instead I decided to pass the associated resources into the finalizer, which
            # disposes of all resources atomically. This is clearly not very obvious and in the future we should
            # look for a cleaner design. The cleaner design can be retrofitted easily while keeping the API
            # unchanged so this should be easy to fix transparently by bumping only the patch version of the library.
            nonlocal done
            assert not done, "Internal protocol violation: double finalization"
            _logger.debug(
                "%s: Finalizing %s (%s) with transport sessions %s",
                self,
                session_specifier,
                session_type,
                transport_sessions,
            )
            done = True
            try:
                self._registry.pop((session_type, session_specifier))
            except Exception as ex:
                _logger.exception("%s could not remove port for %s: %s", self, session_specifier, ex)

            for ts in transport_sessions:
                try:
                    ts.close()
                except Exception as ex:
                    _logger.exception("%s could not finalize (close) %s: %s", self, ts, ex)

        return finalizer

    @staticmethod
    def _make_payload_metadata(dtype: typing.Type[object]) -> pycyphal.transport.PayloadMetadata:
        extent_bytes = pycyphal.dsdl.get_extent_bytes(dtype)
        return pycyphal.transport.PayloadMetadata(extent_bytes=extent_bytes)

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(repr(self))

    @staticmethod
    def _get_fixed_port_id(dtype: typing.Type[object]) -> int:
        port_id = pycyphal.dsdl.get_fixed_port_id(dtype)
        if port_id is None:
            raise TypeError(f"{dtype} has no fixed port-ID")
        return port_id

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self,
            self.transport,
            num_publishers=sum(1 for t, _ in self._registry if issubclass(t, Publisher)),
            num_subscribers=sum(1 for t, _ in self._registry if issubclass(t, Subscriber)),
            num_clients=sum(1 for t, _ in self._registry if issubclass(t, Client)),
            num_servers=sum(1 for t, _ in self._registry if issubclass(t, Server)),
        )
