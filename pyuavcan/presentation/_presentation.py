#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import asyncio
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._session import OutgoingTransferIDCounter, TypedSessionFinalizer, Closable, PresentationSession
from ._session import Publisher, PublisherImpl
from ._session import Subscriber, SubscriberImpl
from ._session import Client, ClientImpl
from ._session import Server


MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


_logger = logging.getLogger(__name__)


class Presentation:
    """
    This is the presentation layer controller. The presentation layer is a thin wrapper around the transport layer
    that manages DSDL object serialization and provides a convenient API for the application. The presentation layer
    also performs data sharing across multiple consumers in the application; for example, when the application
    creates more than one subscriber for a given subject, the presentation layer will take care to distribute the
    received messages into every subscription instance requested by the application.

    Methods named "make_*()" create a new instance upon every invocation. Methods named "get_*()" create a new instance
    only the first time they are invoked for the particular key parameter; the same instance is returned for every
    subsequent call for the same key parameter until it is manually closed by the caller.
    """

    def __init__(self, transport: pyuavcan.transport.Transport) -> None:
        self._transport = transport
        self._closed = False
        self._emitted_transfer_id_map: typing.Dict[pyuavcan.transport.SessionSpecifier, OutgoingTransferIDCounter] = {}
        self._registry: typing.Dict[typing.Tuple[typing.Type[PresentationSession[pyuavcan.dsdl.CompositeObject]],
                                                 pyuavcan.transport.SessionSpecifier],
                                    Closable] = {}

    @property
    def emitted_transfer_id_map(self) -> typing.Dict[pyuavcan.transport.SessionSpecifier, OutgoingTransferIDCounter]:
        """
        This property is designed for very short-lived processes like CLI tools. Most applications will not
        benefit from it and should not use it. The term "emitted transfer-ID map" is borrowed from Specification.

        Access to the emitted transfer ID map allows short-running applications, such as CLI tools,
        to store/restore the map to/from a persistent storage that retains data across restarts of the application.
        That may allow applications with very short life cycles (around several seconds) to adhere to the
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
        nodes under the same node-ID on different transports concurrently, so this needs to be accounted for.
        """
        return self._emitted_transfer_id_map

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        """
        Direct reference to the underlying transport implementation. This instance is used for exchanging serialized
        representations over the network. The presentation layer instance takes ownership of the transport.
        """
        return self._transport

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        A wrapper for :attr:`pyuavcan.transport.Transport.loop`.
        """
        return self._transport.loop

    # ----------------------------------------  SESSION FACTORY METHODS  ----------------------------------------

    def make_publisher(self, dtype: typing.Type[MessageClass], subject_id: int) -> Publisher[MessageClass]:
        """
        Creates a new publisher instance for the specified subject ID. All publishers created for a specific subject
        share the same underlying implementation object which is hidden from the user; the implementation is
        reference counted and it is destroyed automatically along with its underlying transport level session
        instance when the last publisher is closed. The publisher instance will be close()d automatically from
        the finalizer when garbage collected if the user did not bother to do that manually; every such occurrence
        will be logged.
        """
        if issubclass(dtype, pyuavcan.dsdl.ServiceObject):
            raise TypeError(f'Not a message type: {dtype}')

        self._raise_if_closed()

        data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            impl = self._registry[Publisher, session_specifier]
            assert isinstance(impl, PublisherImpl)
        except LookupError:
            transport_session = self._transport.get_output_session(session_specifier,
                                                                   self._make_payload_metadata(dtype))
            transfer_id_counter = self._emitted_transfer_id_map.setdefault(session_specifier,
                                                                           OutgoingTransferIDCounter())
            impl = PublisherImpl(dtype=dtype,
                                 transport_session=transport_session,
                                 transfer_id_counter=transfer_id_counter,
                                 finalizer=self._make_finalizer(Publisher, session_specifier),
                                 loop=self.loop)
            self._registry[Publisher, session_specifier] = impl

        assert isinstance(impl, PublisherImpl)
        return Publisher(impl, self.loop)

    def make_subscriber(self,
                        dtype:          typing.Type[MessageClass],
                        subject_id:     int,
                        queue_capacity: typing.Optional[int] = None) -> Subscriber[MessageClass]:
        """
        Creates a new subscriber instance for the specified subject ID. All subscribers created with a specific
        subject share the same underlying implementation object which is hidden from the user; the implementation
        is reference counted and it is destroyed automatically along with its underlying transport level session
        instance when the last subscriber is closed. The subscriber instance will be close()d automatically from
        the finalizer when garbage collected if the user did not bother to do that manually; every such occurrence
        will be logged.

        By default, the size of the input queue is unlimited; the user may provide a positive integer value to override
        this. If the user is not reading the messages quickly enough and the size of the queue is limited (technically
        it is always limited at least by the amount of the available memory), the queue may become full in which case
        newer messages will be dropped and the overrun counter will be incremented once per dropped message.

        Beware of data sharing issues: if the application uses more than one subscriber for a subject, every received
        message will be passed into each subscriber for the subject. If the object is accidentally mutated by the
        application, it will affect other subscribers. To avoid this, either do not mutate the received message
        objects or clone them beforehand.
        """
        if issubclass(dtype, pyuavcan.dsdl.ServiceObject):
            raise TypeError(f'Not a message type: {dtype}')

        self._raise_if_closed()

        data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            impl = self._registry[Subscriber, session_specifier]
            assert isinstance(impl, SubscriberImpl)
        except LookupError:
            transport_session = self._transport.get_input_session(session_specifier, self._make_payload_metadata(dtype))
            impl = SubscriberImpl(dtype=dtype,
                                  transport_session=transport_session,
                                  finalizer=self._make_finalizer(Subscriber, session_specifier),
                                  loop=self.loop)
            self._registry[Subscriber, session_specifier] = impl

        assert isinstance(impl, SubscriberImpl)
        return Subscriber(impl=impl,
                          loop=self.loop,
                          queue_capacity=queue_capacity)

    def make_client(self,
                    dtype:          typing.Type[ServiceClass],
                    service_id:     int,
                    server_node_id: int) -> Client[ServiceClass]:
        """
        Creates a new client instance for the specified server ID and the remote server node ID.
        All clients created with a specific combination of service ID and server node ID share the same
        underlying implementation object which is hidden from the user; the implementation is reference counted
        and it is destroyed automatically along with its underlying transport level session instances when the
        last client is closed. The client instance will be close()d automatically from the finalizer when garbage
        collected if the  user did not bother to do that manually; every such occurrence will be logged.
        """
        if not issubclass(dtype, pyuavcan.dsdl.ServiceObject):
            raise TypeError(f'Not a service type: {dtype}')

        self._raise_if_closed()

        def transfer_id_modulo_factory() -> int:
            # This might be a tad slow because the protocol parameters may take some time to compute?
            return self._transport.protocol_parameters.transfer_id_modulo

        data_specifier = pyuavcan.transport.ServiceDataSpecifier(
            service_id=service_id,
            role=pyuavcan.transport.ServiceDataSpecifier.Role.CLIENT
        )
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, server_node_id)
        try:
            impl = self._registry[Client, session_specifier]
            assert isinstance(impl, ClientImpl)
        except LookupError:
            output_transport_session = self._transport.get_output_session(session_specifier,
                                                                          self._make_payload_metadata(dtype.Request))
            input_transport_session = self._transport.get_input_session(session_specifier,
                                                                        self._make_payload_metadata(dtype.Response))
            transfer_id_counter = self._emitted_transfer_id_map.setdefault(session_specifier,
                                                                           OutgoingTransferIDCounter())
            impl = ClientImpl(dtype=dtype,
                              input_transport_session=input_transport_session,
                              output_transport_session=output_transport_session,
                              transfer_id_counter=transfer_id_counter,
                              transfer_id_modulo_factory=transfer_id_modulo_factory,
                              finalizer=self._make_finalizer(Client, session_specifier),
                              loop=self.loop)
            self._registry[Client, session_specifier] = impl

        assert isinstance(impl, ClientImpl)
        return Client(impl=impl, loop=self.loop)

    def get_server(self, dtype: typing.Type[ServiceClass], service_id: int) -> Server[ServiceClass]:
        """
        Returns the server instance for the specified service ID. If such instance does not exist, it will be
        created. The instance should be used from one task only. Observe that unlike other typed session instances,
        the server instance is returned as-is without any intermediate proxy objects. The server instance will not
        be garbage collected as long as its presentation layer controller exists, hence it is the responsibility
        of the user to close unwanted servers manually.
        """
        if not issubclass(dtype, pyuavcan.dsdl.ServiceObject):
            raise TypeError(f'Not a service type: {dtype}')

        self._raise_if_closed()

        def output_transport_session_factory(client_node_id: int) -> pyuavcan.transport.OutputSession:
            _logger.info('%r has requested a new output session to client node %s', impl, client_node_id)
            return self._transport.get_output_session(specifier=pyuavcan.transport.SessionSpecifier(data_specifier,
                                                                                                    client_node_id),
                                                      payload_metadata=self._make_payload_metadata(dtype.Response))

        data_specifier = pyuavcan.transport.ServiceDataSpecifier(
            service_id=service_id,
            role=pyuavcan.transport.ServiceDataSpecifier.Role.SERVER
        )
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            impl = self._registry[Server, session_specifier]
            assert isinstance(impl, Server)
        except LookupError:
            input_transport_session = self._transport.get_input_session(session_specifier,
                                                                        self._make_payload_metadata(dtype.Request))
            impl = Server(dtype=dtype,
                          input_transport_session=input_transport_session,
                          output_transport_session_factory=output_transport_session_factory,
                          finalizer=self._make_finalizer(Server, session_specifier),
                          loop=self.loop)
            self._registry[Server, session_specifier] = impl

        assert isinstance(impl, Server)
        return impl

    # ----------------------------------------  CONVENIENCE FACTORY METHODS  ----------------------------------------

    def make_publisher_with_fixed_subject_id(self, dtype: typing.Type[FixedPortMessageClass]) \
            -> Publisher[FixedPortMessageClass]:
        """
        A wrapper for make_publisher() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return self.make_publisher(dtype=dtype, subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    def make_subscriber_with_fixed_subject_id(self,
                                              dtype:          typing.Type[FixedPortMessageClass],
                                              queue_capacity: typing.Optional[int] = None) \
            -> Subscriber[FixedPortMessageClass]:
        """
        A wrapper for make_subscriber() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return self.make_subscriber(dtype=dtype,
                                    subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype),
                                    queue_capacity=queue_capacity)

    def make_client_with_fixed_service_id(self, dtype: typing.Type[FixedPortServiceClass], server_node_id: int) \
            -> Client[FixedPortServiceClass]:
        """
        A wrapper for make_client() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return self.make_client(dtype=dtype,
                                service_id=pyuavcan.dsdl.get_fixed_port_id(dtype),
                                server_node_id=server_node_id)

    def get_server_with_fixed_service_id(self, dtype: typing.Type[FixedPortServiceClass]) \
            -> Server[FixedPortServiceClass]:
        """
        A wrapper for get_server() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return self.get_server(dtype=dtype, service_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    # ----------------------------------------  AUXILIARY ENTITIES  ----------------------------------------

    def close(self) -> None:
        """
        Closes the underlying transport instance and all existing session instances.
        """
        for s in list(self._registry.values()):
            try:
                s.close()
            except Exception as ex:
                _logger.exception('%r.close() could not close session %r: %s', self, s, ex)

        self._closed = True
        self._transport.close()

    @property
    def sessions(self) -> typing.Sequence[typing.Tuple[typing.Type[PresentationSession[pyuavcan.dsdl.CompositeObject]],
                                                       pyuavcan.transport.SessionSpecifier]]:
        """
        A view of session specifiers whose sessions are currently open.

        Neither :class:`pyuavcan.transport.DataSpecifier` nor :class:`pyuavcan.transport.SessionSpecifier`
        provide sufficient information to uniquely identify a presentation-level session, because the transport
        layer makes no distinction between publication and subscription on the wire. It makes sense because
        one node's publication is another node's subscription.

        Locally, however, we should be able to distinguish publishers from subscribers because in the context
        of local node the difference matters. So we amend each session specifier with the presentation session
        type to enable such distinction.
        """
        return list(self._registry.keys()) if not self._closed else []

    def _make_finalizer(self,
                        session_type:      typing.Type[PresentationSession[pyuavcan.dsdl.CompositeObject]],
                        session_specifier: pyuavcan.transport.SessionSpecifier) -> TypedSessionFinalizer:
        done = False

        def finalizer(transport_sessions: typing.Iterable[pyuavcan.transport.Session]) -> None:
            # So this is rather messy. Observe that a typed session instance aggregates two distinct resources that
            # MUST be allocated and deallocated SYNCHRONOUSLY: the local registry entry in this class and the
            # corresponding transport session instance. I don't want to plaster our session objects with locks and
            # container references, so instead I decided to pass the associated resources into the finalizer, which
            # disposes of all resources atomically. This is clearly not very obvious and in the future we should
            # look for a cleaner design. The cleaner design can be retrofitted easily while keeping the API
            # unchanged so this should be easy to fix transparently by bumping only the patch version of the library.
            nonlocal done
            assert not done, 'Internal protocol violation: double finalization'
            done = True
            try:
                self._registry.pop((session_type, session_specifier))
            except Exception as ex:
                _logger.exception('Could not remove the session for the specifier %s: %s', session_specifier, ex)

            for ts in transport_sessions:
                try:
                    ts.close()
                except Exception as ex:
                    _logger.exception('%s could not close the transport session %s: %s', self, ts, ex)

        return finalizer

    @staticmethod
    def _make_payload_metadata(dtype: typing.Type[pyuavcan.dsdl.CompositeObject]) -> pyuavcan.transport.PayloadMetadata:
        model = pyuavcan.dsdl.get_model(dtype)
        max_size_bytes = pyuavcan.dsdl.get_max_serialized_representation_size_bytes(dtype)
        return pyuavcan.transport.PayloadMetadata(data_type_hash=model.data_type_hash, max_size_bytes=max_size_bytes)

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self.transport, sessions=self.sessions)
