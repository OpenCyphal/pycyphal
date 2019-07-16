#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import collections
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._typed_session import OutgoingTransferIDCounter, TypedSessionFinalizer, Closable
from ._typed_session import Publisher, PublisherImpl
from ._typed_session import Subscriber, SubscriberImpl
from ._typed_session import Client, ClientImpl
from ._typed_session import Server


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

        self._outgoing_transfer_id_counter_registry: \
            typing.DefaultDict[pyuavcan.transport.SessionSpecifier, OutgoingTransferIDCounter] = \
            collections.defaultdict(OutgoingTransferIDCounter)

        self._typed_session_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, Closable] = {}

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        """
        Direct reference to the underlying transport implementation. This instance is used for exchanging serialized
        representations over the network. The presentation layer instance takes ownership of the transport.
        """
        return self._transport

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
            impl = self._typed_session_registry[session_specifier]
            assert isinstance(impl, PublisherImpl)
        except LookupError:
            transport_session = self._transport.get_output_session(session_specifier,
                                                                   self._make_payload_metadata(dtype))
            impl = PublisherImpl(dtype=dtype,
                                 transport_session=transport_session,
                                 transfer_id_counter=self._outgoing_transfer_id_counter_registry[session_specifier],
                                 finalizer=self._make_finalizer(session_specifier),
                                 loop=self._transport.loop)
            self._typed_session_registry[session_specifier] = impl

        assert isinstance(impl, PublisherImpl)
        return Publisher(impl, self._transport.loop)

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
            impl = self._typed_session_registry[session_specifier]
            assert isinstance(impl, SubscriberImpl)
        except LookupError:
            transport_session = self._transport.get_input_session(session_specifier, self._make_payload_metadata(dtype))
            impl = SubscriberImpl(dtype=dtype,
                                  transport_session=transport_session,
                                  finalizer=self._make_finalizer(session_specifier),
                                  loop=self._transport.loop)
            self._typed_session_registry[session_specifier] = impl

        assert isinstance(impl, SubscriberImpl)
        return Subscriber(impl=impl,
                          loop=self._transport.loop,
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
            impl = self._typed_session_registry[session_specifier]
            assert isinstance(impl, ClientImpl)
        except LookupError:
            output_transport_session = self._transport.get_output_session(session_specifier,
                                                                          self._make_payload_metadata(dtype.Request))
            input_transport_session = self._transport.get_input_session(session_specifier,
                                                                        self._make_payload_metadata(dtype.Response))
            impl = ClientImpl(dtype=dtype,
                              input_transport_session=input_transport_session,
                              output_transport_session=output_transport_session,
                              transfer_id_counter=self._outgoing_transfer_id_counter_registry[session_specifier],
                              transfer_id_modulo_factory=transfer_id_modulo_factory,
                              finalizer=self._make_finalizer(session_specifier),
                              loop=self._transport.loop)
            self._typed_session_registry[session_specifier] = impl

        assert isinstance(impl, ClientImpl)
        return Client(impl=impl, loop=self._transport.loop)

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
            impl = self._typed_session_registry[session_specifier]
            assert isinstance(impl, Server)
        except LookupError:
            input_transport_session = self._transport.get_input_session(session_specifier,
                                                                        self._make_payload_metadata(dtype.Request))
            impl = Server(dtype=dtype,
                          input_transport_session=input_transport_session,
                          output_transport_session_factory=output_transport_session_factory,
                          finalizer=self._make_finalizer(session_specifier=session_specifier),
                          loop=self._transport.loop)
            self._typed_session_registry[session_specifier] = impl

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
        for s in list(self._typed_session_registry.values()):
            try:
                s.close()
            except Exception as ex:
                _logger.exception('%r.close() could not close session %r: %s', self, s, ex)

        self._closed = True
        self._transport.close()

    @property
    def sessions(self) -> typing.Sequence[pyuavcan.transport.SessionSpecifier]:
        """
        A view of the active session instances that are currently open.
        """
        if not self._closed:
            return list(self._typed_session_registry.keys())
        else:
            return []

    def _make_finalizer(self, session_specifier: pyuavcan.transport.SessionSpecifier) -> TypedSessionFinalizer:
        done = False

        def finalizer(transport_sessions: typing.Iterable[pyuavcan.transport.Session]) -> None:
            # So this is rather messy. Observe that a typed session instance aggregates two distinct resources that
            # MUST be allocated and deallocated SYNCHRONOUSLY: the local registry entry in this class and the
            # corresponding transport session instance. I don't want to plaster our session objects with locks and
            # container references, so instead I decided to pass the associated resources into the finalizer, which
            # disposes of all resources atomically by acquiring an explicit private lock. This is clearly not very
            # obvious and in the future we should look for a cleaner design. The cleaner design can be retrofitted
            # easily while keeping the API unchanged so this should be easy to fix transparently by bumping only
            # the patch version of the library.
            nonlocal done
            assert not done, 'Internal protocol violation: double finalization'
            done = True
            try:
                self._typed_session_registry.pop(session_specifier)
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
        return pyuavcan.util.repr_attributes(self, transport=self.transport, sessions=self.sessions)
