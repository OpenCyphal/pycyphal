#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import asyncio
import collections
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._typed_session import OutgoingTransferIDCounter, TypedSessionFinalizer
from ._typed_session import Publisher, PublisherImpl
from ._typed_session import Subscriber, SubscriberImpl
from ._typed_session import Server


MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


_logger = logging.getLogger(__name__)


class Presentation:
    def __init__(self, transport: pyuavcan.transport.Transport) -> None:
        self._transport = transport

        self._outgoing_transfer_id_counter_registry: \
            typing.DefaultDict[pyuavcan.transport.SessionSpecifier, OutgoingTransferIDCounter] = \
            collections.defaultdict(OutgoingTransferIDCounter)

        self._typed_session_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, typing.Any] = {}

        self._lock = asyncio.Lock(loop=transport.loop)

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        """
        Direct reference to the underlying transport implementation. This instance is used for exchanging serialized
        representations over the network. The presentation layer instance takes ownership of the transport.
        """
        return self._transport

    async def make_publisher(self, dtype: typing.Type[MessageClass], subject_id: int) -> Publisher[MessageClass]:
        """
        Creates a new publisher instance for the specified type and subject ID. All publishers created with a given
        combination of type and subject share the same underlying implementation object which is hidden from the user;
        the implementation is reference counted and it is destroyed automatically along with its underlying transport
        level session instance when the last publisher is closed. The publisher instance will be close()d automatically
        from the finalizer if the user did not bother to do that properly; every such occurrence will be logged at the
        warning level.
        """
        async with self._lock:
            data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
            session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
            try:
                impl = self._typed_session_registry[session_specifier]
                assert isinstance(impl, PublisherImpl)
            except LookupError:
                transport_session = await self._transport.get_output_session(session_specifier,
                                                                             self._make_message_payload_metadata(dtype))
                impl = PublisherImpl(dtype=dtype,
                                     transport_session=transport_session,
                                     transfer_id_counter=self._outgoing_transfer_id_counter_registry[session_specifier],
                                     finalizer=self._make_finalizer(session_specifier),
                                     loop=self._transport.loop)
                self._typed_session_registry[session_specifier] = impl

        assert isinstance(impl, PublisherImpl)
        return Publisher(impl, self._transport.loop)

    async def make_subscriber(self,
                              dtype:          typing.Type[MessageClass],
                              subject_id:     int,
                              queue_capacity: typing.Optional[int] = None) -> Subscriber[MessageClass]:
        """
        Creates a new subscriber instance for the specified type and subject ID. All subscribers created with a given
        combination of type and subject share the same underlying implementation object which is hidden from the user;
        the implementation is reference counted and it is destroyed automatically along with its underlying transport
        level session instance when the last subscriber is closed. The subscriber instance will be close()d
        automatically from the finalizer if the user did not bother to do that properly; every such occurrence will be
        logged at the warning level.
        By default, the size of the input queue is unlimited; the user may provide a positive integer value to override
        this. If the user is not reading the messages quickly enough and the size of the queue is limited (technically
        it is always limited at least by the amount of the available memory), the queue may become full in which case
        newer messages will be dropped and the overrun counter will be incremented once per dropped message.
        """
        async with self._lock:
            data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
            session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
            try:
                impl = self._typed_session_registry[session_specifier]
                assert isinstance(impl, SubscriberImpl)
            except LookupError:
                transport_session = await self._transport.get_input_session(session_specifier,
                                                                            self._make_message_payload_metadata(dtype))
                impl = SubscriberImpl(dtype=dtype,
                                      transport_session=transport_session,
                                      finalizer=self._make_finalizer(session_specifier),
                                      loop=self._transport.loop)
                self._typed_session_registry[session_specifier] = impl

        assert isinstance(impl, SubscriberImpl)
        return Subscriber(impl=impl,
                          loop=self._transport.loop,
                          queue_capacity=queue_capacity)

    async def get_server(self, dtype: typing.Type[ServiceClass], service_id: int) -> Server[ServiceClass]:
        raise NotImplementedError

    async def make_publisher_with_fixed_subject_id(self, dtype: typing.Type[FixedPortMessageClass]) \
            -> Publisher[FixedPortMessageClass]:
        """
        An alias for make_publisher() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return await self.make_publisher(dtype=dtype, subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    async def make_subscriber_with_fixed_subject_id(self,
                                                    dtype: typing.Type[FixedPortMessageClass],
                                                    queue_capacity: typing.Optional[int] = None) \
            -> Subscriber[FixedPortMessageClass]:
        """
        An alias for make_subscriber() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return await self.make_subscriber(dtype=dtype,
                                          subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype),
                                          queue_capacity=queue_capacity)

    async def get_server_with_fixed_service_id(self, dtype: typing.Type[FixedPortServiceClass]) \
            -> Server[FixedPortServiceClass]:
        """
        An alias for get_server() which uses the fixed port ID associated with this type.
        Raises a TypeError if the type has no fixed port ID.
        """
        return await self.get_server(dtype=dtype, service_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    async def close(self) -> None:
        """
        Closes the underlying transport instance. Invalidates all existing session instances.
        """
        async with self._lock:
            await self._transport.close()

    @property
    def sessions(self) -> typing.Sequence[pyuavcan.transport.SessionSpecifier]:
        """
        A view of the active session instances that are currently open. Note that this view also includes instances
        that are scheduled for removal, meaning that one request followed by another may end up returning fewer items
        the second time even if the user did not request any changes explicitly.
        """
        return list(self._typed_session_registry.keys())

    def _make_finalizer(self, session_specifier: pyuavcan.transport.SessionSpecifier) -> TypedSessionFinalizer:
        async def finalizer(transport_sessions: typing.Iterable[pyuavcan.transport.Session]) -> None:
            # So this is rather messy. Observe that a typed session instance aggregates two distinct resources that
            # MUST be allocated and deallocated SYNCHRONOUSLY: the local registry entry in this class and the
            # corresponding transport session instance. I don't want to plaster our session objects with locks and
            # container references, so instead I decided to pass the associated resources into the finalizer, which
            # disposes of all resources atomically by acquiring an explicit private lock. This is clearly not very
            # obvious and in the future we should look for a cleaner design. The cleaner design can be retrofitted
            # easily while keeping the API unchanged so this should be easy to fix transparently by bumping only
            # the patch version of the library.
            nonlocal done
            async with self._lock:
                assert not done, 'Internal protocol violation: double finalization'
                done = True
                try:
                    self._typed_session_registry.pop(session_specifier)
                except Exception as ex:
                    _logger.exception('Could not remove the session for the specifier %s: %s', session_specifier, ex)

                results = await asyncio.gather(*[ts.close() for ts in transport_sessions],
                                               loop=self._transport.loop,
                                               return_exceptions=True)
                errors = list(filter(
                    lambda x: isinstance(x, Exception) and not isinstance(x, pyuavcan.transport.ResourceClosedError),
                    results))
                del results
                if len(errors) > 0:
                    _logger.error('Could not close transport sessions: %r', errors)

        done = False
        return finalizer

    @staticmethod
    def _make_message_payload_metadata(dtype: typing.Type[MessageClass]) -> pyuavcan.transport.PayloadMetadata:
        model = pyuavcan.dsdl.get_model(dtype)
        max_size_bytes = pyuavcan.dsdl.get_max_serialized_representation_size_bytes(dtype)
        return pyuavcan.transport.PayloadMetadata(compact_data_type_id=model.compact_data_type_id,
                                                  max_size_bytes=max_size_bytes)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, transport=self.transport, sessions=self.sessions)
