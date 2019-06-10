#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import collections
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._typed_session import TypedSessionProxy, Subscriber, OutgoingTransferIDCounter, TypedSessionFinalizer
from ._typed_session import Publisher, PublisherImpl


MessageClass = typing.TypeVar('MessageClass', bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar('ServiceClass', bound=pyuavcan.dsdl.ServiceObject)

FixedPortMessageClass = typing.TypeVar('FixedPortMessageClass', bound=pyuavcan.dsdl.FixedPortCompositeObject)
FixedPortServiceClass = typing.TypeVar('FixedPortServiceClass', bound=pyuavcan.dsdl.FixedPortServiceObject)


class Presentation:
    def __init__(self, transport: pyuavcan.transport.Transport) -> None:
        self._transport = transport

        self._outgoing_transfer_id_counter_registry: \
            typing.DefaultDict[pyuavcan.transport.SessionSpecifier, OutgoingTransferIDCounter] = \
            collections.defaultdict(OutgoingTransferIDCounter)

        self._typed_session_registry: typing.Dict[pyuavcan.transport.SessionSpecifier, typing.Any] = {}

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        """
        Direct reference to the underlying transport implementation. This instance is used for exchanging serialized
        representations over the network. The presentation layer instance takes ownership of the transport.
        """
        return self._transport

    async def get_publisher(self, dtype: typing.Type[MessageClass], subject_id: int) -> Publisher[MessageClass]:
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
        out = Publisher(impl, self._transport.loop)
        return out

    async def get_subscriber(self, dtype: typing.Type[MessageClass], subject_id: int) -> Subscriber[MessageClass]:
        data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            out = self._typed_session_registry[session_specifier]
            assert isinstance(out, Subscriber)
        except LookupError:
            transport_session = await self._transport.get_input_session(session_specifier,
                                                                        self._make_message_payload_metadata(dtype))
            out = Subscriber(dtype=dtype,
                             transport_session=transport_session,
                             finalizer=self._make_finalizer(session_specifier))
            self._typed_session_registry[session_specifier] = out
        assert isinstance(out, Subscriber)
        return out

    async def get_publisher_with_fixed_subject_id(self, dtype: typing.Type[FixedPortMessageClass]) \
            -> Publisher[FixedPortMessageClass]:
        return await self.get_publisher(dtype=dtype, subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    async def get_subscriber_with_fixed_subject_id(self, dtype: typing.Type[FixedPortMessageClass]) \
            -> Subscriber[FixedPortMessageClass]:
        return await self.get_subscriber(dtype=dtype, subject_id=pyuavcan.dsdl.get_fixed_port_id(dtype))

    async def close(self) -> None:
        """
        Closes the underlying transport instance. Invalidates all existing session instances.
        """
        await self._transport.close()

    def _make_finalizer(self, session_specifier: pyuavcan.transport.SessionSpecifier) -> TypedSessionFinalizer:
        async def finalizer() -> None:
            # We use a done flag instead of catching KeyError because otherwise a race condition would be possible.
            # Suppose that an entry A under a session specifier S is created and then closed while one of its proxy
            # objects is still alive. Suppose then that before the proxy object is garbage collected, a new entry B
            # under the same session specifier S is created. Then, the proxy object is garbage-collected and its
            # finalizer invokes the close() method. If we were to rely on KeyError, the close method of the old
            # entry would have closed the new entry.
            nonlocal done
            if not done:
                done = True
                self._typed_session_registry.pop(session_specifier)
            else:
                raise pyuavcan.transport.ResourceClosedError(
                    f'The presentation session {session_specifier} is already closed') from None

        done = False
        return finalizer

    @staticmethod
    def _make_message_payload_metadata(dtype: typing.Type[MessageClass]) -> pyuavcan.transport.PayloadMetadata:
        model = pyuavcan.dsdl.get_model(dtype)
        max_size_bytes = pyuavcan.dsdl.get_max_serialized_representation_size_bytes(dtype)
        return pyuavcan.transport.PayloadMetadata(compact_data_type_id=model.compact_data_type_id,
                                                  max_size_bytes=max_size_bytes)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_object(self, transport=self.transport)
