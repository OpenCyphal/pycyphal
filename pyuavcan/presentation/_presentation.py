#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import collections
import pyuavcan.dsdl
import pyuavcan.transport
from ._session import TypedSession, Publisher, Subscriber, OutgoingTransferIDCounter, TypedSessionFinalizer


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

        self._typed_session_registry: typing.Dict[pyuavcan.transport.SessionSpecifier,
                                                  TypedSession[pyuavcan.dsdl.CompositeObject]] = {}

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        return self._transport

    async def get_publisher(self, dtype: typing.Type[MessageClass], subject_id: int) -> Publisher[MessageClass]:
        data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            out = self._typed_session_registry[session_specifier]
        except LookupError:
            transport_session = await self._transport.get_output_session(session_specifier,
                                                                         self._make_message_payload_metadata(dtype))
            out = Publisher(dtype=dtype,
                            transport_session=transport_session,
                            transfer_id_counter=self._outgoing_transfer_id_counter_registry[session_specifier],
                            finalizer=self._make_finalizer(session_specifier))
            self._typed_session_registry[session_specifier] = out
        assert isinstance(out, Publisher)
        return out

    async def get_subscriber(self, dtype: typing.Type[MessageClass], subject_id: int) -> Subscriber[MessageClass]:
        data_specifier = pyuavcan.transport.MessageDataSpecifier(subject_id)
        session_specifier = pyuavcan.transport.SessionSpecifier(data_specifier, None)
        try:
            out = self._typed_session_registry[session_specifier]
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

    def _make_finalizer(self, session_specifier: pyuavcan.transport.SessionSpecifier) -> TypedSessionFinalizer:
        async def finalizer() -> None:
            try:
                self._typed_session_registry.pop(session_specifier)
            except LookupError:
                raise pyuavcan.transport.ResourceClosedError(
                    f'The presentation session {session_specifier} is already closed') from None
        return finalizer

    @staticmethod
    def _make_message_payload_metadata(dtype: typing.Type[MessageClass]) -> pyuavcan.transport.PayloadMetadata:
        model = pyuavcan.dsdl.get_model(dtype)
        max_size_bytes = pyuavcan.dsdl.get_max_serialized_representation_size_bytes(dtype)
        return pyuavcan.transport.PayloadMetadata(compact_data_type_id=model.compact_data_type_id,
                                                  max_size_bytes=max_size_bytes)
