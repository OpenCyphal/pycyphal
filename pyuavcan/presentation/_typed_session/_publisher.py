#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import MessageTypedSession, TypedSessionFinalizer, OutgoingTransferIDCounter, MessageTypeClass


class Publisher(MessageTypedSession[MessageTypeClass]):
    def __init__(self,
                 dtype:               typing.Type[MessageTypeClass],
                 transport_session:   pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 finalizer:           TypedSessionFinalizer):
        self._transport_session = transport_session
        self._transfer_id_counter = transfer_id_counter
        self._finalizer = finalizer
        super(Publisher, self).__init__(dtype=dtype)

    @property
    def transport_session(self) -> pyuavcan.transport.OutputSession:
        return self._transport_session

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        """
        Allows the caller to reach the transfer ID counter object. This may be useful in certain special cases
        such as publication of time synchronization messages.
        """
        return self._transfer_id_counter

    async def publish(self, message:  MessageTypeClass, priority: pyuavcan.transport.Priority) -> None:
        """
        Serializes and publishes the message object at the specified priority level.
        """
        if not isinstance(message, self._dtype):
            raise ValueError(f'Expected a message object of type {self.dtype}, found this: {message}')

        timestamp = pyuavcan.transport.Timestamp.now()
        fragmented_payload = list(pyuavcan.dsdl.serialize(message))
        transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                               priority=priority,
                                               transfer_id=self._transfer_id_counter.get_then_increment(),
                                               fragmented_payload=fragmented_payload)
        await self._transport_session.send(transfer)

    async def close(self) -> None:
        try:
            await self._transport_session.close()
        finally:
            await self._finalizer()
