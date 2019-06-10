#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import MessageTypedSessionProxy, TypedSessionFinalizer, MessageTypeClass


class Subscriber(MessageTypedSessionProxy[MessageTypeClass]):
    def __init__(self,
                 dtype:             typing.Type[MessageTypeClass],
                 transport_session: pyuavcan.transport.InputSession,
                 finalizer:         TypedSessionFinalizer):
        self._dtype = dtype
        self._transport_session = transport_session
        self._finalizer = finalizer
        self._deserialization_failure_count = 0

    @property
    def dtype(self) -> typing.Type[MessageTypeClass]:
        return self._dtype

    @property
    def transport_session(self) -> pyuavcan.transport.InputSession:
        return self._transport_session

    async def receive(self) -> MessageTypeClass:
        """
        Blocks forever until a valid message is received.
        """
        return (await self.receive_with_transfer())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageTypeClass]:
        """
        Blocks until either a valid message is received, in which case it is returned; or until the deadline
        is reached, in which case None is returned. The method may also return None at any time before the deadline.
        """
        out = await self.try_receive_with_transfer(monotonic_deadline)
        return out[0] if out else None

    async def receive_with_transfer(self) -> typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]:
        """
        Blocks forever until a valid message is received. The received message will be returned along with the
        transfer which delivered it.
        """
        out: typing.Optional[typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]] = None
        while out is None:
            out = await self.try_receive_with_transfer(time.monotonic() + _INFINITE_RECEIVE_RETRY_INTERVAL)
        return out

    async def try_receive_with_transfer(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageTypeClass, pyuavcan.transport.TransferFrom]]:
        """
        Blocks until either a valid message is received, in which case it is returned along with the transfer
        which delivered it; or until the deadline is reached, in which case None is returned. The method may
        also return None at any time before the deadline.
        """
        transfer = await self._transport_session.try_receive(monotonic_deadline)
        if transfer is not None:
            message = pyuavcan.dsdl.try_deserialize(self._dtype, transfer.fragmented_payload)
            if message is not None:
                return message, transfer
            else:
                self._deserialization_failure_count += 1
        return None

    async def close(self) -> None:
        try:
            await self._transport_session.close()
        finally:
            await self._finalizer()

    @property
    def deserialization_failure_count(self) -> int:
        """
        The number of valid transfers whose payload could not be deserialized into a valid message object.
        """
        return self._deserialization_failure_count


_INFINITE_RECEIVE_RETRY_INTERVAL = 60
