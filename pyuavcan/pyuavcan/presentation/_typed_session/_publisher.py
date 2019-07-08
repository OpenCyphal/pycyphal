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
from ._base import MessageTypedSession, OutgoingTransferIDCounter, MessageClass
from ._base import DEFAULT_PRIORITY, TypedSessionFinalizer
from ._error import TypedSessionClosedError


_logger = logging.getLogger(__name__)


class Publisher(MessageTypedSession[MessageClass]):
    """
    Each task should request its own independent publisher instance from the presentation layer controller. Do not
    share the same publisher instance across different tasks.
    """
    def __init__(self,
                 impl: PublisherImpl[MessageClass],
                 loop: asyncio.AbstractEventLoop):
        self._maybe_impl: typing.Optional[PublisherImpl[MessageClass]] = impl
        self._dtype = impl.dtype                              # Permit usage after close()
        self._transport_session = impl.transport_session      # Same
        self._transfer_id_counter = impl.transfer_id_counter  # Same
        self._loop = loop
        impl.register_proxy()
        self._priority: pyuavcan.transport.Priority = DEFAULT_PRIORITY

    @property
    def dtype(self) -> typing.Type[MessageClass]:
        return self._dtype

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

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        """
        The priority level used for transfers published via this proxy instance.
        This parameter is configured separately per proxy instance; i.e., it is not shared across different publisher
        instances under the same session specifier.
        """
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        assert value in pyuavcan.transport.Priority
        self._priority = value

    async def publish(self, message: MessageClass) -> None:
        """
        Serializes and publishes the message object at the priority level selected earlier.
        Should not be used simultaneously with publish_soon() because that makes the message ordering undefined.
        """
        if self._maybe_impl is None:
            raise TypedSessionClosedError(repr(self))
        else:
            await self._maybe_impl.publish(message, self._priority)

    def publish_soon(self, message: MessageClass) -> None:
        """
        Serializes and publishes the message object at the priority level selected earlier. Does so without blocking.
        Should not be used simultaneously with publish() because that makes the message ordering undefined.
        """
        async def executor() -> None:
            try:
                await self.publish(message)
            except Exception as ex:
                _logger.exception('%s deferred publication has failed: %s', self, ex)

        asyncio.ensure_future(executor(), loop=self._loop)

    def close(self) -> None:
        impl, self._maybe_impl = self._maybe_impl, None
        if impl is not None:
            impl.remove_proxy()

    def __del__(self) -> None:
        if self._maybe_impl is not None:
            _logger.info('%s has not been disposed of properly; fixing', self)
            self._maybe_impl.remove_proxy()


class PublisherImpl(typing.Generic[MessageClass]):
    """
    The publisher implementation. There is at most one such implementation per session specifier. It may be shared
    across multiple users with the help of the proxy class. When the last proxy is closed or garbage collected,
    the implementation will also be closed and removed. This is not a part of the library API.
    """
    def __init__(self,
                 dtype:               typing.Type[MessageClass],
                 transport_session:   pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 finalizer:           TypedSessionFinalizer,
                 loop:                asyncio.AbstractEventLoop):
        self.dtype = dtype
        self.transport_session = transport_session
        self.transfer_id_counter = transfer_id_counter

        self._finalizer = finalizer
        self._lock = asyncio.Lock(loop=loop)
        self._proxy_count = 0
        self._closed = False

    async def publish(self, message: MessageClass, priority: pyuavcan.transport.Priority) -> None:
        if not isinstance(message, self.dtype):
            raise ValueError(f'Expected a message object of type {self.dtype}, found this: {message}')

        async with self._lock:
            self._raise_if_closed()
            timestamp = pyuavcan.transport.Timestamp.now()
            fragmented_payload = list(pyuavcan.dsdl.serialize(message))
            transfer = pyuavcan.transport.Transfer(timestamp=timestamp,
                                                   priority=priority,
                                                   transfer_id=self.transfer_id_counter.get_then_increment(),
                                                   fragmented_payload=fragmented_payload)
            await self.transport_session.send(transfer)

    def register_proxy(self) -> None:
        self._raise_if_closed()
        assert self._proxy_count >= 0
        self._proxy_count += 1
        _logger.debug('%s got a new proxy, new count %s', self, self._proxy_count)

    def remove_proxy(self) -> None:
        self._raise_if_closed()
        self._proxy_count -= 1
        _logger.debug('%s has lost a proxy, new count %s', self, self._proxy_count)
        assert self._proxy_count >= 0
        if self._proxy_count <= 0:
            if not self._closed:
                _logger.info('%s is being closed', self)
                self._closed = True
                self._finalizer([self.transport_session])

    @property
    def proxy_count(self) -> int:
        """Testing facilitation."""
        assert self._proxy_count >= 0
        return self._proxy_count

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise TypedSessionClosedError(repr(self))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(self,
                                                      dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                                      transport_session=self.transport_session,
                                                      proxy_count=self._proxy_count)
