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
from ._base import MessageTypedSessionProxy, OutgoingTransferIDCounter, MessageClass
from ._base import DEFAULT_PRIORITY


_logger = logging.getLogger(__name__)


class Publisher(MessageTypedSessionProxy[MessageClass]):
    def __init__(self,
                 impl: PublisherImpl[MessageClass],
                 loop: asyncio.AbstractEventLoop):
        self._maybe_impl: typing.Optional[PublisherImpl[MessageClass]] = impl
        self._loop = loop
        impl.register_proxy()
        self._priority: pyuavcan.transport.Priority = DEFAULT_PRIORITY

    @property
    def dtype(self) -> typing.Type[MessageClass]:
        return self._impl.dtype

    @property
    def transport_session(self) -> pyuavcan.transport.OutputSession:
        return self._impl.transport_session

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        """
        Allows the caller to reach the transfer ID counter object. This may be useful in certain special cases
        such as publication of time synchronization messages.
        """
        return self._impl.transfer_id_counter

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

    async def publish(self, message:  MessageClass) -> None:
        """
        Serializes and publishes the message object at the priority level selected earlier.
        """
        await self._impl.publish(message, self._priority)

    async def close(self) -> None:
        impl = self._impl
        self._maybe_impl = None
        await impl.remove_proxy()

    @property
    def _impl(self) -> PublisherImpl[MessageClass]:
        if self._maybe_impl is None:
            raise pyuavcan.transport.ResourceClosedError(repr(self))
        else:
            return self._maybe_impl

    async def __aenter__(self) -> Publisher[MessageClass]:
        return self

    def __del__(self) -> None:
        if self._maybe_impl is not None:
            _logger.warning(f'{self} has not been disposed of properly; fixing')
            # We can't just call close() here because the object is being deleted
            asyncio.ensure_future(self._maybe_impl.remove_proxy(), loop=self._loop)


class PublisherImpl(typing.Generic[MessageClass]):
    """
    The publisher implementation. There is at most one such implementation per session specifier. It may be shared
    across multiple users with the help of the proxy class. When the last proxy is closed or garbage collected,
    the implementation will also be closed and removed.
    """
    def __init__(self,
                 dtype:               typing.Type[MessageClass],
                 transport_session:   pyuavcan.transport.OutputSession,
                 transfer_id_counter: OutgoingTransferIDCounter,
                 finalizer:           typing.Callable[[], None],
                 loop:                asyncio.AbstractEventLoop):
        self.dtype = dtype
        self.transport_session = transport_session
        self.transfer_id_counter = transfer_id_counter

        self._finalizer = finalizer
        self._lock = asyncio.Lock(loop=loop)
        self._proxy_count = 0
        self._closed = False

    async def publish(self, message:  MessageClass, priority: pyuavcan.transport.Priority) -> None:
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
        _logger.debug(f'Typed session instance {self} got a new proxy, new count {self._proxy_count}')

    async def remove_proxy(self) -> None:
        self._raise_if_closed()
        self._proxy_count -= 1
        _logger.debug(f'Typed session instance {self} lost a proxy, new count {self._proxy_count}')
        assert self._proxy_count >= 0
        if self._proxy_count <= 0:
            async with self._lock:
                if not self._closed:
                    _logger.info(f'Typed session instance {self} is being closed')
                    self._closed = True
                    self._finalizer()
                    await self.transport_session.close()  # Race condition?

    @property
    def proxy_count(self) -> int:
        """Testing facilitation."""
        assert self._proxy_count >= 0
        return self._proxy_count

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_object(self,
                                         dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                         transport_session=self.transport_session,
                                         proxy_count=self._proxy_count)

    def __del__(self) -> None:
        assert self._closed
        assert self._proxy_count == 0
