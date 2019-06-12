#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import logging
import asyncio
import dataclasses
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import MessageTypedSession, MessageClass, TypedSessionFinalizer
from ._error import TypedSessionClosedError


_RECEIVE_TIMEOUT = 10


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SubscriberStatistics:
    transport_session:        pyuavcan.transport.Statistics
    messages:                 int
    overruns:                 int
    deserialization_failures: int


class Subscriber(MessageTypedSession[MessageClass]):
    """
    Normally, each task should request its own subscriber instance. An attempt to reuse the same instance across
    different consumer tasks may lead to unpredictable message distribution.
    """
    def __init__(self,
                 impl:           SubscriberImpl[MessageClass],
                 loop:           asyncio.AbstractEventLoop,
                 queue_capacity: typing.Optional[int]):
        if queue_capacity is None:
            queue_capacity = 0      # This case is defined by the Queue API. Means unlimited.
        else:
            queue_capacity = int(queue_capacity)
            if queue_capacity < 1:
                raise ValueError(f'Invalid queue capacity: {queue_capacity}')

        self._closed = False
        self._impl = impl
        self._loop = loop
        self._lock = asyncio.Lock(loop=loop)
        self._rx: _Listener[MessageClass] = _Listener(asyncio.Queue(maxsize=queue_capacity, loop=loop))
        impl.add_listener(self._rx)

    # ----------------------------------------  NAKED RECEIVE  ----------------------------------------

    async def receive(self) -> MessageClass:
        """
        This is a shortcut for receive_with_transfer()[0]; i.e, this method discards the transfer and
        returns only the deserialized message.
        """
        return (await self.receive_with_transfer())[0]

    async def try_receive_until(self, monotonic_deadline: float) -> typing.Optional[MessageClass]:
        """
        This is a shortcut for try_receive_with_transfer_until(..)[0]; i.e, this method discards the transfer and
        returns only the deserialized message.
        """
        out = await self.try_receive_with_transfer_until(monotonic_deadline=monotonic_deadline)
        return out[0] if out else None

    async def try_receive_for(self, timeout: float) -> typing.Optional[MessageClass]:
        """
        This is a shortcut for try_receive_with_transfer_for(..)[0]; i.e, this method discards the transfer and
        returns only the deserialized message.
        """
        out = await self.try_receive_with_transfer_for(timeout=timeout)
        return out[0] if out else None

    # ----------------------------------------  RECEIVE WITH TRANSFER  ----------------------------------------

    async def receive_with_transfer(self) -> typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]:
        """
        Blocks forever until a valid message is received. The received message will be returned along with the
        transfer which delivered it.
        """
        async with self._lock:
            self._raise_if_closed_or_failed()
            message, transfer = await self._rx.queue.get()
            assert isinstance(message, self._impl.dtype), 'Internal protocol violation'
            assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
            return message, transfer

    async def try_receive_with_transfer_until(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]]:
        """
        Blocks until either a valid message is received, in which case it is returned along with the transfer
        which delivered it; or until the deadline is reached, in which case None is returned.
        The method will never return None unless the deadline is reached.
        If the deadline is in the past (e.g., zero), the method will non-blockingly check if there is any data;
        if there is, it will be returned, otherwise None will be returned immediately.
        """
        return await self.try_receive_with_transfer_for(timeout=monotonic_deadline - time.monotonic())

    async def try_receive_with_transfer_for(self, timeout: float) \
            -> typing.Optional[typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]]:
        """
        Blocks until either a valid message is received, in which case it is returned along with the transfer
        which delivered it; or until the timeout is expired, in which case None is returned.
        The method will never return None unless the timeout has expired.
        If the timeout is non-positive, the method will non-blockingly check if there is any data; if there is,
        it will be returned, otherwise None will be returned immediately.
        """
        async with self._lock:
            self._raise_if_closed_or_failed()
            try:
                if timeout > 0:
                    message, transfer = await asyncio.wait_for(self._rx.queue.get(), timeout, loop=self._loop)
                else:
                    message, transfer = self._rx.queue.get_nowait()
            except asyncio.QueueEmpty:
                return None
            except asyncio.TimeoutError:
                return None
            else:
                assert isinstance(message, self._impl.dtype), 'Internal protocol violation'
                assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
                return message, transfer

    # ----------------------------------------  ITERATOR API  ----------------------------------------

    def __aiter__(self) -> Subscriber[MessageClass]:
        return self

    async def __anext__(self) -> typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]:
        try:
            return await self.receive_with_transfer()
        except pyuavcan.transport.ResourceClosedError:
            raise StopAsyncIteration

    # ----------------------------------------  AUXILIARY  ----------------------------------------

    @property
    def dtype(self) -> typing.Type[MessageClass]:
        return self._impl.dtype

    @property
    def transport_session(self) -> pyuavcan.transport.InputSession:
        return self._impl.transport_session

    def sample_statistics(self) -> SubscriberStatistics:
        """
        Returns the statistical counters of this subscriber, including the statistical metrics of the underlying
        transport session, which is shared among all subscribers of the same session specifier.
        """
        return SubscriberStatistics(transport_session=self.transport_session.sample_statistics(),
                                    messages=self._rx.push_count,
                                    deserialization_failures=self._impl.deserialization_failure_count,
                                    overruns=self._rx.overrun_count)

    async def close(self) -> None:
        """
        If this is the last subscriber instance for this session specifier, the underlying implementation object and
        its transport session instance will be closed. The user should explicitly close all objects before disposing
        of the presentation layer instance.
        """
        async with self._lock:
            self._raise_if_closed_or_failed()
            self._closed = True
            await self._impl.remove_listener(self._rx)

    def _raise_if_closed_or_failed(self) -> None:
        assert self._lock.locked(), 'Internal protocol violation: the lock is not acquired'
        if self._closed:
            raise TypedSessionClosedError(repr(self))

        if self._rx.exception is not None:
            self._closed = True
            raise self._rx.exception from RuntimeError('The subscriber has failed and been closed')

    def __del__(self) -> None:
        if not self._closed:
            _logger.warning('%s has not been disposed of properly; fixing', self)
            self._closed = True
            # We can't just call close() here because the object is being deleted
            asyncio.ensure_future(self._impl.remove_listener(self._rx), loop=self._loop)


@dataclasses.dataclass
class _Listener(typing.Generic[MessageClass]):
    """
    The queue-induced extra level of indirection adds processing overhead and latency. In the future we may need to
    consider an optimization where the subscriber would automatically detect whether the underlying implementation
    is shared among many subscribers or not. If not, it should bypass the queue and read from the transport directly
    instead. This would avoid the unnecessary overheads and at the same time would be transparent for the user.
    """
    queue:         asyncio.Queue[typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]]
    push_count:    int = 0
    overrun_count: int = 0
    exception:     typing.Optional[Exception] = None

    def push(self, message: MessageClass, transfer: pyuavcan.transport.TransferFrom) -> None:
        try:
            self.queue.put_nowait((message, transfer))
            self.push_count += 1
        except asyncio.QueueFull:
            self.overrun_count += 1


class SubscriberImpl(typing.Generic[MessageClass]):
    """
    This class implements the actual reception and deserialization logic. It is not visible to the user and is not
    part of the API. There is at most one instance per session specifier. It may be shared across multiple users
    with the help of the proxy class. When the last proxy is closed or garbage collected, the implementation will
    also be closed and removed.
    """
    def __init__(self,
                 dtype:             typing.Type[MessageClass],
                 transport_session: pyuavcan.transport.InputSession,
                 finalizer:         TypedSessionFinalizer,
                 loop:              asyncio.AbstractEventLoop):
        self.dtype = dtype
        self.transport_session = transport_session
        self.deserialization_failure_count = 0
        self._finalizer = finalizer
        self._task = loop.create_task(self._task_function())
        self._listeners: typing.List[_Listener[MessageClass]] = []
        self._closed = False

    async def _task_function(self) -> None:
        exception: typing.Optional[Exception] = None
        try:
            while not self._closed:
                transfer = await self.transport_session.try_receive(time.monotonic() + _RECEIVE_TIMEOUT)
                if transfer is not None:
                    message = pyuavcan.dsdl.try_deserialize(self.dtype, transfer.fragmented_payload)
                    if message is not None:
                        for rx in self._listeners:
                            rx.push(message, transfer)
                    else:
                        self.deserialization_failure_count += 1
        except asyncio.CancelledError:
            _logger.info('Cancelling the subscriber task of %s', self)
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception('Fatal error in the subscriber task of %s: %s', self, ex)

        try:
            self._closed = True
            await self._finalizer([self.transport_session])
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception(f'Failed to finalize %s: %s', self, ex)

        exception = exception if exception is not None else TypedSessionClosedError(repr(self))
        for rx in self._listeners:
            rx.exception = exception

    def add_listener(self, rx: _Listener[MessageClass]) -> None:
        self._raise_if_closed()
        self._listeners.append(rx)

    async def remove_listener(self, rx: _Listener[MessageClass]) -> None:
        self._raise_if_closed()
        self._listeners.remove(rx)
        if len(self._listeners) == 0:
            self._closed = True
            self._task.cancel()         # Force the task to be stopped ASAP without waiting for timeout
            await self._task            # Wait until fully disposed of

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise TypedSessionClosedError(repr(self))

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(self,
                                                      dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                                      transport_session=self.transport_session,
                                                      deserialization_failure_count=self.deserialization_failure_count,
                                                      listeners=self._listeners,
                                                      closed=self._closed)
