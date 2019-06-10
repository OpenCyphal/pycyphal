#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import enum
import typing
import logging
import asyncio
import dataclasses
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import MessageTypedSessionProxy, MessageClass


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SubscriberStatistics:
    transfer:                 pyuavcan.transport.Statistics
    messages:                 int
    overruns:                 int
    deserialization_failures: int


class Subscriber(MessageTypedSessionProxy[MessageClass]):
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
        self._rx: _Listener[MessageClass] = _Listener(asyncio.Queue(maxsize=queue_capacity, loop=loop))
        impl.add_listener(self._rx)

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
        return SubscriberStatistics(transfer=self.transport_session.sample_statistics(),
                                    messages=self._rx.push_count,
                                    deserialization_failures=self._impl.deserialization_failure_count,
                                    overruns=self._rx.overrun_count)

    async def receive(self) -> MessageClass:
        """
        Blocks forever until a valid message is received.
        """
        return (await self.receive_with_transfer())[0]

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[MessageClass]:
        """
        Blocks until either a valid message is received, in which case it is returned; or until the deadline
        is reached, in which case None is returned.
        The method will never return None unless the deadline is reached.
        If the deadline is in the past (e.g., zero), the method will non-blockingly check if there is any data;
        if there is, it will be returned, otherwise None will be returned immediately.
        """
        out = await self.try_receive_with_transfer(monotonic_deadline)
        return out[0] if out else None

    async def receive_with_transfer(self) -> typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]:
        """
        Blocks forever until a valid message is received. The received message will be returned along with the
        transfer which delivered it.
        """
        self._raise_if_closed_or_failed()
        message, transfer = await self._rx.queue.get()
        assert isinstance(message, self._impl.dtype), 'Internal protocol violation'
        assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
        return message, transfer

    async def try_receive_with_transfer(self, monotonic_deadline: float) \
            -> typing.Optional[typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]]:
        """
        Blocks until either a valid message is received, in which case it is returned along with the transfer
        which delivered it; or until the deadline is reached, in which case None is returned.
        The method will never return None unless the deadline is reached.
        If the deadline is in the past (e.g., zero), the method will non-blockingly check if there is any data;
        if there is, it will be returned, otherwise None will be returned immediately.
        """
        self._raise_if_closed_or_failed()
        timeout = monotonic_deadline - time.monotonic()
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

    async def close(self) -> None:
        """
        If this is the last subscriber instance for this session specifier, the underlying implementation object and
        its transport session instance will be closed. The underlying logic makes no guarantees, however, how quickly
        it will be closed. There is a chance that a new subscriber instance created in the future may still reuse the
        old underlying implementation which was previously scheduled for disposal.
        The user should explicitly close all objects before disposing of the presentation layer instance.
        """
        self._raise_if_closed_or_failed()
        self._closed = True
        self._impl.remove_listener(self._rx)

    def _raise_if_closed_or_failed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))

        if self._rx.exception is not None:
            self._closed = True
            raise RuntimeError('The subscriber has failed and been closed') from self._rx.exception

    async def __aenter__(self) -> Subscriber[MessageClass]:
        return self

    def __del__(self) -> None:
        if not self._closed:
            _logger.warning(f'{self} has not been disposed of properly; fixing')
            self._closed = True
            self._impl.remove_listener(self._rx)


@dataclasses.dataclass
class _Listener(typing.Generic[MessageClass]):
    queue:         asyncio.Queue[typing.Tuple[MessageClass, pyuavcan.transport.TransferFrom]]
    push_count:    int = 0
    overrun_count: int = 0
    exception:     typing.Optional[Exception] = None


class SubscriberImpl(typing.Generic[MessageClass]):
    class State(enum.Enum):
        # An additional initialization state is needed to remove usage constraints such as having to add the first
        # listener before the task is started running.
        INIT = enum.auto()
        LIVE = enum.auto()
        DEAD = enum.auto()

    def __init__(self,
                 dtype:             typing.Type[MessageClass],
                 transport_session: pyuavcan.transport.InputSession,
                 finalizer:         typing.Callable[[], None],
                 loop:              asyncio.AbstractEventLoop):
        self.dtype = dtype
        self.transport_session = transport_session
        self.deserialization_failure_count = 0
        self._finalizer = finalizer
        self._task = loop.create_task(self._task_function())
        self._listeners: typing.List[_Listener[MessageClass]] = []
        self._state = self.State.INIT

    async def _task_function(self) -> None:
        exception: typing.Optional[Exception] = None
        try:
            while len(self._listeners) > 0 or self._state == self.State.INIT:
                transfer = await self.transport_session.try_receive(time.monotonic() + _RECEIVE_POLL_INTERVAL)
                if transfer is not None:
                    message = pyuavcan.dsdl.try_deserialize(self.dtype, transfer.fragmented_payload)
                    if message is not None:
                        for rx in self._listeners:
                            try:
                                rx.queue.put_nowait((message, transfer))
                                rx.push_count += 1
                            except asyncio.QueueFull:
                                rx.overrun_count += 1
                    else:
                        self.deserialization_failure_count += 1
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception('Fatal error in the subscriber task of %s: %s', self, ex)

        try:
            self._state = self.State.DEAD
            try:
                self._finalizer()
                _logger.info(f'{self} is being closed')
            finally:
                await self.transport_session.close()    # Race condition?
        except pyuavcan.transport.ResourceClosedError:
            # This is the desired state, no need to panic.
            # The session could be closed manually or by closing the entire transport instance.
            pass
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception(f'Failed to close the subscription session of %s: %s', self, ex)

        exception = exception if exception is not None else pyuavcan.transport.ResourceClosedError(repr(self))
        for rx in self._listeners:
            rx.exception = exception

    def add_listener(self, rx: _Listener[MessageClass]) -> None:
        if self._state == self.State.DEAD:  # pragma: no cover
            raise pyuavcan.transport.ResourceClosedError(repr(self))
        else:
            self._state = self.State.LIVE
            self._listeners.append(rx)

    def remove_listener(self, rx: _Listener[MessageClass]) -> None:
        if self._state == self.State.DEAD:  # pragma: no cover
            raise pyuavcan.transport.ResourceClosedError(repr(self))
        else:
            self._listeners.remove(rx)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_object(self,
                                         dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
                                         transport_session=self.transport_session,
                                         deserialization_failure_count=self.deserialization_failure_count,
                                         listeners=self._listeners,
                                         state=str(self._state))


_RECEIVE_POLL_INTERVAL = 1
