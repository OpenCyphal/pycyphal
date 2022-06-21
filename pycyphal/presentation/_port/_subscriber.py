# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Type, Optional, Generic, Awaitable, Callable, Union
import logging
import asyncio
import dataclasses
import pycyphal.util
import pycyphal.dsdl
import pycyphal.transport
from ._base import MessagePort, T, PortFinalizer, Closable
from ._error import PortClosedError


# Shouldn't be too large as this value defines how quickly the task will detect that the underlying transport is closed.
_RECEIVE_TIMEOUT = 1


_logger = logging.getLogger(__name__)


ReceivedMessageHandler = Union[
    Callable[[T, pycyphal.transport.TransferFrom], None],
    Callable[[T, pycyphal.transport.TransferFrom], Awaitable[None]],
]
"""
The handler may be either sync or async (auto-detected).
"""


@dataclasses.dataclass
class SubscriberStatistics:
    transport_session: pycyphal.transport.SessionStatistics  #: Shared per session specifier.
    messages: int  #: Number of received messages, individual per subscriber.
    overruns: int  #: Number of messages lost to queue overruns; individual per subscriber.
    deserialization_failures: int  #: Number of messages lost to deserialization errors; shared per session specifier.


class Subscriber(MessagePort[T]):
    """
    A task should request its own independent subscriber instance from the presentation layer controller.
    Do not share the same subscriber instance across different tasks. This class implements the RAII pattern.

    Whenever a message is received from a subject, it is deserialized once and the resulting object is
    passed by reference into each subscriber instance. If there is more than one subscriber instance for
    a subject, accidental mutation of the object by one consumer may affect other consumers. To avoid this,
    the application should either avoid mutating received message objects or clone them beforehand.

    This class implements the async iterator protocol yielding received messages.
    Iteration stops shortly after the subscriber is closed.
    It can be used as follows::

        async for message, transfer in subscriber:
            ...  # Handle the message.
        # The loop will be stopped shortly after the subscriber is closed.

    Implementation info: all subscribers sharing the same session specifier also share the same
    underlying implementation object containing the transport session which is reference counted and destroyed
    automatically when the last subscriber with that session specifier is closed;
    the user code cannot access it and generally shouldn't care.
    """

    def __init__(self, impl: SubscriberImpl[T], queue_capacity: Optional[int]):
        """
        Do not call this directly! Use :meth:`Presentation.make_subscriber`.
        """
        assert not impl.is_closed, "Internal logic error"
        if queue_capacity is None:
            queue_capacity = 0  # This case is defined by the Queue API. Means unlimited.
        else:
            queue_capacity = int(queue_capacity)
            if queue_capacity < 1:
                raise ValueError(f"Invalid queue capacity: {queue_capacity}")

        self._closed = False
        self._impl = impl
        self._maybe_task: Optional[asyncio.Task[None]] = None
        self._rx: _Listener[T] = _Listener(asyncio.Queue(maxsize=queue_capacity))
        impl.add_listener(self._rx)

    # ----------------------------------------  HANDLER-BASED API  ----------------------------------------

    def receive_in_background(self, handler: ReceivedMessageHandler[T]) -> None:
        """
        Configures the subscriber to invoke the specified handler whenever a message is received.
        The handler may be an async callable, or it may return an awaitable, or it may return None
        (the latter case is that of a regular synchronous function).

        If the caller attempts to configure multiple handlers by invoking this method repeatedly,
        only the last configured handler will be active (the old ones will be forgotten).
        If the handler throws an exception, it will be suppressed and logged.

        This method internally starts a new task. If the subscriber is closed while the task is running,
        the task will be silently cancelled automatically; the application need not get involved.

        This method of handling messages should not be used with the plain async receive API;
        an attempt to do so may lead to unpredictable message distribution between consumers.
        """

        async def task_function() -> None:
            # This could be an interesting opportunity for optimization: instead of using the queue, just let the
            # implementation class invoke the handler from its own receive task directly. Eliminates extra indirection.
            while not self._closed:
                try:
                    async for message, transfer in self:
                        try:
                            maybe_awaitable = handler(message, transfer)
                            if maybe_awaitable is not None:
                                await maybe_awaitable  # The user provided an async handler function
                        except Exception as ex:
                            if isinstance(ex, asyncio.CancelledError):
                                raise
                            _logger.exception("%s got an unhandled exception in the message handler: %s", self, ex)
                except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError) as ex:
                    _logger.debug("%s receive task is stopping because: %r", self, ex)
                    break
                except Exception as ex:
                    _logger.exception("%s receive task failure: %s", self, ex)
                    await asyncio.sleep(1)  # TODO is this an adequate failure management strategy?

        if self._maybe_task is not None:
            self._maybe_task.cancel()

        self._maybe_task = asyncio.get_event_loop().create_task(task_function())

    # ----------------------------------------  DIRECT RECEIVE  ----------------------------------------

    async def receive(self, monotonic_deadline: float) -> Optional[tuple[T, pycyphal.transport.TransferFrom]]:
        """
        Blocks until either a valid message is received,
        in which case it is returned along with the transfer which delivered it;
        or until the specified deadline is reached, in which case None is returned.
        The deadline value is compared against :meth:`asyncio.AbstractEventLoop.time`.

        The method will never return None unless the deadline has been exceeded or the session is closed;
        in order words, a spurious premature return cannot occur.

        If the deadline is not in the future, the method will non-blockingly check if there is any data;
        if there is, it will be returned, otherwise None will be returned immediately.
        It is guaranteed that no context switch will occur in this case, as if the method was not async.

        If an infinite deadline is desired, consider using :meth:`__aiter__`/:meth:`__anext__`.
        """
        loop = asyncio.get_running_loop()
        return await self.receive_for(timeout=monotonic_deadline - loop.time())

    async def receive_for(self, timeout: float) -> Optional[tuple[T, pycyphal.transport.TransferFrom]]:
        """
        This is like :meth:`receive` but with a relative timeout instead of an absolute deadline.
        """
        self._raise_if_closed_or_failed()
        try:
            if timeout > 0:
                message, transfer = await asyncio.wait_for(self._rx.queue.get(), timeout)
            else:
                message, transfer = self._rx.queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        except asyncio.TimeoutError:
            return None
        else:
            assert isinstance(message, self._impl.dtype), "Internal protocol violation"
            assert isinstance(transfer, pycyphal.transport.TransferFrom), "Internal protocol violation"
            return message, transfer

    async def get(self, timeout: float = 0) -> Optional[T]:
        """
        A convenience wrapper over :meth:`receive_for` where the result does not contain the transfer metadata,
        and the default timeout is zero (which means check for new messages non-blockingly).
        This method approximates the standard Queue API.
        """
        result = await self.receive_for(timeout)
        if result:
            message, _meta = result
            return message
        return None

    # ----------------------------------------  ITERATOR API  ----------------------------------------

    def __aiter__(self) -> Subscriber[T]:
        """
        Iterator API support. Returns self unchanged.
        """
        return self

    async def __anext__(self) -> tuple[T, pycyphal.transport.TransferFrom]:
        """
        This is like :meth:`receive` with an infinite timeout, so it cannot return None.
        """
        try:
            while not self._closed:
                out = await self.receive_for(_RECEIVE_TIMEOUT)
                if out is not None:
                    return out
        except pycyphal.transport.ResourceClosedError:
            pass
        raise StopAsyncIteration

    # ----------------------------------------  AUXILIARY  ----------------------------------------

    @property
    def dtype(self) -> Type[T]:
        return self._impl.dtype

    @property
    def transport_session(self) -> pycyphal.transport.InputSession:
        return self._impl.transport_session

    def sample_statistics(self) -> SubscriberStatistics:
        """
        Returns the statistical counters of this subscriber, including the statistical metrics of the underlying
        transport session, which is shared across all subscribers with the same session specifier.
        """
        return SubscriberStatistics(
            transport_session=self.transport_session.sample_statistics(),
            messages=self._rx.push_count,
            deserialization_failures=self._impl.deserialization_failure_count,
            overruns=self._rx.overrun_count,
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._impl.remove_listener(self._rx)
            if self._maybe_task is not None:  # The task may be holding the lock.
                try:
                    self._maybe_task.cancel()  # We don't wait for it to exit because it's pointless.
                except Exception as ex:
                    _logger.exception("%s task could not be cancelled: %s", self, ex)
                self._maybe_task = None

    def _raise_if_closed_or_failed(self) -> None:
        if self._closed:
            raise PortClosedError(repr(self))

        if self._rx.exception is not None:
            self._closed = True
            raise self._rx.exception from RuntimeError("The subscriber has failed and been closed")

    def __del__(self) -> None:
        try:
            closed = self._closed
        except AttributeError:
            closed = True  # Incomplete construction.
        if not closed:
            # https://docs.python.org/3/reference/datamodel.html#object.__del__
            # DO NOT invoke logging from the finalizer because it may resurrect the object!
            # Once it is resurrected, we may run into resource management issue if __del__() is invoked again.
            # Whether it is invoked the second time is an implementation detail.
            self._closed = True
            self._impl.remove_listener(self._rx)


@dataclasses.dataclass
class _Listener(Generic[T]):
    """
    The queue-induced extra level of indirection adds processing overhead and latency. In the future we may need to
    consider an optimization where the subscriber would automatically detect whether the underlying implementation
    is shared among many subscribers or not. If not, it should bypass the queue and read from the transport directly
    instead. This would avoid the unnecessary overheads and at the same time would be transparent for the user.
    """

    queue: asyncio.Queue[tuple[T, pycyphal.transport.TransferFrom]]
    push_count: int = 0
    overrun_count: int = 0
    exception: Optional[Exception] = None

    def push(self, message: T, transfer: pycyphal.transport.TransferFrom) -> None:
        try:
            self.queue.put_nowait((message, transfer))
            self.push_count += 1
        except asyncio.QueueFull:
            self.overrun_count += 1

    def __repr__(self) -> str:
        """
        Overriding repr() is necessary to avoid the contents of the queue from being printed.
        The queue contains DSDL objects, which may be large and the output of their repr() may be very expensive
        to compute, especially if the queue is long.
        """
        return pycyphal.util.repr_attributes_noexcept(
            self,
            queue_length=self.queue.qsize(),
            push_count=self.push_count,
            overrun_count=self.overrun_count,
            exception=self.exception,
        )


class SubscriberImpl(Closable, Generic[T]):
    """
    This class implements the actual reception and deserialization logic. It is not visible to the user and is not
    part of the API. There is at most one instance per session specifier. It may be shared across multiple users
    with the help of the proxy class. When the last proxy is closed or garbage collected, the implementation will
    also be closed and removed.
    """

    def __init__(
        self,
        dtype: Type[T],
        transport_session: pycyphal.transport.InputSession,
        finalizer: PortFinalizer,
    ):
        assert pycyphal.dsdl.is_message_type(dtype)
        self.dtype = dtype
        self.transport_session = transport_session
        self.deserialization_failure_count = 0
        self._maybe_finalizer: Optional[PortFinalizer] = finalizer
        self._task = asyncio.get_event_loop().create_task(self._task_function())
        self._listeners: list[_Listener[T]] = []

    @property
    def is_closed(self) -> bool:
        return self._maybe_finalizer is None

    async def _task_function(self) -> None:
        exception: Optional[Exception] = None
        loop = asyncio.get_running_loop()
        try:  # pylint: disable=too-many-nested-blocks
            while not self.is_closed:
                transfer = await self.transport_session.receive(loop.time() + _RECEIVE_TIMEOUT)
                if transfer is not None:
                    message = pycyphal.dsdl.deserialize(self.dtype, transfer.fragmented_payload)
                    _logger.debug("%r received message: %r", self, message)
                    if message is not None:
                        for rx in self._listeners:
                            rx.push(message, transfer)
                    else:
                        self.deserialization_failure_count += 1
        except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError) as ex:
            _logger.debug("Cancelling the subscriber task of %s because: %r", self, ex)
        except Exception as ex:
            exception = ex
            # Do not use f-string because it can throw, unlike the built-in formatting facility of the logger
            _logger.exception("Fatal error in the subscriber task of %s: %s", self, ex)
        finally:
            self._finalize(exception)

    def _finalize(self, exception: Optional[Exception] = None) -> None:
        exception = exception if exception is not None else PortClosedError(repr(self))
        try:
            if self._maybe_finalizer is not None:
                self._maybe_finalizer([self.transport_session])
                self._maybe_finalizer = None
        except Exception as ex:
            _logger.exception("Failed to finalize %s: %s", self, ex)
        for rx in self._listeners:
            rx.exception = exception

    def close(self) -> None:
        try:
            self._task.cancel()  # Force the task to be stopped ASAP without waiting for timeout
        except Exception as ex:
            _logger.debug("Explicit close: could not cancel the task %r: %s", self._task, ex, exc_info=True)
        self._finalize()

    def add_listener(self, rx: _Listener[T]) -> None:
        assert not self.is_closed, "Internal logic error: cannot add listener to a closed subscriber implementation"
        self._listeners.append(rx)

    def remove_listener(self, rx: _Listener[T]) -> None:
        try:
            self._listeners.remove(rx)
        except ValueError:
            _logger.exception("%r does not have listener %r", self, rx)
        if len(self._listeners) == 0:
            self.close()

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes_noexcept(
            self,
            dtype=str(pycyphal.dsdl.get_model(self.dtype)),
            transport_session=self.transport_session,
            deserialization_failure_count=self.deserialization_failure_count,
            listeners=self._listeners,
            closed=self.is_closed,
        )
