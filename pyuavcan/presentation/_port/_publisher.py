# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import logging
import asyncio
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import MessagePort, OutgoingTransferIDCounter, MessageClass, Closable
from ._base import DEFAULT_PRIORITY, PortFinalizer
from ._error import PortClosedError


_logger = logging.getLogger(__name__)


class Publisher(MessagePort[MessageClass]):
    """
    A task should request its own independent publisher instance from the presentation layer controller.
    Do not share the same publisher instance across different tasks. This class implements the RAII pattern.

    Implementation info: all publishers sharing the same session specifier (i.e., subject-ID) also share the same
    underlying implementation object containing the transport session which is reference counted and destroyed
    automatically when the last publisher with that session specifier is closed;
    the user code cannot access it and generally shouldn't care.
    None of the settings of a publisher instance, such as send timeout or priority, can affect other publishers;
    this does not apply to the transfer-ID counter objects though because they are transport-layer entities
    and therefore are shared per session specifier.
    """

    DEFAULT_SEND_TIMEOUT = 1.0
    """
    Default value for :attr:`send_timeout`. The value is an implementation detail, not required by Specification.
    """

    def __init__(self, impl: PublisherImpl[MessageClass]):
        """
        Do not call this directly! Use :meth:`Presentation.make_publisher`.
        """
        self._maybe_impl: typing.Optional[PublisherImpl[MessageClass]] = impl
        impl.register_proxy()  # Register ASAP to ensure correct finalization.

        self._dtype = impl.dtype  # Permit usage after close()
        self._transport_session = impl.transport_session  # Same
        self._transfer_id_counter = impl.transfer_id_counter  # Same
        self._priority: pyuavcan.transport.Priority = DEFAULT_PRIORITY
        self._send_timeout = self.DEFAULT_SEND_TIMEOUT

    @property
    def dtype(self) -> typing.Type[MessageClass]:
        return self._dtype

    @property
    def transport_session(self) -> pyuavcan.transport.OutputSession:
        return self._transport_session

    @property
    def transfer_id_counter(self) -> OutgoingTransferIDCounter:
        """
        Allows the caller to reach the transfer-ID counter object of this session (shared per session specifier).
        This may be useful in certain special cases such as publication of time synchronization messages,
        where it may be necessary to override the transfer-ID manually.
        """
        return self._transfer_id_counter

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        """
        The priority level used for transfers published via this instance.
        This parameter is configured separately per proxy instance; i.e., it is not shared across different publisher
        instances under the same session specifier.
        """
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        assert value in pyuavcan.transport.Priority
        self._priority = value

    @property
    def send_timeout(self) -> float:
        """
        Every outgoing transfer initiated via this proxy instance will have to be sent in this amount of time.
        If the time is exceeded, the attempt is aborted and False is returned. Read the transport layer docs for
        an in-depth information on send timeout handling.
        The default is :attr:`DEFAULT_SEND_TIMEOUT`.
        The publication logic is roughly as follows::

            return transport_session.send(message_transfer, self.loop.time() + self.send_timeout)
        """
        return self._send_timeout

    @send_timeout.setter
    def send_timeout(self, value: float) -> None:
        value = float(value)
        if 0 < value < float("+inf"):
            self._send_timeout = value
        else:
            raise ValueError(f"Invalid send timeout value: {value}")

    async def publish(self, message: MessageClass) -> bool:
        """
        Serializes and publishes the message object at the priority level selected earlier.
        Should not be used simultaneously with :meth:`publish_soon` because that makes the message ordering undefined.
        Returns False if the publication could not be completed in :attr:`send_timeout`, True otherwise.
        """
        if self._maybe_impl is None:
            raise PortClosedError(repr(self))
        loop = asyncio.get_running_loop()
        return await self._maybe_impl.publish(message, self._priority, loop.time() + self._send_timeout)

    def publish_soon(self, message: MessageClass) -> None:
        """
        Serializes and publishes the message object at the priority level selected earlier.
        Does so without blocking (observe that this method is not async).
        Should not be used simultaneously with :meth:`publish` because that makes the message ordering undefined.
        The send timeout is still in effect here -- if the operation cannot complete in the selected time,
        send will be cancelled and a low-severity log message will be emitted.
        """
        if self._maybe_impl is None:  # Detect errors as early as possible, do not wait for the task to start.
            raise PortClosedError(repr(self))

        async def executor() -> None:
            try:
                if not await self.publish(message):
                    _logger.info("%s send timeout", self)
            except Exception as ex:
                _logger.exception("%s deferred publication has failed: %s", self, ex)

        asyncio.ensure_future(executor())

    def close(self) -> None:
        impl, self._maybe_impl = self._maybe_impl, None
        if impl is not None:
            impl.remove_proxy()

    def __del__(self) -> None:
        if self._maybe_impl is not None:
            # https://docs.python.org/3/reference/datamodel.html#object.__del__
            # DO NOT invoke logging from the finalizer because it may resurrect the object!
            # Once it is resurrected, we may run into resource management issue if __del__() is invoked again.
            # Whether it is invoked the second time is an implementation detail.
            self._maybe_impl.remove_proxy()
            self._maybe_impl = None


class PublisherImpl(Closable, typing.Generic[MessageClass]):
    """
    The publisher implementation. There is at most one such implementation per session specifier. It may be shared
    across multiple users with the help of the proxy class. When the last proxy is closed or garbage collected,
    the implementation will also be closed and removed. This is not a part of the library API.
    """

    def __init__(
        self,
        dtype: typing.Type[MessageClass],
        transport_session: pyuavcan.transport.OutputSession,
        transfer_id_counter: OutgoingTransferIDCounter,
        finalizer: PortFinalizer,
    ):
        self.dtype = dtype
        self.transport_session = transport_session
        self.transfer_id_counter = transfer_id_counter
        self._maybe_finalizer: typing.Optional[PortFinalizer] = finalizer
        self._lock = asyncio.Lock()
        self._proxy_count = 0

    async def publish(
        self, message: MessageClass, priority: pyuavcan.transport.Priority, monotonic_deadline: float
    ) -> bool:
        if not isinstance(message, self.dtype):
            raise TypeError(f"Expected a message object of type {self.dtype}, found this: {message}")

        async with self._lock:
            if self._is_closed:
                raise PortClosedError(repr(self))
            timestamp = pyuavcan.transport.Timestamp.now()
            fragmented_payload = list(pyuavcan.dsdl.serialize(message))
            transfer = pyuavcan.transport.Transfer(
                timestamp=timestamp,
                priority=priority,
                transfer_id=self.transfer_id_counter.get_then_increment(),
                fragmented_payload=fragmented_payload,
            )
            return await self.transport_session.send(transfer, monotonic_deadline)

    def register_proxy(self) -> None:
        self._proxy_count += 1
        _logger.debug("%s got a new proxy, new count %s", self, self._proxy_count)
        assert not self._is_closed, "Internal protocol violation"
        assert self._proxy_count >= 1

    def remove_proxy(self) -> None:
        self._proxy_count -= 1
        _logger.debug("%s has lost a proxy, new count %s", self, self._proxy_count)
        if self._proxy_count <= 0:
            self.close()  # RAII auto-close
        assert self._proxy_count >= 0

    @property
    def proxy_count(self) -> int:
        """Testing facilitation."""
        assert self._proxy_count >= 0
        return self._proxy_count

    def close(self) -> None:
        if self._maybe_finalizer is not None:
            self._maybe_finalizer([self.transport_session])
            self._maybe_finalizer = None

    @property
    def _is_closed(self) -> bool:
        return self._maybe_finalizer is None

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(
            self,
            dtype=str(pyuavcan.dsdl.get_model(self.dtype)),
            transport_session=self.transport_session,
            proxy_count=self._proxy_count,
        )
