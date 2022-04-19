# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import asyncio
import logging
from typing import Any, Callable, Tuple, Iterable
import pycyphal.util
from pycyphal.transport import TransferFrom
from pycyphal.presentation import Subscriber


MessageWithMetadata = Tuple[Any, TransferFrom]

SynchronizedGroup = Tuple[MessageWithMetadata, ...]

_RECEIVE_TIMEOUT = 1.0


def get_timestamp_field(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the value of the ``timestamp`` field of the message
    converted to seconds.
    The field is expected to be of type ``uavcan.time.SynchronizedTimestamp``.
    This function will fail with an attribute error if such field is not present in the message.
    """
    return float(item[0].timestamp.microsecond) * 1e-6


def get_local_reception_timestamp(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the local system (wall) reception timestamp (in seconds).
    This function works for messages of any type.
    """
    return float(item[1].timestamp.system)


def get_local_reception_monotonic_timestamp(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the local monotonic reception timestamp (in seconds).
    This function works for messages of any type.
    This function may perform worse than the wall time alternative because monotonic timestamp is usually less accurate.
    """
    return float(item[1].timestamp.monotonic)


class Synchronizer(abc.ABC):
    """
    Synchronizer is used to receive messages from multiple subjects concurrently such that messages that
    belong to the same group, and only those,
    are delivered to the application synchronously in one batch.
    Different synchronization policies may be provided by different implementations of this abstract class.

    Related sources:

    - https://github.com/OpenCyphal/pycyphal/issues/65
    - http://wiki.ros.org/message_filters/ApproximateTime
    - https://forum.opencyphal.org/t/si-namespace-design/207/5?u=pavel.kirienko

    ..  warning::

        This API (incl. all derived types) is experimental and subject to breaking changes between minor revisions.
    """

    def __init__(self, subscribers: Iterable[pycyphal.presentation.Subscriber[Any]]) -> None:
        self._subscribers = tuple(subscribers)
        self._closed = False

    @property
    def subscribers(self) -> tuple[Subscriber[Any], ...]:
        """
        The set of subscribers whose outputs are synchronized.
        The ordering matches that of the output data.
        """
        return self._subscribers

    @abc.abstractmethod
    async def receive_for(self, timeout: float) -> SynchronizedGroup | None:
        """See :class:`pycyphal.presentation.Subscriber`"""
        raise NotImplementedError

    async def receive(self, monotonic_deadline: float) -> SynchronizedGroup | None:
        """See :class:`pycyphal.presentation.Subscriber`"""
        return await self.receive_for(timeout=monotonic_deadline - asyncio.get_running_loop().time())

    async def get(self, timeout: float = 0) -> tuple[Any, ...] | None:
        """Like :meth:`receive_for` but without transfer metadata, only message objects."""
        result = await self.receive_for(timeout)
        if result:
            return tuple(msg for msg, _meta in result)
        return None

    @abc.abstractmethod
    def receive_in_background(self, handler: Callable[..., None]) -> None:
        """
        See :class:`pycyphal.presentation.Subscriber`.
        The for N subscribers, the callback receives N tuples of :class:`MessageWithMetadata`.
        """
        raise NotImplementedError

    def get_in_background(self, handler: Callable[..., None]) -> None:
        """
        This is like :meth:`receive_in_background` but the callback receives message objects directly
        rather than the tuples of (message, metadata).
        The two methods cannot be used concurrently.
        """
        self.receive_in_background(lambda *tup: handler(*(msg for msg, _meta in tup)))

    def __aiter__(self) -> Synchronizer:
        """See :class:`pycyphal.presentation.Subscriber`"""
        return self

    async def __anext__(self) -> SynchronizedGroup:
        """See :class:`pycyphal.presentation.Subscriber`"""
        try:
            while not self._closed:
                out = await self.receive_for(_RECEIVE_TIMEOUT)
                if out is not None:
                    return out
        except pycyphal.transport.ResourceClosedError:
            pass
        raise StopAsyncIteration

    def close(self) -> None:
        """Idempotent."""
        self._closed = True
        pycyphal.util.broadcast(x.close for x in self._subscribers)()

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes_noexcept(self, self.subscribers)


_logger = logging.getLogger(__name__)
