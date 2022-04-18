# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import asyncio
from typing import Any, Callable, Tuple
import pycyphal.util
from pycyphal.transport import TransferFrom
from pycyphal.presentation import Subscriber


MessageWithMetadata = Tuple[Any, TransferFrom]

MessageOrderingKeyFunction = Callable[[MessageWithMetadata], float]
"""
Synchronizers use this function to order and cluster messages.
The key shall be monotonically non-decreasing over time.
"""

_RECEIVE_TIMEOUT = 1.0


def get_timestamp_field(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the value of the ``timestamp`` field of the message.
    The field is expected to be of type ``uavcan.time.SynchronizedTimestamp``.
    This function will fail with an attribute error if such field is not present in the message.
    """
    return float(item[0].timestamp.microsecond) * 1e-6


def get_local_reception_timestamp(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the local system (wall) reception timestamp.
    This function works for messages of any type.
    """
    return float(item[1].timestamp.system)


def get_local_reception_monotonic_timestamp(item: MessageWithMetadata) -> float:
    """
    Message ordering key function that defines key as the local monotonic reception timestamp.
    This function works for messages of any type.
    This function may perform worse than the wall time alternative because monotonic timestamp is usually less accurate.
    """
    return float(item[1].timestamp.monotonic)


class Synchronizer(abc.ABC):
    """
    Synchronizer is used to receive messages from multiple subjects concurrently such that messages that
    belong to the same (or nearly the same) point in time, and only those,
    are delivered to the application synchronously in one batch.
    Different synchronization policies may be provided by different implementations of this abstract class.

    Related sources:

    - https://github.com/OpenCyphal/pycyphal/issues/65
    - http://wiki.ros.org/message_filters/ApproximateTime
    - https://forum.opencyphal.org/t/si-namespace-design/207/5?u=pavel.kirienko
    """

    @property
    @abc.abstractmethod
    def subscribers(self) -> tuple[Subscriber[Any], ...]:
        """
        The set of subscribers whose outputs are synchronized.
        The ordering matches that of the output data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """Idempotent."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def _closed(self) -> bool:
        """True if the instance is finalized or if calling close() is not required for other reasons."""
        raise NotImplementedError

    @abc.abstractmethod
    def receive_in_background(self, handler: Callable[..., None]) -> None:
        """See :class:`pycyphal.presentation.Subscriber`"""
        raise NotImplementedError

    @abc.abstractmethod
    async def receive_for(self, timeout: float) -> tuple[MessageWithMetadata, ...] | None:
        """See :class:`pycyphal.presentation.Subscriber`"""
        raise NotImplementedError

    async def receive(self, monotonic_deadline: float) -> tuple[MessageWithMetadata, ...] | None:
        """See :class:`pycyphal.presentation.Subscriber`"""
        return await self.receive_for(timeout=monotonic_deadline - asyncio.get_running_loop().time())

    async def get(self, timeout: float = 0) -> tuple[Any, ...] | None:
        """Like :meth:`receive_for` but without transfer metadata, only message objects."""
        result = await self.receive_for(timeout)
        if result:
            return tuple(msg for msg, _meta in result)
        return None

    def __aiter__(self) -> Synchronizer:
        """See :class:`pycyphal.presentation.Subscriber`"""
        return self

    async def __anext__(self) -> tuple[MessageWithMetadata, ...]:
        """See :class:`pycyphal.presentation.Subscriber`"""
        try:
            while not self._closed:
                out = await self.receive_for(_RECEIVE_TIMEOUT)
                if out is not None:
                    return out
        except pycyphal.transport.ResourceClosedError:
            pass
        raise StopAsyncIteration

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes_noexcept(self, self.subscribers)
