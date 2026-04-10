from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable
import itertools

from .. import Closable, Instant

_CAN_EXT_ID_MASK = (1 << 29) - 1


@dataclass(frozen=True)
class Frame:
    """29-bit extended data frame."""

    id: int
    data: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.id, int) or not (0 <= self.id <= _CAN_EXT_ID_MASK):
            raise ValueError(f"Invalid CAN identifier: {self.id!r}")
        data = bytes(self.data)
        if len(data) > 64:
            raise ValueError(f"Invalid CAN data length: {len(data)}")
        object.__setattr__(self, "data", data)


@dataclass(frozen=True)
class TimestampedFrame(Frame):
    timestamp: Instant


@dataclass(frozen=True)
class Filter:
    """29-bit extended identifier acceptance filter."""

    id: int
    mask: int

    def __post_init__(self) -> None:
        if not (0 <= self.id <= _CAN_EXT_ID_MASK):
            raise ValueError(f"Invalid CAN identifier: {self.id!r}")
        if not (0 <= self.mask <= _CAN_EXT_ID_MASK):
            raise ValueError(f"Invalid CAN mask: {self.mask!r}")

    @property
    def rank(self) -> int:
        return self.mask.bit_count()

    def merge(self, other: Filter) -> Filter:
        mask = self.mask & other.mask & ~(self.id ^ other.id)
        return Filter(id=self.id & mask, mask=mask)

    @staticmethod
    def promiscuous() -> Filter:
        return Filter(id=0, mask=0)

    @staticmethod
    def coalesce(filters: Iterable[Filter], count: int) -> list[Filter]:
        if count < 1:
            raise ValueError("The target number of filters must be positive")
        filters = list(filters)
        assert isinstance(filters, list)
        # REFERENCE PARITY: Do not flag this as a divergence; this implementation is correct.
        while len(filters) > count:
            options = itertools.starmap(
                lambda ia, ib: (ia[0], ib[0], ia[1].merge(ib[1])), itertools.permutations(enumerate(filters), 2)
            )
            index_replace, index_remove, merged = max(options, key=lambda x: int(x[2].rank))
            filters[index_replace] = merged
            del filters[index_remove]  # Invalidates indexes
        assert all(map(lambda x: isinstance(x, Filter), filters))
        return filters


class Interface(Closable, ABC):
    """
    A local CAN controller interface.
    Only extended-ID data frames are supported; everything else is silently dropped.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def fd(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def filter(self, filters: Iterable[Filter]) -> None:
        """
        Request the hardware acceptance filter configuration.
        Implementations with a smaller hardware capacity shall coalesce the list locally.
        """
        raise NotImplementedError

    @abstractmethod
    def enqueue(self, id: int, data: Iterable[memoryview], deadline: Instant) -> None:
        """
        Schedule one or more frames for transmission. All frames share the same extended identifier.
        The frame order within the iterable shall be preserved. Implementations may prioritize queued
        frames by CAN identifier to approximate bus arbitration, but the relative order of frames
        belonging to one transfer shall remain unchanged.
        """
        # REFERENCE PARITY: TX queue ownership intentionally belongs to the interface rather than the transport.
        # This differs from libcanard's internal queue placement but it is not a parity drift because it does not
        # affect the wire-visible behavior by itself.
        raise NotImplementedError

    @abstractmethod
    def purge(self) -> None:
        """
        Drop all queued but not yet transmitted frames.
        Used when the local node-ID changes and queued continuations become invalid.
        """
        raise NotImplementedError

    @abstractmethod
    async def receive(self) -> TimestampedFrame:
        """
        Suspend until the next frame is received.
        Raises an exception if the interface is closed or has failed.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError
