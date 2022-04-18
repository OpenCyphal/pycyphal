# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import bisect
import logging
import functools
from typing import Iterable, Any, Generic, TypeVar
import pycyphal.presentation.subscription_synchronizer


T = TypeVar("T")


class StrictSynchronizer:
    """
    Messages are clustered by the message ordering key with the specified tolerance.
    Once a full cluster is collected, it is delivered to the application, and this and all older clusters are dropped.
    Each received message is used at most once
    (it follows that the output frequency is not higher than the frequency of the slowest subject).
    If a given cluster receives multiple messages from the same subject, the latest one is used.
    The clustering tolerance may be auto-tuned heuristically (this is the default).

    This synchronizer is well-suited for implementation on a real-time embedded system,
    where the role of the clustering container is performed by the
    `Cavl library <https://github.com/pavel-kirienko/cavl>`_.
    The attainable worst-case time complexity is ``O(log c)``,
    where c is the maximum number of concurrently maintained message clusters;
    the memory requirement is ``c*s``, where s is the number of subscribers assuming unity message size.
    """

    def __init__(
        self,
        subscribers: Iterable[pycyphal.presentation.Subscriber[Any]],
        f_key: pycyphal.presentation.subscription_synchronizer.MessageOrderingKeyFunction,
        tolerance: float | tuple[float, float] = (0, float("inf")),
    ) -> None:
        """
        :param subscribers: The set of subscribers to synchronize data from.

        :param f_key: Message ordering key function.

        :param tolerance:
            If scalar, specifies the fixed synchronization tolerance.
            If two-element tuple, specifies the min and max bounds for the auto-tuned tolerance.
            Unconstrained auto-tune is the default.
        """
        self._subscribers = list(subscribers)
        self._f_key = f_key
        if isinstance(tolerance, tuple):
            self._tolerance_bound = float(tolerance[0]), float(tolerance[1])
            self._tolerance = self._tolerance_bound[1]
        else:
            self._tolerance = float(tolerance)
            self._tolerance_bound = self._tolerance, self._tolerance
        if not (self._tolerance_bound[0] <= self._tolerance_bound[1]):
            raise ValueError(f"Invalid tolerance bound: {self._tolerance_bound}")
        self._matcher: _Matcher[pycyphal.presentation.subscription_synchronizer.MessageWithMetadata] = _Matcher(
            len(self._subscribers)
        )


@functools.total_ordering
class _Cluster(Generic[T]):
    def __init__(self, key: float, size: int) -> None:
        self._key = float(key)
        self._collection: list[T | None] = [None] * int(size)

    def put(self, index: int, item: T) -> tuple[T, ...] | None:
        self._collection[index] = item
        if all(x is not None for x in self._collection):
            return tuple(self._collection)  # type:ignore
        return None

    def distance(self, key: float) -> float:
        return abs(self._key - key)

    def __float__(self) -> float:
        return float(self._key)

    def __le__(self, other: Any) -> bool:
        return self._key < float(other)

    def __eq__(self, other: Any) -> bool:
        return False

    def __repr__(self) -> str:
        return f"({self._key:021.9f}:{''.join(('+-'[x is None]) for x in self._collection)})"


class _Matcher(Generic[T]):
    """
    An embedded implementation should use Cavl instead of this.
    """

    def __init__(self, subject_count: int) -> None:
        self._subject_count = int(subject_count)
        if not self._subject_count >= 0:
            raise ValueError("The subject set shall be non-negative")
        self._clusters: list[_Cluster[T]] = []

    def update(self, key: float, tolerance: float, index: int, item: T) -> tuple[T, ...] | None:
        clust: _Cluster | None = None
        # noinspection PyTypeChecker
        ni = bisect.bisect_left(self._clusters, key)
        assert 0 <= ni <= len(self._clusters)
        neigh: list[tuple[float, int]] = []
        if 0 < ni:
            neigh.append((self._clusters[ni - 1].distance(key), ni - 1))
        if ni < len(self._clusters):
            neigh.append((self._clusters[ni].distance(key), ni))
        if ni < (len(self._clusters) - 1):
            neigh.append((self._clusters[ni + 1].distance(key), ni + 1))
        if neigh:
            dist, ni = min(neigh)
            if dist <= tolerance:
                clust = self._clusters[ni]
                _logger.debug("Choosing %r for key=%r distance=%r; candidates: %r", clust, key, dist, neigh)
        if clust is None:
            clust = _Cluster(key, self._subject_count)
            bisect.insort(self._clusters, clust)
            _logger.debug("New cluster %r", clust)
        assert clust is not None
        res = clust.put(index, item)
        _logger.debug("Updated cluster %r at index %r with %r", clust, index, item)
        if res is not None:
            size_before = len(self._clusters)
            self._drop_older(float(clust))
            _logger.debug("Dropped %r clusters; remaining: %r", size_before - len(self._clusters), self._clusters)
        return res

    @property
    def clusters(self) -> list[_Cluster[T]]:
        """Debugging/testing aid."""
        return list(self._clusters)

    def _drop_older(self, key: float) -> None:
        self._clusters = [it for it in self._clusters if float(it) > key]

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self._clusters)


_logger = logging.getLogger(__name__)


# noinspection PyTypeChecker
def _unittest_cluster() -> None:
    from pytest import approx

    cl: _Cluster[int] = _Cluster(5.0, 3)
    assert cl < _Cluster(5.1, 0)
    assert cl > _Cluster(4.9, 0)
    assert cl < 5.1
    assert cl > 4.9
    assert cl.distance(5.1) == approx(0.1)
    assert cl.distance(4.8) == approx(0.2)
    print(cl)
    assert not cl.put(1, 11)
    print(cl)
    assert not cl.put(0, 10)
    print(cl)
    assert (10, 11, 12) == cl.put(2, 12)
    print(cl)


def _unittest_matcher() -> None:
    mat: _Matcher[int] = _Matcher(3)
    assert len(mat.clusters) == 0

    assert not mat.update(5.0, 0.5, 1, 51)
    assert len(mat.clusters) == 1

    assert not mat.update(4.8, 0.5, 0, 50)
    assert len(mat.clusters) == 1

    assert not mat.update(6.0, 0.5, 1, 61)
    assert len(mat.clusters) == 2

    assert not mat.update(6.4, 0.5, 2, 62)
    assert len(mat.clusters) == 2

    assert not mat.update(4.0, 0.5, 0, 40)
    assert len(mat.clusters) == 3

    assert not mat.update(4.0, 0.5, 1, 41)
    assert len(mat.clusters) == 3

    print(mat)

    assert len(mat.clusters) == 3
    assert (50, 51, 52) == mat.update(5.4, 0.5, 2, 52)
    assert len(mat.clusters) == 1
    print(mat)

    assert len(mat.clusters) == 1
    assert (60, 61, 62) == mat.update(9.1, 10.0, 0, 60)
    assert len(mat.clusters) == 0
    print(mat)
