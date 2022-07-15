# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# mypy: warn_unused_ignores=False

from __future__ import annotations
import bisect
import asyncio
import logging
import functools
import typing
from typing import Iterable, Any, Callable
import pycyphal.presentation.subscription_synchronizer

T = typing.TypeVar("T")
_SG = pycyphal.presentation.subscription_synchronizer.SynchronizedGroup


class MonotonicClusteringSynchronizer(pycyphal.presentation.subscription_synchronizer.Synchronizer):
    """
    Messages are clustered by the message ordering key with the specified tolerance.
    The key shall be monotonically non-decreasing except under special circumstances such as time adjustment.
    Once a full cluster is collected, it is delivered to the application, and this and all older clusters are dropped
    (where "older" means smaller key).
    Each received message is used at most once
    (it follows that the output frequency is not higher than the frequency of the slowest subject).
    If a given cluster receives multiple messages from the same subject, the latest one is used
    (this situation occurs if the subjects are updated at different rates).

    The maximum number of clusters, or depth, is limited (oldest dropped).
    This is needed to address the case when the message ordering key leaps backward
    (for example, if the sycnhronized time is adjusted),
    because some clusters may end up in the future and there needs to be a mechanism in place to remove them.
    This is also necessary to ensure that the worst-case complexity is well-bounded.

    Old cluster removal is based on a simple non-overflowing sequence counter that is assigned to each
    new cluster and then incremented; when the limit is exceeded, the cluster with the smallest seq no is dropped.
    This approach allows us to reason about temporal ordering even if the key is not monotonically non-decreasing.

    This synchronizer is well-suited for use in real-time embedded systems,
    where the clustering logic can be based on
    `Cavl <https://github.com/pavel-kirienko/cavl>`_ + `O1Heap <https://github.com/pavel-kirienko/o1heap>`_.
    The attainable worst-case time complexity is ``O(log d)``, where d is the depth limit;
    the memory requirement is ``c*s``, where s is the number of subscribers assuming unity message size.

    The behavior is illustrated on the following timeline:

    .. figure:: /figures/subject_synchronizer_monotonic_clustering.svg

        Time synchronization across multiple subjects with jitter, message loss, and publication frequency variation.
        Time is increasing left to right.
        Messages that were identified as belonging to the same synchronized group are connected.

    A usage example is provided below. First it is necessary to prepare some scaffolding:

    ..  doctest::
        :hide:

        >>> import tests
        >>> _ = tests.dsdl.compile()
        >>> tests.asyncio_allow_event_loop_access_from_top_level()
        >>> from tests import doctest_await

    >>> from uavcan.primitive.scalar import Integer64_1, Bit_1
    >>> from pycyphal.transport.loopback import LoopbackTransport
    >>> from pycyphal.presentation import Presentation
    >>> pres = Presentation(LoopbackTransport(1234))
    >>> pub_a = pres.make_publisher(Integer64_1, 2000)
    >>> pub_b = pres.make_publisher(Integer64_1, 2001)
    >>> pub_c = pres.make_publisher(Bit_1, 2002)
    >>> sub_a = pres.make_subscriber(pub_a.dtype, pub_a.port_id)
    >>> sub_b = pres.make_subscriber(pub_b.dtype, pub_b.port_id)
    >>> sub_c = pres.make_subscriber(pub_c.dtype, pub_c.port_id)

    Set up the synchronizer. It will take ownership of our subscribers.
    In this example, we are using the local reception timestamp for synchronization,
    but we could also use the timestamp field or whatever by swapping the ordering key function here:

    >>> from pycyphal.presentation.subscription_synchronizer import get_local_reception_timestamp
    >>> from pycyphal.presentation.subscription_synchronizer.monotonic_clustering import MonotonicClusteringSynchronizer
    >>> synchronizer = MonotonicClusteringSynchronizer([sub_a, sub_b, sub_c], get_local_reception_timestamp, 0.1)
    >>> synchronizer.tolerance
    0.1
    >>> synchronizer.tolerance = 0.75  # Tolerance can be changed at any moment.

    Publish some messages in an arbitrary order:

    >>> _ = doctest_await(pub_a.publish(Integer64_1(123)))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(234)))  # Replaces the previous one because newer.
    >>> _ = doctest_await(pub_b.publish(Integer64_1(321)))
    >>> _ = doctest_await(pub_c.publish(Bit_1(True)))
    >>> doctest_await(asyncio.sleep(2.0))               # Wait a little and publish another group.
    >>> _ = doctest_await(pub_c.publish(Bit_1(False)))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(654)))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(456)))
    >>> doctest_await(asyncio.sleep(1.5))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(789)))
    >>> # This group is incomplete because we did not publish on subject B, so no output will be generated.
    >>> _ = doctest_await(pub_c.publish(Bit_1(False)))
    >>> doctest_await(asyncio.sleep(1.5))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(741)))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(852)))
    >>> _ = doctest_await(pub_c.publish(Bit_1(True)))
    >>> doctest_await(asyncio.sleep(0.1))

    Now the synchronizer will automatically sort our messages into well-defined synchronized groups:

    >>> doctest_await(synchronizer.get())  # First group.
    (...Integer64.1...(value=234), ...Integer64.1...(value=321), ...Bit.1...(value=True))
    >>> doctest_await(synchronizer.get())  # Second group.
    (...Integer64.1...(value=456), ...Integer64.1...(value=654), ...Bit.1...(value=False))
    >>> doctest_await(synchronizer.get())  # Fourth group -- the third one was incomplete so dropped.
    (...Integer64.1...(value=741), ...Integer64.1...(value=852), ...Bit.1...(value=True))
    >>> doctest_await(synchronizer.get()) is None  # No more groups.
    True

    Closing the synchronizer will also close all subscribers we passed to it
    (if necessary you can create additional subscribers for the same subjects):

    >>> synchronizer.close()

    ..  doctest::
        :hide:

        >>> pres.close()
        >>> doctest_await(asyncio.sleep(1.0))
    """

    KeyFunction = Callable[[pycyphal.presentation.subscription_synchronizer.MessageWithMetadata], float]

    DEFAULT_DEPTH = 15

    def __init__(
        self,
        subscribers: Iterable[pycyphal.presentation.Subscriber[Any]],
        f_key: KeyFunction,
        tolerance: float,
        *,
        depth: int = DEFAULT_DEPTH,
    ) -> None:
        """
        :param subscribers:
            The set of subscribers to synchronize data from.
            The constructed instance takes ownership of the subscribers -- they will be closed on :meth:`close`.

        :param f_key:
            Message ordering key function;
            e.g., :func:`pycyphal.presentation.subscription_synchronizer.get_local_reception_timestamp`.
            Any monotonic non-decreasing function of the received message with its metadata is acceptable,
            and it doesn't necessarily have to be time-related.

        :param tolerance:
            Messages whose absolute key difference does not exceed this limit will be clustered together.
            This value can be changed dynamically, which can be leveraged for automatic tolerance configuration
            as some function of the output frequency.

        :param depth:
            At most this many newest clusters will be maintained at any moment.
            This limits the time and memory requirements.
            If the depth is too small, some valid clusters may be dropped prematurely.
        """
        super().__init__(subscribers)
        self._tolerance = float(tolerance)
        self._f_key = f_key
        self._matcher: _Matcher[pycyphal.presentation.subscription_synchronizer.MessageWithMetadata] = _Matcher(
            subject_count=len(self.subscribers),
            depth=int(depth),
        )
        self._destination: asyncio.Queue[_SG] | Callable[..., None] = asyncio.Queue()

        def mk_handler(idx: int) -> Any:
            return lambda msg, meta: self._cb(idx, (msg, meta))

        for index, sub in enumerate(self.subscribers):
            sub.receive_in_background(mk_handler(index))

    @property
    def tolerance(self) -> float:
        """
        The current tolerance value.

        Auto-tuning with feedback can be implemented on top of this synchronizer
        such that when a new synchronized group is delivered,
        the key delta from the previous group is computed and the tolerance is updated as some function of that.
        If the tolerance is low, more synchronized groups will be skipped (delta increased);
        therefore, at the next successful synchronized group reassembly the tolerance will be increased.
        With this method, if the initial tolerance is large,
        the synchronizer may initially output poorly grouped messages,
        but it will converge to a more sensible tolerance setting in a few iterations.
        """
        return self._tolerance

    @tolerance.setter
    def tolerance(self, value: float) -> None:
        self._tolerance = float(value)

    def _cb(self, index: int, mm: pycyphal.presentation.subscription_synchronizer.MessageWithMetadata) -> None:
        key = self._f_key(mm)
        res = self._matcher.update(key, self._tolerance, index, mm)
        if res is not None:
            # The following may throw, we don't bother catching because the caller will do it for us if needed.
            self._output(res)

    def _output(self, res: _SG) -> None:
        _logger.debug("OUTPUT [tolerance=%r]: %r", self._tolerance, res)
        if isinstance(self._destination, asyncio.Queue):
            self._destination.put_nowait(res)
        else:
            self._destination(*res)

    async def receive_for(self, timeout: float) -> _SG | None:
        if isinstance(self._destination, asyncio.Queue):
            try:
                if timeout > 1e-6:
                    return await asyncio.wait_for(self._destination.get(), timeout)
                return self._destination.get_nowait()
            except asyncio.QueueEmpty:
                return None
            except asyncio.TimeoutError:
                return None
        assert callable(self._destination)
        return None

    def receive_in_background(self, handler: Callable[..., None]) -> None:
        self._destination = handler


@functools.total_ordering
class _Cluster(typing.Generic[T]):
    def __init__(self, *, key: float, size: int, seq_no: int) -> None:
        self._key = float(key)
        self._collection: list[T | None] = [None] * int(size)
        self._seq_no = int(seq_no)

    @property
    def seq_no(self) -> int:
        return self._seq_no

    def put(self, index: int, item: T) -> tuple[T, ...] | None:
        self._collection[index] = item
        if all(x is not None for x in self._collection):
            return tuple(self._collection)  # type:ignore
        return None

    def delta(self, key: float) -> float:
        return abs(self._key - key)

    def __float__(self) -> float:
        return float(self._key)

    def __le__(self, other: Any) -> bool:
        return self._key < float(other)

    def __eq__(self, other: Any) -> bool:
        return False

    def __repr__(self) -> str:
        return f"({self._key:021.9f}:{''.join(('+-'[x is None]) for x in self._collection)})"


class _Matcher(typing.Generic[T]):
    """
    An embedded implementation can be based on Cavl.
    """

    def __init__(self, *, subject_count: int, depth: int) -> None:
        self._subject_count = int(subject_count)
        if self._subject_count < 0:
            raise ValueError("The subject count shall be non-negative")
        self._clusters: list[_Cluster[T]] = []
        self._depth = int(depth)
        self._seq_counter = 0

    def update(self, key: float, tolerance: float, index: int, item: T) -> tuple[T, ...] | None:
        clust: _Cluster[T] | None = None
        # noinspection PyTypeChecker
        ni = bisect.bisect_left(self._clusters, key)  # type: ignore
        assert 0 <= ni <= len(self._clusters)
        neigh: list[tuple[float, int]] = []
        if 0 < ni:
            neigh.append((self._clusters[ni - 1].delta(key), ni - 1))
        if ni < len(self._clusters):
            neigh.append((self._clusters[ni].delta(key), ni))
        if ni < (len(self._clusters) - 1):
            neigh.append((self._clusters[ni + 1].delta(key), ni + 1))
        if neigh:
            dist, ni = min(neigh)
            if dist <= tolerance:
                clust = self._clusters[ni]
                _logger.debug("Choosing %r for key=%r delta=%r; candidates: %r", clust, key, dist, neigh)
        if clust is None:
            clust = self._new_cluster(key)
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
    def counter(self) -> int:
        return self._seq_counter

    @property
    def clusters(self) -> list[_Cluster[T]]:
        """Debugging/testing aid."""
        return list(self._clusters)

    def _drop_older(self, key: float) -> None:
        self._clusters = [it for it in self._clusters if float(it) > key]

    def _new_cluster(self, key: float) -> _Cluster[T]:
        # Trim the set to ensure we will not exceed the limit.
        # This implementation can be improved but it doesn't matter much because the depth is small.
        if len(self._clusters) >= self._depth:
            idx, _ = min(enumerate(self._clusters), key=lambda idx_cl: idx_cl[1].seq_no)
            del self._clusters[idx]
        # Create and insert the new one.
        clust: _Cluster[T] = _Cluster(key=key, size=self._subject_count, seq_no=self._seq_counter)
        self._seq_counter += 1
        bisect.insort(self._clusters, clust)
        assert 0 < len(self._clusters) <= self._depth
        return clust

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self._clusters, seq=self._seq_counter)


_logger = logging.getLogger(__name__)


# noinspection PyTypeChecker
def _unittest_cluster() -> None:
    from pytest import approx

    cl: _Cluster[int] = _Cluster(key=5.0, size=3, seq_no=543210)
    assert cl.seq_no == 543210

    assert cl < _Cluster(key=5.1, size=0, seq_no=0)
    assert cl > _Cluster(key=4.9, size=0, seq_no=0)
    assert cl < 5.1
    assert cl > 4.9
    assert cl.delta(5.1) == approx(0.1)
    assert cl.delta(4.8) == approx(0.2)
    print(cl)
    assert not cl.put(1, 11)
    print(cl)
    assert not cl.put(0, 10)
    print(cl)
    assert (10, 11, 12) == cl.put(2, 12)
    print(cl)


def _unittest_matcher() -> None:
    mat: _Matcher[int] = _Matcher(subject_count=3, depth=3)
    assert len(mat.clusters) == 0

    assert not mat.update(1.0, 0.5, 1, 51)
    assert len(mat.clusters) == 1

    assert not mat.update(5.0, 0.5, 1, 51)
    assert len(mat.clusters) == 2

    assert not mat.update(4.8, 0.5, 0, 50)
    assert len(mat.clusters) == 2

    assert not mat.update(6.0, 0.5, 1, 61)
    assert len(mat.clusters) == 3

    assert not mat.update(6.4, 0.5, 2, 62)
    assert len(mat.clusters) == 3

    print(0, mat)
    assert not mat.update(4.0, 0.5, 0, 40)
    assert len(mat.clusters) == 3  # Depth limit exceeded, first one dropped.
    print(1, mat)

    assert not mat.update(4.0, 0.5, 1, 41)
    assert len(mat.clusters) == 3
    print(2, mat)

    assert len(mat.clusters) == 3
    assert (50, 51, 52) == mat.update(5.4, 0.5, 2, 52)
    assert len(mat.clusters) == 1
    print(3, mat)

    assert len(mat.clusters) == 1
    assert (60, 61, 62) == mat.update(9.1, 10.0, 0, 60)
    assert len(mat.clusters) == 0
    print(4, mat)
