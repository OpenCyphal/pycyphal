# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

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


class StrictSynchronizer(pycyphal.presentation.subscription_synchronizer.Synchronizer):
    """
    Messages are clustered by the *message ordering key* with the specified tolerance.
    Once a full cluster is collected, it is delivered to the application, and this and all older clusters are dropped.
    Each received message is used at most once
    (it follows that the output frequency is not higher than the frequency of the slowest subject).
    If a given cluster receives multiple messages from the same subject, the latest one is used.
    The clustering tolerance may be auto-tuned heuristically (this is the default).

    This synchronizer is well-suited for implementation on a real-time embedded system,
    where the clustering matcher is based on the `Cavl <https://github.com/pavel-kirienko/cavl>`_.
    The attainable worst-case time complexity is ``O(log c)``,
    where c is the maximum number of concurrently maintained message clusters;
    the memory requirement is ``c*s``, where s is the number of subscribers assuming unity message size.

    ..  doctest::
        :hide:

        >>> import tests
        >>> _ = tests.dsdl.compile()
        >>> tests.asyncio_allow_event_loop_access_from_top_level()
        >>> from tests import doctest_await

    Prepare some scaffolding for the demo:

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
    but we could also use the timestamp field by swapping the ordering key function here:

    >>> from pycyphal.presentation.subscription_synchronizer import get_local_reception_timestamp
    >>> from pycyphal.presentation.subscription_synchronizer.strict import StrictSynchronizer
    >>> synchronizer = StrictSynchronizer([sub_a, sub_b, sub_c], get_local_reception_timestamp)

    Publish some messages in an arbitrary order and observe them to be synchronized:

    >>> _ = doctest_await(pub_a.publish(Integer64_1(123)))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(234)))  # Replaces the previous one because newer.
    >>> _ = doctest_await(pub_b.publish(Integer64_1(321)))
    >>> _ = doctest_await(pub_c.publish(Bit_1(True)))
    >>> doctest_await(asyncio.sleep(2.0))               # Wait a little and publish another group.
    >>> _ = doctest_await(pub_a.publish(Integer64_1(456)))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(654)))
    >>> _ = doctest_await(pub_c.publish(Bit_1(False)))
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
    """

    DEFAULT_TOLERANCE_BOUND = 1e-6, 1.0
    """
    Tolerance will be auto-tuned within this range unless overridden during construction.
    """

    def __init__(
        self,
        subscribers: Iterable[pycyphal.presentation.Subscriber[Any]],
        f_key: pycyphal.presentation.subscription_synchronizer.MessageOrderingKeyFunction,
        tolerance: float | tuple[float, float] = DEFAULT_TOLERANCE_BOUND,
    ) -> None:
        """
        :param subscribers:
            The set of subscribers to synchronize data from.
            The constructed instance takes ownership of the subscribers -- they will be closed on :meth:`close`.

        :param f_key:
            Message ordering key function;
            e.g., :func:`pycyphal.presentation.subscription_synchronizer.get_local_reception_timestamp`.

        :param tolerance:
            If scalar, specifies the fixed synchronization tolerance.
            If two-element tuple, specifies the min and max bounds for the auto-tuned tolerance.
            A sensible default is provided that will suit most use cases.
        """
        super().__init__(subscribers)
        self._f_key = f_key
        if isinstance(tolerance, tuple):
            self._tolerance_bound = float(tolerance[0]), float(tolerance[1])
            self._tolerance = max(self._tolerance_bound)
        else:
            self._tolerance = float(tolerance)
            self._tolerance_bound = self._tolerance, self._tolerance
        if not (self._tolerance_bound[0] <= self._tolerance_bound[1]):
            raise ValueError(f"Invalid tolerance bound: {self._tolerance_bound}")
        self._matcher: _Matcher[pycyphal.presentation.subscription_synchronizer.MessageWithMetadata] = _Matcher(
            len(self.subscribers)
        )
        self._destination: asyncio.Queue[_SG] | Callable[..., None] = asyncio.Queue()
        self._last_output_key: float | None = None

        def mk_handler(idx: int) -> Any:
            return lambda msg, meta: self._cb(idx, (msg, meta))

        for index, sub in enumerate(self.subscribers):
            sub.receive_in_background(mk_handler(index))

    @property
    def tolerance(self) -> float:
        """
        The current tolerance value. It may change at runtime if auto-tuning is enabled.

        A feedback loop is formed such that when a new synchronized group is assembled,
        the delta from the previous group is computed and the tolerance is updated as some fraction of that
        (low-pass filtered).
        If the tolerance is low, more synchronized groups will be skipped (delta increased);
        therefore, at the next successful synchronized group reassembly the tolerance will be increased.

        If the initial tolerance is large, the synchronizer may initially output poorly grouped messages,
        but it will quickly converge to a more sensible tolerance in a few iterations.
        """
        return self._tolerance

    def _cb(self, index: int, mm: pycyphal.presentation.subscription_synchronizer.MessageWithMetadata) -> None:
        key = self._f_key(mm)
        res = self._matcher.update(key, self._tolerance, index, mm)
        if res is not None:
            if self._last_output_key is not None:
                new_tol = (key - self._last_output_key) * 0.5  # Tolerance is half the period.
                self._tolerance = _clamp(self._tolerance_bound, (new_tol + self._tolerance) * 0.5)
            self._last_output_key = key
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
                else:
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
    def __init__(self, key: float, size: int) -> None:
        self._key = float(key)
        self._collection: list[T | None] = [None] * int(size)

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
    An embedded implementation should use Cavl instead of this.
    """

    def __init__(self, subject_count: int) -> None:
        self._subject_count = int(subject_count)
        if not self._subject_count >= 0:
            raise ValueError("The subject set shall be non-negative")
        self._clusters: list[_Cluster[T]] = []

    def update(self, key: float, tolerance: float, index: int, item: T) -> tuple[T, ...] | None:
        clust: _Cluster[T] | None = None
        # noinspection PyTypeChecker
        ni = bisect.bisect_left(self._clusters, key)
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


def _clamp(lo_hi: tuple[T, T], val: T) -> T:
    lo, hi = lo_hi
    return min(max(lo, val), hi)  # type: ignore


_logger = logging.getLogger(__name__)


# noinspection PyTypeChecker
def _unittest_cluster() -> None:
    from pytest import approx

    cl: _Cluster[int] = _Cluster(5.0, 3)
    assert cl < _Cluster(5.1, 0)
    assert cl > _Cluster(4.9, 0)
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
