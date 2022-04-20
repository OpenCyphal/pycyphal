# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import asyncio
import logging
import typing
from typing import Iterable, Any, Callable
import pycyphal.presentation.subscription_synchronizer

T = typing.TypeVar("T")
K = typing.TypeVar("K")
_SG = pycyphal.presentation.subscription_synchronizer.SynchronizedGroup


class TransferIDSynchronizer(pycyphal.presentation.subscription_synchronizer.Synchronizer):
    """
    Messages that share the same (source node-ID, transfer-ID) are assumed synchronous
    (i.e., all messages in a synchronized group always originate from the same node).
    Each received message is used at most once
    (it follows that the output frequency is not higher than the frequency of the slowest subject).
    Anonymous messages are dropped unconditionally (because the source node-ID is not defined for them).

    The Cyphal Specification does not recommend this mode of synchronization but it is provided for completeness.
    If not sure, use other synchronizers instead.

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

    Set up the synchronizer. It will take ownership of our subscribers:

    >>> from pycyphal.presentation.subscription_synchronizer.transfer_id import TransferIDSynchronizer
    >>> synchronizer = TransferIDSynchronizer([sub_a, sub_b, sub_c])

    Publish some messages in an arbitrary order:

    >>> _ = doctest_await(pub_a.publish(Integer64_1(123)))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(321)))
    >>> _ = doctest_await(pub_c.publish(Bit_1(True)))
    >>> doctest_await(asyncio.sleep(1.0))               # Wait a little and publish another group.
    >>> _ = doctest_await(pub_c.publish(Bit_1(False)))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(654)))
    >>> _ = doctest_await(pub_a.publish(Integer64_1(456)))
    >>> doctest_await(asyncio.sleep(1.0))
    >>> _ = doctest_await(pub_b.publish(Integer64_1(654)))  # This group is incomplete, no output produced.
    >>> doctest_await(asyncio.sleep(1.0))

    Now the synchronizer will automatically sort our messages into well-defined synchronized groups:

    >>> doctest_await(synchronizer.get())  # First group.
    (...Integer64.1...(value=123), ...Integer64.1...(value=321), ...Bit.1...(value=True))
    >>> doctest_await(synchronizer.get())  # Second group.
    (...Integer64.1...(value=456), ...Integer64.1...(value=654), ...Bit.1...(value=False))
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

    DEFAULT_SPAN = 30  # The default should be below 32 for compatibility with Cyphal/CAN.

    def __init__(
        self,
        subscribers: Iterable[pycyphal.presentation.Subscriber[Any]],
        span: int = DEFAULT_SPAN,
    ) -> None:
        """
        :param subscribers:
            The set of subscribers to synchronize data from.
            The constructed instance takes ownership of the subscribers -- they will be closed on :meth:`close`.

        :param span:
            Old clusters will be removed to ensure that the sequence number delta between the oldest and the newest
            does not exceed this limit.
            This protects against mismatch if cyclic transfer-ID is used and limits the time and memory requirements.
        """
        super().__init__(subscribers)
        self._matcher: _Matcher[
            tuple[int, int],
            pycyphal.presentation.subscription_synchronizer.MessageWithMetadata,
        ] = _Matcher(
            subject_count=len(self.subscribers),
            span=int(span),
        )
        self._destination: asyncio.Queue[_SG] | Callable[..., None] = asyncio.Queue()

        def mk_handler(idx: int) -> Any:
            return lambda msg, meta: self._cb(idx, (msg, meta))

        for index, sub in enumerate(self.subscribers):
            sub.receive_in_background(mk_handler(index))

    def _cb(self, index: int, mm: pycyphal.presentation.subscription_synchronizer.MessageWithMetadata) -> None:
        # Use both node-ID and transfer-ID https://github.com/OpenCyphal/pycyphal/pull/220#discussion_r853500453
        src_nid = mm[1].source_node_id
        tr_id = mm[1].transfer_id
        if src_nid is not None:
            res = self._matcher.update((src_nid, tr_id), index, mm)
            if res is not None:
                # The following may throw, we don't bother catching because the caller will do it for us if needed.
                self._output(res)

    def _output(self, res: _SG) -> None:
        _logger.debug("OUTPUT: %r", res)
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


class _Cluster(typing.Generic[T]):
    def __init__(self, size: int, seq_no: int) -> None:
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

    def __repr__(self) -> str:
        return f"({self._seq_no:09}:{''.join(('+-'[x is None]) for x in self._collection)})"


class _Matcher(typing.Generic[K, T]):
    def __init__(self, *, subject_count: int, span: int) -> None:
        self._subject_count = int(subject_count)
        if self._subject_count < 0:
            raise ValueError("The subject count shall be non-negative")
        self._clusters: dict[K, _Cluster[T]] = {}
        self._span = int(span)
        self._seq_counter = 0

    def update(self, key: K, index: int, item: T) -> tuple[T, ...] | None:
        try:
            clust = self._clusters[key]
        except LookupError:
            # This is a silly implementation but works as an exploratory PoC. May improve later.
            self._clusters = {k: v for k, v in self._clusters.items() if (self._seq_counter - v.seq_no) < self._span}
            clust = _Cluster(size=self._subject_count, seq_no=self._seq_counter)
            self._clusters[key] = clust
            self._seq_counter += 1
            assert 0 < len(self._clusters) <= self._span
        res = clust.put(index, item)
        _logger.debug("Updated cluster %r at index %r with %r", clust, index, item)
        if res is not None:
            del self._clusters[key]
        return res

    @property
    def clusters(self) -> dict[K, _Cluster[T]]:
        return self._clusters

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self._clusters, seq=self._seq_counter)


_logger = logging.getLogger(__name__)


def _unittest_cluster() -> None:
    cl: _Cluster[int] = _Cluster(size=3, seq_no=543210)
    assert cl.seq_no == 543210
    print(cl)
    assert not cl.put(1, 11)
    print(cl)
    assert not cl.put(0, 10)
    print(cl)
    assert (10, 11, 12) == cl.put(2, 12)
    print(cl)


def _unittest_matcher() -> None:
    mat: _Matcher[int, int] = _Matcher(subject_count=3, span=3)
    assert len(mat.clusters) == 0

    assert not mat.update(0, 1, 51)
    assert len(mat.clusters) == 1

    assert not mat.update(1, 1, 51)
    assert len(mat.clusters) == 2

    assert not mat.update(1, 0, 50)
    assert len(mat.clusters) == 2

    assert not mat.update(2, 1, 61)
    assert len(mat.clusters) == 3

    assert not mat.update(2, 2, 62)
    assert len(mat.clusters) == 3

    print(0, mat)
    assert not mat.update(3, 0, 40)
    assert len(mat.clusters) == 3  # Span limit exceeded, first one dropped.
    print(1, mat)

    assert not mat.update(3, 1, 41)
    assert len(mat.clusters) == 3
    print(2, mat)

    assert len(mat.clusters) == 3
    assert (50, 51, 52) == mat.update(1, 2, 52)
    assert len(mat.clusters) == 2
    print(3, mat)

    assert len(mat.clusters) == 2
    assert (60, 61, 62) == mat.update(2, 0, 60)
    assert len(mat.clusters) == 1
    print(4, mat)
