"""Tests for ordered subscription (reordering) in pycyphal.

Exercises _ReorderingState and the Subscriber reordering path that is activated
when ``reordering_window`` is passed to ``Node.subscribe()``.

Implementation detail:  when the *first* message arrives for a given
(remote_id, topic_hash) pair the reordering state is initialized with::

    tag_baseline = tag - _REORDERING_CAPACITY // 2     (i.e. tag - 8)
    last_ejected_lin_tag = 0

So the linearized tag of that first message is ``_REORDERING_CAPACITY // 2``
(8), and ``last_ejected_lin_tag + 1`` is 1.  The fast-path ``lin_tag == 1``
does *not* match, so the first message is *interned* rather than delivered
immediately.  Subsequent sequential messages are likewise interned until either
the capacity limit is reached or the reordering window timer fires.

To get deterministic, immediate delivery in unit tests we prime the state by
directly setting ``last_ejected_lin_tag`` so that the next expected lin_tag
matches the messages we deliver.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

from pycyphal import Arrival, Breadcrumb, Instant, Node, Priority, Subscriber, Topic
from pycyphal._node import _REORDERING_CAPACITY, _ReorderingSlot, _ReorderingState
from pycyphal._common import name_resolve
from pycyphal._wire import topic_hash as compute_topic_hash

from tests.conftest import MockNetwork, MockTransport

# =====================================================================================================================
# Helpers
# =====================================================================================================================


_HALF_CAP = _REORDERING_CAPACITY // 2  # 8 by default


def _make_node(transport: MockTransport) -> Node:
    """Create a Node with a deterministic home name so topic hashes are predictable."""
    return Node(transport, home="test-node")


def _resolved_hash(node: Node, topic_name: str) -> int:
    """Compute the topic hash exactly the way the Node does after name resolution."""
    resolved = name_resolve(topic_name, node.namespace, node.home)
    return compute_topic_hash(resolved)


def _make_breadcrumb(node: Node, topic_name: str, tag: int, remote_id: int = 42) -> Breadcrumb:
    """Build a Breadcrumb for a known topic on *node*."""
    h = _resolved_hash(node, topic_name)
    return Breadcrumb(node, remote_id, h, tag, Priority.NOMINAL)


def _make_arrival(
    node: Node,
    topic_name: str,
    tag: int,
    payload: bytes = b"",
    remote_id: int = 42,
) -> Arrival:
    """Build an Arrival that carries the correct topic-hash for *topic_name*."""
    bc = _make_breadcrumb(node, topic_name, tag, remote_id)
    return Arrival(timestamp=Instant.now(), breadcrumb=bc, message=payload)


def _drain_queue(sub: Subscriber) -> list[Arrival]:
    """Non-blocking drain of every Arrival currently sitting in the subscriber queue."""
    items: list[Arrival] = []
    while True:
        try:
            item = sub._queue.get_nowait()
            if isinstance(item, BaseException):
                break
            items.append(item)
        except asyncio.QueueEmpty:
            break
    return items


def _drain_tags(sub: Subscriber) -> list[int]:
    """Convenience: drain and return just the tags."""
    return [a.breadcrumb.tag for a in _drain_queue(sub)]


def _prime_reordering(
    sub: Subscriber,
    node: Node,
    topic_name: str,
    start_tag: int,
    remote_id: int = 42,
) -> _ReorderingState:
    """Deliver a single primer message and then patch the reordering state so that
    ``last_ejected_lin_tag == lin_tag_of_primer``.  After this call, delivering
    ``start_tag + 1`` will hit the fast path and be emitted immediately.

    Returns the _ReorderingState for further inspection.
    """
    arr = _make_arrival(node, topic_name, start_tag, payload=b"", remote_id=remote_id)
    sub._deliver(arr)
    # The primer was interned -- drain silently
    _drain_queue(sub)

    h = _resolved_hash(sub._node, topic_name)
    key = (remote_id, h)
    rs = sub._reordering[key]
    # Advance frontier so start_tag is "already ejected"
    primer_lin = (start_tag - rs.tag_baseline) % (1 << 64)
    rs.last_ejected_lin_tag = primer_lin
    # Clear the interned primer
    rs.interned.pop(primer_lin, None)
    if rs._timeout_handle is not None:
        rs._timeout_handle.cancel()
        rs._timeout_handle = None
    return rs


# =====================================================================================================================
# 1. _ReorderingState dataclass basics
# =====================================================================================================================


class TestReorderingStateDataclass:
    """Verify default construction and field semantics of _ReorderingState."""

    def test_default_fields(self) -> None:
        rs = _ReorderingState(remote_id=1, topic_hash=0xDEAD)
        assert rs.remote_id == 1
        assert rs.topic_hash == 0xDEAD
        assert rs.tag_baseline == 0
        assert rs.last_ejected_lin_tag == 0
        assert rs.last_active == 0.0
        assert rs.interned == {}
        assert rs._timeout_handle is None

    def test_interned_dict_independence(self) -> None:
        """Each instance must have its own interned dict."""
        a = _ReorderingState(remote_id=1, topic_hash=1)
        b = _ReorderingState(remote_id=2, topic_hash=2)
        a.interned[10] = _ReorderingSlot(
            lin_tag=10, priority=Priority.NOMINAL, timestamp=Instant.now(), message=b"a", remote_id=1
        )
        assert len(b.interned) == 0

    def test_capacity_constant(self) -> None:
        assert _REORDERING_CAPACITY == 16


# =====================================================================================================================
# 2. In-order delivery -- sequential tags pass straight through (after priming)
# =====================================================================================================================


class TestInOrderDelivery:
    """Messages with sequential tags delivered immediately after the frontier is established."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/inorder", reordering_window=0.5)
        return node, sub

    def test_sequential_tags_delivered_in_order(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base_tag = 1000
        _prime_reordering(sub, node, "/test/inorder", base_tag)

        for i in range(1, 11):
            arr = _make_arrival(node, "/test/inorder", base_tag + i, payload=f"msg{i}".encode())
            sub._deliver(arr)
        delivered = _drain_queue(sub)
        assert len(delivered) == 10
        for i, arrival in enumerate(delivered):
            assert arrival.message == f"msg{i+1}".encode()

    def test_single_message_after_primer(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        _prime_reordering(sub, node, "/test/inorder", 500)
        sub._deliver(_make_arrival(node, "/test/inorder", 501, payload=b"only"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 1
        assert delivered[0].message == b"only"

    def test_sequential_tags_from_two_remotes(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Each remote has its own reordering state -- both streams deliver independently."""
        node, sub = node_and_sub
        base = 100
        _prime_reordering(sub, node, "/test/inorder", base, remote_id=10)
        _prime_reordering(sub, node, "/test/inorder", base, remote_id=20)

        for i in range(1, 6):
            sub._deliver(_make_arrival(node, "/test/inorder", base + i, remote_id=10))
            sub._deliver(_make_arrival(node, "/test/inorder", base + i, remote_id=20))
        delivered = _drain_queue(sub)
        assert len(delivered) == 10

    def test_no_reordering_state_leak(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """After sequential delivery the interned buffer should be empty."""
        node, sub = node_and_sub
        _prime_reordering(sub, node, "/test/inorder", 200)
        for i in range(1, 6):
            sub._deliver(_make_arrival(node, "/test/inorder", 200 + i))
        _drain_queue(sub)
        for rs in sub._reordering.values():
            assert len(rs.interned) == 0


# =====================================================================================================================
# 3. Out-of-order: two messages swapped
# =====================================================================================================================


class TestOutOfOrderPair:
    """tag N+2 arrives before tag N+1 -- after both arrive the output order must be N+1, N+2."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/ooo", reordering_window=1.0)
        return node, sub

    def test_swap_two_sequential(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 100
        _prime_reordering(sub, node, "/test/ooo", base)

        # Now send tag base+2 before tag base+1 (gap of 1)
        sub._deliver(_make_arrival(node, "/test/ooo", base + 2, payload=b"second"))
        assert _drain_queue(sub) == []  # buffered, waiting for base+1

        sub._deliver(_make_arrival(node, "/test/ooo", base + 1, payload=b"first"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert delivered[0].message == b"first"  # tag base+1
        assert delivered[1].message == b"second"  # tag base+2

    def test_reverse_three(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Tags arriving in reverse: +3, +2, +1.  All delivered when +1 arrives."""
        node, sub = node_and_sub
        base = 500
        _prime_reordering(sub, node, "/test/ooo", base)

        sub._deliver(_make_arrival(node, "/test/ooo", base + 3, payload=b"c"))
        sub._deliver(_make_arrival(node, "/test/ooo", base + 2, payload=b"b"))
        assert _drain_queue(sub) == []

        sub._deliver(_make_arrival(node, "/test/ooo", base + 1, payload=b"a"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 3
        assert [a.message for a in delivered] == [b"a", b"b", b"c"]


# =====================================================================================================================
# 4. Gap filling
# =====================================================================================================================


class TestGapFilling:
    """tag +1 and +3 arrive -- when +2 arrives gap is filled and +2, +3 both eject."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/gap", reordering_window=5.0)
        return node, sub

    def test_fill_single_gap(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 300
        _prime_reordering(sub, node, "/test/gap", base)

        sub._deliver(_make_arrival(node, "/test/gap", base + 1, payload=b"1"))
        assert len(_drain_queue(sub)) == 1  # immediate delivery

        # Skip tag base+2, deliver base+3
        sub._deliver(_make_arrival(node, "/test/gap", base + 3, payload=b"3"))
        assert _drain_queue(sub) == []  # waiting for base+2

        # Fill the gap
        sub._deliver(_make_arrival(node, "/test/gap", base + 2, payload=b"2"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert delivered[0].message == b"2"
        assert delivered[1].message == b"3"

    def test_fill_two_gaps(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 400
        _prime_reordering(sub, node, "/test/gap", base)

        sub._deliver(_make_arrival(node, "/test/gap", base + 1, payload=b"1"))
        _drain_queue(sub)

        # Deliver base+4 (gap: base+2, base+3)
        sub._deliver(_make_arrival(node, "/test/gap", base + 4, payload=b"4"))
        assert _drain_queue(sub) == []

        # Fill gap at base+2 -- this is the next expected, so it delivers immediately.
        # Scan then looks for base+3 which is missing, so base+4 stays interned.
        sub._deliver(_make_arrival(node, "/test/gap", base + 2, payload=b"2"))
        partial = _drain_queue(sub)
        assert len(partial) == 1
        assert partial[0].message == b"2"

        # Fill gap at base+3 -- now 3, 4 should eject
        sub._deliver(_make_arrival(node, "/test/gap", base + 3, payload=b"3"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert [a.message for a in delivered] == [b"3", b"4"]

    def test_gap_at_start(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """A gap right after the primed baseline should work."""
        node, sub = node_and_sub
        base = 50
        _prime_reordering(sub, node, "/test/gap", base)

        # Skip base+1, deliver base+2
        sub._deliver(_make_arrival(node, "/test/gap", base + 2, payload=b"2"))
        assert _drain_queue(sub) == []

        sub._deliver(_make_arrival(node, "/test/gap", base + 1, payload=b"1"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert delivered[0].message == b"1"
        assert delivered[1].message == b"2"

    def test_interleaved_gaps_two_remotes(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Two remote sources each with a gap should not interfere."""
        node, sub = node_and_sub
        base = 600

        _prime_reordering(sub, node, "/test/gap", base, remote_id=10)
        _prime_reordering(sub, node, "/test/gap", base, remote_id=20)

        # Remote A: skip base+1, deliver base+2
        sub._deliver(_make_arrival(node, "/test/gap", base + 2, remote_id=10))
        # Remote B: skip base+1, deliver base+2
        sub._deliver(_make_arrival(node, "/test/gap", base + 2, remote_id=20))
        assert _drain_queue(sub) == []

        # Fill Remote A gap
        sub._deliver(_make_arrival(node, "/test/gap", base + 1, remote_id=10))
        delivered_a = _drain_queue(sub)
        assert len(delivered_a) == 2  # tags base+1 and base+2 from remote A

        # Remote B gap still open
        sub._deliver(_make_arrival(node, "/test/gap", base + 1, remote_id=20))
        delivered_b = _drain_queue(sub)
        assert len(delivered_b) == 2  # tags base+1 and base+2 from remote B


# =====================================================================================================================
# 5. Window expiration -- force ejection after timeout
# =====================================================================================================================


class TestWindowExpiration:
    """When a gap is not filled within reordering_window seconds the buffered
    messages are force-ejected."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/timeout", reordering_window=0.05)  # 50ms
        return node, sub

    @pytest.mark.asyncio
    async def test_timeout_ejects_buffered(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 700
        _prime_reordering(sub, node, "/test/timeout", base)

        # Deliver base+1 (immediate)
        sub._deliver(_make_arrival(node, "/test/timeout", base + 1))
        _drain_queue(sub)

        # Skip base+2, deliver base+3 -- this will be interned
        sub._deliver(_make_arrival(node, "/test/timeout", base + 3, payload=b"late"))
        assert _drain_queue(sub) == []

        # Wait for the reordering window to expire
        await asyncio.sleep(0.15)
        delivered = _drain_queue(sub)
        assert len(delivered) >= 1
        assert any(a.message == b"late" for a in delivered)

    @pytest.mark.asyncio
    async def test_timeout_handles_multiple_buffered(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 800
        _prime_reordering(sub, node, "/test/timeout", base)

        sub._deliver(_make_arrival(node, "/test/timeout", base + 1))
        _drain_queue(sub)

        # Gap at base+2, deliver base+3 and base+4
        sub._deliver(_make_arrival(node, "/test/timeout", base + 3, payload=b"3"))
        sub._deliver(_make_arrival(node, "/test/timeout", base + 4, payload=b"4"))
        assert _drain_queue(sub) == []

        await asyncio.sleep(0.15)
        delivered = _drain_queue(sub)
        assert len(delivered) >= 1
        tags = [a.breadcrumb.tag for a in delivered]
        # Ejected tags should be monotonically ordered
        assert tags == sorted(tags)

    @pytest.mark.asyncio
    async def test_gap_filled_before_timeout(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """If the gap is filled before the timeout fires, messages deliver normally
        and the timer is effectively a no-op."""
        node, sub = node_and_sub
        base = 900
        _prime_reordering(sub, node, "/test/timeout", base)

        sub._deliver(_make_arrival(node, "/test/timeout", base + 1))
        _drain_queue(sub)

        sub._deliver(_make_arrival(node, "/test/timeout", base + 3, payload=b"3"))
        assert _drain_queue(sub) == []

        # Fill gap quickly
        sub._deliver(_make_arrival(node, "/test/timeout", base + 2, payload=b"2"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert delivered[0].message == b"2"
        assert delivered[1].message == b"3"

        # Wait past timeout -- nothing more should appear
        await asyncio.sleep(0.15)
        assert _drain_queue(sub) == []

    @pytest.mark.asyncio
    async def test_timeout_cancel_on_close(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Closing the subscriber should cancel pending timeout handles."""
        node, sub = node_and_sub
        base = 950
        _prime_reordering(sub, node, "/test/timeout", base)

        sub._deliver(_make_arrival(node, "/test/timeout", base + 2, payload=b"x"))
        assert _drain_queue(sub) == []

        sub.close()
        assert len(sub._reordering) == 0

        await asyncio.sleep(0.15)
        # Queue will have a StopAsyncIteration sentinel from close; filter it
        items: list[Arrival] = []
        while True:
            try:
                item = sub._queue.get_nowait()
                if isinstance(item, Arrival):
                    items.append(item)
            except asyncio.QueueEmpty:
                break
        assert items == []


# =====================================================================================================================
# 6. Late / duplicate drop
# =====================================================================================================================


class TestLateAndDuplicateDrop:
    """Messages with tags at or below the last-ejected frontier are silently dropped."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/drop", reordering_window=1.0)
        return node, sub

    def test_late_message_dropped(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 1000
        _prime_reordering(sub, node, "/test/drop", base)

        # Deliver sequential messages to advance frontier
        for i in range(1, 6):
            sub._deliver(_make_arrival(node, "/test/drop", base + i, payload=f"{i}".encode()))
        delivered = _drain_queue(sub)
        assert len(delivered) == 5

        # Now deliver a late tag that is below the frontier
        sub._deliver(_make_arrival(node, "/test/drop", base + 1, payload=b"late"))
        late_delivered = _drain_queue(sub)
        assert late_delivered == []

    def test_duplicate_tag_dropped(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 1100
        _prime_reordering(sub, node, "/test/drop", base)

        sub._deliver(_make_arrival(node, "/test/drop", base + 1, payload=b"original"))
        assert len(_drain_queue(sub)) == 1

        # Same tag again -- already ejected, so treated as late/duplicate
        sub._deliver(_make_arrival(node, "/test/drop", base + 1, payload=b"dup"))
        assert _drain_queue(sub) == []

    def test_duplicate_interned_tag_dropped(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """A duplicate of a tag that is currently buffered (interned) should be dropped."""
        node, sub = node_and_sub
        base = 1200
        _prime_reordering(sub, node, "/test/drop", base)

        # Deliver base+1 immediately
        sub._deliver(_make_arrival(node, "/test/drop", base + 1))
        _drain_queue(sub)

        # Create a gap -- tag base+3 is interned
        sub._deliver(_make_arrival(node, "/test/drop", base + 3, payload=b"first"))
        assert _drain_queue(sub) == []

        # Re-deliver base+3 -- should be silently dropped (already interned)
        sub._deliver(_make_arrival(node, "/test/drop", base + 3, payload=b"dup"))
        assert _drain_queue(sub) == []

        # Fill gap to verify only one copy of base+3
        sub._deliver(_make_arrival(node, "/test/drop", base + 2))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        # The interned message for base+3 was the first one
        assert delivered[1].message == b"first"

    def test_tag_well_below_frontier(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Very old tag (many positions behind) is silently ignored."""
        node, sub = node_and_sub
        base = 2000
        _prime_reordering(sub, node, "/test/drop", base)

        for i in range(1, 11):
            sub._deliver(_make_arrival(node, "/test/drop", base + i))
        _drain_queue(sub)

        # Deliver a tag far behind the frontier
        sub._deliver(_make_arrival(node, "/test/drop", base - 5, payload=b"ancient"))
        assert _drain_queue(sub) == []

    def test_multiple_late_messages_all_dropped(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 3000
        _prime_reordering(sub, node, "/test/drop", base)

        for i in range(1, 9):
            sub._deliver(_make_arrival(node, "/test/drop", base + i))
        _drain_queue(sub)

        # Several late tags
        for i in range(1, 9):
            sub._deliver(_make_arrival(node, "/test/drop", base + i, payload=b"late"))
        assert _drain_queue(sub) == []


# =====================================================================================================================
# 7. Capacity overflow -- more than _REORDERING_CAPACITY interned messages
# =====================================================================================================================


class TestCapacityOverflow:
    """When the incoming tag exceeds last_ejected_lin_tag + _REORDERING_CAPACITY
    while there are interned messages, the oldest are force-ejected."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/cap", reordering_window=10.0)
        return node, sub

    def test_overflow_ejects_oldest(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 5000
        _prime_reordering(sub, node, "/test/cap", base)

        # Deliver base+1 immediately
        sub._deliver(_make_arrival(node, "/test/cap", base + 1, payload=b"1"))
        _drain_queue(sub)

        # Create a gap at base+2, then fill up interned slots
        for i in range(3, 3 + _REORDERING_CAPACITY):
            sub._deliver(_make_arrival(node, "/test/cap", base + i, payload=f"m{i}".encode()))

        # Now deliver something that pushes beyond capacity
        overflow_tag = base + 3 + _REORDERING_CAPACITY + 1
        sub._deliver(_make_arrival(node, "/test/cap", overflow_tag, payload=b"overflow"))

        delivered = _drain_queue(sub)
        assert len(delivered) > 0
        tags = [a.breadcrumb.tag for a in delivered]
        assert tags == sorted(tags), "Ejected messages must be in tag order"

    def test_capacity_limit_respected(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """The interned buffer should never hold more than _REORDERING_CAPACITY entries."""
        node, sub = node_and_sub
        base = 6000
        _prime_reordering(sub, node, "/test/cap", base)

        sub._deliver(_make_arrival(node, "/test/cap", base + 1))
        _drain_queue(sub)

        # Deliver several messages with a gap at base+2
        for i in range(3, 3 + _REORDERING_CAPACITY):
            sub._deliver(_make_arrival(node, "/test/cap", base + i))

        for rs in sub._reordering.values():
            assert len(rs.interned) <= _REORDERING_CAPACITY

    def test_sequential_overflow_recovery(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """After overflow and ejection, subsequent sequential messages should still work."""
        node, sub = node_and_sub
        base = 7000
        _prime_reordering(sub, node, "/test/cap", base)

        # Large jump that causes force-ejection / resequence
        far_tag = base + _REORDERING_CAPACITY + 10
        sub._deliver(_make_arrival(node, "/test/cap", far_tag, payload=b"far"))
        _drain_queue(sub)

        # After resequence the state is reset; prime again from the far tag
        h = _resolved_hash(node, "/test/cap")
        key = (42, h)
        rs = sub._reordering[key]
        # Adjust frontier so far_tag is the last ejected
        far_lin = (far_tag - rs.tag_baseline) % (1 << 64)
        rs.last_ejected_lin_tag = far_lin
        rs.interned.pop(far_lin, None)
        if rs._timeout_handle is not None:
            rs._timeout_handle.cancel()
            rs._timeout_handle = None

        # Now send sequential tags after the far_tag
        for i in range(1, 5):
            sub._deliver(_make_arrival(node, "/test/cap", far_tag + i, payload=f"seq{i}".encode()))
        delivered = _drain_queue(sub)
        assert len(delivered) == 4
        assert [a.message for a in delivered] == [b"seq1", b"seq2", b"seq3", b"seq4"]


# =====================================================================================================================
# 8. Resequencing -- very large tag jump resets state
# =====================================================================================================================


class TestResequencing:
    """When a tag arrives that is far beyond the current frontier (more than
    _REORDERING_CAPACITY past last_ejected_lin_tag, with nothing interned to
    force-eject first), the reordering state resets."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/reseq", reordering_window=1.0)
        return node, sub

    def test_large_jump_resets_baseline(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 10000
        rs = _prime_reordering(sub, node, "/test/reseq", base)
        old_baseline = rs.tag_baseline

        # Jump far ahead -- nothing interned, so it triggers resequence
        far = base + _REORDERING_CAPACITY * 100
        sub._deliver(_make_arrival(node, "/test/reseq", far, payload=b"reseq"))

        # Baseline should have been recalculated
        assert rs.tag_baseline != old_baseline
        assert rs.tag_baseline == far - _HALF_CAP

    def test_resequence_clears_interned(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 20000
        _prime_reordering(sub, node, "/test/reseq", base)

        # Intern a message at base+2 (gap at base+1)
        sub._deliver(_make_arrival(node, "/test/reseq", base + 2, payload=b"interned"))
        assert _drain_queue(sub) == []

        # Jump far -- should resequence (old interned cleared)
        far = base + _REORDERING_CAPACITY * 50
        sub._deliver(_make_arrival(node, "/test/reseq", far, payload=b"far"))

        # Old gap-filler at base+1 should be dropped since frontier was reset
        sub._deliver(_make_arrival(node, "/test/reseq", base + 1, payload=b"old-gap"))
        late_delivered = _drain_queue(sub)
        old_gap_msgs = [a for a in late_delivered if a.message == b"old-gap"]
        assert old_gap_msgs == [], "Old gap filler must be dropped after resequence"

    def test_resequence_then_normal_operation(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """After resequencing, normal gap-fill behaviour should resume."""
        node, sub = node_and_sub
        base = 30000
        _prime_reordering(sub, node, "/test/reseq", base)

        far = base + _REORDERING_CAPACITY * 200
        sub._deliver(_make_arrival(node, "/test/reseq", far, payload=b"reset"))
        _drain_queue(sub)

        # After resequence, re-prime from the far tag
        h = _resolved_hash(node, "/test/reseq")
        key = (42, h)
        rs = sub._reordering[key]
        far_lin = (far - rs.tag_baseline) % (1 << 64)
        rs.last_ejected_lin_tag = far_lin
        rs.interned.pop(far_lin, None)
        if rs._timeout_handle is not None:
            rs._timeout_handle.cancel()
            rs._timeout_handle = None

        # Deliver far+1 immediately
        sub._deliver(_make_arrival(node, "/test/reseq", far + 1, payload=b"f1"))
        assert len(_drain_queue(sub)) == 1

        # Now create a gap: deliver far+3 (gap at far+2)
        sub._deliver(_make_arrival(node, "/test/reseq", far + 3, payload=b"f3"))
        assert _drain_queue(sub) == []

        sub._deliver(_make_arrival(node, "/test/reseq", far + 2, payload=b"f2"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 2
        assert delivered[0].message == b"f2"
        assert delivered[1].message == b"f3"

    def test_multiple_resequences(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Repeated large jumps all trigger resequencing without corruption."""
        node, sub = node_and_sub

        tag = 40000
        rs = _prime_reordering(sub, node, "/test/reseq", tag)

        for jump_idx in range(5):
            old_baseline = rs.tag_baseline
            tag += _REORDERING_CAPACITY * 1000
            sub._deliver(_make_arrival(node, "/test/reseq", tag, payload=b"jump"))
            # Baseline should be recalculated
            assert rs.tag_baseline == tag - _HALF_CAP
            _drain_queue(sub)


# =====================================================================================================================
# 9. Without reordering_window (None) -- plain FIFO
# =====================================================================================================================


class TestNoReorderingWindow:
    """When reordering_window is None messages go straight to the queue without
    any reordering logic."""

    def test_no_reordering(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/plain")  # reordering_window defaults to None
        assert sub._reordering_window is None

        sub._deliver(_make_arrival(node, "/test/plain", 10, payload=b"a"))
        sub._deliver(_make_arrival(node, "/test/plain", 8, payload=b"b"))  # out of order
        sub._deliver(_make_arrival(node, "/test/plain", 12, payload=b"c"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 3
        # Without reordering, messages come out in arrival order
        assert [a.message for a in delivered] == [b"a", b"b", b"c"]

    def test_no_reordering_state_created(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/plain2")
        sub._deliver(_make_arrival(node, "/test/plain2", 1))
        assert len(sub._reordering) == 0


# =====================================================================================================================
# 10. Edge cases
# =====================================================================================================================


class TestEdgeCases:
    """Miscellaneous edge cases for the reordering logic."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/edge", reordering_window=1.0)
        return node, sub

    def test_zero_reordering_window(self) -> None:
        """reordering_window=0.0 means reordering is active but no timeout fires."""
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/edge0", reordering_window=0.0)
        assert sub._reordering_window == 0.0
        # Should still use the ordered delivery path -- first message gets interned
        sub._deliver(_make_arrival(node, "/test/edge0", 100))
        # With window=0.0 the arm_timeout condition `window > 0` is false,
        # so no timer is scheduled. The message stays interned.
        for rs in sub._reordering.values():
            assert len(rs.interned) > 0 or rs.last_ejected_lin_tag > 0

    def test_closed_subscriber_ignores_delivery(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        sub.close()
        sub._deliver(_make_arrival(node, "/test/edge", 100))
        items = []
        while True:
            try:
                item = sub._queue.get_nowait()
                if isinstance(item, Arrival):
                    items.append(item)
            except asyncio.QueueEmpty:
                break
        assert items == []

    def test_reordering_keyed_by_remote_and_hash(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Each (remote_id, topic_hash) pair gets its own _ReorderingState."""
        node, sub = node_and_sub
        sub._deliver(_make_arrival(node, "/test/edge", 100, remote_id=1))
        sub._deliver(_make_arrival(node, "/test/edge", 200, remote_id=2))
        _drain_queue(sub)
        assert len(sub._reordering) == 2
        keys = list(sub._reordering.keys())
        remote_ids = {k[0] for k in keys}
        assert remote_ids == {1, 2}

    def test_last_active_updated(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        before = time.monotonic()
        sub._deliver(_make_arrival(node, "/test/edge", 100))
        after = time.monotonic()
        _drain_queue(sub)
        for rs in sub._reordering.values():
            assert before <= rs.last_active <= after

    def test_huge_tag_values(self) -> None:
        """Tags near the 64-bit boundary should not cause crashes."""
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/edge_huge", reordering_window=1.0)
        big = (1 << 63) - 5
        sub._deliver(_make_arrival(node, "/test/edge_huge", big, payload=b"big"))
        # Should not crash
        _drain_queue(sub)

    def test_consecutive_same_tag_different_remotes(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Same tag value from different remotes are independent -- both interned in separate states."""
        node, sub = node_and_sub
        tag = 777
        sub._deliver(_make_arrival(node, "/test/edge", tag, remote_id=10, payload=b"r10"))
        sub._deliver(_make_arrival(node, "/test/edge", tag, remote_id=20, payload=b"r20"))
        # Each should create its own ReorderingState
        assert len(sub._reordering) == 2

    def test_interned_slot_stores_correct_fields(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Verify _ReorderingSlot captures priority, timestamp, message, remote_id."""
        node, sub = node_and_sub
        base = 1500
        _prime_reordering(sub, node, "/test/edge", base)

        # Deliver base+1 immediately
        sub._deliver(_make_arrival(node, "/test/edge", base + 1))
        _drain_queue(sub)

        ts_before = Instant.now()
        sub._deliver(_make_arrival(node, "/test/edge", base + 3, payload=b"slotted", remote_id=42))
        ts_after = Instant.now()

        # Inspect the interned slot directly
        for rs in sub._reordering.values():
            if rs.interned:
                for slot in rs.interned.values():
                    assert slot.message == b"slotted"
                    assert slot.remote_id == 42
                    assert slot.priority == Priority.NOMINAL
                    assert ts_before.ns <= slot.timestamp.ns <= ts_after.ns


# =====================================================================================================================
# 11. Timer handle management
# =====================================================================================================================


class TestTimerHandleManagement:
    """Verify _reordering_arm_timeout, _reordering_window_expired, and timer cancellation."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/timer", reordering_window=0.05)
        return node, sub

    @pytest.mark.asyncio
    async def test_timer_armed_on_intern(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 10000
        _prime_reordering(sub, node, "/test/timer", base)

        # Deliver base+1 immediately
        sub._deliver(_make_arrival(node, "/test/timer", base + 1))
        _drain_queue(sub)

        # Intern a message (gap at base+2)
        sub._deliver(_make_arrival(node, "/test/timer", base + 3))
        assert _drain_queue(sub) == []

        # A timer handle should be set
        for rs in sub._reordering.values():
            if rs.interned:
                assert rs._timeout_handle is not None

    @pytest.mark.asyncio
    async def test_timer_cancelled_when_gap_filled(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        base = 11000
        _prime_reordering(sub, node, "/test/timer", base)

        sub._deliver(_make_arrival(node, "/test/timer", base + 1))
        _drain_queue(sub)

        sub._deliver(_make_arrival(node, "/test/timer", base + 3))
        # Fill the gap
        sub._deliver(_make_arrival(node, "/test/timer", base + 2))
        _drain_queue(sub)

        # After filling, if no more interned messages, handle should be None
        for rs in sub._reordering.values():
            if not rs.interned:
                assert rs._timeout_handle is None

    @pytest.mark.asyncio
    async def test_repeated_timeouts(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Multiple rounds of gap -> timeout -> ejection should work."""
        node, sub = node_and_sub
        base = 12000
        _prime_reordering(sub, node, "/test/timer", base)

        sub._deliver(_make_arrival(node, "/test/timer", base + 1))
        _drain_queue(sub)

        # Round 1: gap -> timeout
        sub._deliver(_make_arrival(node, "/test/timer", base + 3, payload=b"r1"))
        await asyncio.sleep(0.15)
        d1 = _drain_queue(sub)
        assert len(d1) >= 1

        # Find the current frontier after ejection
        h = _resolved_hash(node, "/test/timer")
        key = (42, h)
        rs = sub._reordering[key]
        # Deliver the next expected tag
        next_tag_lin = rs.last_ejected_lin_tag + 1
        next_tag = (rs.tag_baseline + next_tag_lin) % (1 << 64)
        sub._deliver(_make_arrival(node, "/test/timer", next_tag))
        _drain_queue(sub)

        # Round 2: gap -> timeout
        sub._deliver(_make_arrival(node, "/test/timer", next_tag + 2, payload=b"r2"))
        await asyncio.sleep(0.15)
        d2 = _drain_queue(sub)
        assert len(d2) >= 1

    @pytest.mark.asyncio
    async def test_timer_re_armed_after_partial_ejection(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """If after timeout ejection there are still interned messages, the timer
        should be re-armed for the next round."""
        node, sub = node_and_sub
        base = 13000
        _prime_reordering(sub, node, "/test/timer", base)

        sub._deliver(_make_arrival(node, "/test/timer", base + 1))
        _drain_queue(sub)

        # Intern two non-consecutive messages: base+3 and base+5 (gaps at base+2, base+4)
        sub._deliver(_make_arrival(node, "/test/timer", base + 3, payload=b"3"))
        sub._deliver(_make_arrival(node, "/test/timer", base + 5, payload=b"5"))
        assert _drain_queue(sub) == []

        # First timeout should eject base+3 (and scan sees base+4 is missing, re-arms)
        await asyncio.sleep(0.15)
        d1 = _drain_queue(sub)
        assert len(d1) >= 1

        # Second timeout should eject base+5
        await asyncio.sleep(0.15)
        d2 = _drain_queue(sub)
        total = len(d1) + len(d2)
        assert total >= 2


# =====================================================================================================================
# 12. Multiple subscribers on same topic with different reordering windows
# =====================================================================================================================


class TestMultipleSubscribers:
    """Multiple subscribers on the same topic can have independent reordering_window values."""

    def test_independent_reordering(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub_ordered = node.subscribe("/test/multi", reordering_window=1.0)
        sub_plain = node.subscribe("/test/multi")  # no reordering

        assert sub_ordered._reordering_window == 1.0
        assert sub_plain._reordering_window is None

    def test_both_receive_messages(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub_a = node.subscribe("/test/multi2", reordering_window=1.0)
        sub_b = node.subscribe("/test/multi2", reordering_window=0.5)

        # Prime both
        _prime_reordering(sub_a, node, "/test/multi2", 100)
        _prime_reordering(sub_b, node, "/test/multi2", 100)

        for i in range(1, 6):
            arr = _make_arrival(node, "/test/multi2", 100 + i, payload=f"{i}".encode())
            sub_a._deliver(arr)
            sub_b._deliver(arr)

        da = _drain_queue(sub_a)
        db = _drain_queue(sub_b)
        assert len(da) == 5
        assert len(db) == 5


# =====================================================================================================================
# 13. Subscriber async iteration with reordering
# =====================================================================================================================


class TestAsyncIteration:
    """Verify that __anext__ yields correctly when reordering is enabled."""

    @pytest.mark.asyncio
    async def test_anext_in_order(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/anext", reordering_window=0.5)
        _prime_reordering(sub, node, "/test/anext", 200)

        for i in range(1, 4):
            sub._deliver(_make_arrival(node, "/test/anext", 200 + i, payload=f"{i}".encode()))

        results = []
        for _ in range(3):
            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            results.append(arrival.message)
        assert results == [b"1", b"2", b"3"]

    @pytest.mark.asyncio
    async def test_anext_reordered(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/anext2", reordering_window=0.5)
        _prime_reordering(sub, node, "/test/anext2", 300)

        # Out of order: 302 before 301
        sub._deliver(_make_arrival(node, "/test/anext2", 302, payload=b"second"))

        # Deliver 301 to release both
        sub._deliver(_make_arrival(node, "/test/anext2", 301, payload=b"first"))

        a1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        a2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        assert a1.message == b"first"
        assert a2.message == b"second"

    @pytest.mark.asyncio
    async def test_anext_stops_on_close(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/anext3", reordering_window=0.5)

        sub.close()
        with pytest.raises(StopAsyncIteration):
            await sub.__anext__()


# =====================================================================================================================
# 14. Integration test -- through Node.subscribe with reordering_window
# =====================================================================================================================


class TestIntegrationThroughNode:
    """End-to-end tests that use Node.subscribe(reordering_window=...) and verify
    that the full message dispatch path respects reordering."""

    @pytest.fixture()
    def nodes(self) -> tuple[Node, Node, MockNetwork]:
        network = MockNetwork()
        transport_pub = MockTransport(node_id=10, network=network)
        transport_sub = MockTransport(node_id=20, network=network)
        pub_node = Node(transport_pub, home="pub")
        sub_node = Node(transport_sub, home="sub")
        return pub_node, sub_node, network

    @pytest.mark.asyncio
    async def test_publish_subscribe_ordered(self, nodes: tuple[Node, Node, MockNetwork]) -> None:
        pub_node, sub_node, _ = nodes

        sub = sub_node.subscribe("/integ/ordered", reordering_window=0.1)
        publisher = pub_node.advertise("/integ/ordered")

        # Publish 5 messages in order
        for i in range(5):
            await publisher(Instant.now() + 1.0, f"msg{i}".encode())

        # Allow event loop to process
        await asyncio.sleep(0.01)

        delivered = _drain_queue(sub)
        # All received messages should be in order
        if delivered:
            tags = [a.breadcrumb.tag for a in delivered]
            assert tags == sorted(tags), "Messages must arrive in tag order"

        pub_node.close()
        sub_node.close()

    @pytest.mark.asyncio
    async def test_subscribe_reordering_window_attribute(self, nodes: tuple[Node, Node, MockNetwork]) -> None:
        _, sub_node, _ = nodes
        sub = sub_node.subscribe("/integ/attr", reordering_window=0.25)
        assert sub._reordering_window == 0.25
        sub.close()
        sub_node.close()

    @pytest.mark.asyncio
    async def test_subscribe_default_no_reordering(self, nodes: tuple[Node, Node, MockNetwork]) -> None:
        _, sub_node, _ = nodes
        sub = sub_node.subscribe("/integ/default")
        assert sub._reordering_window is None
        sub.close()
        sub_node.close()

    @pytest.mark.asyncio
    async def test_subscriber_close_cleanup(self, nodes: tuple[Node, Node, MockNetwork]) -> None:
        _, sub_node, _ = nodes
        sub = sub_node.subscribe("/integ/cleanup", reordering_window=0.1)
        # Deliver something to create reordering state
        arr = _make_arrival(sub_node, "/integ/cleanup", 100)
        sub._deliver(arr)
        _drain_queue(sub)
        assert len(sub._reordering) > 0

        sub.close()
        assert len(sub._reordering) == 0
        sub_node.close()


# =====================================================================================================================
# 15. Stress / pathological patterns
# =====================================================================================================================


class TestStressPatterns:
    """Stress tests with larger message volumes and adversarial ordering."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/stress", reordering_window=5.0)
        return node, sub

    def test_reverse_burst(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """A burst of N messages arriving in reverse order.  All should eventually deliver
        in the correct order (subject to capacity-forced ejections)."""
        node, sub = node_and_sub
        base = 50000
        _prime_reordering(sub, node, "/test/stress", base)

        n = _REORDERING_CAPACITY  # exactly at capacity
        for i in range(n, 0, -1):
            sub._deliver(_make_arrival(node, "/test/stress", base + i, payload=f"{i}".encode()))

        delivered = _drain_queue(sub)
        # All N messages should be delivered
        assert len(delivered) == n
        tags = [a.breadcrumb.tag for a in delivered]
        assert tags == sorted(tags), "Must be delivered in tag order"

    def test_alternating_in_out(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Alternating pattern: +2, +1, +4, +3, +6, +5, ..."""
        node, sub = node_and_sub
        base = 60000
        _prime_reordering(sub, node, "/test/stress", base)

        # Send pairs: (even, odd) so each pair has a mini-swap
        for i in range(1, 21, 2):
            sub._deliver(_make_arrival(node, "/test/stress", base + i + 1))
            sub._deliver(_make_arrival(node, "/test/stress", base + i))

        delivered = _drain_queue(sub)
        tags = [a.breadcrumb.tag for a in delivered]
        assert tags == sorted(tags), "Must be delivered in tag order"
        assert len(delivered) == 20

    def test_many_remotes(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Messages from many different remotes each with their own stream."""
        node, sub = node_and_sub
        n_remotes = 50
        base = 70000

        # Prime all remotes first
        for remote_id in range(n_remotes):
            _prime_reordering(sub, node, "/test/stress", base, remote_id=remote_id)

        # Then deliver messages for all remotes
        for remote_id in range(n_remotes):
            for i in range(1, 6):
                sub._deliver(_make_arrival(node, "/test/stress", base + i, remote_id=remote_id))

        delivered = _drain_queue(sub)
        assert len(delivered) == n_remotes * 5
        assert len(sub._reordering) == n_remotes

    def test_burst_then_sequential(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """A burst of out-of-order messages followed by a long sequential run."""
        node, sub = node_and_sub
        base = 80000
        _prime_reordering(sub, node, "/test/stress", base)

        # Out-of-order burst: +5, +3, +4, +2, +1
        for tag_offset in [5, 3, 4, 2, 1]:
            sub._deliver(_make_arrival(node, "/test/stress", base + tag_offset))

        burst = _drain_queue(sub)
        burst_tags = [a.breadcrumb.tag for a in burst]
        assert burst_tags == sorted(burst_tags)
        assert len(burst) == 5

        # Sequential run
        last = base + 5
        for i in range(1, 50):
            sub._deliver(_make_arrival(node, "/test/stress", last + i))
        seq = _drain_queue(sub)
        assert len(seq) == 49
        seq_tags = [a.breadcrumb.tag for a in seq]
        assert seq_tags == sorted(seq_tags)


# =====================================================================================================================
# 16. _ReorderingSlot construction
# =====================================================================================================================


class TestReorderingSlot:
    """Verify _ReorderingSlot stores all fields correctly."""

    def test_construction(self) -> None:
        ts = Instant(ns=123456789)
        slot = _ReorderingSlot(
            lin_tag=42,
            priority=Priority.FAST,
            timestamp=ts,
            message=b"hello",
            remote_id=7,
        )
        assert slot.lin_tag == 42
        assert slot.priority == Priority.FAST
        assert slot.timestamp == ts
        assert slot.message == b"hello"
        assert slot.remote_id == 7

    def test_slot_equality(self) -> None:
        ts = Instant(ns=100)
        a = _ReorderingSlot(lin_tag=1, priority=Priority.NOMINAL, timestamp=ts, message=b"x", remote_id=1)
        b = _ReorderingSlot(lin_tag=1, priority=Priority.NOMINAL, timestamp=ts, message=b"x", remote_id=1)
        assert a == b

    def test_slot_inequality(self) -> None:
        ts = Instant(ns=100)
        a = _ReorderingSlot(lin_tag=1, priority=Priority.NOMINAL, timestamp=ts, message=b"x", remote_id=1)
        b = _ReorderingSlot(lin_tag=2, priority=Priority.NOMINAL, timestamp=ts, message=b"x", remote_id=1)
        assert a != b


# =====================================================================================================================
# 17. Linearization arithmetic
# =====================================================================================================================


class TestLinearization:
    """Test the lin_tag = (tag - tag_baseline) % 2^64 arithmetic used to convert
    circular 64-bit tags into a linear sequence for comparison."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/linear", reordering_window=1.0)
        return node, sub

    def test_baseline_set_on_first_message(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        node, sub = node_and_sub
        tag = 9999
        sub._deliver(_make_arrival(node, "/test/linear", tag))
        _drain_queue(sub)

        # The baseline should be tag - CAPACITY//2
        for rs in sub._reordering.values():
            expected_baseline = tag - _HALF_CAP
            assert rs.tag_baseline == expected_baseline

    def test_lin_tag_computation(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """After priming, subsequent lin_tags should be predictable."""
        node, sub = node_and_sub
        base_tag = 5000
        rs = _prime_reordering(sub, node, "/test/linear", base_tag)

        baseline = rs.tag_baseline
        # For next tag, lin_tag should be (base_tag + 1 - baseline) % 2^64
        expected_lin = (base_tag + 1 - baseline) % (1 << 64)

        # Deliver next and check it was accepted
        sub._deliver(_make_arrival(node, "/test/linear", base_tag + 1))
        delivered = _drain_queue(sub)
        assert len(delivered) == 1
        # last_ejected_lin_tag should have advanced
        assert rs.last_ejected_lin_tag == expected_lin

    def test_wrap_around_arithmetic(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Linearization uses modular arithmetic, so baseline > tag should still work."""
        node, sub = node_and_sub
        # Use a very large tag so that tag_baseline = tag - 8 is also large
        tag = (1 << 64) - 100
        sub._deliver(_make_arrival(node, "/test/linear", tag))
        _drain_queue(sub)
        for rs in sub._reordering.values():
            assert rs.tag_baseline == tag - _HALF_CAP
            lin = (tag - rs.tag_baseline) % (1 << 64)
            assert lin == _HALF_CAP


# =====================================================================================================================
# 18. Ejection mechanics
# =====================================================================================================================


class TestEjectionMechanics:
    """Detailed tests for _reordering_eject_first and _reordering_scan."""

    @pytest.fixture()
    def node_and_sub(self) -> tuple[Node, Subscriber]:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/eject", reordering_window=10.0)
        return node, sub

    def test_eject_first_picks_min_lin_tag(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """_reordering_eject_first must eject the slot with the smallest lin_tag."""
        node, sub = node_and_sub
        base = 90000
        _prime_reordering(sub, node, "/test/eject", base)

        sub._deliver(_make_arrival(node, "/test/eject", base + 1))
        _drain_queue(sub)

        # Intern several non-consecutive messages (gap at base+2)
        sub._deliver(_make_arrival(node, "/test/eject", base + 3, payload=b"3"))
        sub._deliver(_make_arrival(node, "/test/eject", base + 5, payload=b"5"))
        sub._deliver(_make_arrival(node, "/test/eject", base + 4, payload=b"4"))
        assert _drain_queue(sub) == []

        # Now deliver something far enough to force-eject via capacity
        far = base + 1 + _REORDERING_CAPACITY + 2
        sub._deliver(_make_arrival(node, "/test/eject", far, payload=b"far"))
        delivered = _drain_queue(sub)

        # Ejected messages must be in ascending tag order
        tags = [a.breadcrumb.tag for a in delivered]
        assert tags == sorted(tags)

    def test_scan_delivers_consecutive_run(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """When a gap is filled, _reordering_scan should deliver all consecutive followers."""
        node, sub = node_and_sub
        base = 91000
        _prime_reordering(sub, node, "/test/eject", base)

        sub._deliver(_make_arrival(node, "/test/eject", base + 1))
        _drain_queue(sub)

        # Intern base+3, base+4, base+5 (gap at base+2)
        for i in [3, 4, 5]:
            sub._deliver(_make_arrival(node, "/test/eject", base + i, payload=f"{i}".encode()))
        assert _drain_queue(sub) == []

        # Fill gap
        sub._deliver(_make_arrival(node, "/test/eject", base + 2, payload=b"2"))
        delivered = _drain_queue(sub)
        assert len(delivered) == 4
        assert [a.message for a in delivered] == [b"2", b"3", b"4", b"5"]

    def test_scan_stops_at_next_gap(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """_reordering_scan stops when it encounters a missing tag."""
        node, sub = node_and_sub
        base = 92000
        _prime_reordering(sub, node, "/test/eject", base)

        sub._deliver(_make_arrival(node, "/test/eject", base + 1))
        _drain_queue(sub)

        # Intern base+3, base+4, base+6 (gaps at base+2 and base+5)
        for i in [3, 4, 6]:
            sub._deliver(_make_arrival(node, "/test/eject", base + i))
        assert _drain_queue(sub) == []

        # Fill first gap
        sub._deliver(_make_arrival(node, "/test/eject", base + 2))
        delivered = _drain_queue(sub)
        # Should deliver base+2, base+3, base+4 but not base+6 (gap at base+5)
        assert len(delivered) == 3
        tags = [a.breadcrumb.tag for a in delivered]
        assert tags == sorted(tags)

        # Fill second gap
        sub._deliver(_make_arrival(node, "/test/eject", base + 5))
        delivered2 = _drain_queue(sub)
        assert len(delivered2) == 2  # base+5 and base+6

    def test_eject_empty_interned_noop(self, node_and_sub: tuple[Node, Subscriber]) -> None:
        """Calling _reordering_eject_first with empty interned dict should be a no-op."""
        node, sub = node_and_sub
        rs = _ReorderingState(remote_id=1, topic_hash=0xBEEF)
        # Should not raise
        sub._reordering_eject_first(rs)
        assert rs.last_ejected_lin_tag == 0


# =====================================================================================================================
# 19. First-message behaviour (no priming)
# =====================================================================================================================


class TestFirstMessageBehavior:
    """Verify behavior of the very first message -- it gets interned (not delivered
    immediately) because lin_tag = CAPACITY//2 != last_ejected_lin_tag + 1 = 1."""

    def test_first_message_interned(self) -> None:
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/first", reordering_window=1.0)

        sub._deliver(_make_arrival(node, "/test/first", 1000, payload=b"first"))
        # First message is interned, not delivered
        assert _drain_queue(sub) == []
        for rs in sub._reordering.values():
            assert len(rs.interned) == 1

    @pytest.mark.asyncio
    async def test_first_message_delivered_on_timeout(self) -> None:
        """The first interned message should be ejected when the window timer fires."""
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/first_to", reordering_window=0.05)

        sub._deliver(_make_arrival(node, "/test/first_to", 1000, payload=b"first"))
        assert _drain_queue(sub) == []

        await asyncio.sleep(0.15)
        delivered = _drain_queue(sub)
        assert len(delivered) >= 1
        assert delivered[0].message == b"first"

    def test_first_batch_sequential_all_interned(self) -> None:
        """Without priming, the first CAPACITY//2 sequential messages are all interned
        because they all have lin_tags 8..15 and last_ejected is 0."""
        transport = MockTransport(node_id=1)
        node = _make_node(transport)
        sub = node.subscribe("/test/first_batch", reordering_window=5.0)

        base = 2000
        for i in range(_HALF_CAP):
            sub._deliver(_make_arrival(node, "/test/first_batch", base + i))

        # All interned, none delivered
        assert _drain_queue(sub) == []
        for rs in sub._reordering.values():
            assert len(rs.interned) == _HALF_CAP
