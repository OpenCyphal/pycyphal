"""Tests for deduplication logic in pycyphal.

Covers the _DedupState class (unit tests) and Node-level deduplication (integration tests).
"""

from __future__ import annotations

import time

import pytest

from pycyphal._node import _DedupState, _DEDUP_HISTORY
from pycyphal import Instant, Node, Priority
from pycyphal._wire import HeaderType, pack_msg_header, topic_hash, topic_subject_id, LAGE_MIN

from tests.conftest import MockTransport, DEFAULT_MODULUS

# =====================================================================================================================
# Helpers
# =====================================================================================================================


def _make_dedup(remote_id: int = 42) -> _DedupState:
    """Create a fresh _DedupState with default remote_id."""
    return _DedupState(remote_id=remote_id)


def _now() -> float:
    return time.monotonic()


def _make_msg_header(topic_name: str, tag: int, reliable: bool = True) -> bytes:
    """Build a MSG_REL or MSG_BE wire header for the given topic and tag."""
    h = topic_hash(topic_name)
    ht = HeaderType.MSG_REL if reliable else HeaderType.MSG_BE
    return pack_msg_header(ht, LAGE_MIN, 0, h, tag)


def _make_transport_arrival(msg: bytes, remote_id: int = 99) -> object:
    """Create a TransportArrival for injection."""
    from pycyphal import TransportArrival

    return TransportArrival(
        timestamp=Instant.now(),
        priority=Priority.NOMINAL,
        remote_id=remote_id,
        message=msg,
    )


# =====================================================================================================================
# 1. First message -- update() returns False (not duplicate)
# =====================================================================================================================


class TestFirstMessage:
    """First tag ever seen must not be flagged as duplicate."""

    def test_first_tag_not_duplicate(self) -> None:
        dd = _make_dedup()
        assert dd.update(100, _now()) is False

    def test_first_tag_zero(self) -> None:
        dd = _make_dedup()
        assert dd.update(0, _now()) is False

    def test_first_tag_large(self) -> None:
        dd = _make_dedup()
        assert dd.update(0xFFFF_FFFF_FFFF_FFFF, _now()) is False

    def test_first_tag_adds_to_seen_and_sets_last_tag(self) -> None:
        dd = _make_dedup()
        dd.update(999, _now())
        assert 999 in dd.seen
        assert dd.last_tag == 999
        assert len(dd.seen) == 1

    def test_fresh_state_defaults(self) -> None:
        dd = _make_dedup()
        assert len(dd.seen) == 0
        assert dd.last_tag == 0
        assert dd.last_active == 0.0


# =====================================================================================================================
# 2. Duplicate -- same tag update() returns True
# =====================================================================================================================


class TestDuplicate:
    """Resubmitting the same tag must be detected as a duplicate."""

    def test_immediate_duplicate(self) -> None:
        dd = _make_dedup()
        dd.update(10, _now())
        assert dd.update(10, _now()) is True

    def test_triple_submission(self) -> None:
        dd = _make_dedup()
        dd.update(10, _now())
        dd.update(10, _now())
        assert dd.update(10, _now()) is True

    def test_duplicate_does_not_grow_seen(self) -> None:
        dd = _make_dedup()
        dd.update(10, _now())
        dd.update(10, _now())
        dd.update(10, _now())
        assert len(dd.seen) == 1

    def test_duplicate_after_other_tags(self) -> None:
        dd = _make_dedup()
        dd.update(1, _now())
        dd.update(2, _now())
        dd.update(3, _now())
        assert dd.update(1, _now()) is True

    def test_duplicate_preserves_last_tag(self) -> None:
        """On a duplicate, last_tag stays from the last non-dup update."""
        dd = _make_dedup()
        dd.update(42, _now())
        dd.update(99, _now())
        assert dd.last_tag == 99
        dd.update(42, _now())  # duplicate -- returns early before setting last_tag
        assert dd.last_tag == 99

    def test_duplicate_tag_zero(self) -> None:
        dd = _make_dedup()
        dd.update(0, _now())
        assert dd.update(0, _now()) is True

    def test_duplicate_tag_max_uint64(self) -> None:
        dd = _make_dedup()
        dd.update(0xFFFF_FFFF_FFFF_FFFF, _now())
        assert dd.update(0xFFFF_FFFF_FFFF_FFFF, _now()) is True


# =====================================================================================================================
# 3. Different tags -- each new tag returns False
# =====================================================================================================================


class TestDifferentTags:
    """Each unique tag must be accepted on first arrival."""

    def test_sequential_tags(self) -> None:
        dd = _make_dedup()
        for tag in range(100):
            assert dd.update(tag, _now()) is False

    def test_random_unique_tags(self) -> None:
        import random

        rng = random.Random(12345)
        dd = _make_dedup()
        tags = rng.sample(range(10_000_000), 200)
        for tag in tags:
            assert dd.update(tag, _now()) is False

    def test_seen_grows_with_unique_tags(self) -> None:
        dd = _make_dedup()
        for tag in range(50):
            dd.update(tag, _now())
        assert len(dd.seen) == 50

    def test_last_tag_tracks_latest(self) -> None:
        dd = _make_dedup()
        for tag in [10, 20, 30, 40]:
            dd.update(tag, _now())
        assert dd.last_tag == 40

    def test_interleaved_new_and_dup(self) -> None:
        dd = _make_dedup()
        assert dd.update(1, _now()) is False
        assert dd.update(2, _now()) is False
        assert dd.update(1, _now()) is True
        assert dd.update(3, _now()) is False
        assert dd.update(2, _now()) is True
        assert dd.update(4, _now()) is False

    def test_sparse_tags(self) -> None:
        dd = _make_dedup()
        sparse = [0, 1000, 2000, 3000, 0xFFFF_FFFF_0000_0000]
        for tag in sparse:
            assert dd.update(tag, _now()) is False
        for tag in sparse:
            assert dd.update(tag, _now()) is True


# =====================================================================================================================
# 4. History bound -- after >512 unique tags, oldest may be evicted
# =====================================================================================================================


class TestHistoryBound:
    """The seen set must not grow unboundedly."""

    def test_dedup_history_constant(self) -> None:
        assert _DEDUP_HISTORY == 512

    def test_seen_bounded_after_overflow(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 100):
            dd.update(tag, _now())
        assert len(dd.seen) <= _DEDUP_HISTORY

    def test_exactly_at_limit_no_eviction(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY):
            dd.update(tag, _now())
        assert len(dd.seen) == _DEDUP_HISTORY

    def test_one_over_limit_triggers_eviction(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 1):
            dd.update(tag, _now())
        assert len(dd.seen) <= _DEDUP_HISTORY

    def test_recent_tags_survive_eviction(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 50):
            dd.update(tag, _now())
        latest = _DEDUP_HISTORY + 49
        assert latest in dd.seen
        assert dd.update(latest, _now()) is True

    def test_very_old_tag_evicted(self) -> None:
        """After many insertions past the limit, the first tag should be gone."""
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 200):
            dd.update(tag, _now())
        assert 0 not in dd.seen

    def test_evicted_tag_accepted_again(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 200):
            dd.update(tag, _now())
        # Tag 0 was evicted, so it is no longer a duplicate
        assert dd.update(0, _now()) is False

    def test_continuous_stream_stays_bounded(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY * 3):
            dd.update(tag, _now())
            assert len(dd.seen) <= _DEDUP_HISTORY + 1  # +1 for just-added before trim


# =====================================================================================================================
# 5. Check without update -- check() returns True for seen, False for unseen
# =====================================================================================================================


class TestCheckWithoutUpdate:
    """check() must report duplicates without side effects."""

    def test_check_unseen_returns_false(self) -> None:
        dd = _make_dedup()
        assert dd.check(42) is False

    def test_check_seen_returns_true(self) -> None:
        dd = _make_dedup()
        dd.update(42, _now())
        assert dd.check(42) is True

    def test_check_does_not_add_to_seen(self) -> None:
        dd = _make_dedup()
        dd.check(42)
        assert 42 not in dd.seen
        assert len(dd.seen) == 0

    def test_check_does_not_change_last_active_or_last_tag(self) -> None:
        dd = _make_dedup()
        dd.update(10, 5.0)
        dd.check(99)
        assert dd.last_active == 5.0
        assert dd.last_tag == 10

    def test_check_multiple_seen_and_unseen(self) -> None:
        dd = _make_dedup()
        dd.update(100, _now())
        dd.update(200, _now())
        assert dd.check(100) is True
        assert dd.check(200) is True
        assert dd.check(300) is False
        assert dd.check(0) is False

    def test_check_then_update_sequence(self) -> None:
        dd = _make_dedup()
        assert dd.check(42) is False
        assert dd.update(42, _now()) is False  # first real add
        assert dd.check(42) is True
        assert dd.update(42, _now()) is True  # now a duplicate


# =====================================================================================================================
# 6. Wrapping tags -- tags near uint64 boundary
# =====================================================================================================================


class TestWrappingTags:
    """Tags close to or at the uint64 boundary must be handled correctly."""

    def test_max_tag_dup(self) -> None:
        dd = _make_dedup()
        max_tag = (1 << 64) - 1
        assert dd.update(max_tag, _now()) is False
        assert dd.update(max_tag, _now()) is True

    def test_max_and_zero_are_distinct(self) -> None:
        dd = _make_dedup()
        dd.update((1 << 64) - 1, _now())
        assert dd.update(0, _now()) is False

    def test_near_boundary_distinct(self) -> None:
        dd = _make_dedup()
        base = (1 << 64) - 5
        for i in range(10):
            tag = (base + i) & 0xFFFF_FFFF_FFFF_FFFF
            assert dd.update(tag, _now()) is False

    def test_near_boundary_duplicates(self) -> None:
        dd = _make_dedup()
        base = (1 << 64) - 5
        tags = [(base + i) & 0xFFFF_FFFF_FFFF_FFFF for i in range(10)]
        for tag in tags:
            dd.update(tag, _now())
        for tag in tags:
            assert dd.update(tag, _now()) is True

    def test_eviction_with_wrapping_tags(self) -> None:
        dd = _make_dedup()
        base = (1 << 64) - (_DEDUP_HISTORY // 2)
        for i in range(_DEDUP_HISTORY + 50):
            tag = (base + i) & 0xFFFF_FFFF_FFFF_FFFF
            dd.update(tag, _now())
        assert len(dd.seen) <= _DEDUP_HISTORY

    def test_half_below_half_above_boundary(self) -> None:
        dd = _make_dedup()
        max_tag = (1 << 64) - 1
        for tag in [max_tag - 1, max_tag, 0, 1]:
            dd.update(tag, _now())
        assert len(dd.seen) == 4
        for tag in [max_tag - 1, max_tag, 0, 1]:
            assert dd.check(tag) is True

    def test_power_of_two_boundary(self) -> None:
        dd = _make_dedup()
        dd.update((1 << 63), _now())
        dd.update((1 << 63) - 1, _now())
        dd.update((1 << 63) + 1, _now())
        assert len(dd.seen) == 3
        assert dd.check((1 << 63)) is True


# =====================================================================================================================
# 7. Active timestamp -- update() refreshes last_active
# =====================================================================================================================


class TestActiveTimestamp:
    """update() must always refresh last_active, even on duplicates."""

    def test_first_update_sets_timestamp(self) -> None:
        dd = _make_dedup()
        assert dd.last_active == 0.0
        t = _now()
        dd.update(1, t)
        assert dd.last_active == t

    def test_second_update_advances_timestamp(self) -> None:
        dd = _make_dedup()
        dd.update(1, 100.0)
        dd.update(2, 200.0)
        assert dd.last_active == 200.0

    def test_duplicate_updates_timestamp(self) -> None:
        dd = _make_dedup()
        dd.update(1, 100.0)
        assert dd.last_active == 100.0
        dd.update(1, 300.0)  # duplicate
        assert dd.last_active == 300.0

    def test_timestamp_monotonically_set(self) -> None:
        dd = _make_dedup()
        for i, t in enumerate([1.0, 2.0, 3.0, 4.0, 5.0]):
            dd.update(i, t)
            assert dd.last_active == t

    def test_timestamp_with_duplicates_interleaved(self) -> None:
        dd = _make_dedup()
        dd.update(1, 10.0)
        dd.update(2, 20.0)
        dd.update(1, 30.0)  # dup
        assert dd.last_active == 30.0
        dd.update(3, 40.0)  # new
        assert dd.last_active == 40.0
        dd.update(3, 50.0)  # dup
        assert dd.last_active == 50.0

    def test_check_does_not_update_timestamp(self) -> None:
        dd = _make_dedup()
        dd.update(1, 10.0)
        dd.check(1)
        dd.check(2)
        assert dd.last_active == 10.0

    def test_timestamp_preserved_across_eviction(self) -> None:
        dd = _make_dedup()
        for tag in range(_DEDUP_HISTORY + 10):
            dd.update(tag, float(tag))
        assert dd.last_active == float(_DEDUP_HISTORY + 9)


# =====================================================================================================================
# 8. Integration -- Node-level deduplication via _on_message
# =====================================================================================================================


class TestNodeDedup:
    """Integration tests: feed messages into the Node dispatch path and verify dedup."""

    def _make_node(self) -> tuple[Node, MockTransport]:
        transport = MockTransport(node_id=1)
        node = Node(transport, home="test_dedup_node")
        return node, transport

    def _make_msg(self, topic_name: str, tag: int, reliable: bool = True) -> bytes:
        header = _make_msg_header(topic_name, tag, reliable)
        return header + b"test-payload"

    def _inject_subject(self, transport: MockTransport, subject_id: int, msg: bytes, remote_id: int = 99) -> None:
        arrival = _make_transport_arrival(msg, remote_id)
        for handler in transport._subject_handlers.get(subject_id, []):
            handler(arrival)

    def test_reliable_first_delivery_accepted(self) -> None:
        """First reliable message is accepted and delivered to subscriber."""
        node, transport = self._make_node()
        topic_name = "test/dedup/alpha"
        sub = node.subscribe(topic_name)
        tag = 1000

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        accepted = node._on_message(topic, tag, ts, b"payload1", True, 99, Priority.NOMINAL)
        assert accepted is True
        assert sub._queue.qsize() == 1

    def test_reliable_retransmit_deduplicated(self) -> None:
        """Second delivery of same tag returns True but does not re-deliver."""
        node, transport = self._make_node()
        topic_name = "test/dedup/alpha2"
        sub = node.subscribe(topic_name)
        tag = 1001

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        node._on_message(topic, tag, ts, b"p", True, 99, Priority.NOMINAL)
        r2 = node._on_message(topic, tag, ts, b"p", True, 99, Priority.NOMINAL)
        assert r2 is True  # acknowledged duplicate
        assert sub._queue.qsize() == 1  # only one delivery

    def test_reliable_creates_dedup_state(self) -> None:
        node, transport = self._make_node()
        topic_name = "test/dedup/beta"
        sub = node.subscribe(topic_name)
        tag = 2000

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        assert 99 not in topic.dedup_by_remote

        ts = Instant.now()
        node._on_message(topic, tag, ts, b"payload", True, 99, Priority.NOMINAL)

        assert 99 in topic.dedup_by_remote
        dd = topic.dedup_by_remote[99]
        assert dd.remote_id == 99
        assert tag in dd.seen

    def test_best_effort_no_dedup_state(self) -> None:
        node, transport = self._make_node()
        topic_name = "test/dedup/gamma"
        sub = node.subscribe(topic_name)

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        node._on_message(topic, 3000, ts, b"payload", False, 99, Priority.NOMINAL)
        assert 99 not in topic.dedup_by_remote

    def test_different_remotes_independent(self) -> None:
        """Same tag from different remotes is not a dup."""
        node, transport = self._make_node()
        topic_name = "test/dedup/delta"
        sub = node.subscribe(topic_name)
        tag = 4000

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        r_a = node._on_message(topic, tag, ts, b"p1", True, 100, Priority.NOMINAL)
        r_b = node._on_message(topic, tag, ts, b"p2", True, 200, Priority.NOMINAL)
        assert r_a is True
        assert r_b is True
        assert 100 in topic.dedup_by_remote
        assert 200 in topic.dedup_by_remote
        assert sub._queue.qsize() == 2

    def test_retransmit_subscriber_receives_once(self) -> None:
        """Reliable publish + two retransmits: subscriber gets exactly one copy."""
        node, transport = self._make_node()
        topic_name = "test/dedup/retransmit"
        sub = node.subscribe(topic_name)
        tag = 7000

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        node._on_message(topic, tag, ts, b"hello", True, 99, Priority.NOMINAL)
        node._on_message(topic, tag, ts, b"hello", True, 99, Priority.NOMINAL)
        node._on_message(topic, tag, ts, b"hello", True, 99, Priority.NOMINAL)

        assert sub._queue.qsize() == 1

    def test_multiple_distinct_tags_all_delivered(self) -> None:
        node, transport = self._make_node()
        topic_name = "test/dedup/zeta"
        sub = node.subscribe(topic_name)

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()

        tags = [6000, 6001, 6002, 6003, 6004]
        for tag in tags:
            node._on_message(topic, tag, ts, b"p", True, 99, Priority.NOMINAL)

        dd = topic.dedup_by_remote[99]
        for tag in tags:
            assert tag in dd.seen
        assert sub._queue.qsize() == len(tags)

    def test_no_subscriber_reliable_returns_false(self) -> None:
        """No subscribers and no prior dedup state: returns False."""
        node, transport = self._make_node()
        topic_name = "test/dedup/nosub"
        topic = node._topic_ensure(topic_name)
        assert len(topic.couplings) == 0

        ts = Instant.now()
        r = node._on_message(topic, 8000, ts, b"p", True, 99, Priority.NOMINAL)
        assert r is False

    def test_wire_dispatch_dedup_end_to_end(self) -> None:
        """Inject raw wire messages via subject handler: second is deduplicated."""
        node, transport = self._make_node()
        topic_name = "test/dedup/wire"
        sub = node.subscribe(topic_name)

        h = topic_hash(topic_name)
        sid = topic_subject_id(h, 0, DEFAULT_MODULUS)
        tag = 9000
        msg = self._make_msg(topic_name, tag, reliable=True)

        self._inject_subject(transport, sid, msg, remote_id=55)
        self._inject_subject(transport, sid, msg, remote_id=55)

        assert sub._queue.qsize() == 1

    def test_wire_dispatch_different_tags_all_delivered(self) -> None:
        node, transport = self._make_node()
        topic_name = "test/dedup/wire2"
        sub = node.subscribe(topic_name)

        h = topic_hash(topic_name)
        sid = topic_subject_id(h, 0, DEFAULT_MODULUS)

        for tag in [9100, 9200, 9300]:
            msg = self._make_msg(topic_name, tag, reliable=True)
            self._inject_subject(transport, sid, msg, remote_id=55)

        assert sub._queue.qsize() == 3

    def test_close_with_dedup_state(self) -> None:
        """Closing the node does not crash even with dedup state present."""
        node, transport = self._make_node()
        topic_name = "test/dedup/close"
        sub = node.subscribe(topic_name)

        h = topic_hash(topic_name)
        topic = node._topics_by_hash[h]
        ts = Instant.now()
        node._on_message(topic, 10_000, ts, b"p", True, 99, Priority.NOMINAL)
        assert 99 in topic.dedup_by_remote
        node.close()


# =====================================================================================================================
# Edge cases
# =====================================================================================================================


class TestDedupEdgeCases:
    """Miscellaneous edge cases for _DedupState."""

    def test_remote_id_preserved(self) -> None:
        dd = _DedupState(remote_id=12345)
        assert dd.remote_id == 12345

    def test_independent_instances(self) -> None:
        dd1 = _DedupState(remote_id=1)
        dd2 = _DedupState(remote_id=2)
        dd1.update(100, _now())
        assert dd1.check(100) is True
        assert dd2.check(100) is False

    def test_seen_is_real_set(self) -> None:
        dd = _make_dedup()
        dd.update(1, _now())
        dd.update(2, _now())
        assert isinstance(dd.seen, set)

    def test_bulk_tags_within_history(self) -> None:
        dd = _make_dedup()
        n = 400  # within _DEDUP_HISTORY
        for tag in range(n):
            dd.update(tag, _now())
        for tag in range(n):
            assert dd.update(tag, _now()) is True

    def test_negative_time_accepted(self) -> None:
        dd = _make_dedup()
        assert dd.update(1, -10.0) is False
        assert dd.last_active == -10.0

    def test_dataclass_default_factory_isolation(self) -> None:
        """Each _DedupState instance must have its own seen set."""
        dd1 = _DedupState(remote_id=1)
        dd2 = _DedupState(remote_id=2)
        dd1.update(42, _now())
        assert 42 not in dd2.seen

    def test_return_types(self) -> None:
        dd = _make_dedup()
        assert isinstance(dd.update(1, _now()), bool)
        assert isinstance(dd.update(1, _now()), bool)
        assert isinstance(dd.check(999), bool)
