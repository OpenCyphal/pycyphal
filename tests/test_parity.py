"""Parity tests ensuring semantic alignment with the reference C implementation (reference/cy/cy/cy.c).

These tests cover behaviors from the reference that are not yet exercised by the existing test suite.
"""

from __future__ import annotations

import asyncio
import math
import time

import pytest

import pycyphal2
from pycyphal2 import SUBJECT_ID_PINNED_MAX
from pycyphal2._hash import rapidhash
from pycyphal2._node import (
    ASSOC_SLACK_LIMIT,
    DEDUP_HISTORY,
    SESSION_LIFETIME,
    Association,
    DedupState,
    compute_subject_id,
    resolve_name,
)
from pycyphal2._header import HEADER_SIZE, MsgAckHeader, MsgBeHeader, MsgNackHeader, MsgRelHeader, deserialize_header
from pycyphal2._publisher import ResponseStreamImpl
from pycyphal2._subscriber import BreadcrumbImpl
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport, MockNetwork, DEFAULT_MODULUS
from tests.typing_helpers import expect_arrival, expect_mock_writer, new_node, subscribe_impl

# =====================================================================================================================
# 1. Topic CRDT convergence: two local topics colliding during allocation
# =====================================================================================================================


async def test_crdt_collision_older_topic_wins():
    """When two local topics collide, the older (higher lage) or lower-hash one wins; loser gets evictions bumped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    # Create topic_a first (it will be older).
    pub_a = node.advertise("/topic_a")
    topic_a = node.topics_by_name["topic_a"]
    sid_a = topic_a.subject_id(tr.subject_id_modulus)

    # Search for a colliding name.
    modulus = tr.subject_id_modulus
    colliding_name = None
    for suffix in range(50000):
        name = f"coll_{suffix}"
        h = rapidhash(name)
        if compute_subject_id(h, 0, modulus) == sid_a:
            colliding_name = name
            break

    if colliding_name is None:
        pytest.skip("Could not find colliding name within search space")

    # Make topic_a significantly older so it wins the CRDT comparison.
    topic_a.ts_origin = time.monotonic() - 100000

    pub_b = node.advertise(f"/{colliding_name}")
    topic_b = node.topics_by_name[colliding_name]

    # topic_a should keep its subject-ID since it is older; topic_b should have been evicted.
    assert topic_a.subject_id(tr.subject_id_modulus) != topic_b.subject_id(tr.subject_id_modulus)
    assert topic_b.evictions > 0  # loser got bumped
    assert topic_a.evictions == 0  # winner untouched

    pub_a.close()
    pub_b.close()
    node.close()


# =====================================================================================================================
# 2. Association slack management: missed ACKs
# =====================================================================================================================


async def test_association_slack_nack_capped():
    """After NACK, association slack jumps to ASSOC_SLACK_LIMIT but association is not removed."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = list(node.topics_by_name.values())[0]

    # Pre-register an association and a publish tracker.
    topic.associations[42] = Association(remote_id=42, last_seen=time.monotonic(), pending_count=1)
    tag = topic.next_tag()
    from pycyphal2._node import PublishTracker

    tracker = PublishTracker(
        tag=tag,
        deadline_ns=(pycyphal2.Instant.now() + 10.0).ns,
        remaining={42},
        ack_event=asyncio.Event(),
    )
    topic.publish_futures[tag] = tracker

    # Send a NACK.
    nack_hdr = MsgNackHeader(topic_hash=topic.hash, tag=tag)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=nack_hdr.serialize(),
    )
    node.on_unicast_arrival(arrival)

    assoc = topic.associations[42]
    assert assoc.slack == ASSOC_SLACK_LIMIT
    # Association should still exist (not removed) because pending_count > 0.
    assert 42 in topic.associations

    del topic.publish_futures[tag]
    pub.close()
    node.close()


async def test_association_ack_resets_slack():
    """ACK should reset the association slack to zero."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = list(node.topics_by_name.values())[0]

    # Pre-register an association with slack already at limit.
    topic.associations[42] = Association(remote_id=42, last_seen=0.0, slack=ASSOC_SLACK_LIMIT)
    tag = topic.next_tag()
    from pycyphal2._node import PublishTracker

    tracker = PublishTracker(
        tag=tag,
        deadline_ns=(pycyphal2.Instant.now() + 10.0).ns,
        remaining={42},
        ack_event=asyncio.Event(),
    )
    topic.publish_futures[tag] = tracker

    # Send an ACK.
    ack_hdr = MsgAckHeader(topic_hash=topic.hash, tag=tag)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=ack_hdr.serialize(),
    )
    node.on_unicast_arrival(arrival)

    assert topic.associations[42].slack == 0
    assert tracker.acknowledged

    del topic.publish_futures[tag]
    pub.close()
    node.close()


# =====================================================================================================================
# 3. Dedup: session lifetime cleanup
# =====================================================================================================================


def test_dedup_stale_entries_prunable():
    """Dedup entries older than SESSION_LIFETIME should not block new tags from different epochs."""
    ds = DedupState()
    ds.check_and_record(100, 1.0)
    ds.last_active = 1.0

    # Simulate a long gap: new tag from a "different session".
    far_future = 1.0 + SESSION_LIFETIME + 10
    assert ds.check_and_record(100, far_future) is True

    # But a new tag well beyond frontier should be accepted and prune old ones.
    new_tag = 100 + DEDUP_HISTORY + 50
    assert ds.check_and_record(new_tag, far_future) is True
    # Now tag 100 should have been pruned, so it should be accepted again.
    assert ds.check_and_record(100, far_future) is True


# =====================================================================================================================
# 4. Gossip inline in messages: MsgBe/MsgRel header carries lage and evictions
# =====================================================================================================================


async def test_msg_header_merges_lage():
    """Receiving a message should merge lage if remote claims older origin."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    sub = subscribe_impl(node, "/topic")
    topic = node.topics_by_name["topic"]

    original_lage = topic.lage(time.monotonic())
    # Construct a MsgBe with a much higher lage, simulating a remote that has known the topic longer.
    remote_lage = original_lage + 15
    hdr = MsgBeHeader(
        topic_log_age=remote_lage,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=topic.next_tag(),
    )
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=hdr.serialize() + b"payload",
    )
    node.on_subject_arrival(topic.subject_id(tr.subject_id_modulus), arrival)

    # After merge, our lage should have increased to at least the remote's claim.
    merged_lage = topic.lage(time.monotonic())
    assert merged_lage >= remote_lage

    pub.close()
    sub.close()
    node.close()


# =====================================================================================================================
# 5. Name resolution edge cases from reference
# =====================================================================================================================


def test_resolve_tilde_alone_resolves_to_home():
    name, pin, verbatim = resolve_name("~", "my_home", "ns")
    assert name == "my_home"
    assert pin is None
    assert verbatim is True


def test_resolve_homeful_namespace_with_relative_name():
    """Namespace '~ns' is literal, not homeful."""
    name, _, _ = resolve_name("topic", "my_home", "~ns")
    assert name == "~ns/topic"


def test_resolve_pin_boundary_max_valid():
    """Pin #8191 (SUBJECT_ID_PINNED_MAX) should be valid."""
    assert SUBJECT_ID_PINNED_MAX == 0x1FFF  # 8191
    name, pin, _ = resolve_name(f"/foo#{SUBJECT_ID_PINNED_MAX}", "h", "ns")
    assert pin == 8191
    assert name == "foo"


def test_resolve_pin_boundary_over_max_invalid():
    """Pin #8192 should NOT be recognized as a pin."""
    name, pin, _ = resolve_name("/foo#8192", "h", "ns")
    assert pin is None
    assert name == "foo#8192"


def test_resolve_multiple_hashes_rightmost_wins():
    """Multiple '#' in name: rightmost valid pin wins."""
    name, pin, _ = resolve_name("/a#b#42", "h", "ns")
    assert name == "a#b"
    assert pin == 42


# =====================================================================================================================
# 6. Reordering: duplicate interned message
# =====================================================================================================================


async def test_reorder_duplicate_interned_only_once():
    """Delivering the same out-of-order tag twice should only intern once."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=0.05)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 2000
    arr0 = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m0")
    sub.deliver(arr0, base_tag, 99)
    assert sub.queue.empty()
    await asyncio.sleep(0.1)
    assert expect_arrival(sub.queue.get_nowait()).message == b"m0"

    # Deliver tag+2 twice (out of order, duplicate).
    arr2a = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m2_first")
    arr2b = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m2_dup")
    sub.deliver(arr2a, base_tag + 2, 99)
    sub.deliver(arr2b, base_tag + 2, 99)  # duplicate
    assert sub.queue.empty()  # both interned/dropped

    # Now deliver the gap-closing tag+1.
    arr1 = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m1")
    sub.deliver(arr1, base_tag + 1, 99)

    items = []
    while not sub.queue.empty():
        items.append(expect_arrival(sub.queue.get_nowait()))
    # Should have m1 then only one copy of m2.
    assert len(items) == 2
    assert items[0].message == b"m1"
    assert items[1].message == b"m2_first"  # first copy wins

    sub.close()
    node.close()


# =====================================================================================================================
# 7. Subscriber close during reordering
# =====================================================================================================================


async def test_subscriber_close_ejects_interned():
    """Closing a subscriber with interned messages should force-eject them into the queue."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=0.05)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 3000
    arr0 = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m0")
    sub.deliver(arr0, base_tag, 99)
    assert sub.queue.empty()
    await asyncio.sleep(0.1)
    assert expect_arrival(sub.queue.get_nowait()).message == b"m0"

    # Intern some out-of-order messages.
    arr3 = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m3")
    arr5 = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"m5")
    sub.deliver(arr3, base_tag + 3, 99)
    sub.deliver(arr5, base_tag + 5, 99)
    assert sub.queue.empty()

    # Close should force-eject all interned messages.
    sub.close()

    items = []
    while not sub.queue.empty():
        it = sub.queue.get_nowait()
        if isinstance(it, StopAsyncIteration):
            continue
        items.append(expect_arrival(it))

    assert len(items) == 2
    assert items[0].message == b"m3"
    assert items[1].message == b"m5"

    node.close()


# =====================================================================================================================
# 8. Best-effort message through full pub->transport->sub pipeline
# =====================================================================================================================


async def test_best_effort_full_pipeline():
    """Publish BE, verify transport writer receives correct header, then check subscriber delivery."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    pub = node.advertise("/pipeline")
    sub = subscribe_impl(node, "/pipeline")
    topic = node.topics_by_name["pipeline"]

    await pub(pycyphal2.Instant.now() + 1.0, b"test_payload")

    # Verify the transport writer was invoked.
    writer = tr.writers.get(topic.subject_id(tr.subject_id_modulus))
    assert writer is not None
    assert writer.send_count >= 1

    # Verify the subscriber received the message with correct payload.
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"test_payload"

    # Verify the breadcrumb carries our node_id.
    assert arrival.breadcrumb.remote_id == 1

    pub.close()
    sub.close()
    node.close()


# =====================================================================================================================
# 9. Topic sync_implicit behavior
# =====================================================================================================================


async def test_topic_implicit_with_only_pattern_sub():
    """A topic coupled only to pattern subscribers should be implicit."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    # Create a pattern subscriber first.
    sub_pat = subscribe_impl(node, "/data/>")
    # Create a topic that matches the pattern.
    pub = node.advertise("/data/sensor")
    topic = node.topics_by_name["data/sensor"]

    # Topic has a publisher, so it is explicit.
    assert not topic.is_implicit

    # Close the publisher: only pattern subscriber remains. Topic should become implicit.
    pub.close()
    assert topic.is_implicit

    # Add a verbatim subscriber: topic should become explicit again.
    sub_verb = subscribe_impl(node, "/data/sensor")
    topic.sync_implicit()
    assert not topic.is_implicit

    # Close verbatim subscriber: back to implicit.
    sub_verb.close()
    topic.sync_implicit()
    assert topic.is_implicit

    sub_pat.close()
    node.close()


# =====================================================================================================================
# 10. Pinned topic subject-ID and shared pinning
# =====================================================================================================================


async def test_pinned_topic_formula():
    """Pinning formula: evictions = 0xFFFFFFFF - pin, subject-ID = pin."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    for pin_val in [0, 1, 42, 100, SUBJECT_ID_PINNED_MAX]:
        pub = node.advertise(f"/pin_{pin_val}#{pin_val}")
        topic = node.topics_by_name[f"pin_{pin_val}"]
        assert topic.subject_id(tr.subject_id_modulus) == pin_val
        assert topic.evictions == 0xFFFFFFFF - pin_val
        pub.close()

    node.close()


async def test_multiple_pinned_topics_share_subject_id():
    """Multiple pinned topics can share the same subject-ID (no collision resolution for pinned)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    pub_a = node.advertise("/alpha#42")
    pub_b = node.advertise("/beta#42")
    topic_a = node.topics_by_name["alpha"]
    topic_b = node.topics_by_name["beta"]

    # Both should have subject-ID 42.
    assert topic_a.subject_id(tr.subject_id_modulus) == 42
    assert topic_b.subject_id(tr.subject_id_modulus) == 42
    assert topic_a.pub_writer is topic_b.pub_writer
    assert tr.subject_writer_creations.get(42) == 1

    writer = expect_mock_writer(topic_a.pub_writer)
    await pub_a(pycyphal2.Instant.now() + 1.0, b"alpha")
    await pub_b(pycyphal2.Instant.now() + 1.0, b"beta")
    assert writer.send_count == 2

    pub_a.close()
    pub_b.close()
    node.close()


# =====================================================================================================================
# 11. Pinned cohabitation
# =====================================================================================================================


async def test_pinned_cohabitation_uses_one_listener_and_acks_once():
    """Frames on a shared pinned subject must be processed once and acked once."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    sub_alpha = subscribe_impl(node, "/alpha#42")
    sub_beta = subscribe_impl(node, "/beta#42")
    topic_alpha = node.topics_by_name["alpha"]
    topic_beta = node.topics_by_name["beta"]

    assert 42 in tr.subject_handlers
    assert tr.subject_listener_creations.get(42) == 1
    assert topic_alpha.sub_listener is topic_beta.sub_listener

    be_hdr = MsgBeHeader(
        topic_log_age=0,
        topic_evictions=topic_alpha.evictions,
        topic_hash=topic_alpha.hash,
        tag=1,
    )
    tr.deliver_subject(
        42,
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=99,
            message=be_hdr.serialize() + b"alpha-be",
        ),
    )
    await asyncio.sleep(0)

    assert sub_alpha.queue.qsize() == 1
    assert sub_beta.queue.qsize() == 0
    assert expect_arrival(sub_alpha.queue.get_nowait()).message == b"alpha-be"

    rel_hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic_alpha.evictions,
        topic_hash=topic_alpha.hash,
        tag=2,
    )
    tr.deliver_subject(
        42,
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=99,
            message=rel_hdr.serialize() + b"alpha-rel",
        ),
    )
    await asyncio.sleep(0.02)

    assert sub_alpha.queue.qsize() == 1
    assert sub_beta.queue.qsize() == 0
    assert expect_arrival(sub_alpha.queue.get_nowait()).message == b"alpha-rel"
    assert len(tr.unicast_log) == 1
    assert tr.unicast_log[0][0] == 99
    ack_hdr = deserialize_header(tr.unicast_log[0][1][:HEADER_SIZE])
    assert isinstance(ack_hdr, MsgAckHeader)
    assert ack_hdr.topic_hash == topic_alpha.hash
    assert ack_hdr.tag == 2

    sub_alpha.close()
    sub_beta.close()
    node.close()


# =====================================================================================================================
# 12. ResponseStream: close cleans up request_futures
# =====================================================================================================================


async def test_response_stream_close_removes_from_request_futures():
    """Closing a ResponseStream should remove the entry from topic.request_futures."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    topic = list(node.topics_by_name.values())[0]

    msg_tag = topic.next_tag()
    stream = ResponseStreamImpl(node=node, topic=topic, message_tag=msg_tag, response_timeout=5.0)
    topic.request_futures[msg_tag] = stream
    assert msg_tag in topic.request_futures

    stream.close()
    assert msg_tag not in topic.request_futures

    pub.close()
    node.close()


# =====================================================================================================================
# 13. Gossip shard formula
# =====================================================================================================================


async def test_gossip_shard_formula():
    """Verify shard_sid = PINNED_MAX + modulus + 1 + (hash % shard_count)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
    node = new_node(tr, home="n1")

    modulus = DEFAULT_MODULUS
    sid_max = SUBJECT_ID_PINNED_MAX + modulus
    shard_base = sid_max + 1

    for test_hash in [0, 1, 42, 0xDEADBEEF, 0xCAFEBABE12345678]:
        shard_sid = node.gossip_shard_subject_id(test_hash)
        expected = shard_base + (test_hash % node.gossip_shard_count)
        assert shard_sid == expected, f"hash={test_hash:#x}: got {shard_sid}, expected {expected}"

    node.close()


# =====================================================================================================================
# 14. Broadcast subject-ID formula
# =====================================================================================================================


async def test_broadcast_subject_id_formula():
    """Verify broadcast_sid = (1 << (floor(log2(PINNED_MAX + modulus)) + 1)) - 1."""
    for modulus in [DEFAULT_MODULUS, 8378431, 131071, 65521]:
        net = MockNetwork()
        tr = MockTransport(node_id=1, modulus=modulus, network=net)
        node = new_node(tr, home="n1")

        sid_max = SUBJECT_ID_PINNED_MAX + modulus
        expected = (1 << (int(math.log2(sid_max)) + 1)) - 1
        assert (
            node.broadcast_subject_id == expected
        ), f"modulus={modulus}: got {node.broadcast_subject_id}, want {expected}"

        # Shard count must be positive.
        assert node.gossip_shard_count > 0
        # Broadcast SID must be above all possible subject-IDs.
        assert node.broadcast_subject_id > sid_max

        node.close()
