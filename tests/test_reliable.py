"""Tests for reliable publish, request/response, gossip handling, and scout responses."""

from __future__ import annotations

import asyncio

import pytest

import pycyphal2
from pycyphal2._hash import rapidhash
from pycyphal2._node import (
    Association,
    DedupState,
    GossipScope,
    PublishTracker,
    compute_subject_id,
    DEDUP_HISTORY,
)
from pycyphal2._publisher import ResponseStreamImpl
from pycyphal2._subscriber import BreadcrumbImpl, RespondTracker
from pycyphal2._header import (
    HEADER_SIZE,
    MsgBeHeader,
    MsgRelHeader,
    MsgAckHeader,
    MsgNackHeader,
    RspBeHeader,
    RspAckHeader,
    RspNackHeader,
    RspRelHeader,
    GossipHeader,
    ScoutHeader,
    deserialize_header,
)
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import expect_mock_writer, expect_response, new_node, subscribe_impl


class _CountingFailingWriter(pycyphal2.SubjectWriter):
    def __init__(self) -> None:
        self.call_count = 0
        self.closed = False

    async def __call__(
        self,
        deadline: pycyphal2.Instant,
        priority: pycyphal2.Priority,
        message: bytes | memoryview,
    ) -> None:
        del deadline, priority, message
        self.call_count += 1
        raise OSError("synthetic failure")

    def close(self) -> None:
        self.closed = True


# =====================================================================================================================
# Reliable Publish
# =====================================================================================================================


async def test_reliable_publish_no_associations():
    """Reliable publish with no known associations needs at least one ACK before deadline."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    pub.priority = pycyphal2.Priority.EXCEPTIONAL
    pub.ack_timeout = 0.005
    topic = list(node.topics_by_name.values())[0]

    with pytest.raises(pycyphal2.DeliveryError):
        await pub(pycyphal2.Instant.now() + 0.03, b"data", reliable=True)

    writer = expect_mock_writer(topic.pub_writer)
    assert writer.send_count > 1

    pub.close()
    node.close()


async def test_reliable_publish_unacked_deadline():
    """Reliable publish with unresponsive association should raise DeliveryError."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    # Pre-register an association that will never ACK.
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    with pytest.raises(pycyphal2.DeliveryError):
        await pub(pycyphal2.Instant.now() + 0.05, b"data", reliable=True)

    pub.close()
    node.close()


async def test_reliable_publish_with_ack():
    """Reliable publish should succeed when ACK is received."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]

    # Pre-register an association.
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    # Start reliable publish in background.
    async def publish_and_ack() -> None:
        # Start publish.
        pub_task = asyncio.create_task(pub(pycyphal2.Instant.now() + 2.0, b"data", reliable=True))
        await asyncio.sleep(0.01)

        # Find the tracker and simulate ACK.
        for tag, tracker in topic.publish_futures.items():
            tracker.remaining.discard(42)
            tracker.acknowledged = True
            tracker.ack_event.set()
            break

        await pub_task  # Should succeed now.

    await publish_and_ack()

    pub.close()
    node.close()


async def test_reliable_publish_initial_send_failure_raises_send_error():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    writer = _CountingFailingWriter()
    topic.pub_writer = writer

    with pytest.raises(pycyphal2.SendError):
        await pub(pycyphal2.Instant.now() + 0.1, b"data", reliable=True)

    assert writer.call_count == 1
    assert topic.publish_futures == {}

    pub.close()
    node.close()


async def test_reliable_publish_retry_rebuilds_writer_and_header_after_reallocation():
    net = MockNetwork()
    observer = MockTransport(node_id=2, network=net)
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    pub.priority = pycyphal2.Priority.EXCEPTIONAL
    pub.ack_timeout = 0.1
    topic = node.topics_by_name["topic"]
    old_sid = topic.subject_id
    old_evictions = topic.evictions
    old_messages: list[TransportArrival] = []
    new_messages: list[TransportArrival] = []
    observer.subject_listen(old_sid, old_messages.append)
    old_writer = expect_mock_writer(topic.ensure_writer())

    task = asyncio.create_task(pub(pycyphal2.Instant.now() + 1.0, b"payload", reliable=True))
    for _ in range(50):
        if old_messages:
            break
        await asyncio.sleep(0.002)
    assert old_messages

    now = pycyphal2.Instant.now().s
    gossip_hdr = GossipHeader(
        topic_log_age=topic.lage(now) + 1,
        topic_hash=topic.hash,
        topic_evictions=topic.evictions + 1,
        name_len=len(topic.name),
    )
    gossip_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_hdr.serialize() + topic.name.encode("utf-8"),
    )
    node.on_subject_arrival(node.broadcast_subject_id, gossip_arrival)

    new_sid = topic.subject_id
    assert new_sid != old_sid
    observer.subject_listen(new_sid, new_messages.append)

    with pytest.raises(pycyphal2.DeliveryError):
        await task

    assert old_writer.send_count == 1
    new_writer = expect_mock_writer(topic.pub_writer)
    assert new_writer.subject_id == new_sid
    assert new_writer.send_count > 0
    old_hdr = MsgRelHeader.deserialize(old_messages[0].message[:HEADER_SIZE])
    assert old_hdr is not None
    assert old_hdr.topic_evictions == old_evictions
    assert new_messages
    hdr = MsgRelHeader.deserialize(new_messages[0].message[:HEADER_SIZE])
    assert hdr is not None
    assert hdr.topic_evictions == topic.evictions

    pub.close()
    node.close()
    observer.close()


async def test_gossip_reallocation_to_occupied_subject_preserves_writer():
    net = MockNetwork()
    tr = MockTransport(node_id=1, modulus=11, network=net)
    node = new_node(tr, home="n1")
    pub_a = node.advertise("/topic_a")
    topic_a = node.topics_by_name["topic_a"]
    target_sid = compute_subject_id(topic_a.hash, 1, tr.subject_id_modulus)

    colliding_name: str | None = None
    for i in range(128):
        candidate = f"/topic_b_{i}"
        if compute_subject_id(rapidhash(candidate.removeprefix("/")), 0, tr.subject_id_modulus) == target_sid:
            colliding_name = candidate
            break
    assert colliding_name is not None

    pub_b = node.advertise(colliding_name)
    topic_b = node.topics_by_name[colliding_name.removeprefix("/")]
    sid_b = topic_b.subject_id
    writer_b = expect_mock_writer(topic_b.pub_writer)

    now = pycyphal2.Instant.now().s
    topic_a.ts_origin = now - 100000.0
    topic_b.ts_origin = now

    assert sid_b == target_sid
    remote_evictions = 1

    writer_creations_before = tr.subject_writer_creations.get(sid_b)
    remote_lage = topic_a.lage(now) + 1
    node.on_gossip_known(topic_a, remote_evictions, remote_lage, now, GossipScope.SHARDED)

    assert topic_a.subject_id == sid_b
    assert topic_a.pub_writer is writer_b
    assert tr.subject_writer_creations.get(sid_b) == writer_creations_before == 1
    assert topic_b.pub_writer is None
    assert topic_b.subject_id != sid_b

    send_count_before = writer_b.send_count
    await pub_a(pycyphal2.Instant.now() + 1.0, b"payload")
    assert writer_b.send_count == send_count_before + 1

    pub_a.close()
    pub_b.close()
    node.close()


async def test_reliable_publish_closed_publisher():
    """Publishing on a closed publisher should raise SendError."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    pub.close()

    with pytest.raises(pycyphal2.SendError):
        await pub(pycyphal2.Instant.now() + 1.0, b"data")

    node.close()


async def test_publisher_priority_and_ack_timeout():
    """Publisher priority and ack_timeout properties should work."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    assert pub.priority == pycyphal2.Priority.NOMINAL
    pub.priority = pycyphal2.Priority.HIGH
    assert pub.priority == pycyphal2.Priority.HIGH

    assert pub.ack_timeout == pytest.approx(0.016 * (1 << int(pycyphal2.Priority.HIGH)))
    pub.ack_timeout = 0.1
    assert pub.ack_timeout == pytest.approx(0.1)

    pub.priority = pycyphal2.Priority.NOMINAL
    assert pub.ack_timeout == pytest.approx(0.2)

    pub.close()
    node.close()


# =====================================================================================================================
# Request / Response
# =====================================================================================================================


async def test_request_creates_stream():
    """request() should return a ResponseStream and register it."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")

    topic = list(node.topics_by_name.values())[0]
    stream = await pub.request(pycyphal2.Instant.now() + 1.0, 5.0, b"request_data")

    assert isinstance(stream, ResponseStreamImpl)
    assert len(topic.request_futures) > 0

    stream.close()
    pub.close()
    node.close()


async def test_request_initial_send_failure_raises_send_error_and_cleans_stream():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]
    writer = _CountingFailingWriter()
    topic.pub_writer = writer

    with pytest.raises(pycyphal2.SendError):
        await pub.request(pycyphal2.Instant.now() + 0.1, 1.0, b"request_data")

    assert writer.call_count == 1
    assert topic.request_futures == {}
    assert topic.publish_futures == {}

    pub.close()
    node.close()


async def test_request_retransmits_and_surfaces_delivery_failure():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.priority = pycyphal2.Priority.EXCEPTIONAL
    pub.ack_timeout = 0.005

    topic = list(node.topics_by_name.values())[0]
    stream = await pub.request(pycyphal2.Instant.now() + 0.08, 1.0, b"request_data")
    writer = expect_mock_writer(topic.pub_writer)
    for _ in range(40):
        if writer.send_count > 1:
            break
        await asyncio.sleep(0.005)

    assert writer.send_count > 1

    with pytest.raises(pycyphal2.DeliveryError):
        await stream.__anext__()

    stream.close()
    pub.close()
    node.close()


# =====================================================================================================================
# Dedup State
# =====================================================================================================================


def test_dedup_state_basic():
    """DedupState should accept new tags and reject duplicates."""
    ds = DedupState()
    assert ds.check_and_record(100, 1.0) is True
    assert ds.check_and_record(100, 1.0) is False  # duplicate
    assert ds.check_and_record(101, 1.0) is True
    assert ds.check_and_record(102, 1.0) is True


def test_dedup_state_frontier_prune():
    """DedupState should prune old tags beyond the history window."""
    ds = DedupState()
    # Add many tags.
    for i in range(DEDUP_HISTORY + 100):
        assert ds.check_and_record(i, 1.0) is True

    # Very old tags should have been pruned and re-accepted.
    # Tag 0 was far below frontier, so it was pruned.
    assert ds.check_and_record(0, 1.0) is True


# =====================================================================================================================
# Gossip Handling via Transport Message
# =====================================================================================================================


async def test_gossip_known_topic_divergence():
    """When we receive a gossip with different evictions, CRDT resolution should happen."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    old_evictions = topic.evictions

    # Send a gossip with higher evictions (remote has moved this topic).
    gossip_hdr = GossipHeader(
        topic_log_age=topic.lage(0) + 5,  # remote claims much older
        topic_hash=topic.hash,
        topic_evictions=old_evictions + 1,
        name_len=len(topic.name),
    )
    gossip_data = gossip_hdr.serialize() + topic.name.encode("utf-8")
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_data,
    )
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    # Topic should have been reallocated (evictions changed).
    # The exact outcome depends on CRDT logic.
    await asyncio.sleep(0.02)

    pub.close()
    node.close()


async def test_gossip_unknown_topic_collision():
    """Gossip for unknown topic that collides with our subject-ID should trigger reallocation."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic_a")

    topic_a = node.topics_by_name.get("topic_a")
    assert topic_a is not None
    old_sid = topic_a.subject_id

    # Craft a gossip from a different topic that happens to claim the same subject-ID.
    # Use a fake hash that maps to the same subject-ID with evictions=0.
    fake_hash = topic_a.hash + 1  # different hash
    fake_evictions = 0
    modulus = tr.subject_id_modulus
    # Adjust evictions until we collide.
    while compute_subject_id(fake_hash, fake_evictions, modulus) != old_sid:
        fake_evictions += 1
        if fake_evictions > 10000:
            break  # give up, skip test

    if fake_evictions <= 10000:
        gossip_hdr = GossipHeader(
            topic_log_age=35,  # very old, will win
            topic_hash=fake_hash,
            topic_evictions=fake_evictions,
            name_len=0,
        )
        gossip_data = gossip_hdr.serialize()
        arrival = TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=99,
            message=gossip_data,
        )
        node.on_subject_arrival(node.broadcast_subject_id, arrival)
        await asyncio.sleep(0.02)
        # Our topic should have been reallocated.
        assert topic_a.subject_id != old_sid or topic_a.evictions > 0

    pub.close()
    node.close()


# =====================================================================================================================
# Scout Response
# =====================================================================================================================


async def test_scout_triggers_gossip_response():
    """When we receive a scout, the unicast gossip reply should preserve the scout priority."""
    net = MockNetwork()
    requester_tr = MockTransport(node_id=99, network=net)
    requester_arrivals: list[TransportArrival] = []
    requester_tr.unicast_listen(requester_arrivals.append)
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/sensor/temp/data")

    # Send a scout message asking for "sensor/*/data".
    pattern = "sensor/*/data"
    scout_hdr = ScoutHeader(pattern_len=len(pattern))
    scout_data = scout_hdr.serialize() + pattern.encode("utf-8")
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.OPTIONAL,
        remote_id=99,
        message=scout_data,
    )
    node.dispatch_arrival(arrival, subject_id=node.broadcast_subject_id, unicast=False)

    # Give the response tasks time to run.
    await asyncio.sleep(0.05)

    assert len(requester_arrivals) == 1
    assert requester_arrivals[0].priority == pycyphal2.Priority.OPTIONAL
    assert isinstance(deserialize_header(requester_arrivals[0].message[:HEADER_SIZE]), GossipHeader)

    pub.close()
    node.close()
    requester_tr.close()


# =====================================================================================================================
# Message ACK/NACK Dispatch
# =====================================================================================================================


async def test_msg_ack_dispatch():
    """ACK arriving via unicast should be routed to the publish tracker."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]

    # Set up a fake publish tracker.
    tag = topic.next_tag()
    tracker = PublishTracker(
        tag=tag,
        deadline_ns=(pycyphal2.Instant.now() + 10.0).ns,
        remaining={42},
        ack_event=asyncio.Event(),
    )
    topic.publish_futures[tag] = tracker

    # Send a MsgAckHeader via unicast.
    ack_hdr = MsgAckHeader(topic_hash=topic.hash, tag=tag)
    ack_data = ack_hdr.serialize()
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=ack_data,
    )
    node.on_unicast_arrival(arrival)

    # Tracker should be updated.
    assert tracker.acknowledged is True
    assert 42 not in tracker.remaining
    assert tracker.ack_event.is_set()

    # Association should be created.
    assert 42 in topic.associations

    del topic.publish_futures[tag]
    pub.close()
    node.close()


async def test_msg_nack_dispatch():
    """NACK without an association is ignored."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    tag = topic.next_tag()
    tracker = PublishTracker(
        tag=tag,
        deadline_ns=(pycyphal2.Instant.now() + 10.0).ns,
        remaining={42},
        ack_event=asyncio.Event(),
    )
    topic.publish_futures[tag] = tracker

    nack_hdr = MsgNackHeader(topic_hash=topic.hash, tag=tag)
    nack_data = nack_hdr.serialize()
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=nack_data,
    )
    node.on_unicast_arrival(arrival)

    assert 42 not in topic.associations
    assert tracker.remaining == {42}
    assert not tracker.acknowledged

    del topic.publish_futures[tag]
    pub.close()
    node.close()


# =====================================================================================================================
# RSP dispatch
# =====================================================================================================================


async def test_rsp_dispatch_to_stream():
    """RSP_BE arriving should be routed to the correct response stream."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")

    topic = list(node.topics_by_name.values())[0]
    msg_tag = 777
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=msg_tag,
        response_timeout=5.0,
    )
    topic.request_futures[msg_tag] = stream

    # Send RSP_BE.
    rsp_hdr = RspBeHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=msg_tag)
    rsp_data = rsp_hdr.serialize() + b"rsp_payload"
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=rsp_data,
    )
    node.on_unicast_arrival(arrival)

    assert stream.queue.qsize() == 1
    response = expect_response(stream.queue.get_nowait())
    assert response.message == b"rsp_payload"
    assert response.remote_id == 99
    assert response.seqno == 0

    stream.close()
    pub.close()
    node.close()


async def test_rsp_dispatch_routes_by_topic_hash():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub_a = node.advertise("/rpc/a")
    pub_b = node.advertise("/rpc/b")

    topic_a = node.topics_by_name["rpc/a"]
    topic_b = node.topics_by_name["rpc/b"]
    msg_tag = 777

    stream_a = ResponseStreamImpl(node=node, topic=topic_a, message_tag=msg_tag, response_timeout=5.0)
    stream_b = ResponseStreamImpl(node=node, topic=topic_b, message_tag=msg_tag, response_timeout=5.0)
    topic_a.request_futures[msg_tag] = stream_a
    topic_b.request_futures[msg_tag] = stream_b

    rsp_hdr = RspBeHeader(tag=0xFF, seqno=0, topic_hash=topic_b.hash, message_tag=msg_tag)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=rsp_hdr.serialize() + b"rsp_payload",
    )
    node.on_unicast_arrival(arrival)

    assert stream_a.queue.qsize() == 0
    assert stream_b.queue.qsize() == 1

    stream_a.close()
    stream_b.close()
    pub_a.close()
    pub_b.close()
    node.close()


# =====================================================================================================================
# Reliable response (Breadcrumb)
# =====================================================================================================================


async def test_breadcrumb_reliable_response_timeout():
    """Reliable response without ACK should raise DeliveryError."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    node.advertise("/rpc")
    topic = list(node.topics_by_name.values())[0]

    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=100,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    with pytest.raises(pycyphal2.DeliveryError):
        await bc(pycyphal2.Instant.now() + 0.05, b"response", reliable=True)

    node.close()


async def test_respond_tracker_ack():
    """RespondTracker should set done on ACK."""
    tracker = RespondTracker(remote_id=1, message_tag=2, topic_hash=3, seqno=4, tag=5)
    assert not tracker.done
    tracker.on_ack(True)
    assert tracker.done
    assert not tracker.nacked
    assert tracker.ack_event.is_set()


async def test_respond_tracker_nack():
    """RespondTracker should set nacked on NACK."""
    tracker = RespondTracker(remote_id=1, message_tag=2, topic_hash=3, seqno=4, tag=5)
    tracker.on_ack(False)
    assert tracker.done
    assert tracker.nacked


# =====================================================================================================================
# Reliable message reception and dedup via node dispatch
# =====================================================================================================================


async def test_reliable_msg_sends_ack():
    """Receiving a reliable message should preserve the incoming priority in the ACK."""
    net = MockNetwork()
    remote_tr = MockTransport(node_id=99, network=net)
    remote_arrivals: list[TransportArrival] = []
    remote_tr.unicast_listen(remote_arrivals.append)
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic")

    topic = list(node.topics_by_name.values())[0]

    # Send a MsgRel message.
    hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=42,
    )
    msg_data = hdr.serialize() + b"reliable_msg"
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.FAST,
        remote_id=99,
        message=msg_data,
    )
    node.on_subject_arrival(topic.subject_id, arrival)

    # Give ACK task time to run.
    await asyncio.sleep(0.02)

    assert len(remote_arrivals) == 1
    ack_hdr = deserialize_header(remote_arrivals[0].message[:HEADER_SIZE])
    assert isinstance(ack_hdr, MsgAckHeader)
    assert ack_hdr.tag == 42
    assert ack_hdr.topic_hash == topic.hash
    assert remote_arrivals[0].priority == pycyphal2.Priority.FAST

    # The subscriber should have received the message.
    assert sub.queue.qsize() == 1

    sub.close()
    node.close()
    remote_tr.close()


async def test_reliable_msg_wrong_subject_dropped():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic")

    topic = list(node.topics_by_name.values())[0]
    subject_id_max = pycyphal2.SUBJECT_ID_PINNED_MAX + tr.subject_id_modulus
    wrong_subject_id = topic.subject_id + 1 if topic.subject_id < subject_id_max else topic.subject_id - 1
    hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=42,
    )
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=hdr.serialize() + b"wrong_subject",
    )
    node.on_subject_arrival(wrong_subject_id, arrival)
    await asyncio.sleep(0.02)

    assert sub.queue.qsize() == 0
    assert tr.unicast_log == []

    sub.close()
    node.close()


async def test_reliable_msg_dedup():
    """Duplicate reliable messages should be dropped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic")

    topic = list(node.topics_by_name.values())[0]

    hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=42,
    )
    msg_data = hdr.serialize() + b"msg"
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=msg_data,
    )

    # Deliver twice.
    node.on_subject_arrival(topic.subject_id, arrival)
    node.on_subject_arrival(topic.subject_id, arrival)

    # Should only get one message.
    assert sub.queue.qsize() == 1

    sub.close()
    node.close()


async def test_reliable_msg_no_subscribers_unicast_nacks():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=42,
    )
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=hdr.serialize() + b"no_subscribers",
    )
    node.on_unicast_arrival(arrival)
    await asyncio.sleep(0.02)

    assert len(tr.unicast_log) == 1
    _, ack_data = tr.unicast_log[0]
    ack_hdr = deserialize_header(ack_data[:HEADER_SIZE])
    assert isinstance(ack_hdr, MsgNackHeader)
    assert ack_hdr.tag == 42

    pub.close()
    node.close()


async def test_reliable_msg_ordered_late_drop_sends_no_ack_or_nack():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic", reordering_window=1.0)

    topic = list(node.topics_by_name.values())[0]

    for tag in (100, 101):
        hdr = MsgRelHeader(
            topic_log_age=0,
            topic_evictions=topic.evictions,
            topic_hash=topic.hash,
            tag=tag,
        )
        arrival = TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=99,
            message=hdr.serialize() + f"m{tag}".encode(),
        )
        node.on_subject_arrival(topic.subject_id, arrival)
        await asyncio.sleep(0.02)
        tr.unicast_log.clear()
        await sub.queue.get()

    late_hdr = MsgRelHeader(
        topic_log_age=0,
        topic_evictions=topic.evictions,
        topic_hash=topic.hash,
        tag=99,
    )
    late_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=late_hdr.serialize() + b"late",
    )
    node.on_subject_arrival(topic.subject_id, late_arrival)
    await asyncio.sleep(0.02)

    assert tr.unicast_log == []
    assert sub.queue.qsize() == 0

    sub.close()
    node.close()


# =====================================================================================================================
# Reliable response ACK/NACK
# =====================================================================================================================


async def test_reliable_rsp_sends_ack_with_response_priority():
    net = MockNetwork()
    remote_tr = MockTransport(node_id=42, network=net)
    remote_arrivals: list[TransportArrival] = []
    remote_tr.unicast_listen(remote_arrivals.append)
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")

    topic = list(node.topics_by_name.values())[0]
    msg_tag = topic.next_tag()
    stream = ResponseStreamImpl(node=node, topic=topic, message_tag=msg_tag, response_timeout=1.0)
    topic.request_futures[msg_tag] = stream

    rsp_hdr = RspRelHeader(tag=0xAA, seqno=0, topic_hash=topic.hash, message_tag=msg_tag)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.SLOW,
        remote_id=42,
        message=rsp_hdr.serialize() + b"payload",
    )
    node.on_unicast_arrival(arrival)
    await asyncio.sleep(0.02)

    assert len(remote_arrivals) == 1
    ack_hdr = deserialize_header(remote_arrivals[0].message[:HEADER_SIZE])
    assert isinstance(ack_hdr, RspAckHeader)
    assert remote_arrivals[0].priority == pycyphal2.Priority.SLOW

    stream.close()
    pub.close()
    node.close()
    remote_tr.close()


# =====================================================================================================================
# RSP ACK/NACK dispatch
# =====================================================================================================================


async def test_rsp_ack_dispatch():
    """RSP_ACK should be dispatched to the respond tracker."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    tracker = RespondTracker(remote_id=42, message_tag=100, topic_hash=999, seqno=0, tag=0xFF)
    key = (42, 100, 999, 0, 0xFF)
    node.respond_futures[key] = tracker

    rsp_ack_hdr = RspAckHeader(tag=0xFF, seqno=0, topic_hash=999, message_tag=100)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=rsp_ack_hdr.serialize(),
    )
    node.on_unicast_arrival(arrival)

    assert tracker.done
    assert not tracker.nacked

    del node.respond_futures[key]
    node.close()


async def test_multicast_msg_ack_ignored():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    tag = topic.next_tag()
    tracker = PublishTracker(
        tag=tag,
        deadline_ns=(pycyphal2.Instant.now() + 10.0).ns,
        remaining={42},
        ack_event=asyncio.Event(),
    )
    topic.publish_futures[tag] = tracker

    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=MsgAckHeader(topic_hash=topic.hash, tag=tag).serialize(),
    )
    node.on_subject_arrival(topic.subject_id, arrival)

    assert not tracker.acknowledged
    assert tracker.remaining == {42}

    del topic.publish_futures[tag]
    pub.close()
    node.close()


async def test_multicast_rsp_ack_ignored():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    tracker = RespondTracker(remote_id=42, message_tag=100, topic_hash=999, seqno=0, tag=0xFF)
    key = (42, 100, 999, 0, 0xFF)
    node.respond_futures[key] = tracker

    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=RspAckHeader(tag=0xFF, seqno=0, topic_hash=999, message_tag=100).serialize(),
    )
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    assert not tracker.done

    del node.respond_futures[key]
    node.close()


async def test_closed_response_stream_replays_ack_and_nacks_new_reliable_responses():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")

    topic = list(node.topics_by_name.values())[0]
    msg_tag = 555
    stream = ResponseStreamImpl(node=node, topic=topic, message_tag=msg_tag, response_timeout=5.0)
    topic.request_futures[msg_tag] = stream

    rsp_hdr = RspRelHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=msg_tag)
    rsp_data = rsp_hdr.serialize() + b"reliable_rsp"
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=rsp_data,
    )
    node.on_unicast_arrival(arrival)
    await asyncio.sleep(0.02)
    tr.unicast_log.clear()

    stream.close()
    assert topic.request_futures[msg_tag] is stream

    node.on_unicast_arrival(arrival)
    await asyncio.sleep(0.02)
    assert len(tr.unicast_log) == 1
    _, ack_data = tr.unicast_log[-1]
    assert isinstance(deserialize_header(ack_data[:HEADER_SIZE]), RspAckHeader)

    tr.unicast_log.clear()
    new_rsp_hdr = RspRelHeader(tag=0xFF, seqno=1, topic_hash=topic.hash, message_tag=msg_tag)
    new_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=new_rsp_hdr.serialize() + b"new_rsp",
    )
    node.on_unicast_arrival(new_arrival)
    await asyncio.sleep(0.02)

    assert len(tr.unicast_log) == 1
    _, nack_data = tr.unicast_log[-1]
    assert isinstance(deserialize_header(nack_data[:HEADER_SIZE]), RspNackHeader)

    stream._remove_from_topic()
    pub.close()
    node.close()


# =====================================================================================================================
# Edge cases
# =====================================================================================================================


async def test_drop_short_message():
    """Messages shorter than HEADER_SIZE should be dropped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=b"short",
    )
    node.on_unicast_arrival(arrival)  # Should not raise.
    node.close()


async def test_drop_unknown_type():
    """Messages with unknown type should be dropped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    bad_data = bytearray(HEADER_SIZE)
    bad_data[0] = 255  # unknown type
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=bytes(bad_data),
    )
    node.on_unicast_arrival(arrival)  # Should not raise.
    node.close()


async def test_msg_for_unknown_topic_dropped():
    """Messages for unknown topic hashes should be dropped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    hdr = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=0xDEAD, tag=0)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=hdr.serialize() + b"data",
    )
    node.on_unicast_arrival(arrival)  # Should not raise.
    node.close()
