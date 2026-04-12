"""Tests for gossip protocol, implicit topics, topic destroy, and shard subject IDs."""

from __future__ import annotations

import asyncio
import time

import pycyphal2
from pycyphal2._node import (
    compute_subject_id,
)
from pycyphal2._header import GossipHeader, MsgRelHeader
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import expect_arrival, expect_mock_writer, new_node, subscribe_impl


async def test_gossip_shard_subject_id():
    """Gossip shard subject-ID should be computed correctly."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    # The shard SID should be between (PINNED_MAX + modulus + 1) and broadcast_sid.
    modulus = tr.subject_id_modulus
    sid_max = 0x1FFF + modulus
    for test_hash in [0, 1, 12345, 0xDEADBEEF]:
        shard_sid = node.gossip_shard_subject_id(test_hash)
        assert shard_sid > sid_max
        assert shard_sid < node.broadcast_subject_id

    node.close()


async def test_ensure_gossip_shard_creates_writer():
    """_ensure_gossip_shard should create writer and listener on first call."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    shard_sid = node.gossip_shard_subject_id(12345)
    assert shard_sid not in node.gossip_shard_writers

    writer = node.ensure_gossip_shard(shard_sid)
    assert shard_sid in node.gossip_shard_writers
    assert shard_sid in node.gossip_shard_listeners

    # Second call should return the same writer.
    writer2 = node.ensure_gossip_shard(shard_sid)
    assert writer is writer2

    node.close()


async def test_send_gossip_sharded():
    """Gossip sent non-broadcast should use the shard writer."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    await node.send_gossip(topic, broadcast=False)

    # A shard writer should have been created.
    shard_sid = node.gossip_shard_subject_id(topic.hash)
    assert shard_sid in node.gossip_shard_writers
    writer = expect_mock_writer(node.gossip_shard_writers[shard_sid])
    assert writer.send_count > 0

    pub.close()
    node.close()


async def test_topic_creation_sets_up_gossip_shard_listener():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    shard_sid = node.gossip_shard_subject_id(topic.hash)
    assert shard_sid in node.gossip_shard_writers
    assert shard_sid in node.gossip_shard_listeners

    pub.close()
    node.close()


async def test_send_gossip_broadcast():
    """Gossip sent broadcast should use the broadcast writer."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    broadcast_writer = tr.writers.get(node.broadcast_subject_id)
    initial_count = broadcast_writer.send_count if broadcast_writer else 0

    await node.send_gossip(topic, broadcast=True)

    broadcast_writer = tr.writers.get(node.broadcast_subject_id)
    assert broadcast_writer is not None
    assert broadcast_writer.send_count > initial_count

    pub.close()
    node.close()


async def test_send_gossip_unicast():
    """Gossip unicast should use the transport's unicast method."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    await node.send_gossip_unicast(topic, 42)

    assert len(tr.unicast_log) > 0
    remote_id, data = tr.unicast_log[0]
    assert remote_id == 42
    # Verify it's a gossip header.
    assert data[0] == 8  # GOSSIP type

    pub.close()
    node.close()


async def test_gossip_implicit_topic_creation():
    """Gossip with a name matching a pattern subscriber should create an implicit topic."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    # Subscribe with a pattern.
    sub = node.subscribe("/sensor/>")

    # Send a gossip for a topic matching the pattern.
    topic_name = "sensor/temp"
    from pycyphal2._hash import rapidhash

    topic_hash = rapidhash(topic_name)

    gossip_hdr = GossipHeader(
        topic_log_age=5,
        topic_hash=topic_hash,
        topic_evictions=0,
        name_len=len(topic_name),
    )
    gossip_data = gossip_hdr.serialize() + topic_name.encode("utf-8")
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_data,
    )

    # Deliver as broadcast (which triggers implicit topic creation).
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    # The topic should have been created.
    assert "sensor/temp" in node.topics_by_name
    topic = node.topics_by_name["sensor/temp"]
    assert topic.is_implicit or topic.couplings  # Coupled to the pattern subscriber.

    sub.close()
    node.close()


async def test_implicit_topic_creation_sets_up_gossip_shard_listener():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = node.subscribe("/sensor/>")

    topic_name = "sensor/temp"
    from pycyphal2._hash import rapidhash

    topic_hash = rapidhash(topic_name)
    gossip_hdr = GossipHeader(
        topic_log_age=5,
        topic_hash=topic_hash,
        topic_evictions=0,
        name_len=len(topic_name),
    )
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_hdr.serialize() + topic_name.encode("utf-8"),
    )
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    topic = node.topics_by_name["sensor/temp"]
    shard_sid = node.gossip_shard_subject_id(topic.hash)
    assert shard_sid in node.gossip_shard_writers
    assert shard_sid in node.gossip_shard_listeners

    sub.close()
    node.close()


async def test_gossip_implicit_topic_creation_couples_all_matching_pattern_roots() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub_one = subscribe_impl(node, "/sensor/*")
    sub_any = subscribe_impl(node, "/sensor/>")
    await asyncio.sleep(0)

    topic_name = "sensor/temp"
    from pycyphal2._hash import rapidhash

    topic_hash = rapidhash(topic_name)
    gossip_hdr = GossipHeader(
        topic_log_age=5,
        topic_hash=topic_hash,
        topic_evictions=0,
        name_len=len(topic_name),
    )
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_hdr.serialize() + topic_name.encode("utf-8"),
    )
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    topic = node.topics_by_name["sensor/temp"]
    assert {c.root.name for c in topic.couplings} == {"sensor/*", "sensor/>"}

    sub_one.close()
    tr.unicast_log.clear()
    node.on_unicast_arrival(
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=99,
            message=MsgRelHeader(
                topic_log_age=topic.lage(pycyphal2.Instant.now().s),
                topic_evictions=topic.evictions,
                topic_hash=topic.hash,
                tag=topic.next_tag(),
            ).serialize()
            + b"data",
        )
    )
    await asyncio.sleep(0)

    assert expect_arrival(sub_any.queue.get_nowait()).message == b"data"
    assert tr.unicast_log and tr.unicast_log[-1][1][0] == 2

    sub_any.close()
    node.close()


async def test_topic_destroy():
    """_destroy_topic should clean up all state."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/to_destroy")

    topic = node.topics_by_name.get("to_destroy")
    assert topic is not None
    topic_hash = topic.hash
    sid = topic.subject_id(tr.subject_id_modulus)

    pub.close()  # Allow destroy.
    node.destroy_topic("to_destroy")

    assert "to_destroy" not in node.topics_by_name
    assert topic_hash not in node.topics_by_hash
    assert node.topics_by_subject_id.get(sid) is not topic

    node.close()


async def test_gossip_known_same_evictions_suppress():
    """When gossip matches and evictions agree, gossip should be suppressed."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]

    # Send gossip with same evictions and lage.
    now = time.monotonic()
    my_lage = topic.lage(now)
    gossip_hdr = GossipHeader(
        topic_log_age=my_lage,
        topic_hash=topic.hash,
        topic_evictions=topic.evictions,
        name_len=len(topic.name),
    )
    gossip_data = gossip_hdr.serialize() + topic.name.encode("utf-8")
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=gossip_data,
    )
    # Deliver as sharded (not broadcast, not unicast).
    shard_sid = node.gossip_shard_subject_id(topic.hash)
    node.on_subject_arrival(shard_sid, arrival)

    # Should not crash, gossip should be suppressed.
    await asyncio.sleep(0.01)

    pub.close()
    node.close()


async def test_gossip_known_divergence_we_win():
    """When we receive gossip with different evictions and we win, we should urgent-gossip."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")

    topic = list(node.topics_by_name.values())[0]
    old_evictions = topic.evictions

    # Send gossip with lower evictions (we should win because we have same evictions or higher lage).
    gossip_hdr = GossipHeader(
        topic_log_age=-1,  # Very young remote topic.
        topic_hash=topic.hash,
        topic_evictions=old_evictions + 1,  # Different evictions, but our lage is likely >= -1.
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

    pub.close()
    node.close()


async def test_gossip_unknown_no_collision():
    """Gossip for unknown topic with no subject-ID collision should be a no-op."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    # Send gossip for a topic we don't know about and that doesn't collide.
    gossip_hdr = GossipHeader(
        topic_log_age=0,
        topic_hash=0xCAFEBABE,
        topic_evictions=0,
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
    # Should not crash or create topics.
    assert 0xCAFEBABE not in node.topics_by_hash

    node.close()


async def test_topic_collision_during_allocate():
    """Two topics that collide on subject-ID should resolve via CRDT."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    pub_a = node.advertise("/topic_alpha")
    topic_a = node.topics_by_name["topic_alpha"]
    sid_a = topic_a.subject_id(tr.subject_id_modulus)

    # Find a name that collides with topic_a's subject-ID.
    from pycyphal2._hash import rapidhash

    modulus = tr.subject_id_modulus
    for suffix in range(10000):
        name = f"collision_{suffix}"
        h = rapidhash(name)
        if compute_subject_id(h, 0, modulus) == sid_a:
            # Found a collision!
            pub_b = node.advertise(f"/{name}")
            topic_b = node.topics_by_name[name]
            # One of them should have been reallocated.
            assert topic_a.subject_id(tr.subject_id_modulus) != topic_b.subject_id(tr.subject_id_modulus)
            pub_b.close()
            break

    pub_a.close()
    node.close()


async def test_rsp_ack_sent_for_reliable_response():
    """When a reliable response (RSP_REL) arrives, an RSP_ACK should be sent."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")

    topic = list(node.topics_by_name.values())[0]
    from pycyphal2._publisher import ResponseStreamImpl

    msg_tag = 555
    stream = ResponseStreamImpl(node=node, topic=topic, message_tag=msg_tag, response_timeout=5.0)
    topic.request_futures[msg_tag] = stream

    # Send RSP_REL (reliable response).
    from pycyphal2._header import RspRelHeader

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

    # An RSP_ACK should have been sent.
    assert len(tr.unicast_log) > 0
    _, ack_data = tr.unicast_log[0]
    assert ack_data[0] == 6  # RSP_ACK type

    stream.close()
    pub.close()
    node.close()
