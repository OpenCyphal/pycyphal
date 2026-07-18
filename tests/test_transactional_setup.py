"""Regression tests for finding #11: setup paths must be transactional — a transport failure mid-setup
must not leave unrepairable half-state, and subscribe-path listener failures follow the reference repair
model (logged, retried) rather than raising."""

from __future__ import annotations

import math

import pytest

import pycyphal2
from pycyphal2 import SUBJECT_ID_PINNED_MAX
from pycyphal2._hash import rapidhash
from pycyphal2._header import GossipHeader
from pycyphal2._transport import TransportArrival
from tests.mock_transport import DEFAULT_MODULUS, MockNetwork, MockTransport
from tests.typing_helpers import new_node


def _broadcast_sid(modulus: int = DEFAULT_MODULUS) -> int:
    sid_max = SUBJECT_ID_PINNED_MAX + modulus
    return (1 << (int(math.log2(sid_max)) + 1)) - 1


async def test_node_init_rolls_back_broadcast_writer_on_listen_failure() -> None:
    tr = MockTransport(node_id=1, network=MockNetwork())
    tr.fail_subject_listen.add(_broadcast_sid())
    with pytest.raises(RuntimeError, match="Simulated subject_listen"):
        new_node(tr, home="n")
    assert tr.writers == {}  # The broadcast writer was rolled back, so a retry sees no duplicate.
    assert tr.subject_handlers == {}


async def test_node_init_rolls_back_broadcast_handles_on_unicast_listen_failure() -> None:
    """unicast_listen is the LAST fallible acquisition in the constructor. Both built-in transports
    implement it as a plain assignment, but a third-party one may not -- and nobody closes the transport
    when the constructor raises, so an unguarded failure here strands the broadcast writer and listener
    acquired just above it."""
    tr = MockTransport(node_id=1, network=MockNetwork())

    def failing_unicast_listen(_handler):  # type: ignore[no-untyped-def]
        raise RuntimeError("Simulated unicast_listen failure")

    tr.unicast_listen = failing_unicast_listen  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="Simulated unicast_listen"):
        new_node(tr, home="n")
    assert tr.writers == {}  # Broadcast writer rolled back...
    assert tr.subject_handlers == {}  # ...and so was the broadcast listener.


async def test_advertise_rolls_back_pub_count_on_writer_failure() -> None:
    # Learn the topic's subject-ID from a healthy node (it depends only on name + modulus).
    healthy_tr = MockTransport(node_id=1, network=MockNetwork())
    healthy = new_node(healthy_tr, home="n")
    pub = healthy.advertise("/topic_a")
    topic_sid = healthy.topics_by_name["topic_a"].subject_id(healthy_tr.subject_id_modulus)
    pub.close()
    healthy.close()

    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n")
    tr.fail_subject_advertise.add(topic_sid)
    with pytest.raises(RuntimeError, match="Simulated subject_advertise"):
        node.advertise("/topic_a")

    topic = node.topics_by_name["topic_a"]  # The topic exists but as an ordinary implicit topic.
    assert topic.pub_count == 0
    assert topic.is_implicit
    assert topic.pub_writer is None

    tr.fail_subject_advertise.clear()  # Retry succeeds.
    pub2 = node.advertise("/topic_a")
    assert node.topics_by_name["topic_a"].pub_count == 1
    pub2.close()
    node.close()


async def test_subscribe_listener_failure_is_repaired_not_raised() -> None:
    healthy_tr = MockTransport(node_id=1, network=MockNetwork())
    healthy = new_node(healthy_tr, home="n")
    sub_h = healthy.subscribe("/topic_v")
    topic_sid = healthy.topics_by_name["topic_v"].subject_id(healthy_tr.subject_id_modulus)
    sub_h.close()
    healthy.close()

    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n")
    tr.fail_subject_listen.add(topic_sid)
    sub = node.subscribe("/topic_v")  # Repair model: subscribe does not raise on listener failure.
    topic = node.topics_by_name["topic_v"]
    assert topic.sub_listener is None  # Listener not yet acquired.
    creations_before = tr.subject_listener_creations.get(topic_sid, 0)

    tr.fail_subject_listen.clear()  # The next sync opportunity repairs it.
    topic.sync_listener()
    assert topic.sub_listener is not None
    assert tr.subject_listener_creations.get(topic_sid, 0) == creations_before + 1

    sub.close()
    node.close()


async def test_topic_ensure_rolls_back_on_gossip_shard_failure() -> None:
    healthy_tr = MockTransport(node_id=1, network=MockNetwork())
    healthy = new_node(healthy_tr, home="n")
    shard_sid = healthy.gossip_shard_subject_id(rapidhash("topic_s"))
    healthy.close()

    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n")
    tr.fail_subject_advertise.add(shard_sid)
    with pytest.raises(RuntimeError, match="Simulated subject_advertise"):
        node.advertise("/topic_s")

    assert "topic_s" not in node.topics_by_name  # Fully rolled back, both registries clean.
    assert rapidhash("topic_s") not in node.topics_by_hash

    tr.fail_subject_advertise.clear()  # Retry succeeds.
    pub = node.advertise("/topic_s")
    assert "topic_s" in node.topics_by_name
    pub.close()
    node.close()


async def test_gossip_driven_topic_creation_failure_never_raises() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n")
    sub = node.subscribe("/sensor/>")

    name = "sensor/temp"
    shard_sid = node.gossip_shard_subject_id(rapidhash(name))
    hdr = GossipHeader(topic_log_age=5, topic_hash=rapidhash(name), topic_evictions=0, name_len=len(name))

    def deliver() -> None:
        node.on_subject_arrival(
            node.broadcast_subject_id,
            TransportArrival(
                timestamp=pycyphal2.Instant.now(),
                priority=pycyphal2.Priority.NOMINAL,
                remote_id=99,
                message=hdr.serialize() + name.encode("utf-8"),
            ),
        )

    tr.fail_subject_advertise.add(shard_sid)
    deliver()  # Wire-driven path must not raise even when transport setup fails.
    assert name not in node.topics_by_name

    tr.fail_subject_advertise.clear()  # A later gossip creates the topic.
    deliver()
    assert name in node.topics_by_name

    sub.close()
    node.close()


async def test_subscribe_bad_reordering_window_leaves_no_state() -> None:
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n")
    with pytest.raises(ValueError, match="Reordering window"):
        node.subscribe("/topic_r", reordering_window=-1.0)
    assert "topic_r" not in node.sub_roots_verbatim
    assert "topic_r" not in node.topics_by_name
    node.close()
