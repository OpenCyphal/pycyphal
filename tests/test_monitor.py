"""Tests for Node.monitor()."""

from __future__ import annotations

import logging

import pytest

import pycyphal2
from pycyphal2._hash import rapidhash
from pycyphal2._header import GossipHeader, MsgBeHeader
from pycyphal2._node import TopicImpl
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport
from tests.typing_helpers import new_node


def _make_gossip_arrival(
    *,
    topic_hash: int,
    evictions: int,
    name_bytes: bytes,
    remote_id: int = 42,
) -> TransportArrival:
    hdr = GossipHeader(
        topic_log_age=0,
        topic_hash=topic_hash,
        topic_evictions=evictions,
        name_len=len(name_bytes),
    )
    return TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=remote_id,
        message=hdr.serialize() + name_bytes,
    )


def _deliver_gossip(node: pycyphal2.Node, arrival: TransportArrival, scope: str, *, topic_hash: int) -> None:
    if scope == "broadcast":
        node.on_subject_arrival(node.broadcast_subject_id, arrival)  # type: ignore[attr-defined]
    elif scope == "sharded":
        node.on_subject_arrival(node.gossip_shard_subject_id(topic_hash), arrival)  # type: ignore[attr-defined]
    else:
        assert scope == "unicast"
        node.on_unicast_arrival(arrival)  # type: ignore[attr-defined]


async def test_monitor_registration_close_is_idempotent_and_preserves_other_callbacks() -> None:
    node = new_node(MockTransport(node_id=1), home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    node._cancel_gossip(topic)

    first: list[pycyphal2.Topic] = []
    second: list[pycyphal2.Topic] = []
    stop_first = node.monitor(first.append)
    node.monitor(second.append)

    arrival = _make_gossip_arrival(topic_hash=topic.hash, evictions=topic.evictions, name_bytes=topic.name.encode())
    node.on_subject_arrival(node.broadcast_subject_id, arrival)
    assert first == [topic]
    assert second == [topic]

    stop_first.close()
    stop_first.close()
    node.on_subject_arrival(node.broadcast_subject_id, arrival)
    assert first == [topic]
    assert second == [topic, topic]

    pub.close()
    node.close()


@pytest.mark.parametrize("scope", ["broadcast", "sharded", "unicast"])
async def test_monitor_known_topic_uses_actual_local_topic_for_all_non_inline_scopes(scope: str) -> None:
    node = new_node(MockTransport(node_id=1), home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    node._cancel_gossip(topic)

    received: list[pycyphal2.Topic] = []
    node.monitor(received.append)

    arrival = _make_gossip_arrival(topic_hash=topic.hash, evictions=topic.evictions, name_bytes=topic.name.encode())
    _deliver_gossip(node, arrival, scope, topic_hash=topic.hash)

    assert received == [topic]
    assert received[0] is topic

    pub.close()
    node.close()


async def test_monitor_implicit_topic_creation_reports_local_topic_instead_of_flyweight() -> None:
    node = new_node(MockTransport(node_id=1), home="n1")
    sub = node.subscribe("/sensor/>")

    received: list[pycyphal2.Topic] = []
    node.monitor(received.append)

    name = "sensor/temp"
    topic_hash = rapidhash(name)
    arrival = _make_gossip_arrival(topic_hash=topic_hash, evictions=0, name_bytes=name.encode())
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    topic = node.topics_by_name[name]
    node._cancel_gossip(topic)
    assert received == [topic]
    assert received[0] is topic

    sub.close()
    node.close()


async def test_monitor_unknown_topic_uses_flyweight_with_wire_identity() -> None:
    node = new_node(MockTransport(node_id=1), home="n1")

    received: list[pycyphal2.Topic] = []
    node.monitor(received.append)

    name = "sensor/temp"
    topic_hash = rapidhash(name)
    arrival = _make_gossip_arrival(topic_hash=topic_hash, evictions=0, name_bytes=name.encode())
    node.on_subject_arrival(node.broadcast_subject_id, arrival)

    assert len(received) == 1
    assert not isinstance(received[0], TopicImpl)
    assert received[0].hash == topic_hash
    assert received[0].name == name
    assert received[0].match("sensor/*") == [("temp", 1)]

    node.close()


@pytest.mark.parametrize(
    ("name_bytes", "expected_name"),
    [
        (b"", ""),
        (b"\xff\xfe", b"\xff\xfe".decode("utf-8", errors="replace")),
    ],
)
async def test_monitor_unknown_topic_preserves_decoded_wire_name(name_bytes: bytes, expected_name: str) -> None:
    node = new_node(MockTransport(node_id=1), home="n1")

    received: list[pycyphal2.Topic] = []
    node.monitor(received.append)

    node.on_subject_arrival(
        node.broadcast_subject_id,
        _make_gossip_arrival(topic_hash=0xDEADBEEFCAFEBABE, evictions=3, name_bytes=name_bytes),
    )

    assert len(received) == 1
    assert received[0].hash == 0xDEADBEEFCAFEBABE
    assert received[0].name == expected_name

    node.close()


async def test_monitor_is_not_invoked_for_inline_gossip_on_message_reception() -> None:
    node = new_node(MockTransport(node_id=1), home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    node._cancel_gossip(topic)

    received: list[pycyphal2.Topic] = []
    node.monitor(received.append)

    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=99,
        message=MsgBeHeader(
            topic_log_age=0,
            topic_evictions=topic.evictions,
            topic_hash=topic.hash,
            tag=123,
        ).serialize()
        + b"data",
    )
    node.on_subject_arrival(topic.subject_id, arrival)

    assert received == []

    pub.close()
    node.close()


async def test_monitor_callback_exception_is_logged_and_later_callbacks_still_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    node = new_node(MockTransport(node_id=1), home="n1")
    sub = node.subscribe("/sensor/>")
    received: list[pycyphal2.Topic] = []

    def broken(_topic: pycyphal2.Topic) -> None:
        raise RuntimeError("boom")

    node.monitor(broken)
    node.monitor(received.append)

    name = "sensor/temp"
    topic_hash = rapidhash(name)
    arrival = _make_gossip_arrival(topic_hash=topic_hash, evictions=0, name_bytes=name.encode())

    with caplog.at_level(logging.ERROR, logger="pycyphal2._node"):
        node.on_subject_arrival(node.broadcast_subject_id, arrival)

    topic = node.topics_by_name[name]
    node._cancel_gossip(topic)
    assert received == [topic]
    assert any("monitor() callback failed" in rec.message for rec in caplog.records)

    sub.close()
    node.close()


async def test_monitor_callbacks_are_removed_when_node_is_closed() -> None:
    node = new_node(MockTransport(node_id=1), home="n1")

    received: list[pycyphal2.Topic] = []
    handle = node.monitor(received.append)
    node.close()

    node.on_subject_arrival(
        node.broadcast_subject_id,
        _make_gossip_arrival(topic_hash=0x1234, evictions=0, name_bytes=b"late/topic"),
    )
    handle.close()

    assert received == []
