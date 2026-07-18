"""Regression tests for findings #21 and #22: node close and implicit-topic GC must not orphan or hang an
outstanding response stream, and an open stream / in-flight reliable publish keeps its topic alive."""

from __future__ import annotations

import asyncio

import pytest

import pycyphal2
from pycyphal2._node import Association
from tests.mock_transport import MockNetwork, MockTransport
from tests.typing_helpers import new_node, request_stream


async def test_node_close_unblocks_parked_response_stream() -> None:
    """Node.close() must cancel a library-owned request-publish task and stop a parked iteration, even
    when the stream has an infinite response timeout (finding #21)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 1.0, float("inf"), b"request")
    publish_task = stream._publish_task
    anext_task = asyncio.create_task(stream.__anext__())
    await asyncio.sleep(0)
    assert not anext_task.done()  # Parked with no liveness timeout.

    node.close()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext_task, timeout=1.0)
    assert publish_task is not None
    await asyncio.sleep(0)
    assert publish_task.cancelled() or publish_task.done()


async def test_open_stream_keeps_topic_explicit_until_closed() -> None:
    """An open response stream keeps its topic explicit after the publisher closes, so implicit GC cannot
    destroy it; closing the stream lets the topic become implicit (finding #22)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 1.0, 1.0, b"request")
    pub.close()
    assert topic.is_implicit is False  # The open stream keeps it explicit.

    stream.close()  # Cancels the publish task; its release re-syncs implicitness on the next loop turn.
    for _ in range(50):
        if topic.is_implicit:
            break
        await asyncio.sleep(0.001)
    assert topic.is_implicit is True  # Now it may be GC'd.

    node.close()


async def test_implicit_gc_does_not_destroy_topic_with_open_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """The implicit GC must not reap a topic while a response stream is open, only after it closes."""
    monkeypatch.setattr(pycyphal2._node, "IMPLICIT_TOPIC_TIMEOUT", 0.05)
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 1.0, 1.0, b"request")
    pub.close()
    node.notify_implicit_gc()
    await asyncio.sleep(0.15)
    assert "rpc" in node.topics_by_name  # Still alive: the open stream blocks GC.

    stream.close()
    node.notify_implicit_gc()
    for _ in range(50):
        if "rpc" not in node.topics_by_name:
            break
        await asyncio.sleep(0.01)
    assert "rpc" not in node.topics_by_name  # Reaped once the stream closed.

    node.close()


async def test_zombie_stream_does_not_block_gc() -> None:
    """A closed 'zombie' stream awaiting its dedup-cleanup timer must not keep the topic explicit, and
    destroy_topic clears request_futures."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 1.0, 1.0, b"request")
    # Populate a reliable remote so close() retains a zombie (scheduled cleanup rather than immediate).
    from pycyphal2._publisher import ResponseRemoteState

    stream._reliable_remote_by_id[7] = ResponseRemoteState(seqno_top=0)
    stream.close()  # Cancels the publish task; its release runs on the next loop turn.
    assert topic.request_futures  # The zombie is retained pending its cleanup timer...
    pub.close()
    for _ in range(50):
        if topic.is_implicit:
            break
        await asyncio.sleep(0.001)
    assert topic.is_implicit is True  # ...but a closed stream does not keep the topic explicit.

    node.destroy_topic("rpc")
    assert topic.request_futures == {}  # destroy_topic clears it.
    node.close()


async def test_request_rejects_nan_response_timeout() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    with pytest.raises(ValueError, match="response_timeout"):
        await pub.request(pycyphal2.Instant.now() + 1.0, float("nan"), b"request")
    with pytest.raises(ValueError, match="response_timeout"):
        await pub.request(pycyphal2.Instant.now() + 1.0, -1.0, b"request")
    pub.close()
    node.close()


async def test_pending_reliable_publish_keeps_topic_explicit() -> None:
    """An in-flight reliable publish (tracked in publish_futures) keeps the topic explicit even with no
    publisher, so implicit GC cannot destroy it mid-delivery (Codex D6 addition to finding #22)."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]

    from pycyphal2._node import PublishTracker

    topic.publish_futures[99] = PublishTracker(tag=99, ack_event=asyncio.Event())
    pub.close()
    assert topic.is_implicit is False  # The pending publish keeps it explicit.

    topic.publish_futures.clear()
    topic.sync_implicit()
    assert topic.is_implicit is True

    node.close()


async def test_close_does_not_resurrect_topic_via_disposed_stream_tail() -> None:
    """close() disposes response streams, which cancels each publish task; that task's finally clause runs
    on a LATER loop iteration -- after the transport is closed and the gossip tasks are cancelled -- and
    re-syncs topic implicitness. sync_topic_lifecycle must refuse to act on a closed node, otherwise it
    spawns an uncancellable gossip task on a dead node and calls subject_listen on a closed transport."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")  # Left OPEN, so pub_count keeps the topic explicit across close().
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 5.0, float("inf"), b"request")
    assert stream._publish_task is not None
    listener_creations = dict(tr.subject_listener_creations)

    node.close()
    for _ in range(5):  # Let the cancelled publish task's finally clause run to completion.
        await asyncio.sleep(0)

    assert topic.gossip_task is None  # No gossip task resurrected on a dead node.
    assert tr.subject_listener_creations == listener_creations  # No subject_listen on a closed transport.
    assert not node._implicit_topics  # close() cleared it and the tail did not re-populate it.
