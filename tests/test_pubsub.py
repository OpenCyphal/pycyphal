from __future__ import annotations

import asyncio
import logging

import pytest

import pycyphal2
from pycyphal2 import Arrival, Error, LivenessError, SendError
from pycyphal2._node import resolve_name
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import new_node, subscribe_impl


async def test_basic_best_effort_pubsub():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"hello")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"hello"

    pub.close()
    sub.close()
    node.close()


async def test_node_operations_after_close_raise():
    """Public node operations reject use after close() instead of mutating a dead node."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")
    node.close()

    with pytest.raises(pycyphal2.ClosedError):
        node.advertise("my/topic")
    with pytest.raises(pycyphal2.ClosedError):
        node.subscribe("my/topic")
    with pytest.raises(pycyphal2.ClosedError):
        node.monitor(lambda _t: None)
    with pytest.raises(pycyphal2.ClosedError):
        node.remap("a=b")
    with pytest.raises(pycyphal2.ClosedError):
        await node.scout("pattern/*")


async def test_node_close_unblocks_pending_subscriber():
    """Closing the node ends a pending `async for` on a subscriber instead of hanging it forever."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")
    sub = node.subscribe("my/topic")
    task = asyncio.create_task(sub.__anext__())
    await asyncio.sleep(0)

    node.close()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(task, timeout=1.0)


async def test_gossip_rejects_malformed_names():
    """A gossiped name that is not a normalized verbatim topic name must not become a local topic.

    A match-all '>' pattern subscriber is registered so that, WITHOUT the name guard, every name below
    would be created -- this makes the test fail on the unfixed code rather than passing vacuously (the
    hash matches the name in each case, so only the name guard can reject it).
    """
    from pycyphal2._hash import rapidhash

    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")
    try:
        node.subscribe(">")
        # Control: a valid normalized name IS created through this subscriber, proving the path is live.
        good = node.topic_subscribe_if_matching("sensor/temp", rapidhash("sensor/temp"), 0, 0, 0.0)
        assert good is not None

        for bad_name in ["foo bar", "foo//bar", "/foo", "foo/", "foo/*", "foo/>", "foo#123", "~/foo", "~", ""]:
            result = node.topic_subscribe_if_matching(bad_name, rapidhash(bad_name), 0, 0, 0.0)
            assert result is None, bad_name
            assert bad_name not in node.topics_by_name
    finally:
        node.close()


async def test_publish_multiple_messages():
    """Multiple messages arrive in order."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    for i in range(5):
        await pub(pycyphal2.Instant.now() + 1.0, f"msg{i}".encode())

    for i in range(5):
        arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        assert arrival.message == f"msg{i}".encode()

    pub.close()
    sub.close()
    node.close()


async def test_publish_empty_message():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b""

    pub.close()
    sub.close()
    node.close()


async def test_arrival_has_breadcrumb():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"data")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.breadcrumb is not None
    assert arrival.breadcrumb.remote_id == 1
    assert arrival.breadcrumb.topic.name is not None
    assert isinstance(arrival.breadcrumb.tag, int)

    pub.close()
    sub.close()
    node.close()


async def test_multiple_subscribers_same_topic():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("shared/topic")
    sub1 = node.subscribe("shared/topic")
    sub2 = node.subscribe("shared/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"broadcast")

    arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
    arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
    assert arr1.message == b"broadcast"
    assert arr2.message == b"broadcast"

    pub.close()
    sub1.close()
    sub2.close()
    node.close()


async def test_multiple_subscribers_independent_queues():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("shared/topic")
    sub1 = node.subscribe("shared/topic")
    sub2 = node.subscribe("shared/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"msg1")
    await pub(pycyphal2.Instant.now() + 1.0, b"msg2")

    arr1a = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
    arr1b = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
    assert arr1a.message == b"msg1"
    assert arr1b.message == b"msg2"

    arr2a = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
    arr2b = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
    assert arr2a.message == b"msg1"
    assert arr2b.message == b"msg2"

    pub.close()
    sub1.close()
    sub2.close()
    node.close()


async def test_pattern_subscriber_star():
    """A '*' subscriber matches a single segment in that position."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("~/sensor/data")
    sub = node.subscribe("test_node/*/data")

    await pub(pycyphal2.Instant.now() + 1.0, b"reading")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"reading"

    pub.close()
    sub.close()
    node.close()


async def test_pattern_subscriber_chevron():
    """A trailing '>' subscriber matches all remaining segments."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("~/deep/nested/topic")
    sub = node.subscribe("test_node/>")

    await pub(pycyphal2.Instant.now() + 1.0, b"deep_msg")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"deep_msg"

    pub.close()
    sub.close()
    node.close()


async def test_pattern_subscriber_no_match():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("other_prefix/*/data")
    pub = node.advertise("~/sensor/data")

    await pub(pycyphal2.Instant.now() + 1.0, b"no_match")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sub.__anext__(), timeout=0.05)

    pub.close()
    sub.close()
    node.close()


async def test_pattern_subscriber_substitutions():
    """substitutions() reports which topic segments a wildcard captured."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("test_node/*/data")
    pub = node.advertise("~/sensor/data")

    resolved, _, _ = resolve_name("~/sensor/data", "test_node", "")
    topic = node.topics_by_name[resolved]
    result = sub.substitutions(topic)
    assert result is not None
    assert len(result) == 1
    assert result[0][0] == "sensor"

    pub.close()
    sub.close()
    node.close()


async def test_pattern_subscriber_verbatim_flag():
    """A subscriber is verbatim iff it has no wildcards."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub_verbatim = node.subscribe("test_node/exact")
    sub_pattern = node.subscribe("test_node/*")

    assert sub_verbatim.verbatim is True
    assert sub_pattern.verbatim is False

    sub_verbatim.close()
    sub_pattern.close()
    node.close()


async def test_subscriber_timeout_raises_liveness_error():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    sub.timeout = 0.05

    with pytest.raises(LivenessError):
        await sub.__anext__()

    sub.close()
    node.close()


async def test_subscriber_timeout_default_infinite():
    """The default timeout is infinite, so no LivenessError fires."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    assert sub.timeout == float("inf")

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sub.__anext__(), timeout=0.05)

    sub.close()
    node.close()


async def test_subscriber_timeout_resets_on_message():
    """The liveness timeout applies per call, so a received message does not disarm it for the next."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")
    sub.timeout = 0.5

    await pub(pycyphal2.Instant.now() + 1.0, b"ok")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"ok"

    with pytest.raises(LivenessError):
        await sub.__anext__()

    pub.close()
    sub.close()
    node.close()


async def test_publisher_close_decrements_pub_count():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    topic = node.topics_by_name["my/topic"]
    assert topic.pub_count == 1

    pub.close()
    assert topic.pub_count == 0

    node.close()


async def test_publisher_close_idempotent():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    topic = node.topics_by_name["my/topic"]
    pub.close()
    assert topic.pub_count == 0

    pub.close()
    assert topic.pub_count == 0

    node.close()


async def test_publisher_closed_rejects_publish():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    pub.close()

    with pytest.raises(SendError):
        await pub(pycyphal2.Instant.now() + 1.0, b"fail")

    node.close()


async def test_publisher_close_topic_becomes_implicit():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    topic = node.topics_by_name["my/topic"]
    assert not topic.is_implicit

    pub.close()
    assert topic.is_implicit

    node.close()


async def test_subscriber_close_removes_from_root():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    root = node.sub_roots_verbatim[resolved]
    assert len(root.subscribers) == 1
    assert sub in root.subscribers

    sub.close()
    assert sub not in root.subscribers

    node.close()


async def test_subscriber_close_cleans_up_empty_root():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    assert resolved in node.sub_roots_verbatim

    sub.close()
    assert resolved not in node.sub_roots_verbatim

    node.close()


async def test_subscriber_close_idempotent():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    sub.close()
    sub.close()

    node.close()


async def test_subscriber_close_stops_iteration():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    sub.close()

    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()

    node.close()


async def test_subscriber_close_pattern_cleans_up():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("~/sensor/data")
    sub = node.subscribe("test_node/*/data")
    resolved_pattern, _, _ = resolve_name("test_node/*/data", "test_node", "")
    assert resolved_pattern in node.sub_roots_pattern

    resolved_topic, _, _ = resolve_name("~/sensor/data", "test_node", "")
    topic = node.topics_by_name[resolved_topic]
    assert any(c.root.is_pattern for c in topic.couplings)

    sub.close()
    assert resolved_pattern not in node.sub_roots_pattern
    assert not any(c.root.is_pattern for c in topic.couplings)

    pub.close()
    node.close()


async def test_two_node_pubsub():
    """A message published by one node is received by another node on the same network."""
    net = MockNetwork()
    tr1 = MockTransport(node_id=1, network=net)
    tr2 = MockTransport(node_id=2, network=net)
    node1 = new_node(tr1, home="publisher_node")
    node2 = new_node(tr2, home="subscriber_node")

    pub = node1.advertise("shared/topic")
    sub = node2.subscribe("shared/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"cross_node")
    arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
    assert arrival.message == b"cross_node"
    assert arrival.breadcrumb.remote_id == 1

    pub.close()
    sub.close()
    node1.close()
    node2.close()


async def test_publisher_priority():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    assert pub.priority == pycyphal2.Priority.NOMINAL

    pub.priority = pycyphal2.Priority.HIGH
    assert pub.priority == pycyphal2.Priority.HIGH

    pub.close()
    node.close()


async def test_publisher_ack_timeout():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    default_timeout = pub.ack_timeout
    assert default_timeout == pytest.approx(0.016 * (1 << int(pycyphal2.Priority.NOMINAL)))

    pub.ack_timeout = 2.0
    assert pub.ack_timeout == pytest.approx(2.0)

    pub.priority = pycyphal2.Priority.HIGH
    assert pub.ack_timeout == pytest.approx(1.0)

    pub.close()
    node.close()


async def test_subscriber_pattern_property():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    resolved, _, _ = resolve_name("my/topic", "test_node", "")
    assert sub.pattern == resolved

    sub.close()
    node.close()


async def test_listen_sync_callback():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    received: list[Arrival | Error] = []
    task = sub.listen(received.append)

    for i in range(3):
        await pub(pycyphal2.Instant.now() + 1.0, f"msg{i}".encode())
    for _ in range(20):
        if len(received) >= 3:
            break
        await asyncio.sleep(0.01)

    sub.close()
    await asyncio.wait_for(task, timeout=1.0)

    assert len(received) == 3
    assert [r.message for r in received if isinstance(r, Arrival)] == [b"msg0", b"msg1", b"msg2"]

    pub.close()
    node.close()


async def test_listen_async_callback():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    received: list[Arrival | Error] = []

    async def cb(item: Arrival | Error) -> None:
        # A real await between receive and store exercises the await-path.
        await asyncio.sleep(0)
        received.append(item)

    task = sub.listen(cb)

    for i in range(3):
        await pub(pycyphal2.Instant.now() + 1.0, f"msg{i}".encode())
    for _ in range(20):
        if len(received) >= 3:
            break
        await asyncio.sleep(0.01)

    sub.close()
    await asyncio.wait_for(task, timeout=1.0)

    assert len(received) == 3
    assert [r.message for r in received if isinstance(r, Arrival)] == [b"msg0", b"msg1", b"msg2"]

    pub.close()
    node.close()


async def test_listen_liveness_error_delivered_as_value():
    """LivenessError from __anext__ should be delivered to the callback; the loop keeps running."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")
    sub.timeout = 0.03

    received: list[Arrival | Error] = []
    task = sub.listen(received.append)

    # Give the loop time to fire at least one LivenessError before any message arrives.
    await asyncio.sleep(0.1)
    await pub(pycyphal2.Instant.now() + 1.0, b"after_timeout")
    for _ in range(20):
        if any(isinstance(r, Arrival) for r in received):
            break
        await asyncio.sleep(0.01)

    sub.close()
    await asyncio.wait_for(task, timeout=1.0)

    assert any(isinstance(r, LivenessError) for r in received)
    assert any(isinstance(r, Arrival) and r.message == b"after_timeout" for r in received)
    assert task.exception() is None

    pub.close()
    node.close()


async def test_listen_non_error_exception_fails_task(caplog: pytest.LogCaptureFixture) -> None:
    """A non-Error exception from __anext__ should propagate out and fail the task."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = subscribe_impl(node, "my/topic")

    received: list[Arrival | Error] = []
    with caplog.at_level(logging.ERROR, logger="pycyphal2._api"):
        task = sub.listen(received.append)
        # Inject a non-Error exception into the receive queue; __anext__ will re-raise it.
        sub.queue.put_nowait(OSError("boom"))
        with pytest.raises(OSError, match="boom"):
            await asyncio.wait_for(task, timeout=1.0)

    assert received == []
    assert any("terminated" in rec.message for rec in caplog.records)

    sub.close()
    node.close()


async def test_listen_task_cancellation():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    received: list[Arrival | Error] = []
    task = sub.listen(received.append)

    await asyncio.sleep(0.01)
    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    assert task.cancelled()

    sub.close()
    node.close()


async def test_listen_close_stops_task_cleanly():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    sub = node.subscribe("my/topic")
    task = sub.listen(lambda _item: None)

    await asyncio.sleep(0.01)
    sub.close()
    await asyncio.wait_for(task, timeout=1.0)
    assert task.done()
    assert task.exception() is None

    node.close()


async def test_listen_callback_exception_fails_task(caplog: pytest.LogCaptureFixture) -> None:
    """A callback that raises fails the task, and the error is logged."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="test_node")

    pub = node.advertise("my/topic")
    sub = node.subscribe("my/topic")

    def cb(_item: Arrival | Error) -> None:
        raise ValueError("callback bug")

    with caplog.at_level(logging.ERROR, logger="pycyphal2._api"):
        task = sub.listen(cb)
        await pub(pycyphal2.Instant.now() + 1.0, b"trigger")
        with pytest.raises(ValueError, match="callback bug"):
            await asyncio.wait_for(task, timeout=1.0)

    assert any("terminated" in rec.message for rec in caplog.records)

    pub.close()
    sub.close()
    node.close()
