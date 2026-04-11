"""Additional semantic coverage tests aligned with the reference C implementation."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

import pycyphal2
from pycyphal2._hash import rapidhash
from pycyphal2._header import (
    HEADER_SIZE,
    GossipHeader,
    MsgAckHeader,
    MsgNackHeader,
    RspAckHeader,
    RspBeHeader,
    RspNackHeader,
    RspRelHeader,
    ScoutHeader,
    deserialize_header,
)
from pycyphal2._node import (
    ASSOC_SLACK_LIMIT,
    IMPLICIT_TOPIC_TIMEOUT,
    REORDERING_CAPACITY,
    SESSION_LIFETIME,
    Association,
    DedupState,
)
from pycyphal2._publisher import REQUEST_FUTURE_HISTORY, ResponseRemoteState, ResponseStreamImpl
from pycyphal2._subscriber import BreadcrumbImpl
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockNetwork, MockTransport
from tests.typing_helpers import advertise_impl, expect_mock_writer, new_node, request_stream, subscribe_impl


class _FailingWriter(pycyphal2.SubjectWriter):
    async def __call__(
        self,
        deadline: pycyphal2.Instant,
        priority: pycyphal2.Priority,
        message: bytes | memoryview,
    ) -> None:
        del deadline, priority, message
        raise OSError("synthetic failure")

    def close(self) -> None:
        pass


def test_gossip_header_reserved_u32_rejected() -> None:
    buf = bytearray(GossipHeader(topic_log_age=0, topic_hash=0, topic_evictions=0, name_len=0).serialize())
    buf[4] = 1
    assert GossipHeader.deserialize(bytes(buf)) is None


def test_response_remote_state_history_rollover_and_lookup() -> None:
    state = ResponseRemoteState(seqno_top=10)

    assert state.accept(10) == (True, False)
    assert state.accept(9) == (True, True)
    assert state.accept(9) == (True, False)
    assert not state.accepted_earlier(11)

    assert state.accept(10 + REQUEST_FUTURE_HISTORY) == (True, True)
    assert not state.accepted_earlier(10)
    assert state.accept(10) == (False, False)


async def test_request_closed_publisher_raises_send_error() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.close()

    with pytest.raises(pycyphal2.SendError):
        await pub.request(pycyphal2.Instant.now() + 1.0, 1.0, b"request")

    node.close()


async def test_request_stream_close_cancels_publish_without_queuing_error() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.priority = pycyphal2.Priority.EXCEPTIONAL
    pub.ack_timeout = 0.05
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 1.0, 1.0, b"request")
    assert stream.__aiter__() is stream
    assert len(topic.request_futures) == 1

    stream.close()
    stream.close()
    for _ in range(20):
        if topic.request_futures == {} and topic.associations[42].pending_count == 0:
            break
        await asyncio.sleep(0.001)

    item = stream.queue.get_nowait()
    assert isinstance(item, StopAsyncIteration)
    assert stream.queue.empty()
    assert topic.request_futures == {}
    assert topic.associations[42].slack == 0
    assert topic.associations[42].pending_count == 0

    pub.close()
    node.close()


async def test_response_stream_control_items_raise_through_iterator() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]

    stop_stream = ResponseStreamImpl(node=node, topic=topic, message_tag=1, response_timeout=1.0)
    stop_stream.queue.put_nowait(StopAsyncIteration())
    with pytest.raises(StopAsyncIteration):
        await stop_stream.__anext__()

    error_stream = ResponseStreamImpl(node=node, topic=topic, message_tag=2, response_timeout=1.0)
    error_stream.queue.put_nowait(pycyphal2.DeliveryError("synthetic"))
    with pytest.raises(pycyphal2.DeliveryError):
        await error_stream.__anext__()

    error_stream.on_publish_error(asyncio.CancelledError())
    error_stream.close()
    pub.close()
    node.close()


async def test_request_publish_ack_completes_without_queuing_error() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    pub.priority = pycyphal2.Priority.EXCEPTIONAL
    topic = node.topics_by_name["rpc"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)

    stream = await request_stream(pub, pycyphal2.Instant.now() + 0.5, 0.5, b"request")
    assert stream._publish_task is not None
    await asyncio.sleep(0.02)

    node.on_unicast_arrival(
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=42,
            message=MsgAckHeader(topic_hash=topic.hash, tag=stream._message_tag).serialize(),
        )
    )
    await asyncio.wait_for(stream._publish_task, timeout=1.0)

    assert stream.queue.empty()
    stream.close()
    pub.close()
    node.close()


async def test_response_stream_reliable_history_rollover_and_closed_best_effort_drop() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]
    stream = ResponseStreamImpl(node=node, topic=topic, message_tag=1, response_timeout=1.0)

    seq0 = RspRelHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=1)
    arrival0 = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=seq0.serialize() + b"first",
    )
    assert stream.on_response(arrival0, seq0, b"first")

    seq_far = RspRelHeader(tag=0xFF, seqno=REQUEST_FUTURE_HISTORY, topic_hash=topic.hash, message_tag=1)
    arrival_far = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=seq_far.serialize() + b"far",
    )
    assert stream.on_response(arrival_far, seq_far, b"far")
    assert stream.on_response(arrival_far, seq_far, b"far")
    assert not stream.on_response(arrival0, seq0, b"first")

    stream.close()
    best_effort = RspBeHeader(tag=0xFF, seqno=1, topic_hash=topic.hash, message_tag=1)
    assert not stream.on_response(arrival0, best_effort, b"ignored")

    pub.close()
    node.close()


async def test_prepare_publish_tracker_skips_saturated_associations_and_release_forgets_lost_one() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = advertise_impl(node, "/topic")
    topic = node.topics_by_name["topic"]

    live = Association(remote_id=10, last_seen=0.0, slack=ASSOC_SLACK_LIMIT - 1)
    saturated = Association(remote_id=11, last_seen=0.0, slack=ASSOC_SLACK_LIMIT)
    topic.associations = {10: live, 11: saturated}

    tag = topic.next_tag()
    tracker = node.prepare_publish_tracker(topic, tag, (pycyphal2.Instant.now() + 1.0).ns, b"data")

    assert tracker.remaining == {10}
    assert tracker.associations == [live]
    assert live.pending_count == 1
    assert saturated.pending_count == 0

    node.publish_tracker_release(topic, tracker)

    assert 10 not in topic.associations
    assert 11 in topic.associations
    assert tracker.associations == []
    assert tracker.remaining == set()

    pub.close()
    node.close()


async def test_publish_tracker_release_compromised_does_not_penalize_association() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]

    assoc = Association(remote_id=10, last_seen=0.0, slack=ASSOC_SLACK_LIMIT - 1)
    topic.associations = {10: assoc}
    tag = topic.next_tag()
    tracker = node.prepare_publish_tracker(topic, tag, (pycyphal2.Instant.now() + 1.0).ns, b"data")
    tracker.compromised = True

    node.publish_tracker_release(topic, tracker)

    assert topic.associations[10].slack == ASSOC_SLACK_LIMIT - 1
    assert topic.associations[10].pending_count == 0

    pub.close()
    node.close()


async def test_reliable_publish_scheduler_lag_does_not_penalize_association() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = advertise_impl(node, "/topic")
    topic = node.topics_by_name["topic"]

    assoc = Association(remote_id=10, last_seen=0.0)
    topic.associations = {10: assoc}
    tag = topic.next_tag()
    deadline = pycyphal2.Instant(ns=1_000_000_000)
    tracker = pub._prepare_reliable_publish_tracker(tag, deadline.ns, b"data")
    tracker.ack_timeout = 0.2

    now_ns = 0
    wait_count = 0

    async def fake_wait_for(awaitable: object, timeout: float) -> None:
        nonlocal now_ns, wait_count
        del timeout
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        wait_count += 1
        now_ns = 800_000_000 if wait_count == 1 else deadline.ns
        raise asyncio.TimeoutError

    async def fake_send(*_: object, **__: object) -> None:
        return None

    def fake_now() -> pycyphal2.Instant:
        return pycyphal2.Instant(ns=now_ns)

    with patch("pycyphal2._publisher.Instant.now", side_effect=fake_now):
        with patch("pycyphal2._publisher.asyncio.wait_for", side_effect=fake_wait_for):
            with patch.object(pub, "_send_reliable_publish", side_effect=fake_send):
                with pytest.raises(pycyphal2.DeliveryError):
                    await pub._reliable_publish_continue(deadline, tag, b"data", tracker, (200_000_000, False))

    pub._release_reliable_publish_tracker(tag, tracker)

    assert assoc.slack == 0
    assert assoc.pending_count == 0

    pub.close()
    node.close()


async def test_gossip_control_send_failures_are_swallowed() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]

    node.broadcast_writer = _FailingWriter()
    await node.send_gossip(topic, broadcast=True)

    shard_sid = node.gossip_shard_subject_id(topic.hash)
    node.gossip_shard_writers[shard_sid] = _FailingWriter()
    await node.send_gossip(topic, broadcast=False)

    async def bad_unicast(
        deadline: pycyphal2.Instant,
        priority: pycyphal2.Priority,
        remote_id: int,
        message: bytes | memoryview,
    ) -> None:
        del deadline, priority, remote_id, message
        raise OSError("synthetic failure")

    tr.unicast = bad_unicast  # type: ignore[assignment]
    await node.send_gossip_unicast(topic, 42)
    with pytest.raises(pycyphal2.SendError):
        await node.scout("topic")
    await asyncio.sleep(0.02)

    pub.close()
    node.close()


async def test_pattern_root_scout_sent_once_after_success() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    first = subscribe_impl(node, "/sensor/>")
    await asyncio.sleep(0.02)
    root = node.sub_roots_pattern["sensor/>"]
    writer = expect_mock_writer(node.broadcast_writer)
    assert writer.send_count == 1
    assert not root.needs_scouting
    assert root.scout_task is None

    second = subscribe_impl(node, "/sensor/>")
    await asyncio.sleep(0.02)
    assert writer.send_count == 1
    assert not root.needs_scouting
    assert root.scout_task is None

    first.close()
    second.close()
    node.close()


async def test_pattern_root_scout_retried_after_failure() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    writer = expect_mock_writer(node.broadcast_writer)
    writer.fail_next = True

    first = subscribe_impl(node, "/sensor/>")
    await asyncio.sleep(0.02)
    root = node.sub_roots_pattern["sensor/>"]
    assert root.needs_scouting
    assert root.scout_task is None
    assert writer.send_count == 0

    second = subscribe_impl(node, "/sensor/>")
    await asyncio.sleep(0.02)
    assert not root.needs_scouting
    assert root.scout_task is None
    assert writer.send_count == 1

    first.close()
    second.close()
    node.close()


async def test_invalid_gossip_and_scout_payloads_are_ignored() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    subscribe_impl(node, "/sensor/>")

    invalid_gossip = GossipHeader(topic_log_age=0, topic_hash=0xDEAD, topic_evictions=0, name_len=2)
    gossip_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=invalid_gossip.serialize() + b"x",
    )
    node.on_subject_arrival(node.broadcast_subject_id, gossip_arrival)

    invalid_scout = ScoutHeader(pattern_len=3)
    scout_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=invalid_scout.serialize() + b"x",
    )
    node.on_subject_arrival(node.broadcast_subject_id, scout_arrival)
    await asyncio.sleep(0.02)

    assert "sensor/temp" not in node.topics_by_name
    assert tr.unicast_log == []

    node.close()


async def test_accept_message_without_subscribers_cleans_stale_dedup_state() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]

    topic.dedup[42] = DedupState(tag_frontier=123, bitmap=1, last_active=0.0)
    arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now() + SESSION_LIFETIME + 1.0,
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=b"",
    )

    assert not node.accept_message(topic, arrival, 123, b"", reliable=True)
    assert 42 not in topic.dedup

    pub.close()
    node.close()


async def test_idle_nack_forgets_association_and_unknown_ack_is_ignored() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]

    unknown_ack = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=MsgAckHeader(topic_hash=0xDEADBEEF, tag=0).serialize(),
    )
    node.on_unicast_arrival(unknown_ack)

    tag = topic.next_tag()
    topic.associations[42] = Association(remote_id=42, last_seen=0.0, pending_count=0)
    nack_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=MsgNackHeader(topic_hash=topic.hash, tag=tag).serialize(),
    )
    node.on_unicast_arrival(nack_arrival)

    assert 42 not in topic.associations

    pub.close()
    node.close()


async def test_unknown_reliable_response_is_nacked_and_rsp_ack_without_future_is_ignored() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    rsp_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=RspRelHeader(tag=0xFF, seqno=0, topic_hash=0xDEAD, message_tag=1).serialize() + b"payload",
    )
    node.on_unicast_arrival(rsp_arrival)
    await asyncio.sleep(0.02)

    assert len(tr.unicast_log) == 1
    _, ack_data = tr.unicast_log[-1]
    assert isinstance(deserialize_header(ack_data[:HEADER_SIZE]), RspNackHeader)

    ack_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=RspAckHeader(tag=0xFF, seqno=0, topic_hash=0xDEAD, message_tag=1).serialize(),
    )
    node.on_unicast_arrival(ack_arrival)

    node.close()


async def test_sharded_gossip_does_not_create_implicit_topics_and_hash_mismatch_is_rejected() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    subscribe_impl(node, "/sensor/>")

    name = "sensor/temp"
    topic_hash = rapidhash(name)
    shard_sid = node.gossip_shard_subject_id(topic_hash)
    sharded_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=GossipHeader(topic_log_age=0, topic_hash=topic_hash, topic_evictions=0, name_len=len(name)).serialize()
        + name.encode(),
    )
    node.on_subject_arrival(shard_sid, sharded_arrival)

    assert name not in node.topics_by_name
    assert node.topic_subscribe_if_matching(name, topic_hash + 1, 0, 0, time.monotonic()) is None

    other = new_node(MockTransport(node_id=2, network=net), home="n2")
    assert other.topic_subscribe_if_matching(name, topic_hash, 0, 0, time.monotonic()) is None

    node.close()
    other.close()


async def test_middle_chevron_scout_is_literal_and_matches_nothing() -> None:
    net = MockNetwork()
    requester = MockTransport(node_id=99, network=net)
    requester_arrivals: list[TransportArrival] = []
    requester.unicast_listen(requester_arrivals.append)
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub_zero = node.advertise("/sensor/data")
    pub_many = node.advertise("/sensor/temp/data")
    pub_miss = node.advertise("/sensor/temp/meta")

    pattern = "sensor/>/data"
    scout_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.OPTIONAL,
        remote_id=99,
        message=ScoutHeader(pattern_len=len(pattern)).serialize() + pattern.encode(),
    )
    node.dispatch_arrival(scout_arrival, subject_id=node.broadcast_subject_id, unicast=False)
    await asyncio.sleep(0.05)

    assert requester_arrivals == []

    pub_zero.close()
    pub_many.close()
    pub_miss.close()
    node.close()
    requester.close()


async def test_middle_chevron_implicit_topic_creation_treats_chevron_literally() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    subscribe_impl(node, "/sensor/>/data")

    zero_name = "sensor/data"
    zero_hash = rapidhash(zero_name)
    assert node.topic_subscribe_if_matching(zero_name, zero_hash, 0, 0, time.monotonic()) is None

    mismatch_name = "sensor/temp/meta"
    mismatch_hash = rapidhash(mismatch_name)
    assert node.topic_subscribe_if_matching(mismatch_name, mismatch_hash, 0, 0, time.monotonic()) is None

    node.close()


async def test_implicit_gc_loop_removes_stale_implicit_topics() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    subscribe_impl(node, "/sensor/>")

    name = "sensor/temp"
    topic_hash = rapidhash(name)
    topic = node.topic_subscribe_if_matching(name, topic_hash, 0, 0, time.monotonic())
    assert topic is not None
    topic.ts_animated = time.monotonic() - IMPLICIT_TOPIC_TIMEOUT - 1.0
    node.notify_implicit_gc()

    for _ in range(100):
        if name not in node.topics_by_name:
            break
        await asyncio.sleep(0.001)

    assert name not in node.topics_by_name
    node.close()


async def test_implicit_gc_prefers_lru_tail_over_oldest_timestamp() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    older_tail = node.topic_ensure("older_tail", None)
    older_tail.ts_animated = 100.0

    pub = node.advertise("/newly_demoted")
    newly_demoted = node.topics_by_name["newly_demoted"]
    newly_demoted.ts_animated = 50.0
    pub.close()

    assert older_tail.is_implicit
    assert newly_demoted.is_implicit
    assert node._retire_one_expired_implicit_topic(1_000.0)
    assert "older_tail" not in node.topics_by_name
    assert "newly_demoted" in node.topics_by_name

    node.close()


def test_destroy_topic_missing_name_is_noop() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)

    async def run() -> None:
        node = new_node(tr, home="n1")
        node.destroy_topic("missing")
        node.close()

    asyncio.run(run())


async def test_subscriber_iterator_control_items_and_closed_delivery() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic")
    topic = node.topics_by_name["topic"]
    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=1,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )
    arrival = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"payload")

    assert sub.__aiter__() is sub
    sub.queue.put_nowait(StopAsyncIteration())
    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()

    sub_err = subscribe_impl(node, "/topic_2")
    sub_err.queue.put_nowait(pycyphal2.DeliveryError("synthetic"))
    with pytest.raises(pycyphal2.DeliveryError):
        await sub_err.__anext__()

    sub.close()
    assert not sub.deliver(arrival, 1, 42)
    sub.close()
    sub_err.close()
    node.close()


async def test_subscriber_wraparound_drop_and_head_of_line_rearm() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "/topic", reordering_window=0.5)
    topic = node.topics_by_name["topic"]
    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=1,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    first = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"first")
    assert sub.deliver(first, 1000, 42)
    assert sub.queue.empty()

    baseline = 1000 - (REORDERING_CAPACITY // 2)
    late = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"late")
    assert not sub.deliver(late, baseline - 1, 42)

    key = (42, topic.hash)
    state = sub._reordering[key]
    first_handle = state.timeout_handle
    assert first_handle is not None

    await asyncio.sleep(0.15)
    gap = pycyphal2.Arrival(timestamp=pycyphal2.Instant.now(), breadcrumb=bc, message=b"gap")
    assert sub.deliver(gap, 999, 42)
    second_handle = state.timeout_handle
    assert second_handle is not None
    assert second_handle is not first_handle

    await asyncio.sleep(0.15)
    assert sub.queue.empty()

    sub.close()
    node.close()


async def test_breadcrumb_reliable_initial_send_failure_raises_send_error() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]
    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=123,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    call_count = 0

    async def flaky_unicast(
        deadline: pycyphal2.Instant,
        priority: pycyphal2.Priority,
        remote_id: int,
        message: bytes | memoryview,
    ) -> None:
        nonlocal call_count
        call_count += 1
        del deadline, priority, remote_id, message
        raise OSError("synthetic failure")

    tr.unicast = flaky_unicast  # type: ignore[assignment]

    with pytest.raises(pycyphal2.SendError):
        await bc(pycyphal2.Instant.now() + 0.2, b"response", reliable=True)

    node.on_unicast_arrival(
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=42,
            message=RspAckHeader(tag=0, seqno=0, topic_hash=topic.hash, message_tag=123).serialize(),
        )
    )

    assert call_count == 1
    assert node.respond_futures == {}
    node.close()


async def test_breadcrumb_reliable_nack_raises() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]
    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=124,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    task = asyncio.create_task(bc(pycyphal2.Instant.now() + 0.2, b"response", reliable=True))
    await asyncio.sleep(0.02)
    tag = next(iter(node.respond_futures.values())).tag
    node.on_unicast_arrival(
        TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=42,
            message=RspNackHeader(tag=tag, seqno=0, topic_hash=topic.hash, message_tag=124).serialize(),
        )
    )

    with pytest.raises(pycyphal2.NackError):
        await task

    assert node.respond_futures == {}
    node.close()


async def test_reliable_publish_initial_attempt_stays_multicast() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    topic.associations[42] = Association(remote_id=42, last_seen=0.0)
    writer = topic.ensure_writer()

    with pytest.raises(pycyphal2.DeliveryError):
        await pub(pycyphal2.Instant.now() + 0.03, b"payload", reliable=True)

    assert len(tr.unicast_log) == 0
    assert expect_mock_writer(writer).send_count > 0

    pub.close()
    node.close()


async def test_breadcrumb_reliable_key_collision_increments_tag() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    node.advertise("/rpc")
    topic = node.topics_by_name["rpc"]

    bc_a = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=123,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )
    bc_b = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=123,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    task_a = asyncio.create_task(bc_a(pycyphal2.Instant.now() + 0.2, b"a", reliable=True))
    await asyncio.sleep(0.02)
    tag_a = next(iter(node.respond_futures.values())).tag

    task_b = asyncio.create_task(bc_b(pycyphal2.Instant.now() + 0.2, b"b", reliable=True))
    await asyncio.sleep(0.02)
    tags = {tracker.tag for tracker in node.respond_futures.values()}
    assert tags == {tag_a, tag_a + 1}

    for tracker in list(node.respond_futures.values()):
        node.on_unicast_arrival(
            TransportArrival(
                timestamp=pycyphal2.Instant.now(),
                priority=pycyphal2.Priority.NOMINAL,
                remote_id=42,
                message=RspAckHeader(
                    tag=tracker.tag,
                    seqno=tracker.seqno,
                    topic_hash=tracker.topic_hash,
                    message_tag=tracker.message_tag,
                ).serialize(),
            )
        )

    await task_a
    await task_b
    node.close()


async def test_gossip_scheduler_first_periodic_is_broadcast_and_suppression_delays_next_tick() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    topic = node.topics_by_name["topic"]
    node._cancel_gossip(topic)
    topic.gossip_counter = 10

    with patch("pycyphal2._node.random.uniform", side_effect=lambda a, b: a):
        node._reschedule_gossip_periodic(topic, suppressed=False)
        baseline_deadline = topic.gossip_deadline
        assert baseline_deadline is not None

        node._reschedule_gossip_periodic(topic, suppressed=True)
        suppressed_deadline = topic.gossip_deadline
        assert suppressed_deadline is not None
        assert suppressed_deadline > baseline_deadline

    node._cancel_gossip(topic)
    topic.gossip_counter = 0
    seen: list[bool] = []

    async def fake_send_gossip(_topic: object, *, broadcast: bool = False) -> None:
        seen.append(broadcast)

    node.send_gossip = fake_send_gossip  # type: ignore[assignment]
    await node._gossip_event_periodic(topic)
    assert seen == [True]

    pub.close()
    node.close()


async def test_topic_demotion_cancels_gossip_and_listener_release_tracks_couplings() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("/topic")
    sub = subscribe_impl(node, "/topic")
    topic = node.topics_by_name["topic"]

    assert topic.gossip_task is not None
    assert topic.sub_listener is not None

    sent: list[bool] = []

    async def fake_send_gossip(_topic: object, *, broadcast: bool = False) -> None:
        sent.append(broadcast)

    node.send_gossip = fake_send_gossip  # type: ignore[assignment]

    sub.close()
    assert topic.sub_listener is None
    assert not topic.is_implicit

    pub.close()
    assert topic.is_implicit
    assert topic.gossip_task is None

    await asyncio.sleep(0.02)
    assert sent == []

    node.close()
