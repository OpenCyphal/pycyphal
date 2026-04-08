"""Tests for RPC request-response and breadcrumb functionality."""

from __future__ import annotations

import asyncio

import pycyphal2
from pycyphal2._publisher import ResponseStreamImpl
from pycyphal2._subscriber import BreadcrumbImpl
from pycyphal2._header import RspBeHeader, RspRelHeader, HEADER_SIZE
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import new_node


async def test_breadcrumb_best_effort_response():
    """Breadcrumb should send a best-effort response via unicast."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/rpc")
    topic = list(node.topics_by_name.values())[0]

    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=12345,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    assert bc.remote_id == 42
    assert bc.topic is topic
    assert bc.tag == 12345

    deadline = pycyphal2.Instant.now() + 1.0
    await bc(deadline, b"response_data")

    # Unicast should have been sent.
    assert len(tr.unicast_log) == 1
    remote_id, data = tr.unicast_log[0]
    assert remote_id == 42
    assert len(data) >= HEADER_SIZE
    # Verify it's an RSP_BE header (type=4).
    assert data[0] == 4
    assert data[HEADER_SIZE:] == b"response_data"

    pub.close()
    node.close()


async def test_breadcrumb_seqno_increments():
    """Each response should increment the seqno."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/rpc")
    topic = list(node.topics_by_name.values())[0]

    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=100,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    deadline = pycyphal2.Instant.now() + 1.0
    await bc(deadline, b"r0")
    await bc(deadline, b"r1")
    await bc(deadline, b"r2")

    assert len(tr.unicast_log) == 3
    # Parse seqno from each response header.
    for i, (_, data) in enumerate(tr.unicast_log):
        hdr = RspBeHeader.deserialize(data[:HEADER_SIZE])
        assert hdr is not None
        assert hdr.seqno == i

    pub.close()
    node.close()


async def test_breadcrumb_shared_across_subscribers():
    """When shared, a breadcrumb's seqno should be contiguous across multiple subscribers' responses."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/shared")
    topic = list(node.topics_by_name.values())[0]

    # One breadcrumb shared by two "subscribers".
    bc = BreadcrumbImpl(
        node=node,
        remote_id=42,
        topic=topic,
        message_tag=200,
        initial_priority=pycyphal2.Priority.NOMINAL,
    )

    deadline = pycyphal2.Instant.now() + 1.0
    # "Subscriber A" responds.
    await bc(deadline, b"from_A")
    # "Subscriber B" responds.
    await bc(deadline, b"from_B")
    # "Subscriber A" responds again.
    await bc(deadline, b"from_A_2")

    assert len(tr.unicast_log) == 3
    seqnos = []
    for _, data in tr.unicast_log:
        hdr = RspBeHeader.deserialize(data[:HEADER_SIZE])
        assert hdr is not None
        seqnos.append(hdr.seqno)
    assert seqnos == [0, 1, 2]  # Contiguous!

    pub.close()
    node.close()


async def test_response_stream_receives_responses():
    """ResponseStream should receive and yield Response objects."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    # Create a response stream manually (simulating what request() does).
    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=1.0,
    )
    topic.request_futures[message_tag] = stream

    # Simulate an incoming response.
    rsp_hdr = RspBeHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=message_tag)
    rsp_data = rsp_hdr.serialize() + b"response_payload"
    rsp_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=rsp_data,
    )
    stream.on_response(rsp_arrival, rsp_hdr, b"response_payload")

    # Read from the stream.
    response = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
    assert response.remote_id == 42
    assert response.seqno == 0
    assert response.message == b"response_payload"

    stream.close()
    pub.close()
    node.close()


async def test_response_stream_dedup():
    """Best-effort responses are not deduplicated at the session layer."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=1.0,
    )
    topic.request_futures[message_tag] = stream

    rsp_hdr = RspBeHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=message_tag)
    rsp_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=rsp_hdr.serialize() + b"data",
    )

    # Deliver the same response twice.
    stream.on_response(rsp_arrival, rsp_hdr, b"data")
    stream.on_response(rsp_arrival, rsp_hdr, b"data")

    first = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
    second = await asyncio.wait_for(stream.__anext__(), timeout=1.0)
    assert first.seqno == 0
    assert second.seqno == 0
    assert first.message == second.message == b"data"

    stream.close()
    pub.close()
    node.close()


async def test_response_stream_reliable_dedup():
    """Reliable duplicate responses are deduplicated to shield the application from lost ACK retransmits."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=1.0,
    )
    topic.request_futures[message_tag] = stream

    rsp_hdr = RspRelHeader(tag=0xAA, seqno=0, topic_hash=topic.hash, message_tag=message_tag)
    rsp_arrival = TransportArrival(
        timestamp=pycyphal2.Instant.now(),
        priority=pycyphal2.Priority.NOMINAL,
        remote_id=42,
        message=rsp_hdr.serialize() + b"data",
    )

    assert stream.on_response(rsp_arrival, rsp_hdr, b"data")
    assert stream.on_response(rsp_arrival, rsp_hdr, b"data")
    assert stream.queue.qsize() == 1

    stream.close()
    pub.close()
    node.close()


async def test_response_stream_multiple_remotes():
    """Responses from different remotes should all be delivered."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=1.0,
    )
    topic.request_futures[message_tag] = stream

    # Two different remotes respond with seqno=0.
    for remote_id in (10, 20):
        rsp_hdr = RspBeHeader(tag=0xFF, seqno=0, topic_hash=topic.hash, message_tag=message_tag)
        rsp_arrival = TransportArrival(
            timestamp=pycyphal2.Instant.now(),
            priority=pycyphal2.Priority.NOMINAL,
            remote_id=remote_id,
            message=rsp_hdr.serialize() + b"data",
        )
        stream.on_response(rsp_arrival, rsp_hdr, b"data")

    assert stream.queue.qsize() == 2

    stream.close()
    pub.close()
    node.close()


async def test_response_stream_timeout():
    """ResponseStream should raise LivenessError on timeout."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=0.05,
    )
    topic.request_futures[message_tag] = stream

    import pytest

    with pytest.raises(pycyphal2.LivenessError):
        await stream.__anext__()

    stream.close()
    pub.close()
    node.close()


async def test_response_stream_close():
    """Closed stream should raise StopAsyncIteration."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    pub = node.advertise("test/req")
    topic = list(node.topics_by_name.values())[0]

    message_tag = topic.next_tag()
    stream = ResponseStreamImpl(
        node=node,
        topic=topic,
        message_tag=message_tag,
        response_timeout=1.0,
    )
    topic.request_futures[message_tag] = stream

    stream.close()
    import pytest

    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()

    pub.close()
    node.close()
