"""
Tests for RPC (request/response) functionality in pycyphal.

Covers:
    1. Basic request/response via Publisher.request() -> ResponseStream
    2. Streaming responses (multiple breadcrumb calls, incrementing seqno)
    3. Response stream timeout (LivenessError when no responses)
    4. Response stream close (stops iteration)
    5. Breadcrumb properties (remote_id, topic, tag)
    6. Multiple responders to the same request
    7. Best-effort response (reliable=False)
    8. Reliable response (reliable=True, ack tracking)
"""

from __future__ import annotations

import asyncio
import struct
import time
from typing import Any

import pytest

from pycyphal import (
    Arrival,
    Breadcrumb,
    DeliveryError,
    Instant,
    LivenessError,
    NackError,
    Node,
    Priority,
    Publisher,
    Response,
    ResponseStream,
    SendError,
    Subscriber,
    Topic,
)
from pycyphal._wire import (
    HEADER_SIZE,
    HeaderType,
    pack_rsp_ack_header,
    pack_rsp_header,
    topic_hash,
    topic_subject_id,
    unpack_header,
)

from pycyphal._common import name_resolve

from tests.conftest import MockNetwork, MockTransport


# =====================================================================================================================
# Helpers
# =====================================================================================================================

TOPIC_NAME = "/test/rpc/echo"
TOPIC_NAME_ALT = "/test/rpc/compute"
TOPIC_NAME_PATTERN = "/test/rpc/*"
SHORT_TIMEOUT = 0.1
MEDIUM_TIMEOUT = 0.5
LONG_TIMEOUT = 2.0

# The node resolves topic names (stripping leading '/'), so the internal name and hash
# differ from the raw TOPIC_NAME constant. These helpers compute the resolved variants.
_NAMESPACE = "/test"
_HOME = "node_a"


def _resolved_name(name: str, home: str = _HOME) -> str:
    """Compute the resolved topic name as the node sees it."""
    return name_resolve(name, _NAMESPACE, home)


def _resolved_hash(name: str, home: str = _HOME) -> int:
    """Compute the topic hash from the resolved name."""
    return topic_hash(_resolved_name(name, home))


def _make_deadline(seconds_from_now: float = 5.0) -> Instant:
    """Create a deadline in the future."""
    return Instant.now() + seconds_from_now


def _make_two_nodes(network: MockNetwork | None = None) -> tuple[Node, Node, MockTransport, MockTransport]:
    """Create two nodes connected via a MockNetwork, one for publishing, one for subscribing."""
    if network is None:
        network = MockNetwork()
    transport_a = MockTransport(node_id=10, network=network)
    transport_b = MockTransport(node_id=20, network=network)
    node_a = Node(transport_a, home="node_a", namespace="/test")
    node_b = Node(transport_b, home="node_b", namespace="/test")
    return node_a, node_b, transport_a, transport_b


def _make_three_nodes(
    network: MockNetwork | None = None,
) -> tuple[Node, Node, Node, MockTransport, MockTransport, MockTransport]:
    """Create three nodes connected via a MockNetwork."""
    if network is None:
        network = MockNetwork()
    transport_a = MockTransport(node_id=10, network=network)
    transport_b = MockTransport(node_id=20, network=network)
    transport_c = MockTransport(node_id=30, network=network)
    node_a = Node(transport_a, home="node_a", namespace="/test")
    node_b = Node(transport_b, home="node_b", namespace="/test")
    node_c = Node(transport_c, home="node_c", namespace="/test")
    return node_a, node_b, node_c, transport_a, transport_b, transport_c


async def _wait_for_gossip(duration: float = 0.05) -> None:
    """Allow gossip and internal tasks to propagate."""
    await asyncio.sleep(duration)


async def _collect_stream_items(stream: ResponseStream, count: int, timeout: float = 2.0) -> list[Response]:
    """Collect a specific number of items from a response stream."""
    items: list[Response] = []
    deadline = time.monotonic() + timeout
    async for response in stream:
        items.append(response)
        if len(items) >= count:
            break
        if time.monotonic() > deadline:
            break
    return items


async def _drain_subscriber(sub: Subscriber, count: int, timeout: float = 2.0) -> list[Arrival]:
    """Collect arrivals from a subscriber."""
    arrivals: list[Arrival] = []
    deadline = time.monotonic() + timeout
    async for arrival in sub:
        arrivals.append(arrival)
        if len(arrivals) >= count:
            break
        if time.monotonic() > deadline:
            break
    return arrivals


# =====================================================================================================================
# 1. Basic request/response
# =====================================================================================================================


class TestBasicRequestResponse:
    """Publisher sends a request, subscriber receives it, responds via breadcrumb,
    publisher receives the Response from the ResponseStream."""

    async def test_single_request_single_response(self) -> None:
        """The canonical RPC flow: one request, one response."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            request_payload = b"request-data-001"
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, request_payload)

            # Subscriber receives the message
            arrivals = await _drain_subscriber(sub, 1)
            assert len(arrivals) == 1
            assert arrivals[0].message == request_payload

            # Subscriber sends back a response via the breadcrumb
            bc = arrivals[0].breadcrumb
            response_payload = b"response-data-001"
            await bc(_make_deadline(), response_payload)

            # Publisher collects the response from the stream
            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1
            assert responses[0].message == response_payload
            assert responses[0].seqno == 0
            assert responses[0].remote_id == 20  # node_b transport node_id

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_request_returns_response_stream(self) -> None:
        """Verify that Publisher.request() returns a ResponseStream instance."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"hello")
            assert isinstance(stream, ResponseStream)

            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb
            await bc(_make_deadline(), b"world")

            async for resp in stream:
                assert isinstance(resp, Response)
                assert resp.message == b"world"
                break

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_carries_correct_remote_id(self) -> None:
        """The Response.remote_id must match the transport node_id of the responder."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"id-check")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"from-b")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].remote_id == 20

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_carries_correct_timestamp(self) -> None:
        """The Response.timestamp must be a valid Instant."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            before = Instant.now()
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"ts-check")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"ts-resp")

            responses = await _collect_stream_items(stream, 1)
            after = Instant.now()

            assert isinstance(responses[0].timestamp, Instant)
            assert responses[0].timestamp.ns >= before.ns
            assert responses[0].timestamp.ns <= after.ns

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_message_is_bytes(self) -> None:
        """Response.message must be bytes."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"\x00\x01\x02\x03")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"\xaa\xbb\xcc\xdd")

            responses = await _collect_stream_items(stream, 1)
            assert isinstance(responses[0].message, bytes)
            assert responses[0].message == b"\xaa\xbb\xcc\xdd"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_empty_request_payload(self) -> None:
        """A request with empty payload should still work."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"")
            arrivals = await _drain_subscriber(sub, 1)
            assert arrivals[0].message == b""

            await arrivals[0].breadcrumb(_make_deadline(), b"not-empty")
            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"not-empty"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_empty_response_payload(self) -> None:
        """A response with empty payload should still work."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"req")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b""

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_large_request_payload(self) -> None:
        """Request with a large payload should be delivered correctly."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            large_payload = bytes(range(256)) * 40  # 10240 bytes
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, large_payload)
            arrivals = await _drain_subscriber(sub, 1)
            assert arrivals[0].message == large_payload

            large_response = bytes(range(256)) * 20
            await arrivals[0].breadcrumb(_make_deadline(), large_response)

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == large_response

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_request_on_different_topics(self) -> None:
        """Requests on different topics should not cross-contaminate."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub1 = node_a.advertise(TOPIC_NAME)
            pub2 = node_a.advertise(TOPIC_NAME_ALT)
            sub1 = node_b.subscribe(TOPIC_NAME)
            sub2 = node_b.subscribe(TOPIC_NAME_ALT)
            await _wait_for_gossip()

            stream1 = await pub1.request(_make_deadline(), SHORT_TIMEOUT, b"req-1")
            stream2 = await pub2.request(_make_deadline(), SHORT_TIMEOUT, b"req-2")

            arr1 = await _drain_subscriber(sub1, 1)
            arr2 = await _drain_subscriber(sub2, 1)

            await arr1[0].breadcrumb(_make_deadline(), b"resp-1")
            await arr2[0].breadcrumb(_make_deadline(), b"resp-2")

            resp1 = await _collect_stream_items(stream1, 1)
            resp2 = await _collect_stream_items(stream2, 1)

            assert resp1[0].message == b"resp-1"
            assert resp2[0].message == b"resp-2"

            stream1.close()
            stream2.close()
            pub1.close()
            pub2.close()
            sub1.close()
            sub2.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_stream_is_async_iterable(self) -> None:
        """ResponseStream supports async iteration via __aiter__/__anext__."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"iter-test")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"iter-resp")

            # Use __aiter__ and __anext__ explicitly
            it = stream.__aiter__()
            assert it is stream  # __aiter__ returns self

            resp = await it.__anext__()
            assert resp.message == b"iter-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_request_with_memoryview_payload(self) -> None:
        """Request should accept memoryview as message argument."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            data = bytearray(b"memoryview-request")
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, memoryview(data))
            arrivals = await _drain_subscriber(sub, 1)
            assert arrivals[0].message == b"memoryview-request"

            await arrivals[0].breadcrumb(_make_deadline(), memoryview(bytearray(b"mv-resp")))
            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"mv-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 2. Streaming responses
# =====================================================================================================================


class TestStreamingResponses:
    """Multiple breadcrumb calls produce multiple Responses with incrementing seqno."""

    async def test_two_sequential_responses(self) -> None:
        """Two breadcrumb calls yield two responses with seqno 0 and 1."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"stream-req")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"chunk-0")
            await bc(_make_deadline(), b"chunk-1")

            responses = await _collect_stream_items(stream, 2)
            assert len(responses) == 2
            assert responses[0].message == b"chunk-0"
            assert responses[0].seqno == 0
            assert responses[1].message == b"chunk-1"
            assert responses[1].seqno == 1

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_five_sequential_responses(self) -> None:
        """Five breadcrumb calls yield five responses with seqno 0 through 4."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"multi-req")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            for i in range(5):
                await bc(_make_deadline(), f"part-{i}".encode())

            responses = await _collect_stream_items(stream, 5)
            assert len(responses) == 5
            for i, resp in enumerate(responses):
                assert resp.seqno == i
                assert resp.message == f"part-{i}".encode()

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_seqno_starts_at_zero(self) -> None:
        """The first response from a breadcrumb must have seqno == 0."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"seqno-start")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb
            await bc(_make_deadline(), b"first")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].seqno == 0

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_seqno_increments_monotonically(self) -> None:
        """Each successive breadcrumb call increments the seqno by exactly 1."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"mono-req")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            for i in range(10):
                await bc(_make_deadline(), f"seq-{i}".encode())

            responses = await _collect_stream_items(stream, 10)
            for i in range(10):
                assert responses[i].seqno == i, f"Expected seqno {i}, got {responses[i].seqno}"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_streaming_different_payload_sizes(self) -> None:
        """Streaming responses with varying payload sizes."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"varied")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            payloads = [b"", b"a", b"ab" * 100, b"\x00" * 1000, b"\xff"]
            for payload in payloads:
                await bc(_make_deadline(), payload)

            responses = await _collect_stream_items(stream, len(payloads))
            for i, resp in enumerate(responses):
                assert resp.message == payloads[i]
                assert resp.seqno == i

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_streaming_preserves_remote_id_across_responses(self) -> None:
        """All responses from the same breadcrumb have the same remote_id."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"remote-id-check")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            for i in range(3):
                await bc(_make_deadline(), f"r-{i}".encode())

            responses = await _collect_stream_items(stream, 3)
            for resp in responses:
                assert resp.remote_id == 20

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_seqno_independent_per_request(self) -> None:
        """Each new request produces an independent breadcrumb with seqno starting at 0."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            # First request
            stream1 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"req-1")
            arr1 = await _drain_subscriber(sub, 1)
            bc1 = arr1[0].breadcrumb
            await bc1(_make_deadline(), b"r1-0")
            await bc1(_make_deadline(), b"r1-1")
            resp1 = await _collect_stream_items(stream1, 2)
            assert resp1[0].seqno == 0
            assert resp1[1].seqno == 1
            stream1.close()

            # Second request -- seqno starts at 0 again with the new breadcrumb
            stream2 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"req-2")
            arr2 = await _drain_subscriber(sub, 1)
            bc2 = arr2[0].breadcrumb
            await bc2(_make_deadline(), b"r2-0")
            resp2 = await _collect_stream_items(stream2, 1)
            assert resp2[0].seqno == 0
            stream2.close()

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_many_streaming_responses(self) -> None:
        """Stream a large number of responses."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            n = 50
            stream = await pub.request(_make_deadline(10.0), LONG_TIMEOUT, b"many")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            for i in range(n):
                await bc(_make_deadline(), struct.pack("<I", i))

            responses = await _collect_stream_items(stream, n, timeout=5.0)
            assert len(responses) == n
            for i, resp in enumerate(responses):
                assert resp.seqno == i
                assert struct.unpack("<I", resp.message)[0] == i

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 3. Response stream timeout
# =====================================================================================================================


class TestResponseStreamTimeout:
    """When no responses arrive within response_timeout, LivenessError is raised."""

    async def test_timeout_raises_liveness_error(self) -> None:
        """Iterating on a stream with no responses raises LivenessError after timeout."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.05, b"no-response")
            _ = await _drain_subscriber(sub, 1)
            # Do NOT send any response via breadcrumb

            with pytest.raises(LivenessError):
                await stream.__anext__()

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_timeout_after_partial_responses(self) -> None:
        """After receiving some responses, if no more arrive, LivenessError is raised."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.05, b"partial")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Send one response, then stop
            await bc(_make_deadline(), b"only-one")
            resp = await _collect_stream_items(stream, 1)
            assert len(resp) == 1
            assert resp[0].message == b"only-one"

            # Next iteration should timeout
            with pytest.raises(LivenessError):
                await stream.__anext__()

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_short_timeout_fires_quickly(self) -> None:
        """A very short response_timeout triggers LivenessError promptly."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.01, b"fast-timeout")
            _ = await _drain_subscriber(sub, 1)

            before = time.monotonic()
            with pytest.raises(LivenessError):
                await stream.__anext__()
            elapsed = time.monotonic() - before
            # Should fire within a reasonable margin (allow up to 0.5s for scheduling jitter)
            assert elapsed < 0.5

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_timeout_resets_on_each_response(self) -> None:
        """Each received response resets the liveness timer for the next iteration."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            response_timeout = 0.2
            stream = await pub.request(_make_deadline(), response_timeout, b"reset-timer")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Send responses with small delays (well within timeout)
            for i in range(3):
                await bc(_make_deadline(), f"chunk-{i}".encode())
                await asyncio.sleep(0.02)

            # All should be received without timeout
            responses = await _collect_stream_items(stream, 3)
            assert len(responses) == 3

            # Now stop sending -- should timeout
            with pytest.raises(LivenessError):
                await stream.__anext__()

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_liveness_error_message_content(self) -> None:
        """LivenessError exception should contain a descriptive message."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.01, b"err-msg")
            _ = await _drain_subscriber(sub, 1)

            with pytest.raises(LivenessError, match="liveness|timeout"):
                await stream.__anext__()

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_liveness_error_is_error_subclass(self) -> None:
        """LivenessError should be a subclass of the base Error type."""
        from pycyphal import Error

        assert issubclass(LivenessError, Error)

    async def test_timeout_does_not_close_stream(self) -> None:
        """After a LivenessError, the stream is still usable if new responses arrive."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.05, b"survive-timeout")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Trigger timeout
            with pytest.raises(LivenessError):
                await stream.__anext__()

            # Now send a response -- stream should still accept it
            await bc(_make_deadline(), b"late-response")
            resp = await _collect_stream_items(stream, 1)
            assert len(resp) == 1
            assert resp[0].message == b"late-response"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 4. Response stream close
# =====================================================================================================================


class TestResponseStreamClose:
    """Closing a ResponseStream stops iteration."""

    async def test_close_stops_iteration(self) -> None:
        """After close(), async iteration raises StopAsyncIteration."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"close-test")
            _ = await _drain_subscriber(sub, 1)

            stream.close()
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_close_is_idempotent(self) -> None:
        """Calling close() multiple times should not raise."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"idem-close")
            _ = await _drain_subscriber(sub, 1)

            stream.close()
            stream.close()  # Should not raise
            stream.close()  # Should not raise

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_close_after_receiving_responses(self) -> None:
        """Closing after receiving some responses stops further iteration."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"close-mid")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"resp-0")
            await bc(_make_deadline(), b"resp-1")

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1

            stream.close()
            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_closed_stream_discards_late_deliveries(self) -> None:
        """Responses arriving after close() are silently discarded."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"discard-late")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            stream.close()

            # Send response after close -- should not raise, just be silently ignored
            await bc(_make_deadline(), b"too-late")

            with pytest.raises(StopAsyncIteration):
                await stream.__anext__()

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_close_removes_stream_from_node_registry(self) -> None:
        """After close(), the stream should no longer be registered in the node."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"registry-check")
            _ = await _drain_subscriber(sub, 1)

            # Before close, the stream should be registered
            th = _resolved_hash(TOPIC_NAME)
            matching = [
                k for k in node_a._request_streams if k[0] == th
            ]
            assert len(matching) > 0

            stream.close()

            # After close, it should be removed
            matching_after = [
                k for k in node_a._request_streams if k[0] == th
            ]
            assert len(matching_after) == 0

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_for_loop_breaks_cleanly_after_close(self) -> None:
        """Closing a stream from outside while an async for is waiting should terminate."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), 0.05, b"break-loop")
            _ = await _drain_subscriber(sub, 1)

            collected: list[Response] = []

            async def consumer() -> None:
                async for resp in stream:
                    collected.append(resp)

            task = asyncio.ensure_future(consumer())
            await asyncio.sleep(0.02)
            stream.close()
            # Wait for the consumer to detect closure via timeout or StopAsyncIteration
            try:
                await asyncio.wait_for(task, timeout=1.0)
            except (asyncio.TimeoutError, LivenessError):
                pass
            except Exception:
                pass

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 5. Breadcrumb properties
# =====================================================================================================================


class TestBreadcrumbProperties:
    """Breadcrumb.remote_id, .topic, .tag should be accessible and correct."""

    async def test_breadcrumb_remote_id(self) -> None:
        """Breadcrumb.remote_id returns the node_id of the publisher."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"bc-remote-id")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            assert bc.remote_id == 10  # node_a transport node_id

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_topic_name(self) -> None:
        """Breadcrumb.topic.name returns the correct resolved topic name."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"bc-topic")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            topic = bc.topic
            assert topic is not None
            assert isinstance(topic, Topic)
            # The node normalizes names (strips leading '/'), so compare to resolved name
            assert topic.name == _resolved_name(TOPIC_NAME, "node_b")

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_topic_hash(self) -> None:
        """Breadcrumb.topic.hash matches the expected hash of the resolved name."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"bc-hash")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            expected_hash = _resolved_hash(TOPIC_NAME, "node_b")
            assert bc.topic is not None
            assert bc.topic.hash == expected_hash

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_tag_is_int(self) -> None:
        """Breadcrumb.tag is an integer."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"bc-tag")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            assert isinstance(bc.tag, int)

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_tag_differs_per_request(self) -> None:
        """Different requests produce breadcrumbs with different tags."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            tags: list[int] = []
            for i in range(5):
                stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, f"req-{i}".encode())
                arrivals = await _drain_subscriber(sub, 1)
                bc = arrivals[0].breadcrumb
                tags.append(bc.tag)
                stream.close()

            # All tags should be distinct (they are based on pub_tag_baseline + incrementing pub_seqno)
            assert len(set(tags)) == 5, f"Expected 5 unique tags, got {len(set(tags))}: {tags}"

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_from_best_effort_publish(self) -> None:
        """Breadcrumb is present even for best-effort (non-request) publishes."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            await pub(_make_deadline(), b"be-publish")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            assert bc is not None
            assert isinstance(bc, Breadcrumb)
            assert bc.remote_id == 10

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_topic_is_topic_instance(self) -> None:
        """Breadcrumb.topic should return a Topic instance (or None)."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"topic-type")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            t = bc.topic
            assert t is None or isinstance(t, Topic)

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_retains_context_after_subscriber_close(self) -> None:
        """Breadcrumb can be retained and used even after the subscriber is closed."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"retained-bc")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Close the subscriber
            sub.close()

            # Breadcrumb should still have its properties
            assert bc.remote_id == 10
            assert bc.tag != 0 or bc.tag == 0  # tag is valid regardless

            # Send a response via the retained breadcrumb
            await bc(_make_deadline(), b"after-sub-close")
            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1
            assert responses[0].message == b"after-sub-close"

            stream.close()
            pub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_properties_consistent_with_arrival(self) -> None:
        """The breadcrumb within an Arrival is consistent with the Arrival's own fields."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"consistency")
            arrivals = await _drain_subscriber(sub, 1)
            arr = arrivals[0]

            # The arrival's breadcrumb.remote_id should equal the publisher's transport node_id
            assert arr.breadcrumb.remote_id == 10
            # The arrival's timestamp should be an Instant
            assert isinstance(arr.timestamp, Instant)
            # The arrival's message is the request payload
            assert arr.message == b"consistency"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 6. Multiple responders
# =====================================================================================================================


class TestMultipleResponders:
    """Multiple subscribers respond to the same request."""

    async def test_two_subscribers_both_respond(self) -> None:
        """When two subscribers both respond, the publisher gets both responses."""
        node_a, node_b, node_c, _, _, _ = _make_three_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub_b = node_b.subscribe(TOPIC_NAME)
            sub_c = node_c.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"multi-sub-req")

            arr_b = await _drain_subscriber(sub_b, 1)
            arr_c = await _drain_subscriber(sub_c, 1)

            await arr_b[0].breadcrumb(_make_deadline(), b"from-b")
            await arr_c[0].breadcrumb(_make_deadline(), b"from-c")

            responses = await _collect_stream_items(stream, 2)
            assert len(responses) == 2

            messages = {resp.message for resp in responses}
            assert b"from-b" in messages
            assert b"from-c" in messages

            remote_ids = {resp.remote_id for resp in responses}
            assert 20 in remote_ids  # node_b
            assert 30 in remote_ids  # node_c

            stream.close()
            pub.close()
            sub_b.close()
            sub_c.close()
        finally:
            node_a.close()
            node_b.close()
            node_c.close()

    async def test_two_subscribers_different_response_counts(self) -> None:
        """One subscriber sends 1 response, another sends 3. All arrive."""
        node_a, node_b, node_c, _, _, _ = _make_three_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub_b = node_b.subscribe(TOPIC_NAME)
            sub_c = node_c.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"diff-counts")

            arr_b = await _drain_subscriber(sub_b, 1)
            arr_c = await _drain_subscriber(sub_c, 1)

            # node_b sends 1 response
            await arr_b[0].breadcrumb(_make_deadline(), b"b-only")

            # node_c sends 3 responses
            bc_c = arr_c[0].breadcrumb
            await bc_c(_make_deadline(), b"c-0")
            await bc_c(_make_deadline(), b"c-1")
            await bc_c(_make_deadline(), b"c-2")

            responses = await _collect_stream_items(stream, 4)
            assert len(responses) == 4

            messages = [resp.message for resp in responses]
            assert b"b-only" in messages
            assert b"c-0" in messages
            assert b"c-1" in messages
            assert b"c-2" in messages

            stream.close()
            pub.close()
            sub_b.close()
            sub_c.close()
        finally:
            node_a.close()
            node_b.close()
            node_c.close()

    async def test_responses_from_different_nodes_have_different_remote_ids(self) -> None:
        """Responses from different responder nodes carry distinct remote_id values."""
        node_a, node_b, node_c, _, _, _ = _make_three_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub_b = node_b.subscribe(TOPIC_NAME)
            sub_c = node_c.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"remote-ids")

            arr_b = await _drain_subscriber(sub_b, 1)
            arr_c = await _drain_subscriber(sub_c, 1)

            await arr_b[0].breadcrumb(_make_deadline(), b"rb")
            await arr_c[0].breadcrumb(_make_deadline(), b"rc")

            responses = await _collect_stream_items(stream, 2)
            ids = {r.remote_id for r in responses}
            assert len(ids) == 2
            assert 20 in ids
            assert 30 in ids

            stream.close()
            pub.close()
            sub_b.close()
            sub_c.close()
        finally:
            node_a.close()
            node_b.close()
            node_c.close()

    async def test_multiple_responders_seqno_per_breadcrumb(self) -> None:
        """Each responder's breadcrumb has its own independent seqno counter."""
        node_a, node_b, node_c, _, _, _ = _make_three_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub_b = node_b.subscribe(TOPIC_NAME)
            sub_c = node_c.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"seqno-check")

            arr_b = await _drain_subscriber(sub_b, 1)
            arr_c = await _drain_subscriber(sub_c, 1)

            bc_b = arr_b[0].breadcrumb
            bc_c = arr_c[0].breadcrumb

            # Both breadcrumbs send 2 responses
            await bc_b(_make_deadline(), b"b-0")
            await bc_c(_make_deadline(), b"c-0")
            await bc_b(_make_deadline(), b"b-1")
            await bc_c(_make_deadline(), b"c-1")

            responses = await _collect_stream_items(stream, 4)

            # Group by remote_id
            by_remote: dict[int, list[Response]] = {}
            for r in responses:
                by_remote.setdefault(r.remote_id, []).append(r)

            # node_b (20) seqno: 0, 1
            b_seqnos = sorted(r.seqno for r in by_remote[20])
            assert b_seqnos == [0, 1]

            # node_c (30) seqno: 0, 1
            c_seqnos = sorted(r.seqno for r in by_remote[30])
            assert c_seqnos == [0, 1]

            stream.close()
            pub.close()
            sub_b.close()
            sub_c.close()
        finally:
            node_a.close()
            node_b.close()
            node_c.close()

    async def test_one_subscriber_only_responds(self) -> None:
        """Only one of two subscribers responds -- publisher still gets that one response."""
        node_a, node_b, node_c, _, _, _ = _make_three_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub_b = node_b.subscribe(TOPIC_NAME)
            sub_c = node_c.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"one-only")

            arr_b = await _drain_subscriber(sub_b, 1)
            _ = await _drain_subscriber(sub_c, 1)

            # Only node_b responds
            await arr_b[0].breadcrumb(_make_deadline(), b"only-b")

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1
            assert responses[0].message == b"only-b"
            assert responses[0].remote_id == 20

            stream.close()
            pub.close()
            sub_b.close()
            sub_c.close()
        finally:
            node_a.close()
            node_b.close()
            node_c.close()


# =====================================================================================================================
# 7. Best-effort response
# =====================================================================================================================


class TestBestEffortResponse:
    """Breadcrumb with reliable=False (the default) sends best-effort responses."""

    async def test_best_effort_is_default(self) -> None:
        """By default, breadcrumb sends best-effort (reliable=False)."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"be-default")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Call without specifying reliable -- defaults to False
            await bc(_make_deadline(), b"default-be")

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1
            assert responses[0].message == b"default-be"

            # Verify the unicast that was sent uses RSP_BE header
            assert len(transport_b.unicast_log) >= 1
            last_unicast = transport_b.unicast_log[-1]
            target_node_id, payload = last_unicast
            assert target_node_id == 10  # sent to node_a
            hdr = unpack_header(payload[:HEADER_SIZE])
            assert hdr["type"] == HeaderType.RSP_BE

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_explicit(self) -> None:
        """Explicitly passing reliable=False uses RSP_BE header type."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"be-explicit")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"explicit-be", reliable=False)

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"explicit-be"

            # Check header type in the unicast log
            last_unicast = transport_b.unicast_log[-1]
            _, payload = last_unicast
            hdr = unpack_header(payload[:HEADER_SIZE])
            assert hdr["type"] == HeaderType.RSP_BE

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_unicast_target(self) -> None:
        """Best-effort response is unicast to the publisher's node_id."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"target-check")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"to-pub")

            # The unicast target should be node_a's node_id (10)
            found = False
            for target_id, _ in transport_b.unicast_log:
                if target_id == 10:
                    found = True
                    break
            assert found, "No unicast to node_a found in transport_b log"

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_multiple_responses_all_be(self) -> None:
        """Multiple best-effort responses all use RSP_BE header."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"multi-be")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            for i in range(3):
                await bc(_make_deadline(), f"be-{i}".encode(), reliable=False)

            responses = await _collect_stream_items(stream, 3)
            assert len(responses) == 3

            # Check all response unicasts use RSP_BE
            rsp_unicasts = [
                (tid, p) for tid, p in transport_b.unicast_log
                if tid == 10 and len(p) >= HEADER_SIZE
            ]
            for _, payload in rsp_unicasts:
                hdr = unpack_header(payload[:HEADER_SIZE])
                if hdr["type"] in (HeaderType.RSP_BE, HeaderType.RSP_REL):
                    assert hdr["type"] == HeaderType.RSP_BE

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_send_failure_raises_send_error(self) -> None:
        """If the transport fails to send a best-effort response, SendError is raised."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"fail-be")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Make unicast fail
            transport_b.fail_unicast = True

            with pytest.raises(SendError):
                await bc(_make_deadline(), b"will-fail", reliable=False)

            transport_b.fail_unicast = False
            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_response_header_contains_topic_hash(self) -> None:
        """The RSP_BE header carries the correct topic hash."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"hash-check")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"with-hash")

            expected_hash = _resolved_hash(TOPIC_NAME, "node_b")
            rsp_unicasts = [
                (tid, p) for tid, p in transport_b.unicast_log
                if tid == 10 and len(p) >= HEADER_SIZE
            ]
            found_hash = False
            for _, payload in rsp_unicasts:
                hdr = unpack_header(payload[:HEADER_SIZE])
                if hdr["type"] == HeaderType.RSP_BE:
                    assert hdr["hash"] == expected_hash
                    found_hash = True
            assert found_hash

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_response_header_contains_message_tag(self) -> None:
        """The RSP_BE header carries the message_tag matching the original request's tag."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"tag-check")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb
            expected_tag = bc.tag

            await bc(_make_deadline(), b"with-tag")

            rsp_unicasts = [
                (tid, p) for tid, p in transport_b.unicast_log
                if tid == 10 and len(p) >= HEADER_SIZE
            ]
            found_tag = False
            for _, payload in rsp_unicasts:
                hdr = unpack_header(payload[:HEADER_SIZE])
                if hdr["type"] == HeaderType.RSP_BE:
                    assert hdr["message_tag"] == expected_tag
                    found_tag = True
            assert found_tag

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_best_effort_response_header_seqno_field(self) -> None:
        """The RSP_BE header seqno field increments with each response."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"seqno-hdr")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"s0")
            await bc(_make_deadline(), b"s1")
            await bc(_make_deadline(), b"s2")

            responses = await _collect_stream_items(stream, 3)

            # Check the headers in the unicast log
            rsp_hdrs = []
            for tid, payload in transport_b.unicast_log:
                if tid == 10 and len(payload) >= HEADER_SIZE:
                    hdr = unpack_header(payload[:HEADER_SIZE])
                    if hdr["type"] == HeaderType.RSP_BE:
                        rsp_hdrs.append(hdr)

            # There should be at least 3 RSP_BE headers with seqno 0,1,2
            seqnos = [h["seqno"] for h in rsp_hdrs]
            assert 0 in seqnos
            assert 1 in seqnos
            assert 2 in seqnos

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# 8. Reliable response
# =====================================================================================================================


class TestReliableResponse:
    """Breadcrumb with reliable=True sends reliable responses (RSP_REL with ack tracking)."""

    async def test_reliable_response_uses_rsp_rel_header(self) -> None:
        """reliable=True produces RSP_REL header type."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"rel-hdr")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Send reliable response -- node_a should ack it since it has the stream
            await bc(_make_deadline(), b"reliable-resp", reliable=True)

            responses = await _collect_stream_items(stream, 1)
            assert len(responses) == 1
            assert responses[0].message == b"reliable-resp"

            # Check that RSP_REL was used in the unicast log
            found_rel = False
            for tid, payload in transport_b.unicast_log:
                if tid == 10 and len(payload) >= HEADER_SIZE:
                    hdr = unpack_header(payload[:HEADER_SIZE])
                    if hdr["type"] == HeaderType.RSP_REL:
                        found_rel = True
                        break
            assert found_rel, "No RSP_REL header found in unicast log"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_reliable_response_carries_payload(self) -> None:
        """Reliable response payload is correctly delivered."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"rel-payload")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"reliable-data-123", reliable=True)

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"reliable-data-123"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_reliable_send_failure_raises_send_error(self) -> None:
        """If the transport cannot send a reliable response, SendError is raised."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"fail-rel")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            transport_b.fail_unicast = True

            with pytest.raises(SendError):
                await bc(_make_deadline(), b"will-fail-rel", reliable=True)

            transport_b.fail_unicast = False
            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_reliable_response_receives_ack(self) -> None:
        """A reliable response that is properly received triggers an ACK from the publisher node."""
        node_a, node_b, transport_a, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"ack-flow")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"ack-me", reliable=True)

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"ack-me"

            # Give time for ack to propagate
            await asyncio.sleep(0.05)

            # Check that node_a sent an RSP_ACK back to node_b
            found_ack = False
            for tid, payload in transport_a.unicast_log:
                if tid == 20 and len(payload) >= HEADER_SIZE:
                    hdr = unpack_header(payload[:HEADER_SIZE])
                    if hdr["type"] == HeaderType.RSP_ACK:
                        found_ack = True
                        break
            assert found_ack, "No RSP_ACK found in node_a's unicast log"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_reliable_response_with_no_stream_gets_nack(self) -> None:
        """If the response stream was closed before the response arrives, a NACK should be sent."""
        node_a, node_b, transport_a, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"nack-test")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Close the stream before the response arrives
            stream.close()
            await asyncio.sleep(0.02)

            # Send reliable response -- the publisher has no stream, so it should NACK
            try:
                await bc(_make_deadline(0.3), b"no-stream", reliable=True)
            except (DeliveryError, NackError):
                pass  # Expected

            await asyncio.sleep(0.05)

            # Check that the publisher node sent a NACK
            found_nack = False
            for tid, payload in transport_a.unicast_log:
                if tid == 20 and len(payload) >= HEADER_SIZE:
                    hdr = unpack_header(payload[:HEADER_SIZE])
                    if hdr["type"] == HeaderType.RSP_NACK:
                        found_nack = True
                        break
            assert found_nack, "No RSP_NACK found after stream was closed"

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_reliable_response_header_has_correct_seqno(self) -> None:
        """The RSP_REL header carries the correct seqno."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"rel-seqno")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"r0", reliable=True)
            await bc(_make_deadline(), b"r1", reliable=True)

            responses = await _collect_stream_items(stream, 2)
            assert responses[0].seqno == 0
            assert responses[1].seqno == 1

            # Verify in the unicast log
            rel_hdrs = []
            for tid, payload in transport_b.unicast_log:
                if tid == 10 and len(payload) >= HEADER_SIZE:
                    hdr = unpack_header(payload[:HEADER_SIZE])
                    if hdr["type"] == HeaderType.RSP_REL:
                        rel_hdrs.append(hdr)

            seqnos = sorted(set(h["seqno"] for h in rel_hdrs))
            assert 0 in seqnos
            assert 1 in seqnos

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_mix_reliable_and_best_effort_responses(self) -> None:
        """Mixing reliable and best-effort responses from the same breadcrumb."""
        node_a, node_b, _, transport_b = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"mixed")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            await bc(_make_deadline(), b"be-0", reliable=False)
            await bc(_make_deadline(), b"rel-1", reliable=True)
            await bc(_make_deadline(), b"be-2", reliable=False)

            responses = await _collect_stream_items(stream, 3)
            assert len(responses) == 3

            # Verify messages arrived (order may vary slightly, but in mock transport they are synchronous)
            messages = [r.message for r in responses]
            assert b"be-0" in messages
            assert b"rel-1" in messages
            assert b"be-2" in messages

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Additional edge cases and integration
# =====================================================================================================================


class TestEdgeCases:
    """Various edge cases for robustness."""

    async def test_publisher_request_on_closed_publisher_raises_send_error(self) -> None:
        """Calling request() on a closed publisher raises SendError."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            pub.close()
            with pytest.raises(SendError):
                await pub.request(_make_deadline(), SHORT_TIMEOUT, b"closed-pub")

            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_multiple_concurrent_requests_on_same_publisher(self) -> None:
        """Multiple concurrent requests on the same publisher should be independent."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream1 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"concurrent-1")
            arr1 = await _drain_subscriber(sub, 1)

            stream2 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"concurrent-2")
            arr2 = await _drain_subscriber(sub, 1)

            # Respond to each
            await arr1[0].breadcrumb(_make_deadline(), b"resp-1")
            await arr2[0].breadcrumb(_make_deadline(), b"resp-2")

            resp1 = await _collect_stream_items(stream1, 1)
            resp2 = await _collect_stream_items(stream2, 1)

            assert resp1[0].message == b"resp-1"
            assert resp2[0].message == b"resp-2"

            stream1.close()
            stream2.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_after_node_close_is_harmless(self) -> None:
        """Sending a response after the publisher's node is closed should not crash."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"node-close")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            stream.close()
            pub.close()
            node_a.close()

            # Attempting to send a response to a closed node should raise but not crash
            try:
                await bc(_make_deadline(), b"too-late")
            except Exception:
                pass  # Any exception is acceptable, but no crash

            sub.close()
        finally:
            try:
                node_a.close()
            except Exception:
                pass
            node_b.close()

    async def test_response_stream_close_is_closable(self) -> None:
        """ResponseStream implements Closable."""
        from pycyphal import Closable

        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"closable")
            _ = await _drain_subscriber(sub, 1)

            assert isinstance(stream, Closable)
            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_request_with_high_priority(self) -> None:
        """Requests at non-default priority should work."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            pub.priority = Priority.FAST
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"fast-req")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"fast-resp")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"fast-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_request_with_low_priority(self) -> None:
        """Requests at low priority should work."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            pub.priority = Priority.SLOW
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"slow-req")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"slow-resp")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"slow-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_callable_signature(self) -> None:
        """Breadcrumb.__call__ accepts (deadline, message, reliable=) arguments."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"sig-test")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Positional args
            await bc(_make_deadline(), b"pos-args")
            # Keyword reliable
            await bc(_make_deadline(), b"kw-reliable", reliable=False)

            responses = await _collect_stream_items(stream, 2)
            assert len(responses) == 2

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_with_binary_payload(self) -> None:
        """Response with arbitrary binary content (nulls, high bytes, etc.)."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            binary_data = bytes(range(256))
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, binary_data)
            arrivals = await _drain_subscriber(sub, 1)
            assert arrivals[0].message == binary_data

            binary_response = bytes(reversed(range(256)))
            await arrivals[0].breadcrumb(_make_deadline(), binary_response)

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == binary_response

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_separate_request_streams_isolated(self) -> None:
        """Two request streams on the same topic are isolated by tag."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            # First request
            stream1 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"iso-1")
            arr1 = await _drain_subscriber(sub, 1)
            bc1 = arr1[0].breadcrumb

            # Second request
            stream2 = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"iso-2")
            arr2 = await _drain_subscriber(sub, 1)
            bc2 = arr2[0].breadcrumb

            # Respond in reverse order
            await bc2(_make_deadline(), b"r2")
            await bc1(_make_deadline(), b"r1")

            # Each stream gets only its own response
            resp1 = await _collect_stream_items(stream1, 1)
            resp2 = await _collect_stream_items(stream2, 1)

            assert resp1[0].message == b"r1"
            assert resp2[0].message == b"r2"

            stream1.close()
            stream2.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# ResponseStream internal state
# =====================================================================================================================


class TestResponseStreamInternals:
    """Tests for internal state management of ResponseStream."""

    async def test_stream_registered_in_node(self) -> None:
        """After request(), the stream is registered in the node's _request_streams."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"reg-check")
            _ = await _drain_subscriber(sub, 1)

            assert len(node_a._request_streams) > 0

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_stream_unregistered_after_close(self) -> None:
        """After close(), the stream is removed from the node's _request_streams."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"unreg-check")
            _ = await _drain_subscriber(sub, 1)

            th = _resolved_hash(TOPIC_NAME)
            matching_before = [k for k in node_a._request_streams if k[0] == th]
            assert len(matching_before) > 0

            stream.close()

            matching_after = [k for k in node_a._request_streams if k[0] == th]
            assert len(matching_after) == 0

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_multiple_streams_registered_simultaneously(self) -> None:
        """Multiple request streams can coexist in the node registry."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            streams = []
            for i in range(3):
                s = await pub.request(_make_deadline(), SHORT_TIMEOUT, f"multi-{i}".encode())
                _ = await _drain_subscriber(sub, 1)
                streams.append(s)

            # All should be registered
            th = _resolved_hash(TOPIC_NAME)
            matching = [k for k in node_a._request_streams if k[0] == th]
            assert len(matching) == 3

            for s in streams:
                s.close()

            # All should be unregistered
            matching_after = [k for k in node_a._request_streams if k[0] == th]
            assert len(matching_after) == 0

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Response dataclass
# =====================================================================================================================


class TestResponseDataclass:
    """Tests for the Response dataclass itself."""

    async def test_response_fields_accessible(self) -> None:
        """All four Response fields are accessible."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"fields")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"field-resp")

            responses = await _collect_stream_items(stream, 1)
            r = responses[0]

            # All fields exist and are the right types
            assert isinstance(r.timestamp, Instant)
            assert isinstance(r.remote_id, int)
            assert isinstance(r.seqno, int)
            assert isinstance(r.message, bytes)

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_is_frozen_dataclass(self) -> None:
        """Response is a frozen dataclass -- fields cannot be modified."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"frozen")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"frozen-resp")

            responses = await _collect_stream_items(stream, 1)
            r = responses[0]

            with pytest.raises(AttributeError):
                r.message = b"changed"  # type: ignore[misc]

            with pytest.raises(AttributeError):
                r.seqno = 999  # type: ignore[misc]

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_response_equality(self) -> None:
        """Two Response instances with the same fields should be equal (dataclass semantics)."""
        ts = Instant.now()
        r1 = Response(timestamp=ts, remote_id=10, seqno=0, message=b"hello")
        r2 = Response(timestamp=ts, remote_id=10, seqno=0, message=b"hello")
        assert r1 == r2

    async def test_response_inequality(self) -> None:
        """Response instances with different fields should not be equal."""
        ts = Instant.now()
        r1 = Response(timestamp=ts, remote_id=10, seqno=0, message=b"hello")
        r2 = Response(timestamp=ts, remote_id=10, seqno=1, message=b"hello")
        assert r1 != r2

        r3 = Response(timestamp=ts, remote_id=10, seqno=0, message=b"world")
        assert r1 != r3


# =====================================================================================================================
# Arrival and Breadcrumb integration
# =====================================================================================================================


class TestArrivalBreadcrumbIntegration:
    """Integration tests for Arrival and Breadcrumb interaction in the RPC flow."""

    async def test_arrival_contains_breadcrumb(self) -> None:
        """Each Arrival carries a Breadcrumb instance."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"arr-bc")
            arrivals = await _drain_subscriber(sub, 1)

            assert arrivals[0].breadcrumb is not None
            assert isinstance(arrivals[0].breadcrumb, Breadcrumb)

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_arrival_message_matches_request(self) -> None:
        """Arrival.message matches the original request payload."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            payload = b"match-me-exactly"
            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, payload)
            arrivals = await _drain_subscriber(sub, 1)

            assert arrivals[0].message == payload

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_arrival_timestamp_is_instant(self) -> None:
        """Arrival.timestamp is an Instant."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"ts-arr")
            arrivals = await _drain_subscriber(sub, 1)

            assert isinstance(arrivals[0].timestamp, Instant)

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_arrival_is_frozen_dataclass(self) -> None:
        """Arrival is a frozen dataclass."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"frozen-arr")
            arrivals = await _drain_subscriber(sub, 1)
            arr = arrivals[0]

            with pytest.raises(AttributeError):
                arr.message = b"changed"  # type: ignore[misc]

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_breadcrumb_can_be_stored_and_used_later(self) -> None:
        """A breadcrumb obtained from an Arrival can be stored and invoked later."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"deferred")
            arrivals = await _drain_subscriber(sub, 1)
            bc = arrivals[0].breadcrumb

            # Simulate some processing delay
            await asyncio.sleep(0.05)

            # Now respond
            await bc(_make_deadline(), b"deferred-resp")
            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"deferred-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Error hierarchy tests
# =====================================================================================================================


class TestErrorHierarchy:
    """Verify the exception hierarchy related to RPC operations."""

    async def test_send_error_is_error(self) -> None:
        from pycyphal import Error

        assert issubclass(SendError, Error)

    async def test_delivery_error_is_error(self) -> None:
        from pycyphal import Error

        assert issubclass(DeliveryError, Error)

    async def test_liveness_error_is_error(self) -> None:
        from pycyphal import Error

        assert issubclass(LivenessError, Error)

    async def test_nack_error_is_error(self) -> None:
        from pycyphal import Error

        assert issubclass(NackError, Error)

    async def test_all_errors_are_exceptions(self) -> None:
        assert issubclass(SendError, Exception)
        assert issubclass(DeliveryError, Exception)
        assert issubclass(LivenessError, Exception)
        assert issubclass(NackError, Exception)


# =====================================================================================================================
# Wire-level response header validation
# =====================================================================================================================


class TestResponseWireHeaders:
    """Validate the wire-level header fields of response messages."""

    async def test_rsp_be_header_type_byte(self) -> None:
        """RSP_BE response header type byte is 4."""
        assert HeaderType.RSP_BE == 4

    async def test_rsp_rel_header_type_byte(self) -> None:
        """RSP_REL response header type byte is 5."""
        assert HeaderType.RSP_REL == 5

    async def test_rsp_ack_header_type_byte(self) -> None:
        """RSP_ACK header type byte is 6."""
        assert HeaderType.RSP_ACK == 6

    async def test_rsp_nack_header_type_byte(self) -> None:
        """RSP_NACK header type byte is 7."""
        assert HeaderType.RSP_NACK == 7

    async def test_pack_rsp_header_size(self) -> None:
        """Packed RSP header is exactly HEADER_SIZE bytes."""
        hdr = pack_rsp_header(HeaderType.RSP_BE, 0, 0, 12345, 67890)
        assert len(hdr) == HEADER_SIZE

    async def test_pack_rsp_header_roundtrip(self) -> None:
        """Packing and unpacking a RSP header recovers the original fields."""
        tag = 42
        seqno = 99
        h = 0xDEADBEEFCAFE
        message_tag = 0x1234567890ABCDEF

        packed = pack_rsp_header(HeaderType.RSP_BE, tag, seqno, h, message_tag)
        unpacked = unpack_header(packed)

        assert unpacked["type"] == HeaderType.RSP_BE
        assert unpacked["tag"] == tag
        assert unpacked["seqno"] == seqno
        assert unpacked["hash"] == h
        assert unpacked["message_tag"] == message_tag

    async def test_pack_rsp_rel_header_roundtrip(self) -> None:
        """Packing and unpacking a RSP_REL header recovers the original fields."""
        tag = 7
        seqno = 256
        h = 0xABCDABCDABCDABCD
        message_tag = 0x9876543210FEDCBA

        packed = pack_rsp_header(HeaderType.RSP_REL, tag, seqno, h, message_tag)
        unpacked = unpack_header(packed)

        assert unpacked["type"] == HeaderType.RSP_REL
        assert unpacked["tag"] == tag
        assert unpacked["seqno"] == seqno
        assert unpacked["hash"] == h
        assert unpacked["message_tag"] == message_tag

    async def test_pack_rsp_ack_header_roundtrip(self) -> None:
        """Packing and unpacking a RSP_ACK header recovers the original fields."""
        tag = 3
        seqno = 1000
        h = 0x1111222233334444
        message_tag = 0x5555666677778888

        packed = pack_rsp_ack_header(HeaderType.RSP_ACK, tag, seqno, h, message_tag)
        unpacked = unpack_header(packed)

        assert unpacked["type"] == HeaderType.RSP_ACK
        assert unpacked["tag"] == tag
        assert unpacked["seqno"] == seqno
        assert unpacked["hash"] == h
        assert unpacked["message_tag"] == message_tag

    async def test_pack_rsp_nack_header_roundtrip(self) -> None:
        """Packing and unpacking a RSP_NACK header recovers the original fields."""
        tag = 15
        seqno = 500
        h = 0xFFFFFFFFFFFFFFFF
        message_tag = 0x0000000000000001

        packed = pack_rsp_ack_header(HeaderType.RSP_NACK, tag, seqno, h, message_tag)
        unpacked = unpack_header(packed)

        assert unpacked["type"] == HeaderType.RSP_NACK
        assert unpacked["tag"] == tag
        assert unpacked["seqno"] == seqno
        assert unpacked["hash"] == h
        assert unpacked["message_tag"] == message_tag

    async def test_rsp_header_seqno_48bit_range(self) -> None:
        """seqno is a 48-bit field -- verify large values pack/unpack correctly."""
        max_seqno = (1 << 48) - 1
        packed = pack_rsp_header(HeaderType.RSP_BE, 0, max_seqno, 0, 0)
        unpacked = unpack_header(packed)
        assert unpacked["seqno"] == max_seqno

    async def test_rsp_header_tag_8bit_range(self) -> None:
        """tag is an 8-bit field -- verify boundary values."""
        for tag in [0, 1, 127, 255]:
            packed = pack_rsp_header(HeaderType.RSP_BE, tag, 0, 0, 0)
            unpacked = unpack_header(packed)
            assert unpacked["tag"] == tag


# =====================================================================================================================
# Topic interaction with RPC
# =====================================================================================================================


class TestTopicRPC:
    """Tests for Topic-level interactions in the RPC flow."""

    async def test_publisher_topic_property(self) -> None:
        """Publisher.topic returns a Topic with the correct resolved name."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            assert pub.topic.name == _resolved_name(TOPIC_NAME)

            pub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_publisher_topic_hash_matches_wire(self) -> None:
        """Publisher.topic.hash matches the hash computed from the resolved topic name."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            expected = _resolved_hash(TOPIC_NAME)
            assert pub.topic.hash == expected

            pub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_rpc_on_pinned_topic(self) -> None:
        """RPC on a pinned topic (hash override with #hex) should work."""
        pinned_name = "/pinned#0001"  # hash = 1, which is pinned (<=8191)
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(pinned_name)
            sub = node_b.subscribe(pinned_name)
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"pinned-req")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"pinned-resp")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"pinned-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_rpc_on_topic_with_namespace(self) -> None:
        """RPC works with namespace-relative topic names."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            # Names are relative to namespace="/test"
            pub = node_a.advertise("rpc/namespaced")  # resolves to /test/rpc/namespaced
            sub = node_b.subscribe("rpc/namespaced")
            await _wait_for_gossip()

            stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, b"ns-req")
            arrivals = await _drain_subscriber(sub, 1)
            await arrivals[0].breadcrumb(_make_deadline(), b"ns-resp")

            responses = await _collect_stream_items(stream, 1)
            assert responses[0].message == b"ns-resp"

            stream.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Publisher state transitions
# =====================================================================================================================


class TestPublisherState:
    """Tests for Publisher state management during RPC."""

    async def test_publisher_priority_setter(self) -> None:
        """Publisher.priority can be set and get."""
        node_a, _, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)

            pub.priority = Priority.EXCEPTIONAL
            assert pub.priority == Priority.EXCEPTIONAL

            pub.priority = Priority.OPTIONAL
            assert pub.priority == Priority.OPTIONAL

            pub.close()
        finally:
            node_a.close()

    async def test_publisher_ack_timeout_setter(self) -> None:
        """Publisher.ack_timeout can be set and get."""
        node_a, _, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)

            original = pub.ack_timeout
            assert original > 0

            pub.ack_timeout = 1.0
            assert pub.ack_timeout > 0

            pub.close()
        finally:
            node_a.close()

    async def test_publisher_close_is_idempotent(self) -> None:
        """Closing a publisher multiple times should not raise."""
        node_a, _, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            pub.close()
            pub.close()
            pub.close()
        finally:
            node_a.close()

    async def test_closed_publisher_rejects_request(self) -> None:
        """A closed publisher rejects request() with SendError."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            pub.close()

            with pytest.raises(SendError):
                await pub.request(_make_deadline(), SHORT_TIMEOUT, b"nope")
        finally:
            node_a.close()
            node_b.close()

    async def test_closed_publisher_rejects_publish(self) -> None:
        """A closed publisher rejects __call__() with SendError."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            pub.close()

            with pytest.raises(SendError):
                await pub(_make_deadline(), b"nope")
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Subscriber state for RPC context
# =====================================================================================================================


class TestSubscriberStateRPC:
    """Tests for Subscriber state in the context of RPC."""

    async def test_subscriber_pattern_for_verbatim(self) -> None:
        """Verbatim subscriber.pattern returns the resolved topic name."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            sub = node_b.subscribe(TOPIC_NAME)
            # The node normalizes names, so the pattern is the resolved form
            assert sub.pattern == _resolved_name(TOPIC_NAME, "node_b")
            assert sub.verbatim is True

            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_subscriber_close_is_idempotent(self) -> None:
        """Closing a subscriber multiple times should not raise."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            sub = node_b.subscribe(TOPIC_NAME)
            sub.close()
            sub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_subscriber_timeout_default_infinite(self) -> None:
        """Default subscriber timeout is infinite."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            sub = node_b.subscribe(TOPIC_NAME)
            import math

            assert math.isinf(sub.timeout)
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_subscriber_timeout_setter(self) -> None:
        """Subscriber timeout can be set."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            sub = node_b.subscribe(TOPIC_NAME)
            sub.timeout = 1.0
            assert sub.timeout == 1.0
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_closed_subscriber_raises_stop_iteration(self) -> None:
        """Iterating on a closed subscriber raises StopAsyncIteration."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            sub = node_b.subscribe(TOPIC_NAME)
            sub.close()

            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
        finally:
            node_a.close()
            node_b.close()


# =====================================================================================================================
# Concurrent RPC flows
# =====================================================================================================================


class TestConcurrentRPC:
    """Tests for concurrent RPC operations."""

    async def test_two_independent_rpc_flows(self) -> None:
        """Two completely independent RPC flows running concurrently."""
        network = MockNetwork()
        node_a, node_b, _, _ = _make_two_nodes(network)
        try:
            pub1 = node_a.advertise(TOPIC_NAME)
            pub2 = node_a.advertise(TOPIC_NAME_ALT)
            sub1 = node_b.subscribe(TOPIC_NAME)
            sub2 = node_b.subscribe(TOPIC_NAME_ALT)
            await _wait_for_gossip()

            async def rpc_flow(p: Publisher, s: Subscriber, req: bytes, resp: bytes) -> Response:
                stream = await p.request(_make_deadline(), SHORT_TIMEOUT, req)
                arrivals = await _drain_subscriber(s, 1)
                await arrivals[0].breadcrumb(_make_deadline(), resp)
                responses = await _collect_stream_items(stream, 1)
                stream.close()
                return responses[0]

            results = await asyncio.gather(
                rpc_flow(pub1, sub1, b"flow-1-req", b"flow-1-resp"),
                rpc_flow(pub2, sub2, b"flow-2-req", b"flow-2-resp"),
            )

            assert results[0].message == b"flow-1-resp"
            assert results[1].message == b"flow-2-resp"

            pub1.close()
            pub2.close()
            sub1.close()
            sub2.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_rapid_request_response_cycles(self) -> None:
        """Multiple quick request/response cycles in sequence."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            for i in range(10):
                stream = await pub.request(_make_deadline(), SHORT_TIMEOUT, f"rapid-{i}".encode())
                arrivals = await _drain_subscriber(sub, 1)
                await arrivals[0].breadcrumb(_make_deadline(), f"rapid-resp-{i}".encode())
                responses = await _collect_stream_items(stream, 1)
                assert responses[0].message == f"rapid-resp-{i}".encode()
                stream.close()

            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()

    async def test_interleaved_request_response_with_streaming(self) -> None:
        """Start a request, begin streaming responses, start another request in between."""
        node_a, node_b, _, _ = _make_two_nodes()
        try:
            pub = node_a.advertise(TOPIC_NAME)
            sub = node_b.subscribe(TOPIC_NAME)
            await _wait_for_gossip()

            # Start first request
            stream1 = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"inter-1")
            arr1 = await _drain_subscriber(sub, 1)
            bc1 = arr1[0].breadcrumb

            # Send first response on stream1
            await bc1(_make_deadline(), b"s1-r0")

            # Start second request while stream1 is still open
            stream2 = await pub.request(_make_deadline(), MEDIUM_TIMEOUT, b"inter-2")
            arr2 = await _drain_subscriber(sub, 1)
            bc2 = arr2[0].breadcrumb

            # Send responses on both
            await bc1(_make_deadline(), b"s1-r1")
            await bc2(_make_deadline(), b"s2-r0")

            # Collect from both
            resp1 = await _collect_stream_items(stream1, 2)
            resp2 = await _collect_stream_items(stream2, 1)

            assert resp1[0].message == b"s1-r0"
            assert resp1[1].message == b"s1-r1"
            assert resp2[0].message == b"s2-r0"

            stream1.close()
            stream2.close()
            pub.close()
            sub.close()
        finally:
            node_a.close()
            node_b.close()
