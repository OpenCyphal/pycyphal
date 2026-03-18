"""End-to-end multi-node pub/sub tests for pycyphal.

Tests exercise the full publish/subscribe pipeline across multiple Node instances
connected via MockTransport and MockNetwork.  Each scenario validates message flow
from publisher through the mock network to subscriber queues.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import pytest

from pycyphal import (
    Arrival,
    Instant,
    Node,
    Priority,
    Publisher,
    Subscriber,
    Topic,
)
from tests.conftest import MockNetwork, MockTransport

# Default modulus matching the transport layer constant.
_MODULUS = 122743


# =====================================================================================================================
# Helpers
# =====================================================================================================================


async def _drain(sub: Subscriber, *, timeout: float = 0.5, max_items: int = 1000) -> list[Arrival]:
    """Drain all pending arrivals from a subscriber until timeout."""
    results: list[Arrival] = []
    try:
        sub.timeout = timeout
        while len(results) < max_items:
            arr = await sub.__anext__()
            results.append(arr)
    except Exception:
        pass
    return results


async def _recv_one(sub: Subscriber, *, timeout: float = 1.0) -> Arrival:
    """Receive exactly one arrival, raising on timeout."""
    old_timeout = sub.timeout
    sub.timeout = timeout
    try:
        return await sub.__anext__()
    finally:
        sub.timeout = old_timeout


async def _recv_n(sub: Subscriber, n: int, *, timeout: float = 2.0) -> list[Arrival]:
    """Receive exactly *n* arrivals, raising on timeout."""
    results: list[Arrival] = []
    old_timeout = sub.timeout
    sub.timeout = timeout
    try:
        for _ in range(n):
            results.append(await sub.__anext__())
    finally:
        sub.timeout = old_timeout
    return results


async def _publish_bytes(pub: Publisher, data: bytes, *, priority: Priority = Priority.NOMINAL) -> None:
    """Publish raw bytes using a generous deadline."""
    old_prio = pub.priority
    pub.priority = priority
    try:
        await pub(Instant.now() + 5.0, data)
    finally:
        pub.priority = old_prio


async def _expect_empty(sub: Subscriber, *, timeout: float = 0.15) -> None:
    """Assert that no arrival is pending within *timeout*."""
    old_timeout = sub.timeout
    sub.timeout = timeout
    try:
        arr = await sub.__anext__()
        pytest.fail(f"Expected empty subscriber but got arrival with message length {len(arr.message)}")
    except Exception:
        pass
    finally:
        sub.timeout = old_timeout


def _make_node(node_id: int, net: MockNetwork, **kwargs: Any) -> Node:
    """Create a Node with the given node_id connected to *net*."""
    transport = MockTransport(node_id=node_id, modulus=_MODULUS, network=net)
    return Node(transport, **kwargs)


# =====================================================================================================================
# 1. Two-node pub/sub
# =====================================================================================================================


class TestTwoNodePubSub:
    """Node A publishes, Node B subscribes -- message flows through the network."""

    async def test_single_message(self) -> None:
        """Publish one message on node A, receive it on node B."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/test/topic")
        sub = node_b.subscribe("/test/topic")

        await asyncio.sleep(0.05)

        payload = b"hello world"
        await _publish_bytes(pub, payload)
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == payload

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_multiple_messages_sequential(self) -> None:
        """Several messages published sequentially all arrive in order."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/stream/data")
        sub = node_b.subscribe("/stream/data")
        await asyncio.sleep(0.05)

        payloads = [f"msg-{i}".encode() for i in range(10)]
        for p in payloads:
            await _publish_bytes(pub, p)
            await asyncio.sleep(0.005)

        arrivals = await _recv_n(sub, len(payloads))
        received = [a.message for a in arrivals]
        assert received == payloads

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_message_metadata(self) -> None:
        """Arrival carries correct timestamp and breadcrumb remote_id."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/meta/check")
        sub = node_b.subscribe("/meta/check")
        await asyncio.sleep(0.05)

        before = Instant.now()
        await _publish_bytes(pub, b"metadata-test")
        after = Instant.now()
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"metadata-test"
        assert arr.timestamp.ns >= before.ns
        assert arr.timestamp.ns <= after.ns + 1_000_000_000  # generous 1s window
        assert arr.breadcrumb.remote_id == 1  # node_a transport id

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_priority_propagation(self) -> None:
        """High-priority publish is carried through to arrival breadcrumb."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/prio/test")
        sub = node_b.subscribe("/prio/test")
        await asyncio.sleep(0.05)

        pub.priority = Priority.HIGH
        await pub(Instant.now() + 5.0, b"high-prio")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"high-prio"
        # The breadcrumb captures the priority used for potential responses.
        assert arr.breadcrumb._priority == Priority.HIGH

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_subscribe_before_advertise(self) -> None:
        """Subscriber created before publisher still receives messages once gossip fires."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        sub = node_b.subscribe("/late/adv")
        pub = node_a.advertise("/late/adv")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"late-advertise")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"late-advertise"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_loopback_same_node(self) -> None:
        """A node can receive its own publications (loopback via network)."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")

        pub = node_a.advertise("/self/loop")
        sub = node_a.subscribe("/self/loop")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"loopback")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"loopback"

        pub.close()
        sub.close()
        node_a.close()

    async def test_empty_payload(self) -> None:
        """Zero-length message body passes through correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/empty/msg")
        sub = node_b.subscribe("/empty/msg")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b""

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 2. Three-node fanout
# =====================================================================================================================


class TestThreeNodeFanout:
    """One publisher, two subscribers on different nodes."""

    async def test_fanout_basic(self) -> None:
        """Both subscribers on different nodes receive each published message."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub = node_a.advertise("/fanout/data")
        sub_b = node_b.subscribe("/fanout/data")
        sub_c = node_c.subscribe("/fanout/data")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"fan-1")
        await asyncio.sleep(0.01)

        arr_b = await _recv_one(sub_b)
        arr_c = await _recv_one(sub_c)
        assert arr_b.message == b"fan-1"
        assert arr_c.message == b"fan-1"

        pub.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_fanout_multiple_messages(self) -> None:
        """Multiple messages all arrive at both subscribers."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub = node_a.advertise("/fanout/multi")
        sub_b = node_b.subscribe("/fanout/multi")
        sub_c = node_c.subscribe("/fanout/multi")
        await asyncio.sleep(0.05)

        payloads = [f"fanout-{i}".encode() for i in range(5)]
        for p in payloads:
            await _publish_bytes(pub, p)
            await asyncio.sleep(0.005)

        arrivals_b = await _recv_n(sub_b, 5)
        arrivals_c = await _recv_n(sub_c, 5)
        assert [a.message for a in arrivals_b] == payloads
        assert [a.message for a in arrivals_c] == payloads

        pub.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_fanout_with_local_subscriber(self) -> None:
        """Publisher node also subscribes -- all three see the message."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub = node_a.advertise("/fanout/local")
        sub_a = node_a.subscribe("/fanout/local")
        sub_b = node_b.subscribe("/fanout/local")
        sub_c = node_c.subscribe("/fanout/local")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"local-too")
        await asyncio.sleep(0.01)

        arr_a = await _recv_one(sub_a)
        arr_b = await _recv_one(sub_b)
        arr_c = await _recv_one(sub_c)
        assert arr_a.message == b"local-too"
        assert arr_b.message == b"local-too"
        assert arr_c.message == b"local-too"

        pub.close()
        sub_a.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_fanout_independent_breadcrumbs(self) -> None:
        """Each subscriber gets its own independent Arrival/Breadcrumb objects."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub = node_a.advertise("/fanout/bc")
        sub_b = node_b.subscribe("/fanout/bc")
        sub_c = node_c.subscribe("/fanout/bc")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"bc-check")
        await asyncio.sleep(0.01)

        arr_b = await _recv_one(sub_b)
        arr_c = await _recv_one(sub_c)

        # Both have the same publisher remote_id
        assert arr_b.breadcrumb.remote_id == 1
        assert arr_c.breadcrumb.remote_id == 1
        # But they are distinct objects
        assert arr_b is not arr_c

        pub.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()


# =====================================================================================================================
# 3. Bidirectional pub/sub
# =====================================================================================================================


class TestBidirectional:
    """Node A publishes on topic X, Node B publishes on topic Y, both subscribe to each other."""

    async def test_bidirectional_basic(self) -> None:
        """Each node publishes on one topic and subscribes to the other."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_a = node_a.advertise("/bidir/alpha")
        pub_b = node_b.advertise("/bidir/beta")
        sub_a_on_beta = node_a.subscribe("/bidir/beta")
        sub_b_on_alpha = node_b.subscribe("/bidir/alpha")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"from-a")
        await _publish_bytes(pub_b, b"from-b")
        await asyncio.sleep(0.01)

        arr_b = await _recv_one(sub_b_on_alpha)
        assert arr_b.message == b"from-a"
        assert arr_b.breadcrumb.remote_id == 1

        arr_a = await _recv_one(sub_a_on_beta)
        assert arr_a.message == b"from-b"
        assert arr_a.breadcrumb.remote_id == 2

        pub_a.close()
        pub_b.close()
        sub_a_on_beta.close()
        sub_b_on_alpha.close()
        node_a.close()
        node_b.close()

    async def test_bidirectional_interleaved(self) -> None:
        """Interleaved publishes from both nodes arrive correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_a = node_a.advertise("/bidir/x")
        pub_b = node_b.advertise("/bidir/y")
        sub_a = node_a.subscribe("/bidir/y")
        sub_b = node_b.subscribe("/bidir/x")
        await asyncio.sleep(0.05)

        for i in range(5):
            await _publish_bytes(pub_a, f"a-{i}".encode())
            await _publish_bytes(pub_b, f"b-{i}".encode())
            await asyncio.sleep(0.005)

        arrivals_b = await _recv_n(sub_b, 5)
        arrivals_a = await _recv_n(sub_a, 5)

        assert [a.message for a in arrivals_b] == [f"a-{i}".encode() for i in range(5)]
        assert [a.message for a in arrivals_a] == [f"b-{i}".encode() for i in range(5)]

        pub_a.close()
        pub_b.close()
        sub_a.close()
        sub_b.close()
        node_a.close()
        node_b.close()

    async def test_bidirectional_same_topic(self) -> None:
        """Both nodes publish and subscribe on the same topic."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_a = node_a.advertise("/bidir/shared")
        pub_b = node_b.advertise("/bidir/shared")
        sub_a = node_a.subscribe("/bidir/shared")
        sub_b = node_b.subscribe("/bidir/shared")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"a-shared")
        await asyncio.sleep(0.01)
        await _publish_bytes(pub_b, b"b-shared")
        await asyncio.sleep(0.01)

        # Node A sees its own message (loopback) and B's message
        arrivals_a = await _recv_n(sub_a, 2)
        msgs_a = sorted(a.message for a in arrivals_a)
        assert b"a-shared" in msgs_a
        assert b"b-shared" in msgs_a

        # Node B also sees both
        arrivals_b = await _recv_n(sub_b, 2)
        msgs_b = sorted(a.message for a in arrivals_b)
        assert b"a-shared" in msgs_b
        assert b"b-shared" in msgs_b

        pub_a.close()
        pub_b.close()
        sub_a.close()
        sub_b.close()
        node_a.close()
        node_b.close()

    async def test_bidirectional_high_volume(self) -> None:
        """Higher volume bidirectional traffic without message loss."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_a = node_a.advertise("/bidir/hv_x")
        pub_b = node_b.advertise("/bidir/hv_y")
        sub_a = node_a.subscribe("/bidir/hv_y")
        sub_b = node_b.subscribe("/bidir/hv_x")
        await asyncio.sleep(0.05)

        count = 50
        for i in range(count):
            await _publish_bytes(pub_a, f"hv-a-{i}".encode())
            await _publish_bytes(pub_b, f"hv-b-{i}".encode())
            await asyncio.sleep(0.001)

        arrivals_b = await _recv_n(sub_b, count)
        arrivals_a = await _recv_n(sub_a, count)

        assert len(arrivals_b) == count
        assert len(arrivals_a) == count
        assert arrivals_b[0].message == b"hv-a-0"
        assert arrivals_a[0].message == b"hv-b-0"

        pub_a.close()
        pub_b.close()
        sub_a.close()
        sub_b.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 4. Namespace resolution
# =====================================================================================================================


class TestNamespaceResolution:
    """Nodes with different namespaces -- verify topic names resolve correctly."""

    async def test_absolute_name_ignores_namespace(self) -> None:
        """Absolute topic names (starting with /) bypass namespace."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="ns_a")
        node_b = _make_node(2, net, home="b", namespace="ns_b")

        pub = node_a.advertise("/absolute/topic")
        sub = node_b.subscribe("/absolute/topic")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"absolute")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"absolute"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_relative_name_with_same_namespace(self) -> None:
        """Relative names resolve against same namespace -- nodes communicate."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="shared_ns")
        node_b = _make_node(2, net, home="b", namespace="shared_ns")

        pub = node_a.advertise("relative_topic")
        sub = node_b.subscribe("relative_topic")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"relative-same")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"relative-same"

        # Verify the resolved name includes the namespace prefix
        assert pub.topic.name == "shared_ns/relative_topic"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_relative_name_with_different_namespace(self) -> None:
        """Relative names with different namespaces map to different topics."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="ns_alpha")
        node_b = _make_node(2, net, home="b", namespace="ns_beta")

        pub = node_a.advertise("sensor")
        sub = node_b.subscribe("sensor")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"should-not-arrive")
        await asyncio.sleep(0.05)

        # Different namespaces => different resolved names => no delivery
        await _expect_empty(sub, timeout=0.1)

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_namespace_with_absolute_override(self) -> None:
        """One node uses namespace, other uses absolute -- communication works when resolved names match."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="vehicle")
        node_b = _make_node(2, net, home="b", namespace="")

        # node_a resolves "speed" -> "vehicle/speed"
        pub = node_a.advertise("speed")
        # node_b uses absolute name matching the resolved name
        sub = node_b.subscribe("/vehicle/speed")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"42mph")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"42mph"
        assert pub.topic.name == "vehicle/speed"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_deep_namespace(self) -> None:
        """Multi-level namespace prefix resolves correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="vehicle/subsystem/controller")
        node_b = _make_node(2, net, home="b", namespace="vehicle/subsystem/controller")

        pub = node_a.advertise("status")
        sub = node_b.subscribe("status")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"deep-ns")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"deep-ns"
        assert pub.topic.name == "vehicle/subsystem/controller/status"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_namespace_normalization(self) -> None:
        """Extra separators in the namespace are normalized away."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="foo//bar/")
        node_b = _make_node(2, net, home="b", namespace="foo/bar")

        pub = node_a.advertise("baz")
        sub = node_b.subscribe("baz")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"normalized")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"normalized"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 5. Home expansion
# =====================================================================================================================


class TestHomeExpansion:
    """Topics starting with ~/ expand to the node's home."""

    async def test_home_expansion_same_home(self) -> None:
        """Two nodes with the same home can communicate via ~/topic."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="myhome")
        node_b = _make_node(2, net, home="myhome")

        pub = node_a.advertise("~/status")
        sub = node_b.subscribe("~/status")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"home-msg")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"home-msg"
        assert pub.topic.name == "myhome/status"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_home_expansion_different_homes(self) -> None:
        """Different homes => ~/ expands differently => separate topics."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="home_a")
        node_b = _make_node(2, net, home="home_b")

        pub = node_a.advertise("~/data")
        sub = node_b.subscribe("~/data")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"no-match")
        await asyncio.sleep(0.05)

        await _expect_empty(sub, timeout=0.1)

        # Cross-subscribe with the correct expanded name
        sub_correct = node_b.subscribe("/home_a/data")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"match")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub_correct)
        assert arr.message == b"match"

        pub.close()
        sub.close()
        sub_correct.close()
        node_a.close()
        node_b.close()

    async def test_home_expansion_with_deep_path(self) -> None:
        """~/a/b/c expands home correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="root")
        node_b = _make_node(2, net, home="root")

        pub = node_a.advertise("~/sensors/temp/celsius")
        sub = node_b.subscribe("~/sensors/temp/celsius")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"23.5")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"23.5"
        assert pub.topic.name == "root/sensors/temp/celsius"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_home_expansion_mixed_absolute_and_home(self) -> None:
        """One node uses ~/, other uses absolute matching expanded name."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="nodeA")
        node_b = _make_node(2, net, home="nodeB")

        pub = node_a.advertise("~/events")
        sub = node_b.subscribe("/nodeA/events")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"event-1")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"event-1"
        assert pub.topic.name == "nodeA/events"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_home_in_namespace(self) -> None:
        """Home-relative namespace with relative topic name."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="mynode", namespace="~/subsys")
        node_b = _make_node(2, net, home="mynode", namespace="~/subsys")

        pub = node_a.advertise("cmd")
        sub = node_b.subscribe("cmd")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"namespace-home")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"namespace-home"
        # namespace ~/subsys with home "mynode" -> "mynode/subsys", then join "cmd" -> "mynode/subsys/cmd"
        assert pub.topic.name == "mynode/subsys/cmd"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 6. Pattern subscription across nodes
# =====================================================================================================================


class TestPatternSubscription:
    """Node B subscribes to a pattern, Node A publishes on a matching topic.
    After gossip/scout propagation, B discovers and receives."""

    async def test_star_pattern_single_segment(self) -> None:
        """sensors/* matches sensors/temp after gossip."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/sensors/temp")
        sub = node_b.subscribe("/sensors/*")
        # Let gossip/scout fire
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"22.5C")
        await asyncio.sleep(0.05)

        arr = await _recv_one(sub)
        assert arr.message == b"22.5C"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_star_pattern_multiple_matching_topics(self) -> None:
        """Pattern matches multiple topics published by the same node."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_temp = node_a.advertise("/sensors/temp")
        pub_hum = node_a.advertise("/sensors/humidity")
        sub = node_b.subscribe("/sensors/*")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub_temp, b"temp-data")
        await _publish_bytes(pub_hum, b"hum-data")
        await asyncio.sleep(0.05)

        arrivals = await _recv_n(sub, 2, timeout=2.0)
        messages = sorted(a.message for a in arrivals)
        assert b"hum-data" in messages
        assert b"temp-data" in messages

        pub_temp.close()
        pub_hum.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_star_pattern_no_match(self) -> None:
        """Pattern does not match topics with different prefix."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/actuators/motor")
        sub = node_b.subscribe("/sensors/*")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"motor-data")
        await asyncio.sleep(0.1)

        await _expect_empty(sub, timeout=0.15)

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_gt_pattern_multi_segment(self) -> None:
        """'>' pattern matches one or more trailing segments."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/telemetry/imu/accel/x")
        sub = node_b.subscribe("/telemetry/>")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"accel-x")
        await asyncio.sleep(0.05)

        arr = await _recv_one(sub)
        assert arr.message == b"accel-x"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_pattern_subscribe_then_publish(self) -> None:
        """Pattern subscription set up before any publishers exist.
        Scout is sent, then publisher gossips its topic -- subscriber discovers it."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        sub = node_b.subscribe("/events/*")
        await asyncio.sleep(0.05)  # scout fires

        pub = node_a.advertise("/events/click")
        await asyncio.sleep(0.2)  # gossip fires, B discovers

        await _publish_bytes(pub, b"click-event")
        await asyncio.sleep(0.05)

        arr = await _recv_one(sub)
        assert arr.message == b"click-event"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_pattern_with_multiple_subscribers(self) -> None:
        """Two pattern subscribers on the same node both receive."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/data/stream")
        sub1 = node_b.subscribe("/data/*")
        sub2 = node_b.subscribe("/data/*")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"stream-data")
        await asyncio.sleep(0.05)

        arr1 = await _recv_one(sub1)
        arr2 = await _recv_one(sub2)
        assert arr1.message == b"stream-data"
        assert arr2.message == b"stream-data"

        pub.close()
        sub1.close()
        sub2.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 7. Topic isolation
# =====================================================================================================================


class TestTopicIsolation:
    """Messages on one topic must not leak to subscribers on a different topic."""

    async def test_different_topics_no_crosstalk(self) -> None:
        """Subscribers on topic A do not receive messages published on topic B."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_alpha = node_a.advertise("/isolation/alpha")
        pub_beta = node_a.advertise("/isolation/beta")
        sub_alpha = node_b.subscribe("/isolation/alpha")
        sub_beta = node_b.subscribe("/isolation/beta")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_alpha, b"alpha-msg")
        await _publish_bytes(pub_beta, b"beta-msg")
        await asyncio.sleep(0.01)

        arr_alpha = await _recv_one(sub_alpha)
        arr_beta = await _recv_one(sub_beta)
        assert arr_alpha.message == b"alpha-msg"
        assert arr_beta.message == b"beta-msg"

        # Verify no extra messages
        await _expect_empty(sub_alpha, timeout=0.1)
        await _expect_empty(sub_beta, timeout=0.1)

        pub_alpha.close()
        pub_beta.close()
        sub_alpha.close()
        sub_beta.close()
        node_a.close()
        node_b.close()

    async def test_similar_topic_names_isolated(self) -> None:
        """Topics with similar names (e.g., /foo and /foobar) are separate."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_foo = node_a.advertise("/foo")
        pub_foobar = node_a.advertise("/foobar")
        sub_foo = node_b.subscribe("/foo")
        sub_foobar = node_b.subscribe("/foobar")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_foo, b"foo-only")
        await _publish_bytes(pub_foobar, b"foobar-only")
        await asyncio.sleep(0.01)

        arr_foo = await _recv_one(sub_foo)
        arr_foobar = await _recv_one(sub_foobar)
        assert arr_foo.message == b"foo-only"
        assert arr_foobar.message == b"foobar-only"

        await _expect_empty(sub_foo, timeout=0.1)
        await _expect_empty(sub_foobar, timeout=0.1)

        pub_foo.close()
        pub_foobar.close()
        sub_foo.close()
        sub_foobar.close()
        node_a.close()
        node_b.close()

    async def test_many_topics_no_crosstalk(self) -> None:
        """Publish on many topics, each subscriber receives only its topic's messages."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        topic_count = 10
        pubs: list[Publisher] = []
        subs: list[Subscriber] = []
        for i in range(topic_count):
            pubs.append(node_a.advertise(f"/multi/{i}"))
            subs.append(node_b.subscribe(f"/multi/{i}"))
        await asyncio.sleep(0.05)

        for i, pub in enumerate(pubs):
            await _publish_bytes(pub, f"data-{i}".encode())
            await asyncio.sleep(0.002)

        for i, sub in enumerate(subs):
            arr = await _recv_one(sub)
            assert arr.message == f"data-{i}".encode()
            await _expect_empty(sub, timeout=0.05)

        for pub in pubs:
            pub.close()
        for sub in subs:
            sub.close()
        node_a.close()
        node_b.close()

    async def test_namespace_provides_isolation(self) -> None:
        """Same relative topic name in different namespaces are isolated."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a", namespace="ns1")
        node_b = _make_node(2, net, home="b", namespace="ns2")
        node_c = _make_node(3, net, home="c", namespace="ns1")

        pub_a = node_a.advertise("sensor")  # -> ns1/sensor
        sub_b = node_b.subscribe("sensor")  # -> ns2/sensor  (different!)
        sub_c = node_c.subscribe("sensor")  # -> ns1/sensor  (matches)
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"ns1-data")
        await asyncio.sleep(0.05)

        arr_c = await _recv_one(sub_c)
        assert arr_c.message == b"ns1-data"

        await _expect_empty(sub_b, timeout=0.1)

        pub_a.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_three_topics_strict_isolation(self) -> None:
        """Three publishers on three topics; each subscriber only gets its own."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub_x = node_a.advertise("/iso/x")
        pub_y = node_a.advertise("/iso/y")
        pub_z = node_a.advertise("/iso/z")
        sub_x = node_b.subscribe("/iso/x")
        sub_y = node_b.subscribe("/iso/y")
        sub_z = node_b.subscribe("/iso/z")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_x, b"x-data")
        await _publish_bytes(pub_y, b"y-data")
        await _publish_bytes(pub_z, b"z-data")
        await asyncio.sleep(0.01)

        assert (await _recv_one(sub_x)).message == b"x-data"
        assert (await _recv_one(sub_y)).message == b"y-data"
        assert (await _recv_one(sub_z)).message == b"z-data"

        await _expect_empty(sub_x, timeout=0.05)
        await _expect_empty(sub_y, timeout=0.05)
        await _expect_empty(sub_z, timeout=0.05)

        pub_x.close()
        pub_y.close()
        pub_z.close()
        sub_x.close()
        sub_y.close()
        sub_z.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 8. Publisher lifecycle
# =====================================================================================================================


class TestPublisherLifecycle:
    """Create, publish, close -- verify subscriber sees messages then stops."""

    async def test_publish_then_close(self) -> None:
        """Subscriber receives messages while publisher is open, stops after close."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/lifecycle/test")
        sub = node_b.subscribe("/lifecycle/test")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"before-close")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"before-close"

        pub.close()

        # Verify the publisher rejects further publishes
        with pytest.raises(Exception):
            await _publish_bytes(pub, b"after-close")

        sub.close()
        node_a.close()
        node_b.close()

    async def test_subscriber_close_stops_delivery(self) -> None:
        """After subscriber is closed, further publishes do not queue."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/lifecycle/sub_close")
        sub = node_b.subscribe("/lifecycle/sub_close")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"before")
        await asyncio.sleep(0.01)
        arr = await _recv_one(sub)
        assert arr.message == b"before"

        sub.close()

        await _publish_bytes(pub, b"after")
        await asyncio.sleep(0.01)

        # The subscriber is closed -- iterating raises StopAsyncIteration
        with pytest.raises(StopAsyncIteration):
            await sub.__anext__()

        pub.close()
        node_a.close()
        node_b.close()

    async def test_node_close_cleans_up(self) -> None:
        """Closing a node closes the underlying transport."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=_MODULUS, network=net)
        node = Node(transport, home="h")

        pub = node.advertise("/lifecycle/node_close")
        pub.close()
        node.close()

        assert transport.closed

    async def test_multiple_close_idempotent(self) -> None:
        """Closing a publisher or subscriber multiple times does not raise."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")

        pub = node_a.advertise("/lifecycle/idem")
        sub = node_a.subscribe("/lifecycle/idem")

        pub.close()
        pub.close()  # second close is no-op

        sub.close()
        sub.close()  # second close is no-op

        node_a.close()
        node_a.close()

    async def test_publisher_recreate_after_close(self) -> None:
        """After closing a publisher, a new one on the same topic works."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        sub = node_b.subscribe("/lifecycle/recreate")

        pub1 = node_a.advertise("/lifecycle/recreate")
        await asyncio.sleep(0.05)
        await _publish_bytes(pub1, b"pub1-msg")
        await asyncio.sleep(0.01)
        arr1 = await _recv_one(sub)
        assert arr1.message == b"pub1-msg"
        pub1.close()

        pub2 = node_a.advertise("/lifecycle/recreate")
        await asyncio.sleep(0.05)
        await _publish_bytes(pub2, b"pub2-msg")
        await asyncio.sleep(0.01)
        arr2 = await _recv_one(sub)
        assert arr2.message == b"pub2-msg"

        pub2.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_subscriber_recreate_after_close(self) -> None:
        """After closing a subscriber, a new one on the same topic works."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/lifecycle/sub_recreate")
        sub1 = node_b.subscribe("/lifecycle/sub_recreate")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"sub1-msg")
        await asyncio.sleep(0.01)
        arr1 = await _recv_one(sub1)
        assert arr1.message == b"sub1-msg"
        sub1.close()

        sub2 = node_b.subscribe("/lifecycle/sub_recreate")
        await asyncio.sleep(0.05)
        await _publish_bytes(pub, b"sub2-msg")
        await asyncio.sleep(0.01)
        arr2 = await _recv_one(sub2)
        assert arr2.message == b"sub2-msg"

        pub.close()
        sub2.close()
        node_a.close()
        node_b.close()

    async def test_close_order_does_not_matter(self) -> None:
        """Closing pub before sub or sub before pub -- both work."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        # Close pub first
        pub1 = node_a.advertise("/lifecycle/order1")
        sub1 = node_b.subscribe("/lifecycle/order1")
        pub1.close()
        sub1.close()

        # Close sub first
        pub2 = node_a.advertise("/lifecycle/order2")
        sub2 = node_b.subscribe("/lifecycle/order2")
        sub2.close()
        pub2.close()

        node_a.close()
        node_b.close()


# =====================================================================================================================
# 9. Multiple publishers on the same topic
# =====================================================================================================================


class TestMultiplePublishersSameTopic:
    """Two publishers on the same topic -- subscriber gets all messages."""

    async def test_two_publishers_same_node_same_topic(self) -> None:
        """Two publishers from the same node on the same topic."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub1 = node_a.advertise("/multi_pub/topic")
        pub2 = node_a.advertise("/multi_pub/topic")
        sub = node_b.subscribe("/multi_pub/topic")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub1, b"from-pub1")
        await _publish_bytes(pub2, b"from-pub2")
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub, 2)
        messages = sorted(a.message for a in arrivals)
        assert b"from-pub1" in messages
        assert b"from-pub2" in messages

        pub1.close()
        pub2.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_two_publishers_different_nodes_same_topic(self) -> None:
        """Two publishers from different nodes on the same topic."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub_a = node_a.advertise("/multi_pub/cross")
        pub_b = node_b.advertise("/multi_pub/cross")
        sub_c = node_c.subscribe("/multi_pub/cross")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"node-a")
        await _publish_bytes(pub_b, b"node-b")
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub_c, 2)
        messages = sorted(a.message for a in arrivals)
        assert b"node-a" in messages
        assert b"node-b" in messages

        # Verify different remote_ids
        remote_ids = {a.breadcrumb.remote_id for a in arrivals}
        assert remote_ids == {1, 2}

        pub_a.close()
        pub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_close_one_publisher_other_continues(self) -> None:
        """Close one publisher; the other continues delivering."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub1 = node_a.advertise("/multi_pub/partial")
        pub2 = node_a.advertise("/multi_pub/partial")
        sub = node_b.subscribe("/multi_pub/partial")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub1, b"both-open-1")
        await _publish_bytes(pub2, b"both-open-2")
        await asyncio.sleep(0.01)
        arrivals_both = await _recv_n(sub, 2)
        assert len(arrivals_both) == 2

        pub1.close()

        await _publish_bytes(pub2, b"only-pub2")
        await asyncio.sleep(0.01)
        arr = await _recv_one(sub)
        assert arr.message == b"only-pub2"

        pub2.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_multiple_publishers_interleaved(self) -> None:
        """Interleaved publishes from two publishers."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub1 = node_a.advertise("/multi_pub/inter")
        pub2 = node_a.advertise("/multi_pub/inter")
        sub = node_b.subscribe("/multi_pub/inter")
        await asyncio.sleep(0.05)

        expected: list[bytes] = []
        for i in range(5):
            d1 = f"p1-{i}".encode()
            d2 = f"p2-{i}".encode()
            await _publish_bytes(pub1, d1)
            await _publish_bytes(pub2, d2)
            expected.append(d1)
            expected.append(d2)
            await asyncio.sleep(0.002)

        arrivals = await _recv_n(sub, 10)
        received = [a.message for a in arrivals]
        assert sorted(received) == sorted(expected)

        pub1.close()
        pub2.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_three_publishers_same_topic(self) -> None:
        """Three publishers on the same topic from three different nodes."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")
        node_d = _make_node(4, net, home="d")

        pub_a = node_a.advertise("/multi_pub/three")
        pub_b = node_b.advertise("/multi_pub/three")
        pub_c = node_c.advertise("/multi_pub/three")
        sub_d = node_d.subscribe("/multi_pub/three")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"from-1")
        await _publish_bytes(pub_b, b"from-2")
        await _publish_bytes(pub_c, b"from-3")
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub_d, 3)
        messages = sorted(a.message for a in arrivals)
        assert messages == [b"from-1", b"from-2", b"from-3"]

        pub_a.close()
        pub_b.close()
        pub_c.close()
        sub_d.close()
        node_a.close()
        node_b.close()
        node_c.close()
        node_d.close()

    async def test_multiple_subscribers_and_multiple_publishers(self) -> None:
        """Two publishers, two subscribers on different nodes -- all get all messages."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")
        node_d = _make_node(4, net, home="d")

        pub_a = node_a.advertise("/multi_pub/full")
        pub_b = node_b.advertise("/multi_pub/full")
        sub_c = node_c.subscribe("/multi_pub/full")
        sub_d = node_d.subscribe("/multi_pub/full")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_a, b"pa")
        await _publish_bytes(pub_b, b"pb")
        await asyncio.sleep(0.01)

        arrivals_c = await _recv_n(sub_c, 2)
        arrivals_d = await _recv_n(sub_d, 2)
        msgs_c = sorted(a.message for a in arrivals_c)
        msgs_d = sorted(a.message for a in arrivals_d)
        assert msgs_c == [b"pa", b"pb"]
        assert msgs_d == [b"pa", b"pb"]

        pub_a.close()
        pub_b.close()
        sub_c.close()
        sub_d.close()
        node_a.close()
        node_b.close()
        node_c.close()
        node_d.close()


# =====================================================================================================================
# 10. Large messages
# =====================================================================================================================


class TestLargeMessages:
    """64 KB payloads pass through correctly."""

    async def test_64kb_payload(self) -> None:
        """A 64 KB message is published and received intact."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/large/data")
        sub = node_b.subscribe("/large/data")
        await asyncio.sleep(0.05)

        payload = bytes(range(256)) * 256  # 64 KB
        assert len(payload) == 65536

        await _publish_bytes(pub, payload)
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == payload
        assert len(arr.message) == 65536

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_large_payload_multiple(self) -> None:
        """Multiple large messages in sequence."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/large/seq")
        sub = node_b.subscribe("/large/seq")
        await asyncio.sleep(0.05)

        count = 5
        payloads = [bytes([i & 0xFF] * 65536) for i in range(count)]

        for p in payloads:
            await _publish_bytes(pub, p)
            await asyncio.sleep(0.005)

        arrivals = await _recv_n(sub, count)
        for i, arr in enumerate(arrivals):
            assert arr.message == payloads[i]
            assert len(arr.message) == 65536

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_large_payload_fanout(self) -> None:
        """64 KB message fanout to multiple subscribers."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")
        node_c = _make_node(3, net, home="c")

        pub = node_a.advertise("/large/fan")
        sub_b = node_b.subscribe("/large/fan")
        sub_c = node_c.subscribe("/large/fan")
        await asyncio.sleep(0.05)

        payload = b"\xab" * 65536
        await _publish_bytes(pub, payload)
        await asyncio.sleep(0.01)

        arr_b = await _recv_one(sub_b)
        arr_c = await _recv_one(sub_c)
        assert arr_b.message == payload
        assert arr_c.message == payload

        pub.close()
        sub_b.close()
        sub_c.close()
        node_a.close()
        node_b.close()
        node_c.close()

    async def test_mixed_small_and_large(self) -> None:
        """Interleave small and large payloads -- all arrive correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/large/mixed")
        sub = node_b.subscribe("/large/mixed")
        await asyncio.sleep(0.05)

        small = b"tiny"
        large = b"\xff" * 65536

        await _publish_bytes(pub, small)
        await _publish_bytes(pub, large)
        await _publish_bytes(pub, small)
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub, 3)
        assert arrivals[0].message == small
        assert arrivals[1].message == large
        assert arrivals[2].message == small

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_max_payload_boundary(self) -> None:
        """Payloads at 65535 and 65537 bytes to test boundary behavior."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/large/boundary")
        sub = node_b.subscribe("/large/boundary")
        await asyncio.sleep(0.05)

        payload_under = b"\x01" * 65535
        payload_over = b"\x02" * 65537

        await _publish_bytes(pub, payload_under)
        await _publish_bytes(pub, payload_over)
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub, 2)
        assert arrivals[0].message == payload_under
        assert len(arrivals[0].message) == 65535
        assert arrivals[1].message == payload_over
        assert len(arrivals[1].message) == 65537

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()


# =====================================================================================================================
# 11. Additional edge cases
# =====================================================================================================================


class TestEdgeCases:
    """Assorted edge-case scenarios not covered above."""

    async def test_topic_object_properties(self) -> None:
        """Topic view returned by Publisher.topic has correct name and hash."""
        net = MockNetwork()
        node = _make_node(1, net, home="h")

        pub = node.advertise("/topic/props")
        topic = pub.topic
        assert topic.name == "topic/props"
        assert isinstance(topic.hash, int)
        assert topic.hash > 0

        pub.close()
        node.close()

    async def test_subscriber_as_async_iterator(self) -> None:
        """Subscriber works with async-for."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/iter/test")
        sub = node_b.subscribe("/iter/test")
        sub.timeout = 0.5
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"iter-1")
        await _publish_bytes(pub, b"iter-2")
        await asyncio.sleep(0.01)

        collected: list[bytes] = []
        count = 0
        async for arrival in sub:
            collected.append(arrival.message)
            count += 1
            if count >= 2:
                break

        assert collected == [b"iter-1", b"iter-2"]

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_multiple_subscribers_same_node_same_topic(self) -> None:
        """Two subscribers on the same node for the same topic both receive."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/dup_sub/topic")
        sub1 = node_b.subscribe("/dup_sub/topic")
        sub2 = node_b.subscribe("/dup_sub/topic")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"dup-test")
        await asyncio.sleep(0.01)

        arr1 = await _recv_one(sub1)
        arr2 = await _recv_one(sub2)
        assert arr1.message == b"dup-test"
        assert arr2.message == b"dup-test"

        pub.close()
        sub1.close()
        sub2.close()
        node_a.close()
        node_b.close()

    async def test_binary_payload_integrity(self) -> None:
        """Binary payload with all byte values 0x00-0xFF survives round trip."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/binary/integrity")
        sub = node_b.subscribe("/binary/integrity")
        await asyncio.sleep(0.05)

        payload = bytes(range(256))
        await _publish_bytes(pub, payload)
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == payload

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_rapid_publish_burst(self) -> None:
        """Rapid burst of publishes without sleep between them."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/burst/test")
        sub = node_b.subscribe("/burst/test")
        await asyncio.sleep(0.05)

        burst_count = 100
        for i in range(burst_count):
            await _publish_bytes(pub, f"burst-{i}".encode())

        await asyncio.sleep(0.05)
        arrivals = await _recv_n(sub, burst_count, timeout=5.0)
        assert len(arrivals) == burst_count
        assert arrivals[0].message == b"burst-0"
        assert arrivals[-1].message == f"burst-{burst_count - 1}".encode()

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_publish_with_different_priorities(self) -> None:
        """Messages at different priorities all arrive."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/prio/multi")
        sub = node_b.subscribe("/prio/multi")
        await asyncio.sleep(0.05)

        priorities = [Priority.EXCEPTIONAL, Priority.FAST, Priority.NOMINAL, Priority.SLOW, Priority.OPTIONAL]
        for prio in priorities:
            await _publish_bytes(pub, f"prio-{prio.name}".encode(), priority=prio)
            await asyncio.sleep(0.002)

        arrivals = await _recv_n(sub, len(priorities))
        messages = [a.message for a in arrivals]
        for prio in priorities:
            assert f"prio-{prio.name}".encode() in messages

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_four_node_ring(self) -> None:
        """Four nodes in a ring-like pattern: A->B, B->C, C->D topics."""
        net = MockNetwork()
        nodes = [_make_node(i, net, home=f"n{i}") for i in range(1, 5)]

        pub_ab = nodes[0].advertise("/ring/ab")
        pub_bc = nodes[1].advertise("/ring/bc")
        pub_cd = nodes[2].advertise("/ring/cd")

        sub_ab = nodes[1].subscribe("/ring/ab")
        sub_bc = nodes[2].subscribe("/ring/bc")
        sub_cd = nodes[3].subscribe("/ring/cd")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub_ab, b"ab-data")
        await _publish_bytes(pub_bc, b"bc-data")
        await _publish_bytes(pub_cd, b"cd-data")
        await asyncio.sleep(0.01)

        assert (await _recv_one(sub_ab)).message == b"ab-data"
        assert (await _recv_one(sub_bc)).message == b"bc-data"
        assert (await _recv_one(sub_cd)).message == b"cd-data"

        pub_ab.close()
        pub_bc.close()
        pub_cd.close()
        sub_ab.close()
        sub_bc.close()
        sub_cd.close()
        for n in nodes:
            n.close()

    async def test_unicode_topic_segment(self) -> None:
        """Topic names with valid ASCII chars (printable 33-126) work correctly."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        # Use valid ASCII chars in topic names
        pub = node_a.advertise("/data_v2.1/sensor-3")
        sub = node_b.subscribe("/data_v2.1/sensor-3")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"v2-data")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"v2-data"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_subscribe_close_resubscribe_pattern(self) -> None:
        """Close a pattern subscriber, resubscribe with same pattern, still works."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/resub/topic1")

        sub1 = node_b.subscribe("/resub/*")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"first-sub")
        await asyncio.sleep(0.05)
        arr1 = await _recv_one(sub1)
        assert arr1.message == b"first-sub"
        sub1.close()

        sub2 = node_b.subscribe("/resub/*")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"second-sub")
        await asyncio.sleep(0.05)
        arr2 = await _recv_one(sub2)
        assert arr2.message == b"second-sub"

        pub.close()
        sub2.close()
        node_a.close()
        node_b.close()

    async def test_publish_after_subscriber_gone(self) -> None:
        """Publishing after all subscribers are gone does not raise."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/ghost/topic")
        sub = node_b.subscribe("/ghost/topic")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"has-sub")
        await asyncio.sleep(0.01)
        arr = await _recv_one(sub)
        assert arr.message == b"has-sub"

        sub.close()

        # Publishing when subscriber is gone should not raise
        await _publish_bytes(pub, b"no-sub")
        await asyncio.sleep(0.01)
        # No crash means success

        pub.close()
        node_a.close()
        node_b.close()

    async def test_home_property(self) -> None:
        """Node.home returns the home string."""
        net = MockNetwork()
        node = _make_node(1, net, home="myhome123")
        assert node.home == "myhome123"
        node.close()

    async def test_namespace_property(self) -> None:
        """Node.namespace returns the namespace string."""
        net = MockNetwork()
        node = _make_node(1, net, home="h", namespace="my/ns")
        assert node.namespace == "my/ns"
        node.close()

    async def test_default_home_is_generated(self) -> None:
        """If no home is specified, a random hex string is used."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=_MODULUS, network=net)
        node = Node(transport)
        assert len(node.home) == 16  # 64 bits -> 16 hex chars
        assert all(c in "0123456789abcdef" for c in node.home)
        node.close()

    async def test_breadcrumb_topic_view(self) -> None:
        """Breadcrumb.topic returns a Topic view for the received message."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/bc/topic_view")
        sub = node_b.subscribe("/bc/topic_view")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"view-test")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        topic = arr.breadcrumb.topic
        assert topic is not None
        assert topic.name == "bc/topic_view"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_concurrent_publish_different_topics(self) -> None:
        """Concurrent publishes on different topics from the same node."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pubs = [node_a.advertise(f"/conc/{i}") for i in range(5)]
        subs = [node_b.subscribe(f"/conc/{i}") for i in range(5)]
        await asyncio.sleep(0.05)

        tasks = [_publish_bytes(pubs[i], f"conc-{i}".encode()) for i in range(5)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.01)

        for i in range(5):
            arr = await _recv_one(subs[i])
            assert arr.message == f"conc-{i}".encode()

        for p in pubs:
            p.close()
        for s in subs:
            s.close()
        node_a.close()
        node_b.close()

    async def test_subscriber_timeout_raises_liveness_error(self) -> None:
        """A subscriber with a short timeout raises LivenessError when no messages arrive."""
        from pycyphal import LivenessError

        net = MockNetwork()
        node = _make_node(1, net, home="h")

        sub = node.subscribe("/timeout/test")
        sub.timeout = 0.05

        with pytest.raises(LivenessError):
            await sub.__anext__()

        sub.close()
        node.close()

    async def test_network_message_log(self) -> None:
        """MockNetwork accumulates messages for debugging if instrumented."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/log/test")
        sub = node_b.subscribe("/log/test")
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"logged")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"logged"

        # The network object itself is reachable from transports
        assert len(net.transports) == 2
        assert 1 in net.transports
        assert 2 in net.transports

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_five_node_mesh(self) -> None:
        """Five nodes: one publisher, four subscribers. All receive."""
        net = MockNetwork()
        nodes = [_make_node(i, net, home=f"n{i}") for i in range(1, 6)]

        pub = nodes[0].advertise("/mesh/data")
        subs = [nodes[i].subscribe("/mesh/data") for i in range(1, 5)]
        await asyncio.sleep(0.05)

        await _publish_bytes(pub, b"mesh-payload")
        await asyncio.sleep(0.01)

        for sub in subs:
            arr = await _recv_one(sub)
            assert arr.message == b"mesh-payload"

        pub.close()
        for s in subs:
            s.close()
        for n in nodes:
            n.close()

    async def test_publish_memoryview(self) -> None:
        """Publishing via memoryview works the same as bytes."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/memview/test")
        sub = node_b.subscribe("/memview/test")
        await asyncio.sleep(0.05)

        data = bytearray(b"memoryview-content")
        await pub(Instant.now() + 5.0, memoryview(data))
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"memoryview-content"

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_repeated_advertise_same_topic_same_node(self) -> None:
        """Calling advertise multiple times on the same node/topic returns new publishers."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        sub = node_b.subscribe("/repeat/adv")

        pub1 = node_a.advertise("/repeat/adv")
        pub2 = node_a.advertise("/repeat/adv")
        pub3 = node_a.advertise("/repeat/adv")
        await asyncio.sleep(0.05)

        assert pub1 is not pub2
        assert pub2 is not pub3

        await _publish_bytes(pub1, b"p1")
        await _publish_bytes(pub2, b"p2")
        await _publish_bytes(pub3, b"p3")
        await asyncio.sleep(0.01)

        arrivals = await _recv_n(sub, 3)
        messages = sorted(a.message for a in arrivals)
        assert messages == [b"p1", b"p2", b"p3"]

        pub1.close()
        pub2.close()
        pub3.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_pattern_star_does_not_match_deeper(self) -> None:
        """'*' matches exactly one segment -- /a/*/c does not match /a/b/x/c."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/a/b/x/c")
        sub = node_b.subscribe("/a/*/c")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub, b"should-not-arrive")
        await asyncio.sleep(0.1)

        await _expect_empty(sub, timeout=0.15)

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_gt_pattern_requires_at_least_one_segment(self) -> None:
        """'>' requires at least one trailing segment."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        # /prefix/> should match /prefix/anything but not /prefix itself
        pub_match = node_a.advertise("/prefix/something")
        sub = node_b.subscribe("/prefix/>")
        await asyncio.sleep(0.2)

        await _publish_bytes(pub_match, b"matched")
        await asyncio.sleep(0.05)

        arr = await _recv_one(sub)
        assert arr.message == b"matched"

        pub_match.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_publisher_priority_setter(self) -> None:
        """Publisher.priority setter changes the priority for subsequent publishes."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/prio/setter")
        sub = node_b.subscribe("/prio/setter")
        await asyncio.sleep(0.05)

        assert pub.priority == Priority.NOMINAL

        pub.priority = Priority.EXCEPTIONAL
        assert pub.priority == Priority.EXCEPTIONAL

        await pub(Instant.now() + 5.0, b"exceptional")
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == b"exceptional"
        assert arr.breadcrumb._priority == Priority.EXCEPTIONAL

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()

    async def test_subscriber_pattern_property(self) -> None:
        """Subscriber.pattern returns the subscription pattern."""
        net = MockNetwork()
        node = _make_node(1, net, home="h")

        sub_v = node.subscribe("/exact/topic")
        assert sub_v.pattern == "exact/topic"
        assert sub_v.verbatim is True

        sub_p = node.subscribe("/wildcard/*")
        assert sub_p.pattern == "wildcard/*"
        assert sub_p.verbatim is False

        sub_v.close()
        sub_p.close()
        node.close()

    async def test_topic_match_method(self) -> None:
        """Topic.match returns substitutions for matching patterns."""
        net = MockNetwork()
        node = _make_node(1, net, home="h")

        pub = node.advertise("/vehicle/speed")
        topic = pub.topic
        # Resolved name has leading slash stripped: "vehicle/speed"
        assert topic.name == "vehicle/speed"

        result = topic.match("vehicle/*")
        assert result is not None
        assert len(result) == 1
        assert result[0][0] == "speed"

        result_no = topic.match("sensor/*")
        assert result_no is None

        pub.close()
        node.close()

    async def test_large_payload_exactly_64kb(self) -> None:
        """Exactly 65536 bytes payload with specific content verification."""
        net = MockNetwork()
        node_a = _make_node(1, net, home="a")
        node_b = _make_node(2, net, home="b")

        pub = node_a.advertise("/exact64k/test")
        sub = node_b.subscribe("/exact64k/test")
        await asyncio.sleep(0.05)

        # Create a payload with a predictable pattern
        payload = bytearray(65536)
        for i in range(65536):
            payload[i] = (i * 7 + 13) & 0xFF
        payload = bytes(payload)

        await _publish_bytes(pub, payload)
        await asyncio.sleep(0.01)

        arr = await _recv_one(sub)
        assert arr.message == payload
        # Verify specific bytes
        assert arr.message[0] == 13
        assert arr.message[1] == 20
        assert arr.message[65535] == (65535 * 7 + 13) & 0xFF

        pub.close()
        sub.close()
        node_a.close()
        node_b.close()
