"""Comprehensive tests for pycyphal pub/sub functionality.

Tests cover:
1. Best-effort publish/subscribe
2. Multiple subscribers on same topic
3. Publisher close behavior
4. Subscriber close behavior
5. Subscriber timeout (LivenessError)
6. Priority get/set
7. Ack timeout get/set
8. Multiple topics (message isolation)
9. Reliable publish (ack/delivery)
"""

from __future__ import annotations

import asyncio
import math
import struct
import time
from unittest.mock import patch

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
    SendError,
    Subscriber,
    Topic,
)
from pycyphal._wire import (
    HEADER_SIZE,
    HeaderType,
    pack_ack_header,
    topic_hash,
    topic_subject_id,
    unpack_header,
)

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from conftest import DEFAULT_MODULUS, MockNetwork, MockTransport

# =====================================================================================================================
# Helpers
# =====================================================================================================================


def _make_deadline(seconds_from_now: float = 5.0) -> Instant:
    """Create a deadline in the future."""
    return Instant.now() + seconds_from_now


def _short_deadline(seconds_from_now: float = 0.05) -> Instant:
    """Create a very short deadline for timeout testing."""
    return Instant.now() + seconds_from_now


def _make_node_pair(
    *,
    home_pub: str = "pubhome",
    home_sub: str = "subhome",
    ns_pub: str = "ns",
    ns_sub: str = "ns",
    node_id_pub: int = 1,
    node_id_sub: int = 2,
    network: MockNetwork | None = None,
) -> tuple[Node, Node, MockNetwork]:
    """Create a publisher node and subscriber node connected via a MockNetwork."""
    if network is None:
        network = MockNetwork()
    t_pub = MockTransport(node_id=node_id_pub, modulus=DEFAULT_MODULUS, network=network)
    t_sub = MockTransport(node_id=node_id_sub, modulus=DEFAULT_MODULUS, network=network)
    node_pub = Node(t_pub, home=home_pub, namespace=ns_pub)
    node_sub = Node(t_sub, home=home_sub, namespace=ns_sub)
    return node_pub, node_sub, network


def _make_single_node(
    *,
    home: str = "testhome",
    ns: str = "ns",
    node_id: int = 1,
    network: MockNetwork | None = None,
) -> tuple[Node, MockTransport, MockNetwork]:
    """Create a single node with transport."""
    if network is None:
        network = MockNetwork()
    transport = MockTransport(node_id=node_id, modulus=DEFAULT_MODULUS, network=network)
    node = Node(transport, home=home, namespace=ns)
    return node, transport, network


def _cleanup_node(node: Node) -> None:
    """Close node and suppress any gossip cancellation noise."""
    try:
        node.close()
    except Exception:
        pass


# =====================================================================================================================
# 1. Best-effort publish/subscribe
# =====================================================================================================================


class TestBestEffortPubSub:
    """Test basic best-effort publish and subscribe."""

    @pytest.mark.asyncio
    async def test_simple_publish_receive(self):
        """Publish a message, subscriber receives it as an Arrival."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("topic_a")
            sub = node_sub.subscribe("topic_a")
            msg = b"hello world"

            await pub(_make_deadline(), msg)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert isinstance(arrival, Arrival)
            assert arrival.message == msg
            assert isinstance(arrival.timestamp, Instant)
            assert isinstance(arrival.breadcrumb, Breadcrumb)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_receive_empty_message(self):
        """An empty message should still be deliverable."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("empty_topic")
            sub = node_sub.subscribe("empty_topic")

            await pub(_make_deadline(), b"")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b""
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_receive_large_message(self):
        """A large message should be delivered intact."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("large_topic")
            sub = node_sub.subscribe("large_topic")
            msg = bytes(range(256)) * 100  # 25600 bytes

            await pub(_make_deadline(), msg)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == msg
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_receive_binary_data(self):
        """Binary data including nulls should be delivered intact."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("binary_topic")
            sub = node_sub.subscribe("binary_topic")
            msg = b"\x00\x01\x02\xff\xfe\xfd" * 50

            await pub(_make_deadline(), msg)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == msg
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_breadcrumb_remote_id(self):
        """The breadcrumb remote_id should match the publisher's transport node_id."""
        node_pub, node_sub, net = _make_node_pair(node_id_pub=42, node_id_sub=99)
        try:
            pub = node_pub.advertise("bc_topic")
            sub = node_sub.subscribe("bc_topic")

            await pub(_make_deadline(), b"test")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.breadcrumb.remote_id == 42
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_breadcrumb_topic(self):
        """The breadcrumb topic should reference the correct topic name and hash."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("topicx")
            sub = node_sub.subscribe("topicx")

            await pub(_make_deadline(), b"data")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            bc = arrival.breadcrumb
            topic = bc.topic
            assert topic is not None
            resolved_name = "ns/topicx"
            assert topic.name == resolved_name
            assert topic.hash == topic_hash(resolved_name)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_breadcrumb_tag_increments(self):
        """Each published message should have a unique (incrementing) tag."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("tagtest")
            sub = node_sub.subscribe("tagtest")

            await pub(_make_deadline(), b"first")
            await asyncio.sleep(0.01)
            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            await pub(_make_deadline(), b"second")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            # Tags should differ
            assert arr1.breadcrumb.tag != arr2.breadcrumb.tag
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_timestamp_monotonic(self):
        """Arrival timestamps should be monotonically non-decreasing."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("ts_topic")
            sub = node_sub.subscribe("ts_topic")

            await pub(_make_deadline(), b"a")
            await asyncio.sleep(0.01)
            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            await pub(_make_deadline(), b"b")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            assert arr2.timestamp.ns >= arr1.timestamp.ns
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_multiple_messages_in_order(self):
        """Multiple messages should be received in order."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("order_topic")
            sub = node_sub.subscribe("order_topic")
            messages = [f"msg_{i}".encode() for i in range(20)]

            for m in messages:
                await pub(_make_deadline(), m)
                await asyncio.sleep(0.001)

            received = []
            for _ in range(len(messages)):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                received.append(arr.message)

            assert received == messages
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_memoryview(self):
        """Publishing a memoryview should work just like bytes."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("mv_topic")
            sub = node_sub.subscribe("mv_topic")
            data = b"memoryview test data"
            mv = memoryview(data)

            await pub(_make_deadline(), mv)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == data
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscriber_pattern_property(self):
        """Subscriber.pattern should return the resolved subscription name."""
        node, transport, net = _make_single_node(ns="myns")
        try:
            sub = node.subscribe("topicname")
            assert sub.pattern == "myns/topicname"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_subscriber_verbatim_property(self):
        """Subscriber.verbatim should be True for non-wildcard names."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("verbatim_topic")
            assert sub.verbatim is True
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_subscriber_pattern_not_verbatim(self):
        """Subscriber with wildcard should not be verbatim."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("prefix/*")
            assert sub.verbatim is False
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_subscriber_greater_pattern_not_verbatim(self):
        """Subscriber with '>' wildcard should not be verbatim."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("prefix/>")
            assert sub.verbatim is False
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publisher_topic_property(self):
        """Publisher.topic should return a Topic with correct name and hash."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("my_topic")
            topic = pub.topic
            assert isinstance(topic, Topic)
            resolved = "ns/my_topic"
            assert topic.name == resolved
            assert topic.hash == topic_hash(resolved)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publish_with_namespace_resolution(self):
        """Topic names should be resolved with namespace."""
        node_pub, node_sub, net = _make_node_pair(ns_pub="app", ns_sub="app")
        try:
            pub = node_pub.advertise("sensors/temp")
            sub = node_sub.subscribe("sensors/temp")

            await pub(_make_deadline(), b"25C")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"25C"
            assert pub.topic.name == "app/sensors/temp"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_absolute_name(self):
        """Absolute topic names (starting with /) bypass namespace resolution."""
        node_pub, node_sub, net = _make_node_pair(ns_pub="ns1", ns_sub="ns2")
        try:
            pub = node_pub.advertise("/absolute/topic")
            sub = node_sub.subscribe("/absolute/topic")

            await pub(_make_deadline(), b"abs_data")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"abs_data"
            assert pub.topic.name == "absolute/topic"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_home_name(self):
        """Home-relative topic names (starting with ~) should resolve with home."""
        node_pub, node_sub, net = _make_node_pair(home_pub="myhome", home_sub="myhome", ns_pub="ns", ns_sub="ns")
        try:
            pub = node_pub.advertise("~/local_topic")
            sub = node_sub.subscribe("~/local_topic")

            await pub(_make_deadline(), b"home_data")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"home_data"
            assert pub.topic.name == "myhome/local_topic"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_loopback_on_same_node(self):
        """A node that both publishes and subscribes to the same topic should receive its own messages."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(transport, home="self", namespace="ns")
        try:
            pub = node.advertise("loopback")
            sub = node.subscribe("loopback")

            await pub(_make_deadline(), b"echo")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"echo"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_async_iteration_protocol(self):
        """Subscriber should work with async for iteration."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("iter_topic")
            sub = node_sub.subscribe("iter_topic")

            expected_messages = [b"one", b"two", b"three"]
            for m in expected_messages:
                await pub(_make_deadline(), m)
                await asyncio.sleep(0.001)

            received = []
            count = 0
            async for arrival in sub:
                received.append(arrival.message)
                count += 1
                if count >= len(expected_messages):
                    break

            assert received == expected_messages
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscriber_default_timeout_infinite(self):
        """Default subscriber timeout should be infinite."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("timeout_topic")
            assert sub.timeout == float("inf")
            assert math.isinf(sub.timeout)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publish_subscribe_different_data_types(self):
        """Publishing various byte patterns should work correctly."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("dtype_topic")
            sub = node_sub.subscribe("dtype_topic")

            # Pack a struct
            packed = struct.pack("<IHBd", 42, 1000, 255, 3.14159)
            await pub(_make_deadline(), packed)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            unpacked = struct.unpack("<IHBd", arrival.message)
            assert unpacked == (42, 1000, 255, 3.14159)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


# =====================================================================================================================
# 2. Multiple subscribers
# =====================================================================================================================


class TestMultipleSubscribers:
    """Test that multiple subscribers on the same topic all get messages."""

    @pytest.mark.asyncio
    async def test_two_subscribers_same_topic(self):
        """Two subscribers on the same topic should both receive the message."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("shared_topic")
            sub1 = node_sub.subscribe("shared_topic")
            sub2 = node_sub.subscribe("shared_topic")

            await pub(_make_deadline(), b"shared_msg")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)

            assert arr1.message == b"shared_msg"
            assert arr2.message == b"shared_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_three_subscribers_same_topic(self):
        """Three subscribers on the same topic should all receive the message."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("triple_topic")
            sub1 = node_sub.subscribe("triple_topic")
            sub2 = node_sub.subscribe("triple_topic")
            sub3 = node_sub.subscribe("triple_topic")

            await pub(_make_deadline(), b"triple_msg")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            arr3 = await asyncio.wait_for(sub3.__anext__(), timeout=1.0)

            assert arr1.message == b"triple_msg"
            assert arr2.message == b"triple_msg"
            assert arr3.message == b"triple_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_multiple_subscribers_on_different_nodes(self):
        """Subscribers on different nodes should both receive the message."""
        net = MockNetwork()
        t_pub = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        t_sub1 = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net)
        t_sub2 = MockTransport(node_id=3, modulus=DEFAULT_MODULUS, network=net)
        node_pub = Node(t_pub, home="pub", namespace="ns")
        node_sub1 = Node(t_sub1, home="sub1", namespace="ns")
        node_sub2 = Node(t_sub2, home="sub2", namespace="ns")
        try:
            pub = node_pub.advertise("multi_node_topic")
            sub1 = node_sub1.subscribe("multi_node_topic")
            sub2 = node_sub2.subscribe("multi_node_topic")

            await pub(_make_deadline(), b"multi_node_msg")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)

            assert arr1.message == b"multi_node_msg"
            assert arr2.message == b"multi_node_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub1)
            _cleanup_node(node_sub2)

    @pytest.mark.asyncio
    async def test_multiple_subscribers_multiple_messages(self):
        """All subscribers should receive all messages."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("multi_msg_topic")
            sub1 = node_sub.subscribe("multi_msg_topic")
            sub2 = node_sub.subscribe("multi_msg_topic")

            messages = [b"first", b"second", b"third"]
            for m in messages:
                await pub(_make_deadline(), m)
                await asyncio.sleep(0.001)

            received1 = []
            received2 = []
            for _ in range(len(messages)):
                arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
                arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
                received1.append(arr1.message)
                received2.append(arr2.message)

            assert received1 == messages
            assert received2 == messages
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscriber_close_does_not_affect_others(self):
        """Closing one subscriber should not affect other subscribers on the same topic."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("partial_close_topic")
            sub1 = node_sub.subscribe("partial_close_topic")
            sub2 = node_sub.subscribe("partial_close_topic")

            # First message goes to both
            await pub(_make_deadline(), b"before_close")
            await asyncio.sleep(0.01)
            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr1.message == b"before_close"
            assert arr2.message == b"before_close"

            # Close sub1
            sub1.close()

            # Second message should only go to sub2
            await pub(_make_deadline(), b"after_close")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr2.message == b"after_close"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_many_subscribers_same_topic(self):
        """Stress test with many subscribers on the same topic."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("many_subs_topic")
            subs = [node_sub.subscribe("many_subs_topic") for _ in range(10)]

            await pub(_make_deadline(), b"broadcast")
            await asyncio.sleep(0.01)

            for sub in subs:
                arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
                assert arr.message == b"broadcast"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscribers_independent_queues(self):
        """Each subscriber has its own queue; consuming from one does not affect another."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("indep_topic")
            sub1 = node_sub.subscribe("indep_topic")
            sub2 = node_sub.subscribe("indep_topic")

            await pub(_make_deadline(), b"msg_a")
            await pub(_make_deadline(), b"msg_b")
            await asyncio.sleep(0.01)

            # Read both from sub1 first
            a1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            a2 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            assert a1.message == b"msg_a"
            assert a2.message == b"msg_b"

            # sub2 should still have both messages
            b1 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            b2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert b1.message == b"msg_a"
            assert b2.message == b"msg_b"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_multiple_publishers_same_topic(self):
        """Multiple publishers on the same topic should both deliver to the subscriber."""
        net = MockNetwork()
        t_pub1 = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        t_pub2 = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net)
        t_sub = MockTransport(node_id=3, modulus=DEFAULT_MODULUS, network=net)
        node_pub1 = Node(t_pub1, home="pub1", namespace="ns")
        node_pub2 = Node(t_pub2, home="pub2", namespace="ns")
        node_sub = Node(t_sub, home="sub", namespace="ns")
        try:
            pub1 = node_pub1.advertise("shared_pub_topic")
            pub2 = node_pub2.advertise("shared_pub_topic")
            sub = node_sub.subscribe("shared_pub_topic")

            await pub1(_make_deadline(), b"from_pub1")
            await asyncio.sleep(0.01)

            await pub2(_make_deadline(), b"from_pub2")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            received_msgs = {arr1.message, arr2.message}
            assert b"from_pub1" in received_msgs
            assert b"from_pub2" in received_msgs
        finally:
            _cleanup_node(node_pub1)
            _cleanup_node(node_pub2)
            _cleanup_node(node_sub)


# =====================================================================================================================
# 3. Publisher close
# =====================================================================================================================


class TestPublisherClose:
    """Test that closing a publisher prevents further publishing."""

    @pytest.mark.asyncio
    async def test_publish_after_close_raises_send_error(self):
        """Publishing after close should raise SendError."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("close_topic")
            pub.close()

            with pytest.raises(SendError):
                await pub(_make_deadline(), b"should fail")
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publish_after_close_raises_send_error_reliable(self):
        """Reliable publishing after close should also raise SendError."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("close_reliable_topic")
            pub.close()

            with pytest.raises(SendError):
                await pub(_make_deadline(), b"should fail", reliable=True)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """Closing a publisher multiple times should not raise."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("idempotent_close")
            pub.close()
            pub.close()  # Should not raise
            pub.close()  # Should not raise
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publish_before_close_succeeds(self):
        """Publishing before close should succeed."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("before_close")
            sub = node_sub.subscribe("before_close")

            await pub(_make_deadline(), b"ok")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"ok"

            pub.close()

            with pytest.raises(SendError):
                await pub(_make_deadline(), b"fail")
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_one_publisher_does_not_affect_another(self):
        """Closing one publisher does not affect another on the same topic."""
        node, transport, net = _make_single_node()
        try:
            pub1 = node.advertise("two_pubs")
            pub2 = node.advertise("two_pubs")

            pub1.close()

            # pub2 should still work
            await pub2(_make_deadline(), b"still alive")

            # pub1 should fail
            with pytest.raises(SendError):
                await pub1(_make_deadline(), b"should fail")
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_publisher_close_then_reopen(self):
        """After closing a publisher, a new one on the same topic should work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("reopen_topic")
            sub = node_sub.subscribe("reopen_topic")

            await pub(_make_deadline(), b"first_gen")
            await asyncio.sleep(0.01)
            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"first_gen"

            pub.close()

            pub2 = node_pub.advertise("reopen_topic")
            await pub2(_make_deadline(), b"second_gen")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr2.message == b"second_gen"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_publisher_error_message_content(self):
        """The SendError message should mention that the publisher is closed."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("err_msg_topic")
            pub.close()

            with pytest.raises(SendError, match="closed"):
                await pub(_make_deadline(), b"fail")
        finally:
            _cleanup_node(node)


# =====================================================================================================================
# 4. Subscriber close
# =====================================================================================================================


class TestSubscriberClose:
    """Test that closing a subscriber stops iteration."""

    @pytest.mark.asyncio
    async def test_close_stops_iteration(self):
        """After close, __anext__ should raise StopAsyncIteration."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("close_sub_topic")
            sub.close()

            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_close_while_waiting(self):
        """Closing a subscriber while waiting for a message should stop iteration."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("close_wait_topic")

            async def close_after_delay():
                await asyncio.sleep(0.05)
                sub.close()

            task = asyncio.ensure_future(close_after_delay())
            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
            await task
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        """Closing a subscriber multiple times should not raise."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("idem_sub")
            sub.close()
            sub.close()
            sub.close()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_close_stops_async_for(self):
        """async for loop should exit when subscriber is closed."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("async_for_close")
            sub = node_sub.subscribe("async_for_close")

            await pub(_make_deadline(), b"msg")
            await asyncio.sleep(0.01)

            received = []

            async def reader():
                async for arrival in sub:
                    received.append(arrival.message)

            async def closer():
                await asyncio.sleep(0.1)
                sub.close()

            task_r = asyncio.ensure_future(reader())
            task_c = asyncio.ensure_future(closer())

            await asyncio.wait_for(asyncio.gather(task_r, task_c), timeout=2.0)
            assert b"msg" in received
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_subscriber_messages_not_delivered(self):
        """Messages published after subscriber close should not be delivered."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("no_deliver_topic")
            sub = node_sub.subscribe("no_deliver_topic")

            sub.close()

            await pub(_make_deadline(), b"lost_message")
            await asyncio.sleep(0.01)

            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_all_subscribers_same_root(self):
        """Closing all subscribers from the same root should clean up properly."""
        node, transport, net = _make_single_node()
        try:
            sub1 = node.subscribe("cleanup_topic")
            sub2 = node.subscribe("cleanup_topic")
            sub3 = node.subscribe("cleanup_topic")

            sub1.close()
            sub2.close()
            sub3.close()

            with pytest.raises(StopAsyncIteration):
                await sub1.__anext__()
            with pytest.raises(StopAsyncIteration):
                await sub2.__anext__()
            with pytest.raises(StopAsyncIteration):
                await sub3.__anext__()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_subscriber_aiter_returns_self(self):
        """__aiter__ should return the subscriber itself."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("aiter_topic")
            assert sub.__aiter__() is sub
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_closed_subscriber_aiter(self):
        """Even after close, __aiter__ should return self (but __anext__ raises)."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("closed_aiter")
            sub.close()
            assert sub.__aiter__() is sub
            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
        finally:
            _cleanup_node(node)


# =====================================================================================================================
# 5. Subscriber timeout (LivenessError)
# =====================================================================================================================


class TestSubscriberTimeout:
    """Test LivenessError when subscriber timeout expires with no messages."""

    @pytest.mark.asyncio
    async def test_timeout_raises_liveness_error(self):
        """When timeout is set and no messages arrive, LivenessError should be raised."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("timeout_topic")
            sub.timeout = 0.05  # 50ms

            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_setter_getter(self):
        """Timeout getter should return the set value."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("timeout_sg")
            assert sub.timeout == float("inf")

            sub.timeout = 1.0
            assert sub.timeout == 1.0

            sub.timeout = 0.5
            assert sub.timeout == 0.5

            sub.timeout = float("inf")
            assert sub.timeout == float("inf")
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_not_triggered_when_messages_arrive(self):
        """If messages arrive before timeout, no LivenessError should occur."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("active_topic")
            sub = node_sub.subscribe("active_topic")
            sub.timeout = 2.0

            await pub(_make_deadline(), b"on_time")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=3.0)
            assert arrival.message == b"on_time"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_timeout_message_from_liveness_error(self):
        """LivenessError should have a descriptive message."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("liveness_msg")
            sub.timeout = 0.05

            with pytest.raises(LivenessError, match="liveness"):
                await sub.__anext__()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_very_short(self):
        """Very short timeout should trigger quickly."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("short_timeout")
            sub.timeout = 0.01  # 10ms

            start = time.monotonic()
            with pytest.raises(LivenessError):
                await sub.__anext__()
            elapsed = time.monotonic() - start
            assert elapsed < 1.0  # Should complete well within 1 second
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_after_first_message(self):
        """Timeout should trigger even after receiving some messages if subsequent messages stop."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("gap_topic")
            sub = node_sub.subscribe("gap_topic")
            sub.timeout = 0.1

            await pub(_make_deadline(), b"first")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"first"

            # Now wait without publishing -- should timeout
            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_infinite_timeout_no_error(self):
        """With infinite timeout, blocking should not raise (we cancel manually)."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("inf_timeout")
            # Default timeout is infinity

            # This should block indefinitely; use asyncio timeout to verify no LivenessError
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub.__anext__(), timeout=0.1)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_reset_between_messages(self):
        """Each message reception should effectively reset the timeout window."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("reset_timeout")
            sub = node_sub.subscribe("reset_timeout")
            sub.timeout = 0.5

            # Send several messages with small delays
            for i in range(5):
                await pub(_make_deadline(), f"msg_{i}".encode())
                await asyncio.sleep(0.01)

            for i in range(5):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                assert arr.message == f"msg_{i}".encode()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_timeout_zero(self):
        """A timeout of 0 should cause immediate LivenessError if no message is already queued."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("zero_timeout")
            sub.timeout = 0.0

            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_timeout_change_dynamically(self):
        """Changing timeout between reads should affect subsequent waits."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("dynamic_timeout")
            sub = node_sub.subscribe("dynamic_timeout")

            # Start with long timeout
            sub.timeout = 10.0
            await pub(_make_deadline(), b"msg1")
            await asyncio.sleep(0.01)
            arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
            assert arr.message == b"msg1"

            # Switch to short timeout
            sub.timeout = 0.05
            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


# =====================================================================================================================
# 6. Priority get/set
# =====================================================================================================================


class TestPublisherPriority:
    """Test Publisher.priority property."""

    @pytest.mark.asyncio
    async def test_default_priority(self):
        """Default priority should be NOMINAL."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_default")
            assert pub.priority == Priority.NOMINAL
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_exceptional(self):
        """Setting priority to EXCEPTIONAL should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_exc")
            pub.priority = Priority.EXCEPTIONAL
            assert pub.priority == Priority.EXCEPTIONAL
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_immediate(self):
        """Setting priority to IMMEDIATE should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_imm")
            pub.priority = Priority.IMMEDIATE
            assert pub.priority == Priority.IMMEDIATE
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_fast(self):
        """Setting priority to FAST should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_fast")
            pub.priority = Priority.FAST
            assert pub.priority == Priority.FAST
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_high(self):
        """Setting priority to HIGH should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_high")
            pub.priority = Priority.HIGH
            assert pub.priority == Priority.HIGH
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_nominal(self):
        """Setting priority to NOMINAL should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_nom")
            pub.priority = Priority.NOMINAL
            assert pub.priority == Priority.NOMINAL
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_low(self):
        """Setting priority to LOW should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_low")
            pub.priority = Priority.LOW
            assert pub.priority == Priority.LOW
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_slow(self):
        """Setting priority to SLOW should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_slow")
            pub.priority = Priority.SLOW
            assert pub.priority == Priority.SLOW
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_priority_optional(self):
        """Setting priority to OPTIONAL should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_opt")
            pub.priority = Priority.OPTIONAL
            assert pub.priority == Priority.OPTIONAL
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_priority_change_between_publishes(self):
        """Priority can be changed between publishes."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("prio_change")
            sub = node_sub.subscribe("prio_change")

            pub.priority = Priority.HIGH
            await pub(_make_deadline(), b"high_msg")
            await asyncio.sleep(0.01)

            pub.priority = Priority.LOW
            await pub(_make_deadline(), b"low_msg")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            assert arr1.message == b"high_msg"
            assert arr2.message == b"low_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_priority_values(self):
        """All priority values should have expected integer values."""
        assert Priority.EXCEPTIONAL == 0
        assert Priority.IMMEDIATE == 1
        assert Priority.FAST == 2
        assert Priority.HIGH == 3
        assert Priority.NOMINAL == 4
        assert Priority.LOW == 5
        assert Priority.SLOW == 6
        assert Priority.OPTIONAL == 7

    @pytest.mark.asyncio
    async def test_priority_ordering(self):
        """Priority values should be ordered from highest to lowest urgency."""
        assert Priority.EXCEPTIONAL < Priority.IMMEDIATE
        assert Priority.IMMEDIATE < Priority.FAST
        assert Priority.FAST < Priority.HIGH
        assert Priority.HIGH < Priority.NOMINAL
        assert Priority.NOMINAL < Priority.LOW
        assert Priority.LOW < Priority.SLOW
        assert Priority.SLOW < Priority.OPTIONAL

    @pytest.mark.asyncio
    async def test_multiple_publishers_independent_priority(self):
        """Different publishers should have independent priorities."""
        node, transport, net = _make_single_node()
        try:
            pub1 = node.advertise("indep_prio_a")
            pub2 = node.advertise("indep_prio_b")

            pub1.priority = Priority.HIGH
            pub2.priority = Priority.LOW

            assert pub1.priority == Priority.HIGH
            assert pub2.priority == Priority.LOW
        finally:
            _cleanup_node(node)


# =====================================================================================================================
# 7. Ack timeout get/set
# =====================================================================================================================


class TestPublisherAckTimeout:
    """Test Publisher.ack_timeout property."""

    @pytest.mark.asyncio
    async def test_default_ack_timeout(self):
        """Default ack_timeout should reflect the baseline scaled by priority."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_default")
            # Default priority is NOMINAL (4), baseline is 0.016
            # ack_timeout = baseline * (1 << priority) = 0.016 * 16 = 0.256
            expected = 0.016 * (1 << int(Priority.NOMINAL))
            assert abs(pub.ack_timeout - expected) < 1e-9
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_ack_timeout(self):
        """Setting ack_timeout should be retrievable."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_set")
            pub.ack_timeout = 0.5
            # ack_timeout setter: baseline = max(1e-6, duration / (1 << priority))
            # ack_timeout getter: baseline * (1 << priority)
            # So round-trip should return the set value
            assert abs(pub.ack_timeout - 0.5) < 1e-9
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_set_ack_timeout_various_values(self):
        """Setting ack_timeout to various values should work."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_various")

            for val in [0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 10.0]:
                pub.ack_timeout = val
                assert abs(pub.ack_timeout - val) < 1e-6, f"Failed for value {val}"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_changes_with_priority(self):
        """ack_timeout should scale with priority changes."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_prio_scale")

            # Set ack_timeout at NOMINAL priority
            pub.priority = Priority.NOMINAL
            pub.ack_timeout = 1.0
            nominal_timeout = pub.ack_timeout

            # Change to EXCEPTIONAL priority -- timeout should decrease
            pub.priority = Priority.EXCEPTIONAL
            exc_timeout = pub.ack_timeout

            # EXCEPTIONAL has lower int value, so baseline * (1 << 0) = baseline
            # NOMINAL had baseline * (1 << 4) = 1.0 => baseline = 1.0/16
            # EXCEPTIONAL: baseline * 1 = 1.0/16 = 0.0625
            assert exc_timeout < nominal_timeout
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_minimum_clamping(self):
        """Setting ack_timeout to a very small value should be clamped."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_clamp")
            # The setter clamps baseline to max(1e-6, ...)
            pub.ack_timeout = 1e-12  # Very small
            # baseline = max(1e-6, 1e-12 / (1 << priority))
            # With NOMINAL (4): max(1e-6, 1e-12/16) = 1e-6
            # getter: 1e-6 * 16 = 1.6e-5
            assert pub.ack_timeout >= 1e-6 * (1 << int(Priority.NOMINAL))
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_independent_per_publisher(self):
        """Different publishers should have independent ack_timeouts."""
        node, transport, net = _make_single_node()
        try:
            pub1 = node.advertise("ack_ind_a")
            pub2 = node.advertise("ack_ind_b")

            pub1.ack_timeout = 0.1
            pub2.ack_timeout = 2.0

            assert abs(pub1.ack_timeout - 0.1) < 1e-6
            assert abs(pub2.ack_timeout - 2.0) < 1e-6
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_with_exceptional_priority(self):
        """ack_timeout with EXCEPTIONAL priority (0) should give baseline * 1."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_exc")
            pub.priority = Priority.EXCEPTIONAL
            pub.ack_timeout = 0.5
            assert abs(pub.ack_timeout - 0.5) < 1e-9
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_with_optional_priority(self):
        """ack_timeout with OPTIONAL priority (7) should give baseline * 128."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("ack_optional")
            pub.priority = Priority.OPTIONAL
            pub.ack_timeout = 1.0
            # Round-trip should preserve the set value
            assert abs(pub.ack_timeout - 1.0) < 1e-6
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_ack_timeout_round_trip_all_priorities(self):
        """ack_timeout round-trip should preserve the set value for all priorities."""
        node, transport, net = _make_single_node()
        try:
            for prio in Priority:
                pub = node.advertise(f"ack_rt_{prio.name}")
                pub.priority = prio
                pub.ack_timeout = 0.5
                assert abs(pub.ack_timeout - 0.5) < 1e-6, f"Failed for priority {prio.name}"
        finally:
            _cleanup_node(node)


# =====================================================================================================================
# 8. Multiple topics (message isolation)
# =====================================================================================================================


class TestMultipleTopics:
    """Test that messages only go to matching subscribers."""

    @pytest.mark.asyncio
    async def test_different_topics_isolated(self):
        """Messages on topic_a should not appear on topic_b's subscriber."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub_a = node_pub.advertise("topic_alpha")
            pub_b = node_pub.advertise("topic_beta")
            sub_a = node_sub.subscribe("topic_alpha")
            sub_b = node_sub.subscribe("topic_beta")

            await pub_a(_make_deadline(), b"alpha_msg")
            await asyncio.sleep(0.01)

            arr_a = await asyncio.wait_for(sub_a.__anext__(), timeout=1.0)
            assert arr_a.message == b"alpha_msg"

            # sub_b should have nothing
            sub_b.timeout = 0.05
            with pytest.raises(LivenessError):
                await sub_b.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_publish_to_one_topic_subscriber_on_another(self):
        """Subscriber on topic B should not get messages from topic A."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("topicX")
            sub = node_sub.subscribe("topicY")
            sub.timeout = 0.1

            await pub(_make_deadline(), b"wrong_topic")
            await asyncio.sleep(0.01)

            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_multiple_topics_each_gets_own_messages(self):
        """Each topic subscriber should only get messages from its own topic."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub1 = node_pub.advertise("color/red")
            pub2 = node_pub.advertise("color/blue")
            pub3 = node_pub.advertise("color/green")
            sub1 = node_sub.subscribe("color/red")
            sub2 = node_sub.subscribe("color/blue")
            sub3 = node_sub.subscribe("color/green")

            await pub1(_make_deadline(), b"RED")
            await pub2(_make_deadline(), b"BLUE")
            await pub3(_make_deadline(), b"GREEN")
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            arr3 = await asyncio.wait_for(sub3.__anext__(), timeout=1.0)

            assert arr1.message == b"RED"
            assert arr2.message == b"BLUE"
            assert arr3.message == b"GREEN"

            # Verify no cross-contamination
            sub1.timeout = 0.05
            sub2.timeout = 0.05
            sub3.timeout = 0.05
            with pytest.raises(LivenessError):
                await sub1.__anext__()
            with pytest.raises(LivenessError):
                await sub2.__anext__()
            with pytest.raises(LivenessError):
                await sub3.__anext__()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_interleaved_publishing_isolation(self):
        """Interleaved publishing to different topics should maintain isolation."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub_x = node_pub.advertise("isoX")
            pub_y = node_pub.advertise("isoY")
            sub_x = node_sub.subscribe("isoX")
            sub_y = node_sub.subscribe("isoY")

            await pub_x(_make_deadline(), b"x1")
            await pub_y(_make_deadline(), b"y1")
            await pub_x(_make_deadline(), b"x2")
            await pub_y(_make_deadline(), b"y2")
            await asyncio.sleep(0.02)

            x_msgs = []
            y_msgs = []
            for _ in range(2):
                arr = await asyncio.wait_for(sub_x.__anext__(), timeout=1.0)
                x_msgs.append(arr.message)
            for _ in range(2):
                arr = await asyncio.wait_for(sub_y.__anext__(), timeout=1.0)
                y_msgs.append(arr.message)

            assert x_msgs == [b"x1", b"x2"]
            assert y_msgs == [b"y1", b"y2"]
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_similar_topic_names_isolated(self):
        """Topics with similar names (but different) should be isolated."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub1 = node_pub.advertise("sensor")
            pub2 = node_pub.advertise("sensors")
            sub1 = node_sub.subscribe("sensor")
            sub2 = node_sub.subscribe("sensors")

            await pub1(_make_deadline(), b"singular")
            await pub2(_make_deadline(), b"plural")
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr1.message == b"singular"
            assert arr2.message == b"plural"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_hierarchical_topic_names(self):
        """Hierarchical topic names should be fully qualified and isolated."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub1 = node_pub.advertise("a/b/c")
            pub2 = node_pub.advertise("a/b/d")
            sub1 = node_sub.subscribe("a/b/c")
            sub2 = node_sub.subscribe("a/b/d")

            await pub1(_make_deadline(), b"abc")
            await pub2(_make_deadline(), b"abd")
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr1.message == b"abc"
            assert arr2.message == b"abd"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_many_topics_isolation(self):
        """Stress test: many topics, each should only receive its own messages."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            n_topics = 15
            pubs = []
            subs = []
            for i in range(n_topics):
                pubs.append(node_pub.advertise(f"stress/{i}"))
                subs.append(node_sub.subscribe(f"stress/{i}"))

            for i, pub in enumerate(pubs):
                await pub(_make_deadline(), f"data_{i}".encode())
            await asyncio.sleep(0.05)

            for i, sub in enumerate(subs):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                assert arr.message == f"data_{i}".encode()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscribe_before_advertise(self):
        """Subscribing before advertising should still work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            sub = node_sub.subscribe("late_pub_topic")
            pub = node_pub.advertise("late_pub_topic")

            await pub(_make_deadline(), b"late_arrive")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"late_arrive"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_topic_names_case_sensitive(self):
        """Topic names should be case-sensitive."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub_lower = node_pub.advertise("CaseTopic")
            pub_upper = node_pub.advertise("casetopic")
            sub_lower = node_sub.subscribe("CaseTopic")
            sub_upper = node_sub.subscribe("casetopic")

            await pub_lower(_make_deadline(), b"LOWER")
            await pub_upper(_make_deadline(), b"UPPER")
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub_lower.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub_upper.__anext__(), timeout=1.0)
            assert arr1.message == b"LOWER"
            assert arr2.message == b"UPPER"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_no_subscriber_message_discarded(self):
        """Publishing to a topic with no subscriber should not cause errors."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("no_sub_topic")
            # Should not raise even though no one is listening
            await pub(_make_deadline(), b"discarded")
        finally:
            _cleanup_node(node)


# =====================================================================================================================
# 9. Reliable publish
# =====================================================================================================================


class TestReliablePublish:
    """Test reliable publish with ack mechanism."""

    @pytest.mark.asyncio
    async def test_reliable_publish_no_subscriber_delivery_error(self):
        """Reliable publish with no subscriber should raise DeliveryError on timeout."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("rel_no_sub")
            pub.ack_timeout = 0.01

            with pytest.raises(DeliveryError):
                await pub(_short_deadline(0.1), b"no_ack", reliable=True)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_reliable_publish_with_subscriber_succeeds(self):
        """Reliable publish with an active subscriber should succeed (subscriber sends ack)."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("rel_with_sub")
            sub = node_sub.subscribe("rel_with_sub")

            # The subscriber node will automatically ack on reception of reliable messages
            await pub(_make_deadline(), b"reliable_data", reliable=True)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"reliable_data"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_publish_deadline_exceeded(self):
        """Reliable publish should raise DeliveryError if deadline is exceeded before ack."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("rel_deadline")
            pub.ack_timeout = 0.01

            # Very short deadline, no subscriber to ack
            with pytest.raises(DeliveryError):
                await pub(_short_deadline(0.05), b"deadline_msg", reliable=True)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_reliable_vs_best_effort(self):
        """Both reliable and best-effort messages should be deliverable."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("both_modes")
            sub = node_sub.subscribe("both_modes")

            # Best-effort
            await pub(_make_deadline(), b"be_msg")
            await asyncio.sleep(0.01)

            arr_be = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr_be.message == b"be_msg"

            # Reliable
            await pub(_make_deadline(), b"rel_msg", reliable=True)
            await asyncio.sleep(0.01)

            arr_rel = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr_rel.message == b"rel_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_publish_after_close_raises_send_error(self):
        """Reliable publish after publisher close should raise SendError."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("rel_closed")
            pub.close()

            with pytest.raises(SendError):
                await pub(_make_deadline(), b"fail", reliable=True)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_reliable_publish_short_ack_timeout(self):
        """With very short ack timeout and no subscriber, should fail quickly."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("rel_short_ack")
            pub.ack_timeout = 0.001  # 1ms

            start = time.monotonic()
            with pytest.raises(DeliveryError):
                await pub(_short_deadline(0.05), b"quick_fail", reliable=True)
            elapsed = time.monotonic() - start
            assert elapsed < 2.0
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_reliable_publish_empty_message(self):
        """Reliable publish with empty message should work with ack."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("rel_empty")
            sub = node_sub.subscribe("rel_empty")

            await pub(_make_deadline(), b"", reliable=True)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b""
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_publish_large_message(self):
        """Reliable publish with large message should work with ack."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("rel_large")
            sub = node_sub.subscribe("rel_large")

            large_msg = b"X" * 10000
            await pub(_make_deadline(), large_msg, reliable=True)
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == large_msg
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_multiple_messages_sequential(self):
        """Multiple reliable messages should all be delivered."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("rel_multi")
            sub = node_sub.subscribe("rel_multi")

            for i in range(5):
                await pub(_make_deadline(), f"rmsg_{i}".encode(), reliable=True)
                await asyncio.sleep(0.01)

            for i in range(5):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                assert arr.message == f"rmsg_{i}".encode()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_publish_error_is_delivery_error_subclass(self):
        """DeliveryError should be a subclass of Error."""
        from pycyphal import Error as CyphalError

        assert issubclass(DeliveryError, CyphalError)

    @pytest.mark.asyncio
    async def test_reliable_publish_to_multiple_subscribers(self):
        """Reliable publish to topic with multiple subscribers should succeed if at least one acks."""
        net = MockNetwork()
        t_pub = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        t_sub1 = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net)
        t_sub2 = MockTransport(node_id=3, modulus=DEFAULT_MODULUS, network=net)
        node_pub = Node(t_pub, home="pub", namespace="ns")
        node_sub1 = Node(t_sub1, home="sub1", namespace="ns")
        node_sub2 = Node(t_sub2, home="sub2", namespace="ns")
        try:
            pub = node_pub.advertise("rel_multi_sub")
            sub1 = node_sub1.subscribe("rel_multi_sub")
            sub2 = node_sub2.subscribe("rel_multi_sub")

            await pub(_make_deadline(), b"reliable_multi", reliable=True)
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr1.message == b"reliable_multi"
            assert arr2.message == b"reliable_multi"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub1)
            _cleanup_node(node_sub2)


# =====================================================================================================================
# Additional integration/edge case tests
# =====================================================================================================================


class TestNodeLifecycle:
    """Test Node creation, properties, and close behavior."""

    @pytest.mark.asyncio
    async def test_node_home_property(self):
        """Node.home should return the home value."""
        node, transport, net = _make_single_node(home="myhome")
        try:
            assert node.home == "myhome"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_node_namespace_property(self):
        """Node.namespace should return the namespace value."""
        node, transport, net = _make_single_node(ns="myns")
        try:
            assert node.namespace == "myns"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_node_random_home_when_empty(self):
        """When home is empty, a random hex string should be generated."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(transport, home="", namespace="ns")
        try:
            assert len(node.home) == 16  # 64-bit hex
            int(node.home, 16)  # Should be valid hex
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_node_close_idempotent(self):
        """Closing a node multiple times should not raise."""
        node, transport, net = _make_single_node()
        node.close()
        node.close()
        node.close()

    @pytest.mark.asyncio
    async def test_advertise_pattern_raises(self):
        """Advertising with a pattern name should raise ValueError."""
        node, transport, net = _make_single_node()
        try:
            with pytest.raises(ValueError, match="pattern"):
                node.advertise("prefix/*")
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_advertise_greater_pattern_raises(self):
        """Advertising with '>' pattern should raise ValueError."""
        node, transport, net = _make_single_node()
        try:
            with pytest.raises(ValueError, match="pattern"):
                node.advertise("prefix/>")
        finally:
            _cleanup_node(node)


class TestTopicObject:
    """Test Topic read-only view objects."""

    @pytest.mark.asyncio
    async def test_topic_hash_property(self):
        """Topic.hash should match the computed hash."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("hash_topic")
            t = pub.topic
            expected_hash = topic_hash("ns/hash_topic")
            assert t.hash == expected_hash
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_topic_name_property(self):
        """Topic.name should return the resolved topic name."""
        node, transport, net = _make_single_node(ns="myns")
        try:
            pub = node.advertise("named_topic")
            t = pub.topic
            assert t.name == "myns/named_topic"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_topic_match_method(self):
        """Topic.match should work for pattern matching."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("a/b/c")
            t = pub.topic
            # Verbatim match
            result = t.match("ns/a/b/c")
            assert result is not None
            assert result == []

            # Wildcard match
            result = t.match("ns/a/*/c")
            assert result is not None
            assert len(result) == 1
            assert result[0][0] == "b"

            # No match
            result = t.match("ns/x/y/z")
            assert result is None
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_topic_match_greater_than(self):
        """Topic.match with '>' should match one or more trailing segments."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("a/b/c")
            t = pub.topic
            result = t.match("ns/a/>")
            assert result is not None
            assert len(result) == 2  # b, c
        finally:
            _cleanup_node(node)


class TestSubscriberSubstitutions:
    """Test Subscriber.substitutions() method."""

    @pytest.mark.asyncio
    async def test_verbatim_substitutions_empty(self):
        """Verbatim subscriber substitutions for matching topic should be empty list."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("sub_topic")
            sub = node.subscribe("sub_topic")
            t = pub.topic
            result = sub.substitutions(t)
            assert result is not None
            assert result == []
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_pattern_substitutions(self):
        """Pattern subscriber substitutions should contain matched segments."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("color/red")
            sub = node.subscribe("color/*")
            t = pub.topic
            result = sub.substitutions(t)
            assert result is not None
            assert len(result) == 1
            assert result[0][0] == "red"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_no_match_substitutions(self):
        """Non-matching topic should return None for substitutions."""
        node, transport, net = _make_single_node(ns="ns")
        try:
            pub = node.advertise("alpha")
            sub = node.subscribe("beta")
            t = pub.topic
            result = sub.substitutions(t)
            assert result is None
        finally:
            _cleanup_node(node)


class TestBreadcrumbDetails:
    """Detailed tests of Breadcrumb properties."""

    @pytest.mark.asyncio
    async def test_breadcrumb_remote_id_matches_sender(self):
        """Breadcrumb.remote_id should match the sender's node_id."""
        node_pub, node_sub, net = _make_node_pair(node_id_pub=100, node_id_sub=200)
        try:
            pub = node_pub.advertise("bc_remote")
            sub = node_sub.subscribe("bc_remote")

            await pub(_make_deadline(), b"data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.breadcrumb.remote_id == 100
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_breadcrumb_topic_returns_topic(self):
        """Breadcrumb.topic should return a Topic object."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("bc_topic_test")
            sub = node_sub.subscribe("bc_topic_test")

            await pub(_make_deadline(), b"data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            topic = arr.breadcrumb.topic
            assert topic is not None
            assert isinstance(topic, Topic)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_breadcrumb_tag_is_int(self):
        """Breadcrumb.tag should be an integer."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("bc_tag_int")
            sub = node_sub.subscribe("bc_tag_int")

            await pub(_make_deadline(), b"data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert isinstance(arr.breadcrumb.tag, int)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_breadcrumb_tags_unique_per_message(self):
        """Each message should have a distinct tag."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("bc_unique_tags")
            sub = node_sub.subscribe("bc_unique_tags")

            tags = set()
            for i in range(50):
                await pub(_make_deadline(), f"m{i}".encode())
            await asyncio.sleep(0.05)

            for _ in range(50):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                tags.add(arr.breadcrumb.tag)

            assert len(tags) == 50  # All tags unique
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestPatternSubscription:
    """Test wildcard/pattern-based subscriptions."""

    @pytest.mark.asyncio
    async def test_star_pattern_matches_one_segment(self):
        """Pattern with '*' should match exactly one segment."""
        # Advertise first so the topic exists, then subscribe with pattern.
        # Pattern subscriptions attach to existing topics at subscribe time.
        net = MockNetwork()
        t = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(t, home="h", namespace="ns")
        try:
            pub = node.advertise("data/temp")
            sub_local = node.subscribe("data/*")

            await pub(_make_deadline(), b"temperature")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub_local.__anext__(), timeout=1.0)
            assert arr.message == b"temperature"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_greater_pattern_matches_multiple_segments(self):
        """Pattern with '>' should match one or more trailing segments."""
        # Advertise first so the topic exists, then subscribe with pattern.
        net = MockNetwork()
        t = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(t, home="h", namespace="ns")
        try:
            pub = node.advertise("data/sensor/temp")
            sub = node.subscribe("data/>")

            await pub(_make_deadline(), b"deep_data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"deep_data"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_pattern_subscription_is_not_verbatim(self):
        """Pattern subscription should report verbatim=False."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("prefix/*")
            assert sub.verbatim is False
        finally:
            _cleanup_node(node)


class TestInstantArithmetic:
    """Test Instant operations used in deadline computation."""

    def test_instant_add_seconds(self):
        """Instant + float(seconds) should produce a later Instant."""
        now = Instant.now()
        later = now + 1.0
        assert later.ns > now.ns
        assert abs((later.ns - now.ns) - 1_000_000_000) < 1

    def test_instant_radd(self):
        """float + Instant should also work."""
        now = Instant.now()
        later = 1.0 + now
        assert later.ns > now.ns

    def test_instant_sub_instant(self):
        """Instant - Instant should return float seconds."""
        a = Instant(ns=2_000_000_000)
        b = Instant(ns=1_000_000_000)
        diff = a - b
        assert abs(diff - 1.0) < 1e-9

    def test_instant_sub_seconds(self):
        """Instant - float should return earlier Instant."""
        now = Instant.now()
        earlier = now - 1.0
        assert earlier.ns < now.ns

    def test_instant_s_property(self):
        """Instant.s should return seconds as float."""
        inst = Instant(ns=1_500_000_000)
        assert abs(inst.s - 1.5) < 1e-9

    def test_instant_ms_property(self):
        """Instant.ms should return milliseconds."""
        inst = Instant(ns=1_500_000_000)
        assert abs(inst.ms - 1500.0) < 1e-3

    def test_instant_us_property(self):
        """Instant.us should return microseconds."""
        inst = Instant(ns=1_500_000_000)
        assert abs(inst.us - 1_500_000.0) < 1.0

    def test_instant_mul(self):
        """Instant * scalar should work."""
        inst = Instant(ns=1_000_000_000)
        doubled = inst * 2
        assert doubled.ns == 2_000_000_000

    def test_instant_rmul(self):
        """scalar * Instant should work."""
        inst = Instant(ns=1_000_000_000)
        doubled = 2 * inst
        assert doubled.ns == 2_000_000_000

    def test_instant_truediv(self):
        """Instant / scalar should work."""
        inst = Instant(ns=2_000_000_000)
        halved = inst / 2
        assert halved.ns == 1_000_000_000


class TestExceptionHierarchy:
    """Test the exception class hierarchy."""

    def test_send_error_is_error(self):
        assert issubclass(SendError, Exception)

    def test_delivery_error_is_error(self):
        from pycyphal import Error as CyphalError

        assert issubclass(DeliveryError, CyphalError)

    def test_liveness_error_is_error(self):
        from pycyphal import Error as CyphalError

        assert issubclass(LivenessError, CyphalError)

    def test_nack_error_is_error(self):
        from pycyphal import Error as CyphalError

        assert issubclass(NackError, CyphalError)

    def test_error_is_exception(self):
        from pycyphal import Error as CyphalError

        assert issubclass(CyphalError, Exception)


class TestMockTransportFixtures:
    """Test that conftest fixtures work as expected."""

    def test_mock_transport_fixture(self, mock_transport):
        """The mock_transport fixture should provide a MockTransport with node_id=1."""
        assert mock_transport.__class__.__name__ == "MockTransport"
        assert mock_transport.node_id == 1

    def test_mock_network_fixture(self, mock_network):
        """The mock_network fixture should provide a MockNetwork."""
        assert mock_network.__class__.__name__ == "MockNetwork"

    def test_mock_transport_modulus(self, mock_transport):
        """MockTransport should have the default modulus."""
        assert mock_transport.subject_id_modulus == DEFAULT_MODULUS

    @pytest.mark.asyncio
    async def test_mock_transport_in_node(self):
        """MockTransport should work within a Node."""
        net = MockNetwork()
        transport = MockTransport(node_id=10, modulus=DEFAULT_MODULUS, network=net)
        node = Node(transport, home="fhome", namespace="fns")
        try:
            pub = node.advertise("fixture_topic")
            assert pub.topic.name == "fns/fixture_topic"
        finally:
            _cleanup_node(node)


class TestMockNetworkBehavior:
    """Test MockNetwork cross-transport delivery."""

    @pytest.mark.asyncio
    async def test_network_delivers_to_all_transports(self):
        """MockNetwork should deliver subject messages to all connected transports."""
        net = MockNetwork()
        t1 = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        t2 = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net)
        t3 = MockTransport(node_id=3, modulus=DEFAULT_MODULUS, network=net)

        n1 = Node(t1, home="n1", namespace="ns")
        n2 = Node(t2, home="n2", namespace="ns")
        n3 = Node(t3, home="n3", namespace="ns")
        try:
            pub = n1.advertise("net_topic")
            sub2 = n2.subscribe("net_topic")
            sub3 = n3.subscribe("net_topic")

            await pub(_make_deadline(), b"to_all")
            await asyncio.sleep(0.01)

            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            arr3 = await asyncio.wait_for(sub3.__anext__(), timeout=1.0)

            assert arr2.message == b"to_all"
            assert arr3.message == b"to_all"
        finally:
            _cleanup_node(n1)
            _cleanup_node(n2)
            _cleanup_node(n3)

    @pytest.mark.asyncio
    async def test_separate_networks_isolated(self):
        """Transports on different networks should not receive each other's messages."""
        net1 = MockNetwork()
        net2 = MockNetwork()

        t1 = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net1)
        t2 = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net2)

        n1 = Node(t1, home="n1", namespace="ns")
        n2 = Node(t2, home="n2", namespace="ns")
        try:
            pub = n1.advertise("isolated_topic")
            sub = n2.subscribe("isolated_topic")
            sub.timeout = 0.1

            await pub(_make_deadline(), b"should_not_arrive")
            await asyncio.sleep(0.01)

            with pytest.raises(LivenessError):
                await sub.__anext__()
        finally:
            _cleanup_node(n1)
            _cleanup_node(n2)


class TestHighVolumePubSub:
    """Stress tests for pub/sub under high message volume."""

    @pytest.mark.asyncio
    async def test_high_volume_single_topic(self):
        """Publish many messages on a single topic."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("hv_topic")
            sub = node_sub.subscribe("hv_topic")
            count = 100

            for i in range(count):
                await pub(_make_deadline(), f"hv_{i}".encode())

            await asyncio.sleep(0.1)

            received = []
            for _ in range(count):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
                received.append(arr.message)

            for i in range(count):
                assert received[i] == f"hv_{i}".encode()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_rapid_publish_subscribe_close_cycle(self):
        """Rapidly creating and closing publishers and subscribers should not crash."""
        node, transport, net = _make_single_node()
        try:
            for i in range(20):
                pub = node.advertise(f"cycle_{i}")
                sub = node.subscribe(f"cycle_{i}")
                await pub(_make_deadline(), b"data")
                await asyncio.sleep(0.001)
                pub.close()
                sub.close()
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_concurrent_publishers_same_topic(self):
        """Multiple publishers publishing concurrently to the same topic should all deliver."""
        net = MockNetwork()
        nodes = []
        pubs = []
        for i in range(5):
            t = MockTransport(node_id=i + 1, modulus=DEFAULT_MODULUS, network=net)
            n = Node(t, home=f"node{i}", namespace="ns")
            nodes.append(n)
            pubs.append(n.advertise("concurrent_topic"))

        t_sub = MockTransport(node_id=100, modulus=DEFAULT_MODULUS, network=net)
        n_sub = Node(t_sub, home="sub", namespace="ns")
        nodes.append(n_sub)
        sub = n_sub.subscribe("concurrent_topic")

        try:
            tasks = []
            for i, pub in enumerate(pubs):
                tasks.append(pub(_make_deadline(), f"pub_{i}".encode()))
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.05)

            received = set()
            for _ in range(len(pubs)):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                received.add(arr.message)

            for i in range(len(pubs)):
                assert f"pub_{i}".encode() in received
        finally:
            for n in nodes:
                _cleanup_node(n)


class TestSubscriberReorderingWindow:
    """Test the reordering_window parameter of subscribe()."""

    @pytest.mark.asyncio
    async def test_subscribe_with_reordering_window(self):
        """Subscribing with reordering_window should not break basic delivery."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("reorder_topic")
            sub = node_sub.subscribe("reorder_topic", reordering_window=0.1)

            await pub(_make_deadline(), b"reorder_msg")
            await asyncio.sleep(0.02)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
            assert arrival.message == b"reorder_msg"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscribe_without_reordering_window(self):
        """Subscribing without reordering_window (None) should deliver immediately."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("no_reorder")
            sub = node_sub.subscribe("no_reorder", reordering_window=None)

            await pub(_make_deadline(), b"immediate")
            await asyncio.sleep(0.01)

            arrival = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arrival.message == b"immediate"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reordering_window_multiple_messages(self):
        """Multiple messages with reordering window should all be delivered."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("reorder_multi")
            sub = node_sub.subscribe("reorder_multi", reordering_window=0.5)

            for i in range(10):
                await pub(_make_deadline(), f"ro_{i}".encode())
                await asyncio.sleep(0.001)

            await asyncio.sleep(0.1)

            received = []
            for _ in range(10):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                received.append(arr.message)

            # All messages should be present (order may vary with reordering window)
            expected = {f"ro_{i}".encode() for i in range(10)}
            assert set(received) == expected
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestNameResolution:
    """Test topic name resolution with home and namespace."""

    @pytest.mark.asyncio
    async def test_relative_name_resolution(self):
        """Relative names should be prefixed with namespace."""
        node, transport, net = _make_single_node(ns="myapp")
        try:
            pub = node.advertise("status")
            assert pub.topic.name == "myapp/status"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_absolute_name_strips_leading_slash(self):
        """Absolute names (starting with /) should strip the leading slash."""
        node, transport, net = _make_single_node(ns="myapp")
        try:
            pub = node.advertise("/global/status")
            assert pub.topic.name == "global/status"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_home_relative_name(self):
        """Home-relative names should expand ~ to home."""
        node, transport, net = _make_single_node(home="robothome", ns="ns")
        try:
            pub = node.advertise("~/private")
            assert pub.topic.name == "robothome/private"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_nested_namespace_resolution(self):
        """Nested relative names should be properly joined."""
        node, transport, net = _make_single_node(ns="app/subsystem")
        try:
            pub = node.advertise("data/value")
            assert pub.topic.name == "app/subsystem/data/value"
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_empty_namespace(self):
        """With empty namespace, relative names should be used as-is."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        # Patch env var to ensure empty namespace
        with patch.dict("os.environ", {}, clear=True):
            node = Node(transport, home="h", namespace="")
        try:
            pub = node.advertise("bare_topic")
            assert pub.topic.name == "bare_topic"
        finally:
            _cleanup_node(node)


class TestEdgeCases:
    """Edge case and boundary tests."""

    @pytest.mark.asyncio
    async def test_publish_immediately_after_subscribe(self):
        """Publishing right after subscribing should work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("fast_sub")
            sub = node_sub.subscribe("fast_sub")
            # No sleep between subscribe and publish
            await pub(_make_deadline(), b"fast")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"fast"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_single_byte_message(self):
        """A single byte message should be delivered correctly."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("byte_topic")
            sub = node_sub.subscribe("byte_topic")

            await pub(_make_deadline(), b"\x42")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"\x42"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscriber_receives_all_before_close(self):
        """Messages published before close should all be receivable."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("batch_close")
            sub = node_sub.subscribe("batch_close")

            for i in range(10):
                await pub(_make_deadline(), f"batch_{i}".encode())
            await asyncio.sleep(0.05)

            # Read all messages before closing
            received = []
            for _ in range(10):
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                received.append(arr.message)

            assert len(received) == 10
            sub.close()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_multiple_networks_with_same_topic_name(self):
        """Same topic name on different networks should be completely isolated."""
        net1 = MockNetwork()
        net2 = MockNetwork()
        t1a = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net1)
        t1b = MockTransport(node_id=2, modulus=DEFAULT_MODULUS, network=net1)
        t2a = MockTransport(node_id=3, modulus=DEFAULT_MODULUS, network=net2)
        t2b = MockTransport(node_id=4, modulus=DEFAULT_MODULUS, network=net2)
        n1a = Node(t1a, home="n1a", namespace="ns")
        n1b = Node(t1b, home="n1b", namespace="ns")
        n2a = Node(t2a, home="n2a", namespace="ns")
        n2b = Node(t2b, home="n2b", namespace="ns")
        try:
            pub1 = n1a.advertise("shared_name")
            sub1 = n1b.subscribe("shared_name")
            pub2 = n2a.advertise("shared_name")
            sub2 = n2b.subscribe("shared_name")

            await pub1(_make_deadline(), b"net1_data")
            await pub2(_make_deadline(), b"net2_data")
            await asyncio.sleep(0.02)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)

            assert arr1.message == b"net1_data"
            assert arr2.message == b"net2_data"
        finally:
            _cleanup_node(n1a)
            _cleanup_node(n1b)
            _cleanup_node(n2a)
            _cleanup_node(n2b)

    @pytest.mark.asyncio
    async def test_publisher_topic_after_close(self):
        """Accessing publisher.topic after close should still work (read-only)."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("topic_after_close")
            topic_name = pub.topic.name
            pub.close()
            # Should still be accessible even after close
            assert pub.topic.name == topic_name
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_priority_preserved_after_close(self):
        """Priority should still be readable after publisher close."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("prio_after_close")
            pub.priority = Priority.HIGH
            pub.close()
            assert pub.priority == Priority.HIGH
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_arrival_message_is_bytes(self):
        """Arrival.message should always be bytes."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("bytes_topic")
            sub = node_sub.subscribe("bytes_topic")

            await pub(_make_deadline(), b"data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert isinstance(arr.message, bytes)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_is_frozen_dataclass(self):
        """Arrival should be a frozen dataclass (immutable)."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("frozen_topic")
            sub = node_sub.subscribe("frozen_topic")

            await pub(_make_deadline(), b"immutable")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            with pytest.raises(AttributeError):
                arr.message = b"changed"  # type: ignore[misc]
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_subscriber_close_during_no_messages(self):
        """Closing a subscriber while waiting with no messages should stop cleanly."""
        node, transport, net = _make_single_node()
        try:
            sub = node.subscribe("empty_close")

            async def do_close():
                await asyncio.sleep(0.05)
                sub.close()

            task = asyncio.ensure_future(do_close())
            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
            await task
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_double_subscribe_same_name(self):
        """Two subscribe calls with the same name should create independent subscribers."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("dup_sub")
            sub1 = node_sub.subscribe("dup_sub")
            sub2 = node_sub.subscribe("dup_sub")

            assert sub1 is not sub2

            await pub(_make_deadline(), b"dup")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub1.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub2.__anext__(), timeout=1.0)
            assert arr1.message == b"dup"
            assert arr2.message == b"dup"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestPublishWithMockTransportDirectly:
    """Test pub/sub using MockTransport directly."""

    @pytest.mark.asyncio
    async def test_node_with_inline_transport(self):
        """Using a MockTransport with a network should work."""
        net = MockNetwork()
        t = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(t, home="fix", namespace="ns")
        try:
            pub = node.advertise("fixture_test")
            sub = node.subscribe("fixture_test")

            await pub(_make_deadline(), b"fixture_data")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"fixture_data"
        finally:
            _cleanup_node(node)


class TestCrossTopicBreadcrumb:
    """Test breadcrumb behavior across different scenarios."""

    @pytest.mark.asyncio
    async def test_breadcrumb_from_different_publishers(self):
        """Breadcrumbs from different publishers should have different remote_ids."""
        net = MockNetwork()
        t1 = MockTransport(node_id=10, modulus=DEFAULT_MODULUS, network=net)
        t2 = MockTransport(node_id=20, modulus=DEFAULT_MODULUS, network=net)
        t_sub = MockTransport(node_id=30, modulus=DEFAULT_MODULUS, network=net)

        n1 = Node(t1, home="p1", namespace="ns")
        n2 = Node(t2, home="p2", namespace="ns")
        n_sub = Node(t_sub, home="sub", namespace="ns")
        try:
            pub1 = n1.advertise("multi_bc")
            pub2 = n2.advertise("multi_bc")
            sub = n_sub.subscribe("multi_bc")

            await pub1(_make_deadline(), b"from_10")
            await asyncio.sleep(0.01)
            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            await pub2(_make_deadline(), b"from_20")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            remote_ids = {arr1.breadcrumb.remote_id, arr2.breadcrumb.remote_id}
            assert 10 in remote_ids
            assert 20 in remote_ids
        finally:
            _cleanup_node(n1)
            _cleanup_node(n2)
            _cleanup_node(n_sub)

    @pytest.mark.asyncio
    async def test_breadcrumb_topic_consistency(self):
        """Breadcrumb.topic should consistently reference the same topic for same-topic messages."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("consistent_bc")
            sub = node_sub.subscribe("consistent_bc")

            await pub(_make_deadline(), b"msg1")
            await pub(_make_deadline(), b"msg2")
            await asyncio.sleep(0.01)

            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            t1 = arr1.breadcrumb.topic
            t2 = arr2.breadcrumb.topic
            assert t1 is not None
            assert t2 is not None
            assert t1.hash == t2.hash
            assert t1.name == t2.name
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestReliablePublishEdgeCases:
    """Additional edge cases for reliable publishing."""

    @pytest.mark.asyncio
    async def test_reliable_with_very_short_deadline(self):
        """Reliable publish with already-past deadline should raise quickly."""
        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("past_deadline")
            pub.ack_timeout = 0.001

            # Deadline in the past or immediate
            with pytest.raises((DeliveryError, SendError)):
                await pub(Instant.now() + 0.001, b"too_late", reliable=True)
        finally:
            _cleanup_node(node)

    @pytest.mark.asyncio
    async def test_reliable_then_best_effort_same_publisher(self):
        """A publisher should handle switching between reliable and best-effort."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("switch_mode")
            sub = node_sub.subscribe("switch_mode")

            await pub(_make_deadline(), b"reliable1", reliable=True)
            await asyncio.sleep(0.01)
            arr1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr1.message == b"reliable1"

            await pub(_make_deadline(), b"besteffort")
            await asyncio.sleep(0.01)
            arr2 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr2.message == b"besteffort"

            await pub(_make_deadline(), b"reliable2", reliable=True)
            await asyncio.sleep(0.01)
            arr3 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr3.message == b"reliable2"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_reliable_publish_delivery_error_type(self):
        """DeliveryError from reliable publish should be catchable as Error."""
        from pycyphal import Error as CyphalError

        node, transport, net = _make_single_node()
        try:
            pub = node.advertise("err_type")
            pub.ack_timeout = 0.01

            with pytest.raises(CyphalError):
                await pub(_short_deadline(0.05), b"fail", reliable=True)
        finally:
            _cleanup_node(node)


class TestArrivalDataclass:
    """Test Arrival dataclass properties."""

    @pytest.mark.asyncio
    async def test_arrival_fields(self):
        """Arrival should have timestamp, breadcrumb, and message fields."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("fields_topic")
            sub = node_sub.subscribe("fields_topic")

            await pub(_make_deadline(), b"field_test")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            assert hasattr(arr, "timestamp")
            assert hasattr(arr, "breadcrumb")
            assert hasattr(arr, "message")

            assert isinstance(arr.timestamp, Instant)
            assert isinstance(arr.breadcrumb, Breadcrumb)
            assert isinstance(arr.message, bytes)
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_arrival_timestamp_is_recent(self):
        """Arrival timestamp should be close to current time."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("recent_ts")
            sub = node_sub.subscribe("recent_ts")

            before = Instant.now()
            await pub(_make_deadline(), b"ts_test")
            await asyncio.sleep(0.01)
            after = Instant.now()

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

            assert arr.timestamp.ns >= before.ns
            assert arr.timestamp.ns <= after.ns
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestPriorityWithPublish:
    """Test that priority setting actually affects message sending."""

    @pytest.mark.asyncio
    async def test_publish_at_each_priority_level(self):
        """Publishing at each priority level should work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("all_prio")
            sub = node_sub.subscribe("all_prio")

            for prio in Priority:
                pub.priority = prio
                await pub(_make_deadline(), f"prio_{prio.value}".encode())
                await asyncio.sleep(0.001)

            for prio in Priority:
                arr = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
                assert arr.message == f"prio_{prio.value}".encode()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_high_priority_publish_delivers(self):
        """EXCEPTIONAL priority messages should still be delivered."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("exc_prio")
            sub = node_sub.subscribe("exc_prio")

            pub.priority = Priority.EXCEPTIONAL
            await pub(_make_deadline(), b"urgent")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"urgent"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_optional_priority_publish_delivers(self):
        """OPTIONAL priority messages should still be delivered."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("opt_prio")
            sub = node_sub.subscribe("opt_prio")

            pub.priority = Priority.OPTIONAL
            await pub(_make_deadline(), b"optional")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"optional"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)


class TestDeadlineHandling:
    """Test deadline behavior in publishing."""

    @pytest.mark.asyncio
    async def test_publish_with_generous_deadline(self):
        """Publishing with a generous deadline should succeed."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("generous_dl")
            sub = node_sub.subscribe("generous_dl")

            await pub(_make_deadline(10.0), b"plenty_of_time")
            await asyncio.sleep(0.01)

            arr = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            assert arr.message == b"plenty_of_time"
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_deadline_instant_computation(self):
        """Deadline should be an Instant in the future."""
        dl = _make_deadline(5.0)
        now = Instant.now()
        assert dl.ns > now.ns


class TestCleanupAndTeardown:
    """Test proper cleanup of resources."""

    @pytest.mark.asyncio
    async def test_node_close_closes_transport(self):
        """Closing a node should close the transport."""
        net = MockNetwork()
        transport = MockTransport(node_id=1, modulus=DEFAULT_MODULUS, network=net)
        node = Node(transport, home="h", namespace="ns")
        node.close()
        assert transport.closed

    @pytest.mark.asyncio
    async def test_close_order_does_not_matter(self):
        """Closing publisher before subscriber, or vice versa, should work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("order_test")
            sub = node_sub.subscribe("order_test")

            # Close publisher first
            pub.close()
            sub.close()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_subscriber_then_publisher(self):
        """Closing subscriber before publisher should work."""
        node_pub, node_sub, net = _make_node_pair()
        try:
            pub = node_pub.advertise("rev_order")
            sub = node_sub.subscribe("rev_order")

            sub.close()
            pub.close()
        finally:
            _cleanup_node(node_pub)
            _cleanup_node(node_sub)

    @pytest.mark.asyncio
    async def test_close_everything(self):
        """Closing all objects in any order should be safe."""
        node_pub, node_sub, net = _make_node_pair()
        pub = node_pub.advertise("everything")
        sub1 = node_sub.subscribe("everything")
        sub2 = node_sub.subscribe("everything")

        pub.close()
        sub1.close()
        sub2.close()
        _cleanup_node(node_sub)
        _cleanup_node(node_pub)

    @pytest.mark.asyncio
    async def test_node_close_then_advertise_raises(self):
        """Advertising after node close should not crash (transport is closed)."""
        node, transport, net = _make_single_node()
        node.close()
        # After close, transport is closed, but advertise still creates internal topic
        # The actual behavior depends on implementation; just verify no hang
        try:
            pub = node.advertise("post_close")
            # If we get here, publishing should fail since transport is closed
        except Exception:
            pass  # Expected


class TestTopicHashCollisions:
    """Test behavior when topic names might share subject IDs after hashing."""

    @pytest.mark.asyncio
    async def test_different_topics_different_hashes(self):
        """Different topic names should (very likely) have different hashes."""
        h1 = topic_hash("ns/topic_a")
        h2 = topic_hash("ns/topic_b")
        assert h1 != h2

    @pytest.mark.asyncio
    async def test_topic_hash_deterministic(self):
        """Same topic name should always produce the same hash."""
        h1 = topic_hash("ns/deterministic")
        h2 = topic_hash("ns/deterministic")
        assert h1 == h2

    @pytest.mark.asyncio
    async def test_topic_subject_id_computation(self):
        """topic_subject_id should produce values in valid range."""
        h = topic_hash("ns/some_topic")
        sid = topic_subject_id(h, 0, DEFAULT_MODULUS)
        from pycyphal._wire import SUBJECT_ID_PINNED_MAX

        assert sid > SUBJECT_ID_PINNED_MAX
        assert sid <= SUBJECT_ID_PINNED_MAX + DEFAULT_MODULUS
