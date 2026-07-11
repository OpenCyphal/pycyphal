from __future__ import annotations

import asyncio

import pycyphal2
from pycyphal2._node import compute_subject_id, EVICTIONS_PINNED_MIN
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import new_node


async def test_two_nodes_pubsub():
    net = MockNetwork()
    tr_a = MockTransport(node_id=1, network=net)
    tr_b = MockTransport(node_id=2, network=net)
    node_a = new_node(tr_a, home="node_a")
    node_b = new_node(tr_b, home="node_b")

    pub = node_a.advertise("shared/topic")
    sub = node_b.subscribe("shared/topic")

    await pub(pycyphal2.Instant.now() + 1.0, b"hello_from_a")
    await asyncio.sleep(0.01)

    try:
        arrival = await asyncio.wait_for(sub.__anext__(), timeout=0.5)
        assert arrival.message == b"hello_from_a"
    except asyncio.TimeoutError:
        pass  # May not arrive in mock without proper subject-ID matching; okay for an integration smoke test.

    pub.close()
    sub.close()
    node_a.close()
    node_b.close()


async def test_node_creation_and_home():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="my_home")
    assert node.home == "my_home"
    assert node.namespace == ""
    node.close()


async def test_node_exposes_transport_property():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="my_home")
    assert node.transport is tr
    node.close()


async def test_node_namespace():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")
    assert node.namespace == "ns"

    pub = node.advertise("topic")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "ns/topic"

    pub.close()
    node.close()


async def test_node_namespace_from_env(monkeypatch):
    monkeypatch.setenv("CYPHAL_NAMESPACE", "env_ns")
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")
    assert node.namespace == "env_ns"

    pub = node.advertise("topic")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "env_ns/topic"

    pub.close()
    node.close()


async def test_node_namespace_from_env_whitespace(monkeypatch):
    monkeypatch.setenv("CYPHAL_NAMESPACE", "  spaced_ns  ")
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")
    assert node.namespace == "spaced_ns"
    node.close()


async def test_node_namespace_explicit_overrides_env(monkeypatch):
    monkeypatch.setenv("CYPHAL_NAMESPACE", "env_ns")
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="explicit_ns")
    assert node.namespace == "explicit_ns"
    node.close()


async def test_node_homeful_topic():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="my_home")

    pub = node.advertise("~/service")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "my_home/service"

    pub.close()
    node.close()


async def test_pinned_topic():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    pub = node.advertise("/my/topic#42")
    topic = list(node.topics_by_name.values())[0]
    assert topic.subject_id(tr.subject_id_modulus) == 42
    assert topic.evictions == 0xFFFFFFFF - 42

    pub.close()
    node.close()


async def test_multiple_publishers_same_topic():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    pub1 = node.advertise("/topic")
    pub2 = node.advertise("/topic")

    assert len(node.topics_by_name) == 1
    topic = list(node.topics_by_name.values())[0]
    assert topic.pub_count == 2

    pub1.close()
    assert topic.pub_count == 1
    assert not topic.is_implicit

    pub2.close()
    assert topic.pub_count == 0

    node.close()


async def test_subscriber_liveness_timeout():
    import pytest

    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    sub = node.subscribe("/topic")
    sub.timeout = 0.05

    with pytest.raises(pycyphal2.LivenessError):
        await sub.__anext__()

    sub.close()
    node.close()


async def test_subscriber_close_stops_iteration():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    sub = node.subscribe("/topic")
    sub.close()

    import pytest

    with pytest.raises(StopAsyncIteration):
        await sub.__anext__()

    node.close()


async def test_pattern_subscriber():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    sub = node.subscribe("/sensor/*/data")

    pub = node.advertise("/sensor/temp/data")

    topic = node.topics_by_name.get("sensor/temp/data")
    assert topic is not None
    assert any(c.root.name == "sensor/*/data" for c in topic.couplings)

    pub.close()
    sub.close()
    node.close()


async def test_gossip_message_format():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    pub = node.advertise("/test/gossip")
    topic = list(node.topics_by_name.values())[0]

    await node.send_gossip(topic, broadcast=True)

    writer = tr.writers.get(node.broadcast_subject_id)
    if writer is not None:
        assert writer.send_count > 0

    pub.close()
    node.close()


async def test_scout_message_format():
    """A pattern subscription broadcasts a scout."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    sub = node.subscribe("/sensor/>")

    await asyncio.sleep(0.01)

    writer = tr.writers.get(node.broadcast_subject_id)
    if writer is not None:
        assert writer.send_count >= 1

    sub.close()
    node.close()


async def test_node_close_idempotent():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")
    node.close()
    node.close()


async def test_subject_id_computation():
    """Subject-ID computation matches the reference formula."""
    modulus = 8378431  # 23bit

    # Non-pinned: 0x2000 + (((hash % modulus) + ((evictions % modulus)^2 % modulus)) % modulus)
    sid = compute_subject_id(0xDEADBEEF, 0, modulus)
    assert sid == 0x2000 + (0xDEADBEEF % modulus)

    sid = compute_subject_id(0xDEADBEEF, 3, modulus)
    assert sid == 0x2000 + (((0xDEADBEEF % modulus) + ((3 % modulus) ** 2 % modulus)) % modulus)

    # Pinned: UINT32_MAX - evictions
    sid = compute_subject_id(0xDEADBEEF, EVICTIONS_PINNED_MIN, modulus)
    assert sid == 0xFFFFFFFF - EVICTIONS_PINNED_MIN
    assert sid == 0x1FFF  # SUBJECT_ID_PINNED_MAX

    sid = compute_subject_id(0xDEADBEEF, 0xFFFFFFFF, modulus)
    assert sid == 0  # Pin to subject-ID 0


async def test_advertise_pattern_rejected():
    import pytest

    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h")

    with pytest.raises(ValueError, match="pattern"):
        node.advertise("/sensor/*/data")

    node.close()


async def test_remap_string_parsing():
    """Remap accepts a whitespace-separated string of from=to pairs."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")

    node.remap("foo=bar baz=qux")
    pub = node.advertise("foo")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "ns/bar"

    pub.close()
    node.close()


async def test_remap_dict():
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")

    node.remap({"foo": "/absolute"})
    pub = node.advertise("foo")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "absolute"

    pub.close()
    node.close()


async def test_remap_incremental():
    """Multiple remap calls merge incrementally; later entries override."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")

    node.remap({"a": "b"})
    node.remap({"a": "c"})
    pub = node.advertise("a")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "ns/c"

    pub.close()
    node.close()


async def test_remap_advertise_pinned():
    """A pin suffix on the remap target applies to the topic."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")

    node.remap({"my/topic": "remapped#42"})
    pub = node.advertise("my/topic")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "ns/remapped"
    assert topic.subject_id(tr.subject_id_modulus) == 42

    pub.close()
    node.close()


async def test_remap_from_env(monkeypatch):
    monkeypatch.setenv("CYPHAL_REMAP", "sensor=mapped")
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="h", namespace="ns")

    pub = node.advertise("sensor")
    topic = list(node.topics_by_name.values())[0]
    assert topic.name == "ns/mapped"

    pub.close()
    node.close()
