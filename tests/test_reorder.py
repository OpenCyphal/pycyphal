"""Tests for the subscriber reordering window."""

from __future__ import annotations

import asyncio

import pycyphal2
from pycyphal2._node import REORDERING_CAPACITY, SESSION_LIFETIME
from pycyphal2._subscriber import BreadcrumbImpl, SubscriberImpl
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import expect_arrival, new_node, subscribe_impl

ORDERED_WINDOW = 0.05


def _make_arrival(ts_offset: float, breadcrumb: BreadcrumbImpl, payload: bytes = b"") -> pycyphal2.Arrival:
    return pycyphal2.Arrival(
        timestamp=pycyphal2.Instant.now() + ts_offset,
        breadcrumb=breadcrumb,
        message=payload,
    )


async def _bootstrap_ordered(
    sub: SubscriberImpl,
    bc: BreadcrumbImpl,
    base_tag: int,
    remote_id: int,
    payload: bytes,
) -> None:
    sub.deliver(_make_arrival(0.0, bc, payload), base_tag, remote_id)
    assert sub.queue.empty()
    await asyncio.sleep(ORDERED_WINDOW + 0.05)
    assert expect_arrival(sub.queue.get_nowait()).message == payload
    assert sub.queue.empty()


async def test_reorder_in_order():
    """A new ordered stream is delivered only after the first reordering window closes."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    for i in range(5):
        arr = _make_arrival(0.0, bc, f"msg{i}".encode())
        sub.deliver(arr, base_tag + i, 99)

    assert sub.queue.empty()
    await asyncio.sleep(ORDERED_WINDOW + 0.05)

    for i in range(5):
        assert expect_arrival(sub.queue.get_nowait()).message == f"msg{i}".encode()

    assert sub.queue.empty()
    sub.close()
    node.close()


async def test_reorder_out_of_order():
    """Out-of-order messages within capacity should be buffered and delivered in order."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    await _bootstrap_ordered(sub, bc, base_tag, 99, b"first")

    sub.deliver(_make_arrival(0.0, bc, b"third"), base_tag + 2, 99)
    assert sub.queue.empty()

    sub.deliver(_make_arrival(0.0, bc, b"second"), base_tag + 1, 99)
    assert expect_arrival(sub.queue.get_nowait()).message == b"second"
    assert expect_arrival(sub.queue.get_nowait()).message == b"third"

    sub.close()
    node.close()


async def test_reorder_late_message_dropped():
    """Messages with tags behind the frontier should be dropped."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    await _bootstrap_ordered(sub, bc, base_tag, 99, b"m0")
    sub.deliver(_make_arrival(0.0, bc, b"m1"), base_tag + 1, 99)
    assert expect_arrival(sub.queue.get_nowait()).message == b"m1"

    sub.deliver(_make_arrival(0.0, bc, b"late"), base_tag, 99)
    assert sub.queue.empty()

    sub.close()
    node.close()


async def test_reorder_timeout_ejects():
    """The first arrival is interned and then force-ejected when its window expires."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    sub.deliver(_make_arrival(0.0, bc, b"m0"), base_tag, 99)
    assert sub.queue.empty()
    await asyncio.sleep(ORDERED_WINDOW + 0.05)
    assert expect_arrival(sub.queue.get_nowait()).message == b"m0"

    sub.close()
    node.close()


async def test_reorder_capacity_overflow():
    """A far-ahead tag force-ejects older interned slots, then waits in the resequenced window."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    await _bootstrap_ordered(sub, bc, base_tag, 99, b"m0")

    sub.deliver(_make_arrival(0.0, bc, b"m3"), base_tag + 3, 99)
    sub.deliver(_make_arrival(0.0, bc, b"m5"), base_tag + 5, 99)
    assert sub.queue.empty()

    far_tag = base_tag + REORDERING_CAPACITY + 5
    sub.deliver(_make_arrival(0.0, bc, b"far"), far_tag, 99)

    items = []
    while not sub.queue.empty():
        items.append(expect_arrival(sub.queue.get_nowait()))
    assert [i.message for i in items] == [b"m3", b"m5"]

    await asyncio.sleep(ORDERED_WINDOW + 0.05)
    assert expect_arrival(sub.queue.get_nowait()).message == b"far"

    sub.close()
    node.close()


async def test_reorder_gap_closure():
    """Delivering the missing message should close the gap and eject buffered messages."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    await _bootstrap_ordered(sub, bc, base_tag, 99, b"m0")

    sub.deliver(_make_arrival(0.0, bc, b"m2"), base_tag + 2, 99)
    sub.deliver(_make_arrival(0.0, bc, b"m3"), base_tag + 3, 99)
    sub.deliver(_make_arrival(0.0, bc, b"m4"), base_tag + 4, 99)
    assert sub.queue.empty()

    sub.deliver(_make_arrival(0.0, bc, b"m1"), base_tag + 1, 99)

    items = []
    while not sub.queue.empty():
        items.append(expect_arrival(sub.queue.get_nowait()))
    assert [i.message for i in items] == [b"m1", b"m2", b"m3", b"m4"]

    sub.close()
    node.close()


async def test_reorder_no_reordering():
    """Without reordering window, messages are delivered ASAP regardless of order."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic")  # No reordering window.

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    # Deliver out-of-order.
    sub.deliver(_make_arrival(0.0, bc, b"m2"), 1002, 99)
    sub.deliver(_make_arrival(0.0, bc, b"m0"), 1000, 99)
    sub.deliver(_make_arrival(0.0, bc, b"m1"), 1001, 99)

    items = []
    while not sub.queue.empty():
        items.append(expect_arrival(sub.queue.get_nowait()))
    # Should arrive in delivery order, not tag order.
    assert [i.message for i in items] == [b"m2", b"m0", b"m1"]

    sub.close()
    node.close()


async def test_reorder_multiple_remotes():
    """Reordering is per (remote_id, topic_hash), so different remotes are independent."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc1 = BreadcrumbImpl(
        node=node, remote_id=10, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )
    bc2 = BreadcrumbImpl(
        node=node, remote_id=20, topic=topic, message_tag=2, initial_priority=pycyphal2.Priority.NOMINAL
    )

    await _bootstrap_ordered(sub, bc1, 100, 10, b"r10-m0")
    await _bootstrap_ordered(sub, bc2, 200, 20, b"r20-m0")

    sub.deliver(_make_arrival(0.0, bc1, b"r10-m2"), 102, 10)
    assert sub.queue.empty()

    sub.deliver(_make_arrival(0.0, bc2, b"r20-m1"), 201, 20)
    assert expect_arrival(sub.queue.get_nowait()).message == b"r20-m1"

    sub.deliver(_make_arrival(0.0, bc1, b"r10-m1"), 101, 10)
    items = []
    while not sub.queue.empty():
        items.append(expect_arrival(sub.queue.get_nowait()))
    assert [i.message for i in items] == [b"r10-m1", b"r10-m2"]

    sub.close()
    node.close()


async def test_reorder_state_expires_after_session_lifetime():
    """An idle ordered stream should be resequenced after SESSION_LIFETIME."""
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    sub = subscribe_impl(node, "test/topic", reordering_window=ORDERED_WINDOW)

    topic = list(node.topics_by_name.values())[0]
    bc = BreadcrumbImpl(
        node=node, remote_id=99, topic=topic, message_tag=1, initial_priority=pycyphal2.Priority.NOMINAL
    )

    base_tag = 1000
    await _bootstrap_ordered(sub, bc, base_tag, 99, b"first")

    state = sub._reordering[(99, topic.hash)]
    state.last_active_at -= SESSION_LIFETIME + 1.0

    sub.deliver(_make_arrival(0.0, bc, b"restart"), base_tag, 99)
    assert sub.queue.empty()

    await asyncio.sleep(ORDERED_WINDOW + 0.05)
    assert expect_arrival(sub.queue.get_nowait()).message == b"restart"

    sub.close()
    node.close()
