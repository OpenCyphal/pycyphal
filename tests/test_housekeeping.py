"""Regression tests for finding #3: per-remote dedup and reordering state must be swept in aggregate on a
time basis, not only when the arriving remote sends again, so neither grows without bound under untrusted
traffic."""

from __future__ import annotations

import asyncio
import time

import pytest

import pycyphal2
from pycyphal2._node import SESSION_LIFETIME, DedupState
from pycyphal2._subscriber import ReorderingState, SubscriberImpl
from tests.mock_transport import MockNetwork, MockTransport
from tests.typing_helpers import new_node


async def test_sweep_drops_stale_dedup_for_departed_remotes() -> None:
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n1")
    sub = node.subscribe("/t")
    topic = node.topics_by_name["t"]

    base = 1000.0
    topic.dedup[10] = DedupState(tag_frontier=1, last_active=base)
    topic.dedup[11] = DedupState(tag_frontier=1, last_active=base)
    topic.dedup[12] = DedupState(tag_frontier=1, last_active=base + 10_000.0)  # Recently active.

    node.sweep_stale_states(base + SESSION_LIFETIME + 1.0)
    assert 10 not in topic.dedup  # Departed remotes are swept in aggregate...
    assert 11 not in topic.dedup
    assert 12 in topic.dedup  # ...while a recently-active remote is retained.

    sub.close()
    node.close()


async def test_sweep_drops_stale_reordering_states() -> None:
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n1")
    sub = node.subscribe("/t", reordering_window=0.2)
    assert isinstance(sub, SubscriberImpl)

    base = 1000.0
    sub._reordering[(10, 0xAAAA)] = ReorderingState(last_active_at=base)
    sub._reordering[(11, 0xBBBB)] = ReorderingState(last_active_at=base + 10_000.0)

    node.sweep_stale_states(base + SESSION_LIFETIME + 1.0)
    assert (10, 0xAAAA) not in sub._reordering  # Idle stream swept...
    assert (11, 0xBBBB) in sub._reordering  # ...recently-active one retained.

    sub.close()
    node.close()


async def test_housekeeping_loop_sweeps_without_new_traffic(monkeypatch: pytest.MonkeyPatch) -> None:
    """The background loop must retire stale state on its own schedule, even if the remote never sends
    again (previously the sweep was only triggered by a new arrival)."""
    monkeypatch.setattr(pycyphal2._node, "HOUSEKEEPING_PERIOD", 0.02)
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n1")
    sub = node.subscribe("/t")
    topic = node.topics_by_name["t"]

    topic.dedup[10] = DedupState(tag_frontier=1, last_active=time.monotonic() - SESSION_LIFETIME - 5.0)
    for _ in range(100):
        if 10 not in topic.dedup:
            break
        await asyncio.sleep(0.01)
    assert 10 not in topic.dedup  # The loop swept it with no further traffic.

    sub.close()
    node.close()


async def test_housekeeping_loop_survives_a_raising_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """The sweep is the only bound on per-remote state growth, so the loop must outlive a faulty sweep.
    Catching only CancelledError let one stray exception kill the task silently -- nothing retrieves its
    result -- leaving the node unbounded for the rest of its life."""
    monkeypatch.setattr(pycyphal2._node, "HOUSEKEEPING_PERIOD", 0.02)
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n1")
    sub = node.subscribe("/t")
    topic = node.topics_by_name["t"]

    calls = 0
    real_sweep = node.sweep_stale_states

    def flaky_sweep(now: float) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated sweep fault")
        real_sweep(now)

    monkeypatch.setattr(node, "sweep_stale_states", flaky_sweep)

    topic.dedup[10] = DedupState(tag_frontier=1, last_active=time.monotonic() - SESSION_LIFETIME - 5.0)
    for _ in range(200):
        if calls >= 2 and 10 not in topic.dedup:
            break
        await asyncio.sleep(0.01)
    assert calls >= 2, "the loop died on the first faulty sweep"
    assert 10 not in topic.dedup  # It recovered and swept on a later tick.
    assert not node._housekeeping_task.done()

    sub.close()
    node.close()


async def test_implicit_gc_loop_survives_a_raising_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    """A faulty retirement must not permanently disable implicit-topic GC. An expired topic yields a zero
    delay, so the recovery path must also back off rather than spin at full speed."""
    monkeypatch.setattr(pycyphal2._node, "HOUSEKEEPING_PERIOD", 0.02)
    monkeypatch.setattr(pycyphal2._node, "IMPLICIT_TOPIC_TIMEOUT", 0.02)
    tr = MockTransport(node_id=1, network=MockNetwork())
    node = new_node(tr, home="n1")

    calls = 0
    real_retire = node._retire_one_expired_implicit_topic

    def flaky_retire(now: float) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated retirement fault")
        return real_retire(now)

    monkeypatch.setattr(node, "_retire_one_expired_implicit_topic", flaky_retire)

    node.topic_ensure("gc-me", None)  # Implicit: no publisher, no subscriber.
    for _ in range(200):
        if calls >= 2 and "gc-me" not in node.topics_by_name:
            break
        await asyncio.sleep(0.01)
    assert calls >= 2, "the loop died on the first faulty retirement"
    assert "gc-me" not in node.topics_by_name  # It recovered and retired the topic.
    assert not node._gc_task.done()

    node.close()
