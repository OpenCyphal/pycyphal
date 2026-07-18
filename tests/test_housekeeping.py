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
