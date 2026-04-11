"""Tests for Node.scout()."""

from __future__ import annotations

import pytest

import pycyphal2
from pycyphal2._header import HEADER_SIZE, ScoutHeader, deserialize_header
from pycyphal2._transport import TransportArrival
from tests.mock_transport import MockTransport, MockNetwork
from tests.typing_helpers import expect_mock_writer, new_node


async def test_public_scout_broadcasts_one_exact_query() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    observer = MockTransport(node_id=99, network=net)
    arrivals: list[TransportArrival] = []
    node = new_node(tr, home="n1")
    observer.subject_listen(node.broadcast_subject_id, arrivals.append)

    await node.scout("/sensor/temp")

    writer = expect_mock_writer(node.broadcast_writer)
    assert writer.send_count == 1
    assert len(arrivals) == 1
    hdr = deserialize_header(arrivals[0].message[:HEADER_SIZE])
    assert isinstance(hdr, ScoutHeader)
    assert arrivals[0].message[HEADER_SIZE:] == b"sensor/temp"

    node.close()
    observer.close()


async def test_public_scout_resolves_pattern_with_namespace_home_and_remap() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    observer = MockTransport(node_id=99, network=net)
    arrivals: list[TransportArrival] = []
    node = new_node(tr, home="me", namespace="ns")
    observer.subject_listen(node.broadcast_subject_id, arrivals.append)
    node.remap({"sensor/*": "~/diag/*"})

    await node.scout("sensor/*")

    assert len(arrivals) == 1
    hdr = deserialize_header(arrivals[0].message[:HEADER_SIZE])
    assert isinstance(hdr, ScoutHeader)
    assert arrivals[0].message[HEADER_SIZE:] == b"me/diag/*"

    node.close()
    observer.close()


async def test_public_scout_rejects_pinned_names() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")

    with pytest.raises(ValueError, match="pinned"):
        await node.scout("/sensor/temp#42")

    node.close()


async def test_public_scout_send_failure_raises_send_error() -> None:
    net = MockNetwork()
    tr = MockTransport(node_id=1, network=net)
    node = new_node(tr, home="n1")
    writer = expect_mock_writer(node.broadcast_writer)
    writer.fail_next = True

    with pytest.raises(pycyphal2.SendError, match="Scout send failed"):
        await node.scout("sensor/*")

    node.close()
