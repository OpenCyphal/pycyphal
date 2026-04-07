"""Typed helpers for white-box tests that exercise private implementation details."""

from __future__ import annotations

from typing import assert_type

import pycyphal
from pycyphal._node import NodeImpl, TopicImpl
from pycyphal._publisher import PublisherImpl, ResponseStreamImpl
from pycyphal._subscriber import SubscriberImpl
from tests.mock_transport import MockSubjectWriter


def new_node(transport: pycyphal.Transport, *, home: str = "", namespace: str = "") -> NodeImpl:
    node = pycyphal.new(transport, home=home, namespace=namespace)
    assert isinstance(node, NodeImpl)
    return node


def first_topic(node: NodeImpl) -> TopicImpl:
    topic = next(iter(node.topics_by_name.values()))
    assert_type(topic, TopicImpl)
    return topic


def advertise_impl(node: NodeImpl, name: str) -> PublisherImpl:
    pub = node.advertise(name)
    assert isinstance(pub, PublisherImpl)
    return pub


def subscribe_impl(node: NodeImpl, name: str, *, reordering_window: float | None = None) -> SubscriberImpl:
    sub = node.subscribe(name, reordering_window=reordering_window)
    assert isinstance(sub, SubscriberImpl)
    return sub


async def request_stream(
    pub: pycyphal.Publisher,
    delivery_deadline: pycyphal.Instant,
    response_timeout: float,
    message: memoryview | bytes,
) -> ResponseStreamImpl:
    stream = await pub.request(delivery_deadline, response_timeout, message)
    assert isinstance(stream, ResponseStreamImpl)
    return stream


def expect_arrival(item: pycyphal.Arrival | BaseException) -> pycyphal.Arrival:
    assert isinstance(item, pycyphal.Arrival)
    return item


def expect_response(item: pycyphal.Response | BaseException) -> pycyphal.Response:
    assert isinstance(item, pycyphal.Response)
    return item


def expect_mock_writer(writer: pycyphal.SubjectWriter | None) -> MockSubjectWriter:
    assert isinstance(writer, MockSubjectWriter)
    return writer
