"""Pytest fixtures for the test suite."""

from __future__ import annotations

import asyncio

import pytest

from tests.mock_transport import MockTransport, MockNetwork


@pytest.fixture
def mock_network() -> MockNetwork:
    return MockNetwork()


@pytest.fixture
def mock_transport() -> MockTransport:
    return MockTransport(node_id=1)


@pytest.fixture
def event_loop():  # type: ignore[no-untyped-def]
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
