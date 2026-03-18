"""Mock transport and network for testing."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from pycyphal import Closable, Instant, Priority, TransportArrival

# Default modulus matching CY_SUBJECT_ID_MODULUS_17bit
DEFAULT_MODULUS = 122743


# =====================================================================================================================
# MockSubjectWriter
# =====================================================================================================================


class MockSubjectWriter(Closable):
    def __init__(self, transport: MockTransport, subject_id: int) -> None:
        self.transport = transport
        self.subject_id = subject_id
        self.closed = False
        self.send_count = 0
        self.fail_next = False

    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        if self.closed:
            raise RuntimeError("Writer closed")
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("Simulated send failure")
        self.send_count += 1
        msg_bytes = bytes(message)
        arrival = TransportArrival(
            timestamp=Instant.now(),
            priority=priority,
            remote_id=self.transport.node_id,
            message=msg_bytes,
        )
        # Deliver to all listeners on this subject across the network
        if self.transport.network is not None:
            self.transport.network.deliver_subject(self.subject_id, arrival, sender=self.transport)
        else:
            # Local-only delivery
            for handler in self.transport._subject_handlers.get(self.subject_id, []):
                handler(arrival)

    def close(self) -> None:
        self.closed = True


# =====================================================================================================================
# MockSubjectListener
# =====================================================================================================================


class MockSubjectListener(Closable):
    def __init__(self, transport: MockTransport, subject_id: int, handler: Callable[[TransportArrival], None]) -> None:
        self.transport = transport
        self.subject_id = subject_id
        self.handler = handler
        self.closed = False

    def close(self) -> None:
        self.closed = True
        handlers = self.transport._subject_handlers.get(self.subject_id, [])
        if self.handler in handlers:
            handlers.remove(self.handler)
        if not handlers:
            self.transport._subject_handlers.pop(self.subject_id, None)


# =====================================================================================================================
# MockTransport
# =====================================================================================================================


class MockTransport(Closable):
    def __init__(self, node_id: int = 0, modulus: int = DEFAULT_MODULUS, network: MockNetwork | None = None) -> None:
        self.node_id = node_id
        self._modulus = modulus
        self.network = network
        self._subject_handlers: dict[int, list[Callable[[TransportArrival], None]]] = {}
        self._unicast_handler: Callable[[TransportArrival], None] | None = None
        self._writers: dict[int, MockSubjectWriter] = {}
        self.unicast_log: list[tuple[int, bytes]] = []
        self.closed = False
        self.fail_unicast = False

        if network is not None:
            network.add_transport(self)

    @property
    def subject_id_modulus(self) -> int:
        return self._modulus

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if subject_id not in self._subject_handlers:
            self._subject_handlers[subject_id] = []
        self._subject_handlers[subject_id].append(handler)
        return MockSubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> MockSubjectWriter:
        writer = MockSubjectWriter(self, subject_id)
        self._writers[subject_id] = writer
        return writer

    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        self._unicast_handler = handler

    async def unicast(self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview) -> None:
        if self.closed:
            raise RuntimeError("Transport closed")
        if self.fail_unicast:
            raise RuntimeError("Simulated unicast failure")
        msg_bytes = bytes(message)
        self.unicast_log.append((remote_id, msg_bytes))
        arrival = TransportArrival(
            timestamp=Instant.now(),
            priority=priority,
            remote_id=self.node_id,
            message=msg_bytes,
        )
        if self.network is not None:
            self.network.deliver_unicast(remote_id, arrival)
        else:
            # Local unicast: deliver to own handler
            if self._unicast_handler is not None:
                self._unicast_handler(arrival)

    def close(self) -> None:
        self.closed = True

    def deliver_subject(self, subject_id: int, arrival: TransportArrival) -> None:
        """Deliver a subject message to local handlers."""
        for handler in self._subject_handlers.get(subject_id, []):
            handler(arrival)

    def deliver_unicast(self, arrival: TransportArrival) -> None:
        """Deliver a unicast message to local handler."""
        if self._unicast_handler is not None:
            self._unicast_handler(arrival)


# =====================================================================================================================
# MockNetwork
# =====================================================================================================================


class MockNetwork:
    """Simulates a network connecting multiple MockTransport instances."""

    def __init__(self, *, delay: float = 0.0, drop_rate: float = 0.0) -> None:
        self.transports: dict[int, MockTransport] = {}
        self.delay = delay
        self.drop_rate = drop_rate
        self.message_log: list[tuple[str, int, bytes]] = []

    def add_transport(self, transport: MockTransport) -> None:
        self.transports[transport.node_id] = transport

    def deliver_subject(self, subject_id: int, arrival: TransportArrival, sender: MockTransport) -> None:
        """Deliver subject message to all transports (including sender for loopback)."""
        for tid, transport in self.transports.items():
            if random.random() < self.drop_rate:
                continue
            transport.deliver_subject(subject_id, arrival)

    def deliver_unicast(self, remote_id: int, arrival: TransportArrival) -> None:
        """Deliver unicast message to specific transport."""
        transport = self.transports.get(remote_id)
        if transport is not None:
            if random.random() >= self.drop_rate:
                transport.deliver_unicast(arrival)


# =====================================================================================================================
# Fixtures
# =====================================================================================================================


@pytest.fixture
def mock_network():
    return MockNetwork()


@pytest.fixture
def mock_transport():
    return MockTransport(node_id=1)


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
