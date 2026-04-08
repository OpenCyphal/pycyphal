"""Mock transport and network for testing."""

from __future__ import annotations

import random
from collections.abc import Callable

from pycyphal2 import Closable, Instant, Priority, SubjectWriter, Transport, TransportArrival

# A small prime modulus suitable for testing.
DEFAULT_MODULUS = 122743


class MockSubjectWriter(SubjectWriter):
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
        if self.transport.network is not None:
            self.transport.network.deliver_subject(self.subject_id, arrival, sender=self.transport)
        else:
            handler = self.transport.subject_handlers.get(self.subject_id)
            if handler is not None:
                handler(arrival)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.transport.remove_subject_writer(self.subject_id, self)


class MockSubjectListener(Closable):
    def __init__(self, transport: MockTransport, subject_id: int, handler: Callable[[TransportArrival], None]) -> None:
        self.transport = transport
        self.subject_id = subject_id
        self.handler = handler
        self.closed = False

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.transport.remove_subject_listener(self.subject_id, self.handler)


class MockTransport(Transport):
    def __init__(self, node_id: int = 0, modulus: int = DEFAULT_MODULUS, network: MockNetwork | None = None) -> None:
        self.node_id = node_id
        self._modulus = modulus
        self.network = network
        self.subject_handlers: dict[int, Callable[[TransportArrival], None]] = {}
        self.subject_listener_creations: dict[int, int] = {}
        self.unicast_handler: Callable[[TransportArrival], None] | None = None
        self.writers: dict[int, MockSubjectWriter] = {}
        self.subject_writer_creations: dict[int, int] = {}
        self.unicast_log: list[tuple[int, bytes]] = []
        self.closed = False
        self.fail_unicast = False

        if network is not None:
            network.add_transport(self)

    def __repr__(self) -> str:
        return f"MockTransport(node_id={self.node_id}, modulus={self._modulus})"

    @property
    def subject_id_modulus(self) -> int:
        return self._modulus

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if subject_id in self.subject_handlers:
            raise ValueError(f"Subject {subject_id} already has an active listener")
        self.subject_handlers[subject_id] = handler
        self.subject_listener_creations[subject_id] = self.subject_listener_creations.get(subject_id, 0) + 1
        return MockSubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> MockSubjectWriter:
        if subject_id in self.writers:
            raise ValueError(f"Subject {subject_id} already has an active writer")
        writer = MockSubjectWriter(self, subject_id)
        self.writers[subject_id] = writer
        self.subject_writer_creations[subject_id] = self.subject_writer_creations.get(subject_id, 0) + 1
        return writer

    def remove_subject_listener(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> None:
        if self.subject_handlers.get(subject_id) is handler:
            self.subject_handlers.pop(subject_id, None)

    def remove_subject_writer(self, subject_id: int, writer: MockSubjectWriter) -> None:
        if self.writers.get(subject_id) is writer:
            self.writers.pop(subject_id, None)

    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        self.unicast_handler = handler

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
            if self.unicast_handler is not None:
                self.unicast_handler(arrival)

    def close(self) -> None:
        self.closed = True

    def deliver_subject(self, subject_id: int, arrival: TransportArrival) -> None:
        handler = self.subject_handlers.get(subject_id)
        if handler is not None:
            handler(arrival)

    def deliver_unicast(self, arrival: TransportArrival) -> None:
        if self.unicast_handler is not None:
            self.unicast_handler(arrival)


class MockNetwork:
    """Simulates a network connecting multiple MockTransport instances."""

    def __init__(self, *, delay: float = 0.0, drop_rate: float = 0.0) -> None:
        self.transports: dict[int, MockTransport] = {}
        self.delay = delay
        self.drop_rate = drop_rate

    def add_transport(self, transport: MockTransport) -> None:
        self.transports[transport.node_id] = transport

    def deliver_subject(self, subject_id: int, arrival: TransportArrival, sender: MockTransport) -> None:
        for _tid, transport in self.transports.items():
            if random.random() < self.drop_rate:
                continue
            transport.deliver_subject(subject_id, arrival)

    def deliver_unicast(self, remote_id: int, arrival: TransportArrival) -> None:
        transport = self.transports.get(remote_id)
        if transport is not None:
            if random.random() >= self.drop_rate:
                transport.deliver_unicast(arrival)
