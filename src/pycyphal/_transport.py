from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from ._common import Closable, Instant, Priority

SUBJECT_ID_MODULUS_17bit = 122743  # Suitable for all Cyphal transports
SUBJECT_ID_MODULUS_23bit = 8378431  # Incompatible with Cyphal/CAN
SUBJECT_ID_MODULUS_32bit = 4294954663  # Incompatible with Cyphal/CAN and Cyphal/UDPv4


class SubjectWriter(Closable):
    @abstractmethod
    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class TransportArrival:
    timestamp: Instant
    priority: Priority
    remote_id: int
    message: bytes


class Transport(Closable):
    @property
    @abstractmethod
    def subject_id_modulus(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        raise NotImplementedError

    @abstractmethod
    def subject_advertise(self, subject_id: int) -> SubjectWriter:
        raise NotImplementedError

    @abstractmethod
    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def unicast(self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview) -> None:
        raise NotImplementedError
