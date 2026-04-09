"""
The bottom-layer API that connects the session layer to the underlying transport layer.
Normally, applications don't care about this unless a custom transport is needed (very uncommon),
so it is moved into a separate module.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from ._api import Closable, Instant, Priority

SUBJECT_ID_MODULUS_16bit = 57203  # Suitable for all Cyphal transports
SUBJECT_ID_MODULUS_23bit = 8378431  # Incompatible with Cyphal/CAN
SUBJECT_ID_MODULUS_32bit = 4294954663  # Incompatible with Cyphal/CAN and Cyphal/UDPv4


class SubjectWriter(Closable):
    @abstractmethod
    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class TransportArrival:
    """
    Arrival of a transfer from the underlying transport.
    The session layer (this library) will parse the header and process the message.
    """

    timestamp: Instant
    priority: Priority
    remote_id: int
    message: bytes


class Transport(Closable):
    """
    Serves the same purpose as cy_platform_t in Cy, with several Pythonic deviations documented below.
    """

    @property
    @abstractmethod
    def subject_id_modulus(self) -> int:
        """
        Constant, cannot be changed while the transport is in used because that would invalidate subject allocations.
        """
        raise NotImplementedError

    @abstractmethod
    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        """
        Subscribe to a subject to receive messages from it until the returned closable handle is closed.
        The session layer may request at most one listener per subject at any given time, similar to the reference impl.
        Duplicate requests for the same subject should raise ValueError.

        REFERENCE PARITY: Unlike the reference implementation, our listeners do not have the extent setting --
        the extent mostly matters for high-reliability/real-time applications; this Python implementation
        assumes infinite extent.
        """
        raise NotImplementedError

    @abstractmethod
    def subject_advertise(self, subject_id: int) -> SubjectWriter:
        """
        Begin sending messages on a subject.
        The session layer may request at most one writer per subject at any given time, similar to the reference impl.
        Duplicate requests for the same subject should raise ValueError.
        """
        raise NotImplementedError

    @abstractmethod
    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        """
        The session layer will invoke this once to configure the handler that will process incoming unicast messages.
        Normally it will happen very early in initialization so no messages are lost; if, however, it somehow comes
        to pass that messages arrive while the handler is still not set, they may be silently dropped.
        """
        raise NotImplementedError

    @abstractmethod
    async def unicast(self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview) -> None:
        """
        Send a unicast message to the specified remote node.
        """
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError
