"""
This is the main public contract. The rest of the codebase is hidden behind it and can be morphed ad-hoc.
There is also the downward-facing contract for the transport layer in the adjacent interface module.
"""

# Top-level exported API entities. Keep pristine! The rest of the library can be noisy but not this!

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
import random
import platform
from typing import Any, Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ._transport import Transport as Transport

_logger = logging.getLogger(__name__)

SUBJECT_ID_PINNED_MAX = 0x1FFF


class Error(Exception):
    """The base type for all application-specific errors."""


class SendError(Error):
    """Message could not be sent before the deadline."""


class ClosedError(SendError):
    """The operation cannot proceed because the object has been closed permanently."""


class DeliveryError(Error):
    """Message was sent, but the remote did not acknowledge. The remote might be unreachable or dysfunctional."""


class LivenessError(Error):
    """A message was expected, but it did not arrive."""


class NackError(Error):
    """The remote node was reached, but it explicitly rejected the message."""


@dataclass(frozen=True)
class Instant:
    """
    Monotonic time elapsed from an unspecified origin instant; used to represent a point in time.
    Durations use plain float seconds instead.
    """

    ns: int

    def __init__(self, *, ns: int) -> None:
        object.__setattr__(self, "ns", int(ns))

    @staticmethod
    def now() -> Instant:
        return Instant(ns=time.monotonic_ns())

    @property
    def s(self) -> float:
        return self.ns * 1e-9

    @property
    def ms(self) -> float:
        return self.ns * 1e-6

    @property
    def us(self) -> float:
        return self.ns * 1e-3

    def __add__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=self.ns + round(other * 1e9))
        return NotImplemented

    def __radd__(self, other: Any) -> Instant:
        return self.__add__(other)

    def __sub__(self, other: Any) -> Instant | float:
        if isinstance(other, Instant):
            return (self.ns - other.ns) * 1e-9
        if isinstance(other, (float, int)):
            return Instant(ns=self.ns - round(other * 1e9))
        return NotImplemented

    def __mul__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=round(self.ns * other))
        return NotImplemented

    def __rmul__(self, other: Any) -> Instant:
        return self.__mul__(other)

    def __truediv__(self, other: Any) -> Instant:
        if isinstance(other, (float, int)):
            return Instant(ns=round(self.ns / other))
        return NotImplemented

    def __str__(self) -> str:
        return f"{self.s:.3f}s"


class Priority(IntEnum):
    EXCEPTIONAL = 0
    IMMEDIATE = 1
    FAST = 2
    HIGH = 3
    NOMINAL = 4
    LOW = 5
    SLOW = 6
    OPTIONAL = 7


class Closable(ABC):
    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class Topic(ABC):
    """
    Topics are managed automatically by the library, created and destroyed as necessary.
    This is just a compact view to expose some auxiliary information.
    """

    @property
    @abstractmethod
    def hash(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def match(self, pattern: str) -> list[tuple[str, int]] | None:
        """
        If the pattern matches the topic name, returns the name segment substitutions needed to achieve the match.
        None if there is no match. Empty list for verbatim subscribers (match only one topic), where pattern==name.
        Each substitution is the segment and the index of the substitution character in the pattern.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        return f"T{self.hash:016x}{self.name!r}"

    def __repr__(self) -> str:
        return f"Topic({self.name!r}, hash=0x{self.hash:016x})"


@dataclass(frozen=True)
class Response:
    """
    Represents a response to a published message from the specified remote node.
    Seqno is managed by the remote, incrementing by one with each response starting from zero.
    """

    timestamp: Instant
    remote_id: int
    seqno: int
    message: bytes


class ResponseStream(Closable, ABC):
    """
    Represents the expectation of response arrivals, used for one-off RPC and streaming.
    Generates multiple async results, one per received response or per error.
    Async iterator will continue to yield new messages until close()d, even after delivery/liveness errors.
    """

    def __aiter__(self) -> ResponseStream:
        return self

    async def __anext__(self) -> Response:
        """
        Blocks until response is received.
        Raises DeliveryError if request could not be delivered by the deadline, LivenessError on response timeout,
        SendError if failed to send the response before the deadline.
        """
        raise NotImplementedError


class Publisher(Closable, ABC):
    """
    Represents the intent to send messages on a topic.
    This is callable; an invocation triggers publication.
    For publications that expect a response, use the ``request`` method.
    """

    @property
    @abstractmethod
    def topic(self) -> Topic:
        raise NotImplementedError

    @property
    @abstractmethod
    def priority(self) -> Priority:
        raise NotImplementedError

    @priority.setter
    @abstractmethod
    def priority(self, priority: Priority) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def ack_timeout(self) -> float:
        """
        The effective ack timeout at the current priority.
        The number of attempts is controlled by the deadline specified at publish time.
        """
        raise NotImplementedError

    @ack_timeout.setter
    @abstractmethod
    def ack_timeout(self, duration: float) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self, deadline: Instant, message: memoryview | bytes, *, reliable: bool = False) -> None:
        """
        Blocks at most until the deadline. Raises SendError if couldn't be sent before the deadline.
        If reliable, DeliveryError will be raised unless acked by the remote subscribers before deadline.
        """
        raise NotImplementedError

    @abstractmethod
    async def request(
        self, delivery_deadline: Instant, response_timeout: float, message: memoryview | bytes
    ) -> ResponseStream:
        """
        Publish a message and expect responses. See ResponseStream for details.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Publisher(topic={self.topic}, priority={self.priority}, ack_timeout={self.ack_timeout})"


class Breadcrumb(ABC):
    """
    The breadcrumb can be used, optionally, to send responses RPC-style or streamed back to a message publisher.
    Instances can be retained after message reception for as long as necessary.
    One instance is shared across all subscribers receiving the same message, ensuring contiguous seqno across
    all responses.

    Responses are always sent at the same priority as that of the request.
    Internally, the library tracks the seqno that starts at zero and is incremented with every response.

    The set of (remote-ID, topic hash, message tag) forms a globally unique stream identification triplet,
    which can be hashed down to a single number for convenience.
    """

    @property
    @abstractmethod
    def remote_id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def topic(self) -> Topic:
        raise NotImplementedError

    @property
    @abstractmethod
    def tag(self) -> int:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self, deadline: Instant, message: memoryview | bytes, *, reliable: bool = False) -> None:
        """
        Invoke multiple times to stream multiple responses.
        Blocks at most until the deadline. Raises SendError if couldn't be sent before the deadline.
        If reliable:
        - DeliveryError will be raised unless acked by the remote subscribers before deadline.
        - NackError will be raised if the remote is no longer accepting responses.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Breadcrumb(remote_id={self.remote_id:016x}, tag={self.tag:016x}, topic={self.topic})"


@dataclass(frozen=True)
class Arrival:
    """
    Represents a message received from a topic.
    The breadcrumb allows sending responses back to the publisher, thus enabling RPC and streaming.
    """

    timestamp: Instant
    breadcrumb: Breadcrumb
    message: bytes


class Subscriber(Closable, ABC):
    @property
    @abstractmethod
    def pattern(self) -> str:
        """
        The topic name used when creating the subscriber.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def verbatim(self) -> bool:
        """
        True if the pattern does not contain substitution segments named `*` and `>`.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def timeout(self) -> float:
        """
        By default, the timeout is infinite, meaning that LivenessError will never be returned.
        The user can override this as needed. Setting a non-finite timeout disables this feature.
        """
        raise NotImplementedError

    @timeout.setter
    @abstractmethod
    def timeout(self, duration: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def substitutions(self, topic: Topic) -> list[tuple[str, int]] | None:
        """
        Pattern name segment substitutions needed to match the name of this subscriber to the name of the
        specified topic. None if no match. Empty list for verbatim subscribers (match only one topic).
        """
        raise NotImplementedError

    def __aiter__(self) -> Subscriber:
        return self

    @abstractmethod
    async def __anext__(self) -> Arrival:
        """
        Raises LivenessError if messages cease arriving for longer than the timeout, unless timeout is non-finite.
        """
        raise NotImplementedError

    def listen(
        self,
        callback: Callable[[Arrival | Error], Awaitable[None] | None],
    ) -> asyncio.Task[None]:
        """
        Launch a background task that forwards every received message to ``callback``.
        The callback may be sync or async and is invoked with either an :class:`Arrival` or a library-level
        :class:`Error` raised by the receive side (e.g. :class:`LivenessError`).
        Such errors are delivered as values and the loop keeps running; the callback decides how to react.

        The task terminates cleanly when the subscriber is closed or when the caller cancels the task.
        Any non-:class:`Error` exception from ``__anext__``, or any exception raised by the callback itself,
        fails the task and is logged.

        The caller must retain a reference to the returned task; otherwise the event loop may garbage-collect it.
        """

        async def loop() -> None:
            while True:
                item: Arrival | Error
                try:
                    item = await self.__anext__()
                except StopAsyncIteration:
                    return
                except Error as exc:  # Library-level errors are delivered as values.
                    item = exc
                result = callback(item)
                if inspect.isawaitable(result):
                    await result

        task = asyncio.create_task(loop(), name=f"pycyphal2.listen:{self.pattern}")

        def on_done(t: asyncio.Task[None]) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                _logger.error("listen() task for %r terminated with %r", self.pattern, exc)

        task.add_done_callback(on_done)
        return task

    def __repr__(self) -> str:
        return f"Subscriber(pattern={self.pattern!r}, verbatim={self.verbatim}, timeout={self.timeout})"


class Node(Closable, ABC):
    """
    The top-level entity that represents a node in the network.
    """

    @property
    @abstractmethod
    def home(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def namespace(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def advertise(self, name: str) -> Publisher:
        """
        Begin publishing on a topic; this also includes sending RPC requests.
        """
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, name: str, *, reordering_window: float | None = None) -> Subscriber:
        """
        Receive messages from a single topic or multiple, if ``name`` is a pattern.
        If the reordering window is set, ordered subscription is used that guarantees monotonically
        increasing message tags, otherwise messages arrive ASAP.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Node(home={self.home!r}, namespace={self.namespace!r})"

    @staticmethod
    def new(transport: Transport, *, home: str = "", namespace: str = "") -> Node:
        """
        Construct a new node using the specified transport. This is the main entry point of the library.

        The transport is constructed using one of the stock transport implementations like ``pycyphal2.udp``,
        depending on the needs of the application, or it could be custom.

        Every node needs a unique nonempty home. If the home string is not provided, a random home will be generated.
        If home ends with a `/`, a unique string will be automatically appended to generate a prefixed unique home;
        e.g., `my_node` stays as-is; `my_node/` becomes like `my_node/abcdef0123456789`,
        an empty string becomes a random string.

        If namespace is not set, it is read from the CYPHAL_NAMESPACE environment variable if available,
        otherwise it remains empty.
        """
        from ._node import NodeImpl

        # Add random suffix if requested or generate pure random home. Leading/trailing separators will be normalized away.
        home = home.strip() or "/"
        home = f"{home}{eui64():016x}" if home.endswith("/") else home
        return NodeImpl(transport, home=home, namespace=namespace)


def eui64() -> int:
    """
    Generate a globally unique random EUI-64 identifier where:
    - 20 most significant bits (5 hexadecimals) are a function of the host machine identity.
    - 44 least significant bits (11 hexadecimals) are random.
    The EIU-64 format is: The I/G bit is cleared (unicast). The U/L bit is set (locally administered).
    The protocol doesn't care about this structure, it is just an optional default convention for better diagnostics.
    """
    from ._hash import rapidhash

    host_20 = rapidhash(platform.node().encode()) & 0xFFFFF
    rand_44 = random.getrandbits(44)
    out = (host_20 << 44) | rand_44
    out &= ~(1 << 56)  # clear I/G bit (unicast)
    out |= 1 << 57  # set U/L bit (locally administered)
    return out
