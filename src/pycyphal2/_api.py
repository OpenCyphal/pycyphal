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
import os
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

    @property
    def us(self) -> float:
        return self.ns * 1e-3

    @property
    def ms(self) -> float:
        return self.ns * 1e-6

    @property
    def s(self) -> float:
        return self.ns * 1e-9

    @staticmethod
    def now() -> Instant:
        return Instant(ns=time.monotonic_ns())

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
    """

    @property
    @abstractmethod
    def hash(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def evictions(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def subject_id(self, modulus: int) -> int:
        """The modulus can be obtained from :attr:`Transport.subject_id_modulus`."""
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
    One response yielded by :class:`ResponseStream`.

    A single request may elicit responses from multiple remote subscribers; ``remote_id`` identifies which one sent
    this item. ``seqno`` is scoped to that remote responder: the first response is zero, then it increments by one
    for each subsequent streamed response.
    """

    timestamp: Instant
    remote_id: int
    seqno: int
    message: bytes


class ResponseStream(Closable, ABC):
    """
    Async iterator of responses produced by :meth:`Publisher.request`.

    One request may yield zero, one, or many responses, possibly from different remotes.
    Keeping the stream open enables streaming: later responses to the same request are yielded as they arrive.
    If the remote uses reliable delivery for streaming (usually the case), then it will be notified if the client
    stream is closed (explicit NACK) or if the client becomes unreachable (absence of ACK).

    Library-level errors are reported through iteration and do not automatically close the stream.
    """

    def __aiter__(self) -> ResponseStream:
        return self

    async def __anext__(self) -> Response:
        """
        Wait for the next response or the next library-level failure.

        Raises :class:`LivenessError` if no response arrives for longer than the configured response timeout; the
        timeout restarts after every accepted response, so it also bounds the gaps inside a stream.

        Raises :class:`DeliveryError` or :class:`SendError` if the request publication itself fails.
        Such errors do not close the stream automatically; later iterations may still yield more responses until
        :meth:`close`d.
        """
        raise NotImplementedError


class Publisher(Closable, ABC):
    """
    Represents the intent to send messages on a topic.

    Calling the publisher sends one message.
    By default this is best-effort publication: the message is sent once and only immediate send failures are reported.
    With ``reliable=True``, the library retransmits until the deadline and waits for acknowledgments from remote
    subscribers.

    For publications that expect responses, use :meth:`request`, which returns a :class:`ResponseStream`.
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
        The effective initial ACK timeout at the current priority; retries back off exponentially.
        The deadline limits the entire reliable publication, not just one attempt.
        """
        raise NotImplementedError

    @ack_timeout.setter
    @abstractmethod
    def ack_timeout(self, duration: float) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self, deadline: Instant, message: memoryview | bytes, *, reliable: bool = False) -> None:
        """
        Send one message.
        Blocks at most until ``deadline``.
        Raises :class:`SendError` if the message could not be sent before the deadline.

        If ``reliable`` is false, the message is sent once.
        If ``reliable`` is true, the library retransmits until ``deadline`` leveraging :attr:`ack_timeout`.
        """
        raise NotImplementedError

    @abstractmethod
    async def request(
        self, delivery_deadline: Instant, response_timeout: float, message: memoryview | bytes
    ) -> ResponseStream:
        """
        Publish a request and return a stream of responses.

        The request publication uses reliable delivery governed by ``delivery_deadline`` and :attr:`ack_timeout`.
        Once the request is in flight, the returned :class:`ResponseStream` yields unicast responses
        from any subscriber that chooses to answer.

        ``response_timeout`` is the maximum idle gap (liveness timeout) between accepted responses,
        so it applies both to one-off RPC and to streaming.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Publisher(topic={self.topic}, priority={self.priority}, ack_timeout={self.ack_timeout})"


class Breadcrumb(ABC):
    """
    Response handle attached to a received message.

    It can be used, optionally, to send one or more unicast responses back to the original publisher,
    enabling RPC and streaming alongside pub/sub.
    Instances may be retained after message reception for as long as necessary.
    One instance is shared across all subscribers receiving the same message, ensuring contiguous sequence numbers
    across all responses emitted for that arrival.

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
        Send one response to the original publisher.

        Invoke multiple times on the same breadcrumb to stream multiple responses. Blocks at most until ``deadline``.
        Raises :class:`SendError` if the response could not be sent before the deadline.

        If ``reliable`` is true, the response is retransmitted until acknowledged or until ``deadline`` expires.
        :class:`DeliveryError` means the requester could not be reached in time; :class:`NackError` means the
        requester is reachable but is no longer accepting responses for this stream (stream closed).
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Breadcrumb(remote_id={self.remote_id:016x}, tag={self.tag:016x}, topic={self.topic})"


@dataclass(frozen=True)
class Arrival:
    """
    Represents one message received from a topic.
    ``breadcrumb`` captures the responder context for this arrival.
    Calling it sends a unicast response back to the original publisher, enabling RPC and streaming.
    """

    timestamp: Instant
    breadcrumb: Breadcrumb
    message: bytes


class Subscriber(Closable, ABC):
    """
    Async source of :class:`Arrival` objects produced by :meth:`Node.subscribe`.

    Without reordering, arrivals are yielded as soon as they are accepted.
    With a reordering window, each ``(remote_id, topic)`` stream may be delayed to reconstruct monotonically
    increasing publication tags. In-order arrivals are not delayed.
    """

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
        Wait for the next deliverable arrival.

        Raises :class:`LivenessError` if messages cease arriving for longer than :attr:`timeout`, unless the timeout
        is non-finite (default).
        For ordered subscriptions, out-of-order messages may be withheld until the gap closes or the reordering
        window expires.
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

    Conventionally, topic names are hardcoded in the application.
    Integration of a node into a network requires some way of altering such hardcoded names to match the actual network
    configuration. Several facilities are provided to that end (readers familiar with ROS will feel right at home):

    - Namespacing. When a node is created, the namespace is specified; if not given explicitly, it defaults to the
      ``CYPHAL_NAMESPACE`` environment variable. This name is added to all relative topic names.
    - Home, aka node name. Topic names starting with `~/` are updated to replace `~` with the home.
    - Remapping. A set of replacements is provided that matches hardcoded names and replaces them with arbitrary
      target names. These are configured via a dedicated method after the node is created; the initial remapping
      configuration is seeded from the ``CYPHAL_REMAP`` environment variable (whitespace-separated pairs of `from=to`).
    """

    @property
    @abstractmethod
    def home(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def namespace(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def transport(self) -> Transport:
        raise NotImplementedError

    @abstractmethod
    def remap(self, spec: str | dict[str, str]) -> None:
        """
        Accepts either a string containing ASCII whitespace-separated remapping pairs, where each pair is formed like
        `from=to`, or a dict where keys match hardcoded names and the values are their replacements.
        If invoked multiple times, the effect is incremental. Newer entries override older ones in case of conflict.

        When the node is constructed, the default remapping set is configured immediately as
        ``self.remap(os.getenv("CYPHAL_REMAP", ""))`` (no need to do it manually).

        Remapping examples:

            NAME        FROM    TO      NAMESPACE   HOME    RESOLVED    PINNING REMARK
            foo/bar     foo/bar zoo     ns          me      ns/zoo      -       relative remap
            foo/bar     foo/bar zoo#123 ns          me      ns/zoo      123     pinned relative remap
            foo/bar#456 foo/bar zoo     ns          me      ns/zoo      -       matched rule discards user pin
            foo/bar     foo/bar /zoo    ns          me      zoo         -       absolute remap (ns ignored)
            foo/bar     foo/bar ~/zoo   ns          me      me/zoo      -       homeful remap (home expanded)
        """
        raise NotImplementedError

    @abstractmethod
    def advertise(self, name: str) -> Publisher:
        """
        Begin publishing on a topic.

        The returned :class:`Publisher` is used for ordinary publication and for RPC-style requests sent with
        :meth:`Publisher.request`.
        """
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, name: str, *, reordering_window: float | None = None) -> Subscriber:
        """
        Receive messages from one topic or from several if ``name`` is a pattern.

        If ``reordering_window`` is ``None``, messages are yielded in arrival order.
        Otherwise, each ``(remote_id, topic)`` stream is reordered independently to ensure that the application
        sees a monotonically increasing tag sequence; this is useful for sensor feeds, state estimators, etc.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Node(home={self.home!r}, namespace={self.namespace!r})"

    @staticmethod
    def new(transport: Transport, home: str = "", namespace: str = "") -> Node:
        """
        Construct a new node using the specified transport. This is the main entry point of the library.

        The transport is constructed using one of the stock transport implementations like ``pycyphal2.udp``,
        depending on the needs of the application, or it could be custom.

        Every node needs a unique nonempty home. If the home string is not provided, a random home will be generated.
        If home ends with a `/`, a unique string will be automatically appended to generate a prefixed unique home;
        e.g., `my_node` stays as-is; `my_node/` becomes like `my_node/abcdef0123456789`,
        an empty string becomes a random string.

        If the namespace is not set, it is read from the CYPHAL_NAMESPACE environment variable,
        which is the main intended use case. Direct assignment might be considered an anti-pattern in most cases.
        """
        from ._node import NodeImpl

        # Add random suffix if requested or generate pure random home.
        # Leading/trailing separators will be normalized away.
        home = home.strip() or "/"
        if home.endswith("/"):
            uid = transport.uid if hasattr(transport, "uid") else eui64()
            home += f"{uid:016x}"

        # Initialize the namespace: if not given explicitly, read it from the standard environment.
        namespace = namespace.strip() or os.getenv("CYPHAL_NAMESPACE", "").strip()

        # Construct the node.
        node = NodeImpl(transport, home=home, namespace=namespace)
        _logger.info("Constructed %s", node)

        # Set up default name remapping.
        try:
            node.remap(os.getenv("CYPHAL_REMAP", ""))
        except Exception as ex:
            _logger.exception("Failed to set up default remapping from CYPHAL_REMAP: %s", ex)
        return node

    @abstractmethod
    def monitor(self, callback: Callable[[Topic], None]) -> Closable:
        """
        *Advanced diagnostic utility.*

        Install a listener callback invoked whenever the local node receives a non-inline gossip message.
        This can be used to discover the full set of topics in the network for diagnostic purposes.

        The :class:`Topic` instance is the actual local topic instance for locally known topics;
        for topics not known locally it is a short-lived flyweight object.

        The returned :class:`Closable` can be closed to remove the callback.
        """
        raise NotImplementedError

    @abstractmethod
    async def scout(self, pattern: str) -> None:
        """
        *Advanced diagnostic utility.*

        Query the network for topics matching the pattern.
        The :meth:`monitor` should be installed beforehand to process the responses.
        """
        raise NotImplementedError


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
