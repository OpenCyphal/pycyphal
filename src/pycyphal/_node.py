from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any

from ._common import (
    Closable,
    DeliveryError,
    Instant,
    LivenessError,
    NackError,
    Priority,
    SendError,
    name_is_verbatim,
    name_match,
    name_resolve,
)
from ._transport import SubjectWriter, Transport, TransportArrival
from ._wire import (
    HEADER_SIZE,
    LAGE_MAX,
    LAGE_MIN,
    HeaderType,
    broadcast_subject_id,
    gossip_shard_count as _gossip_shard_count,
    gossip_shard_subject_id,
    is_pinned,
    left_wins,
    log_age,
    pack_ack_header,
    pack_gossip_header,
    pack_msg_header,
    pack_rsp_ack_header,
    pack_rsp_header,
    pack_scout_header,
    subject_id_max,
    topic_hash as compute_topic_hash,
    topic_subject_id,
    unpack_header,
)

_logger = logging.getLogger("pycyphal")

_GOSSIP_PERIOD = 5.0  # seconds
_GOSSIP_PERIOD_DITHER_RATIO = 8
_GOSSIP_URGENT_DELAY_MAX = 0.01  # 10ms
_GOSSIP_BROADCAST_RATIO = 10
_IMPLICIT_TOPIC_TIMEOUT = 600.0  # 10 min
_ACK_BASELINE_TIMEOUT = 0.016  # 16ms
_ACK_TX_TIMEOUT = 1.0
_SESSION_LIFETIME = 60.0
_DEDUP_HISTORY = 512
_REORDERING_CAPACITY = 16


# =====================================================================================================================
# Internal data structures
# =====================================================================================================================


@dataclass
class _DedupState:
    remote_id: int
    last_tag: int = 0
    seen: set[int] = field(default_factory=set)
    last_active: float = 0.0

    def check(self, tag: int) -> bool:
        """True if this tag was already seen (duplicate)."""
        return tag in self.seen

    def update(self, tag: int, now: float) -> bool:
        """Returns True if duplicate. Marks as seen."""
        self.last_active = now
        if tag in self.seen:
            return True
        self.seen.add(tag)
        # Bound the set size
        if len(self.seen) > _DEDUP_HISTORY:
            # Remove the oldest entries by keeping only tags near the current frontier
            # Simple approach: keep only the most recent _DEDUP_HISTORY tags
            sorted_tags = sorted(self.seen, key=lambda t: (tag - t) % (1 << 64))
            self.seen = set(sorted_tags[:_DEDUP_HISTORY])
        self.last_tag = tag
        return False


@dataclass
class _Association:
    remote_id: int
    last_seen: float = 0.0
    pending_count: int = 0
    slack: int = 0
    seqno_witness: int = 0
    unicast_ctx: Any = None  # opaque transport context


@dataclass
class _ReorderingSlot:
    lin_tag: int
    priority: Priority
    timestamp: Instant
    message: bytes
    remote_id: int


@dataclass
class _ReorderingState:
    remote_id: int
    topic_hash: int
    tag_baseline: int = 0
    last_ejected_lin_tag: int = 0
    last_active: float = 0.0
    interned: dict[int, _ReorderingSlot] = field(default_factory=dict)
    _timeout_handle: asyncio.TimerHandle | None = None


class _SubscriberRoot:
    __slots__ = ("name", "is_pattern", "subscribers", "needs_scouting")

    def __init__(self, name: str, is_pattern: bool) -> None:
        self.name = name
        self.is_pattern = is_pattern
        self.subscribers: list[Subscriber] = []
        self.needs_scouting = is_pattern


@dataclass
class _TopicCoupling:
    root: _SubscriberRoot
    substitutions: list[tuple[str, int]]


class _Topic:
    __slots__ = (
        "name",
        "hash",
        "evictions",
        "ts_origin",
        "ts_animated",
        "pub_tag_baseline",
        "pub_seqno",
        "pub_count",
        "pub_writer",
        "couplings",
        "sub_listener",
        "dedup_by_remote",
        "associations",
        "assoc_slack_limit",
        "gossip_task",
        "gossip_counter",
        "is_implicit",
        "_node",
    )

    def __init__(self, name: str, h: int, evictions: int, lage: int, node: Node) -> None:
        self.name = name
        self.hash = h
        self.evictions = evictions
        self._node = node
        now = time.monotonic()
        if lage < 0:
            self.ts_origin = now
        else:
            self.ts_origin = now - (2.0**lage)
        self.ts_animated = now
        self.pub_tag_baseline = random.getrandbits(64)
        self.pub_seqno = 0
        self.pub_count = 0
        self.pub_writer: SubjectWriter | None = None
        self.couplings: list[_TopicCoupling] = []
        self.sub_listener: Closable | None = None
        self.dedup_by_remote: dict[int, _DedupState] = {}
        self.associations: dict[int, _Association] = {}
        self.assoc_slack_limit = 2
        self.gossip_task: asyncio.Task[None] | None = None
        self.gossip_counter = 0
        self.is_implicit = True

    def lage(self) -> int:
        return log_age(self.ts_origin, time.monotonic())

    def subject_id(self) -> int:
        return topic_subject_id(self.hash, self.evictions, self._node._transport.subject_id_modulus)

    def animate(self) -> None:
        self.ts_animated = time.monotonic()

    def sync_implicit(self) -> None:
        self.is_implicit = self.pub_count == 0 and not any(
            not root.is_pattern for cpl in self.couplings for root in [cpl.root]
        )


# =====================================================================================================================
# Public data types
# =====================================================================================================================


class Topic:
    """Read-only view of a topic."""

    def __init__(self, t: _Topic) -> None:
        self._t = t

    @property
    def hash(self) -> int:
        return self._t.hash

    @property
    def name(self) -> str:
        return self._t.name

    def match(self, pattern: str) -> list[tuple[str, int]] | None:
        return name_match(pattern, self._t.name)


@dataclass(frozen=True)
class Response:
    timestamp: Instant
    remote_id: int
    seqno: int
    message: bytes


@dataclass(frozen=True)
class Arrival:
    timestamp: Instant
    breadcrumb: Breadcrumb
    message: bytes


class Breadcrumb:
    """Captures context for sending RPC-style responses back to a message publisher."""

    def __init__(
        self,
        node: Node,
        remote_id: int,
        topic_hash: int,
        message_tag: int,
        priority: Priority,
    ) -> None:
        self._node = node
        self._remote_id = remote_id
        self._topic_hash = topic_hash
        self._message_tag = message_tag
        self._priority = priority
        self._seqno = 0

    @property
    def remote_id(self) -> int:
        return self._remote_id

    @property
    def topic(self) -> Topic | None:
        t = self._node._topics_by_hash.get(self._topic_hash)
        return Topic(t) if t else None

    @property
    def tag(self) -> int:
        return self._message_tag

    async def __call__(self, deadline: Instant, message: bytes | memoryview, *, reliable: bool = False) -> None:
        """Send a response. Invoke multiple times to stream responses."""
        node = self._node
        header = pack_rsp_header(
            HeaderType.RSP_REL if reliable else HeaderType.RSP_BE,
            0xFF if not reliable else 0,
            self._seqno,
            self._topic_hash,
            self._message_tag,
        )
        payload = header + bytes(message)
        seqno = self._seqno
        self._seqno += 1

        if not reliable:
            try:
                await node._transport.unicast(deadline, self._priority, self._remote_id, payload)
            except Exception as e:
                raise SendError(str(e)) from e
            return

        # Reliable: retransmit with exponential backoff
        ack_timeout = _derive_ack_timeout(node._ack_baseline_timeout, self._priority)
        ack_future: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
        respond_key = _respond_key(self._remote_id, self._message_tag, self._topic_hash, seqno, 0)
        node._respond_futures[respond_key] = ack_future

        tag = 0
        header = pack_rsp_header(HeaderType.RSP_REL, tag, seqno, self._topic_hash, self._message_tag)
        payload = header + bytes(message)

        try:
            now_inst = Instant.now()
            total_deadline = deadline
            try:
                await node._transport.unicast(total_deadline, self._priority, self._remote_id, payload)
            except Exception as e:
                raise SendError(str(e)) from e

            while True:
                now_inst = Instant.now()
                remaining = total_deadline.s - now_inst.s
                if remaining <= 0:
                    raise DeliveryError("Response delivery deadline exceeded")
                wait_time = min(ack_timeout, remaining)
                try:
                    positive = await asyncio.wait_for(asyncio.shield(ack_future), timeout=wait_time)
                    if positive:
                        return
                    raise NackError("Response was NACKed")
                except asyncio.TimeoutError:
                    pass
                now_inst = Instant.now()
                if now_inst.s >= total_deadline.s:
                    raise DeliveryError("Response delivery deadline exceeded")
                ack_timeout *= 2
                try:
                    await node._transport.unicast(total_deadline, self._priority, self._remote_id, payload)
                except Exception:
                    pass  # retransmit failure is transient
        finally:
            node._respond_futures.pop(respond_key, None)


class ResponseStream(Closable):
    """Async iterator for collecting unicast responses to a published request."""

    def __init__(self, node: Node, topic_hash: int, message_tag: int, response_timeout: float) -> None:
        self._node = node
        self._topic_hash = topic_hash
        self._message_tag = message_tag
        self._response_timeout = response_timeout
        self._queue: asyncio.Queue[Response | BaseException] = asyncio.Queue()
        self._closed = False

    def _deliver(self, response: Response) -> None:
        if not self._closed:
            self._queue.put_nowait(response)

    def _deliver_error(self, error: BaseException) -> None:
        if not self._closed:
            self._queue.put_nowait(error)

    def close(self) -> None:
        self._closed = True
        key = (self._topic_hash, self._message_tag)
        self._node._request_streams.pop(key, None)

    def __aiter__(self) -> ResponseStream:
        return self

    async def __anext__(self) -> Response:
        if self._closed:
            raise StopAsyncIteration
        try:
            item = await asyncio.wait_for(self._queue.get(), timeout=self._response_timeout)
        except asyncio.TimeoutError:
            raise LivenessError("Response stream liveness timeout")
        if isinstance(item, StopAsyncIteration):
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item


# =====================================================================================================================
# Subscriber
# =====================================================================================================================


class Subscriber(Closable):
    """Async iterator yielding Arrival instances from subscribed topics."""

    def __init__(self, node: Node, root: _SubscriberRoot, reordering_window: float | None) -> None:
        self._node = node
        self._root = root
        self._pattern = root.name
        self._verbatim = not root.is_pattern
        self._timeout = float("inf")
        self._reordering_window = reordering_window
        self._queue: asyncio.Queue[Arrival | BaseException] = asyncio.Queue()
        self._closed = False
        self._reordering: dict[tuple[int, int], _ReorderingState] = {}  # keyed by (remote_id, topic_hash)

    @property
    def pattern(self) -> str:
        return self._pattern

    @property
    def verbatim(self) -> bool:
        return self._verbatim

    @property
    def timeout(self) -> float:
        return self._timeout

    @timeout.setter
    def timeout(self, duration: float) -> None:
        self._timeout = duration

    def substitutions(self, topic: Topic) -> list[tuple[str, int]] | None:
        return name_match(self._pattern, topic.name)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Cancel reordering timers
        for rs in self._reordering.values():
            if rs._timeout_handle is not None:
                rs._timeout_handle.cancel()
        self._reordering.clear()
        # Remove from root
        if self in self._root.subscribers:
            self._root.subscribers.remove(self)
        # If root has no more subscribers, clean up couplings
        if not self._root.subscribers:
            self._node._cleanup_subscriber_root(self._root)
        self._queue.put_nowait(StopAsyncIteration())

    def __aiter__(self) -> Subscriber:
        return self

    async def __anext__(self) -> Arrival:
        if self._closed:
            raise StopAsyncIteration
        timeout = self._timeout if math.isfinite(self._timeout) else None
        try:
            item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise LivenessError("Subscriber liveness timeout")
        if isinstance(item, StopAsyncIteration):
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    def _deliver(self, arrival: Arrival) -> None:
        if self._closed:
            return
        if self._reordering_window is not None:
            self._deliver_ordered(arrival)
        else:
            self._queue.put_nowait(arrival)

    def _deliver_ordered(self, arrival: Arrival) -> None:
        """Reordering delivery: buffer out-of-order messages."""
        remote_id = arrival.breadcrumb.remote_id
        topic_hash = arrival.breadcrumb._topic_hash
        tag = arrival.breadcrumb.tag
        key = (remote_id, topic_hash)

        rs = self._reordering.get(key)
        if rs is None:
            rs = _ReorderingState(
                remote_id=remote_id,
                topic_hash=topic_hash,
                tag_baseline=tag - (_REORDERING_CAPACITY // 2),
                last_ejected_lin_tag=0,
            )
            self._reordering[key] = rs

        rs.last_active = time.monotonic()
        lin_tag = (tag - rs.tag_baseline) % (1 << 64)

        # Late arrival / duplicate
        if lin_tag <= rs.last_ejected_lin_tag or lin_tag > (1 << 63):
            return

        # Force eject if too far ahead
        while rs.interned and lin_tag > (rs.last_ejected_lin_tag + _REORDERING_CAPACITY):
            self._reordering_eject_first(rs)

        # Next expected -- fast path
        if lin_tag == rs.last_ejected_lin_tag + 1:
            rs.last_ejected_lin_tag = lin_tag
            self._queue.put_nowait(arrival)
            self._reordering_scan(rs)
            return

        # Still too far ahead -- resequence
        if lin_tag > (rs.last_ejected_lin_tag + _REORDERING_CAPACITY):
            rs.tag_baseline = tag - (_REORDERING_CAPACITY // 2)
            rs.last_ejected_lin_tag = 0
            rs.interned.clear()
            if rs._timeout_handle is not None:
                rs._timeout_handle.cancel()
                rs._timeout_handle = None
            lin_tag = (tag - rs.tag_baseline) % (1 << 64)

        # Intern the message
        if lin_tag in rs.interned:
            return  # duplicate
        rs.interned[lin_tag] = _ReorderingSlot(
            lin_tag=lin_tag,
            priority=arrival.breadcrumb._priority,
            timestamp=arrival.timestamp,
            message=arrival.message,
            remote_id=remote_id,
        )
        self._reordering_arm_timeout(rs)

    def _reordering_eject_first(self, rs: _ReorderingState) -> None:
        if not rs.interned:
            return
        min_tag = min(rs.interned)
        slot = rs.interned.pop(min_tag)
        rs.last_ejected_lin_tag = slot.lin_tag
        t = self._node._topics_by_hash.get(rs.topic_hash)
        if t is not None:
            bc = Breadcrumb(self._node, slot.remote_id, rs.topic_hash, slot.lin_tag + rs.tag_baseline, slot.priority)
            self._queue.put_nowait(Arrival(timestamp=slot.timestamp, breadcrumb=bc, message=slot.message))

    def _reordering_scan(self, rs: _ReorderingState) -> None:
        while rs.interned:
            next_expected = rs.last_ejected_lin_tag + 1
            if next_expected in rs.interned:
                slot = rs.interned.pop(next_expected)
                rs.last_ejected_lin_tag = slot.lin_tag
                t = self._node._topics_by_hash.get(rs.topic_hash)
                if t is not None:
                    bc = Breadcrumb(
                        self._node, slot.remote_id, rs.topic_hash, slot.lin_tag + rs.tag_baseline, slot.priority
                    )
                    self._queue.put_nowait(Arrival(timestamp=slot.timestamp, breadcrumb=bc, message=slot.message))
            else:
                break
        if rs.interned:
            self._reordering_arm_timeout(rs)
        elif rs._timeout_handle is not None:
            rs._timeout_handle.cancel()
            rs._timeout_handle = None

    def _reordering_arm_timeout(self, rs: _ReorderingState) -> None:
        if rs._timeout_handle is not None:
            rs._timeout_handle.cancel()
        if self._reordering_window is not None and self._reordering_window > 0:
            loop = asyncio.get_event_loop()
            rs._timeout_handle = loop.call_later(self._reordering_window, self._reordering_window_expired, rs)

    def _reordering_window_expired(self, rs: _ReorderingState) -> None:
        rs._timeout_handle = None
        if rs.interned:
            self._reordering_eject_first(rs)
            self._reordering_scan(rs)


# =====================================================================================================================
# Publisher
# =====================================================================================================================


class Publisher(Closable):
    """Publishes messages on a topic."""

    def __init__(self, node: Node, topic: _Topic) -> None:
        self._node = node
        self._topic = topic
        self._priority = Priority.NOMINAL
        self._ack_timeout = _ACK_BASELINE_TIMEOUT
        self._closed = False

    @property
    def topic(self) -> Topic:
        return Topic(self._topic)

    @property
    def priority(self) -> Priority:
        return self._priority

    @priority.setter
    def priority(self, priority: Priority) -> None:
        self._priority = priority

    @property
    def ack_timeout(self) -> float:
        return _derive_ack_timeout(self._ack_timeout, self._priority)

    @ack_timeout.setter
    def ack_timeout(self, duration: float) -> None:
        self._ack_timeout = max(1e-6, duration / (1 << self._priority))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._topic.pub_count -= 1
        self._topic.sync_implicit()
        if self._topic.is_implicit and not self._topic.couplings:
            self._node._maybe_retire_topic(self._topic)

    async def __call__(self, deadline: Instant, message: bytes | memoryview, *, reliable: bool = False) -> None:
        if self._closed:
            raise SendError("Publisher is closed")
        t = self._topic
        tag = (t.pub_tag_baseline + t.pub_seqno) & 0xFFFFFFFFFFFFFFFF
        t.pub_seqno += 1

        header_type = HeaderType.MSG_REL if reliable else HeaderType.MSG_BE
        header = pack_msg_header(header_type, t.lage(), t.evictions, t.hash, tag)
        payload = header + bytes(message)

        if not reliable:
            await self._node._send_on_topic(t, deadline, self._priority, payload)
            return

        # Reliable publish: retransmit with exponential backoff, collect acks
        await self._node._publish_reliable(self, t, tag, payload, deadline, message)

    async def request(
        self, delivery_deadline: Instant, response_timeout: float, message: bytes | memoryview
    ) -> ResponseStream:
        """Publish a message and collect responses as a stream."""
        if self._closed:
            raise SendError("Publisher is closed")
        t = self._topic
        tag = (t.pub_tag_baseline + t.pub_seqno) & 0xFFFFFFFFFFFFFFFF
        t.pub_seqno += 1

        # Create response stream before publishing
        stream = ResponseStream(self._node, t.hash, tag, response_timeout)
        key = (t.hash, tag)
        self._node._request_streams[key] = stream

        # Publish reliably
        header = pack_msg_header(HeaderType.MSG_REL, t.lage(), t.evictions, t.hash, tag)
        payload = header + bytes(message)
        try:
            await self._node._publish_reliable(self, t, tag, payload, delivery_deadline, message)
        except Exception as e:
            stream._deliver_error(e)
        return stream


# =====================================================================================================================
# Node
# =====================================================================================================================


def _derive_ack_timeout(baseline: float, priority: Priority) -> float:
    return baseline * (1 << int(priority))


def _respond_key(remote_id: int, message_tag: int, h: int, seqno: int, tag: int) -> int:
    return remote_id ^ message_tag ^ h ^ (seqno << 16) ^ (tag << 56)


class Node:
    """Core pub/sub node. Manages topics, publishers, subscribers, gossip, and message dispatch."""

    def __init__(self, transport: Transport, *, home: str = "", namespace: str = "") -> None:
        self._transport = transport
        if not home:
            home = f"{random.getrandbits(64):016x}"
        self._home = home
        if not namespace:
            namespace = os.environ.get("CYPHAL_NAMESPACE", "")
        self._namespace = namespace

        self._topics_by_hash: dict[int, _Topic] = {}
        self._topics_by_name: dict[str, _Topic] = {}
        self._topics_by_subject_id: dict[int, _Topic] = {}  # non-pinned only
        self._subscribers_by_name: dict[str, _SubscriberRoot] = {}
        self._subscribers_by_pattern: dict[str, _SubscriberRoot] = {}
        self._request_streams: dict[tuple[int, int], ResponseStream] = {}  # (hash, tag) -> stream
        self._respond_futures: dict[int, asyncio.Future[bool]] = {}  # respond_key -> future
        self._pub_ack_futures: dict[tuple[int, int], asyncio.Future[bool]] = {}  # (hash, tag) -> future

        self._ack_baseline_timeout = _ACK_BASELINE_TIMEOUT
        self._gossip_period = _GOSSIP_PERIOD
        self._gossip_urgent_delay_max = _GOSSIP_URGENT_DELAY_MAX
        self._broadcast_ratio = _GOSSIP_BROADCAST_RATIO

        modulus = transport.subject_id_modulus
        self._broadcast_subject = broadcast_subject_id(modulus)
        self._shard_count = _gossip_shard_count(modulus)

        # Set up broadcast listener and writer
        self._broadcast_writer = transport.subject_advertise(self._broadcast_subject)
        self._broadcast_listener = transport.subject_listen(self._broadcast_subject, self._on_broadcast_arrival)

        # Gossip shard listeners/writers: lazily created
        self._shard_writers: dict[int, SubjectWriter] = {}
        self._shard_listeners: dict[int, Closable] = {}

        # Unicast handler
        transport.unicast_listen(self._on_unicast_arrival)

        self._closed = False

    @property
    def home(self) -> str:
        return self._home

    @property
    def namespace(self) -> str:
        return self._namespace

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Cancel all gossip tasks
        for t in list(self._topics_by_hash.values()):
            if t.gossip_task is not None:
                t.gossip_task.cancel()
                t.gossip_task = None
            if t.pub_writer is not None:
                t.pub_writer.close()
            if t.sub_listener is not None:
                t.sub_listener.close()
        # Close shard resources
        for w in self._shard_writers.values():
            w.close()
        for lis in self._shard_listeners.values():
            lis.close()
        self._broadcast_writer.close()
        self._broadcast_listener.close()
        self._transport.close()

    # -----------------------------------------------------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------------------------------------------------

    def advertise(self, name: str) -> Publisher:
        resolved = name_resolve(name, self._namespace, self._home)
        if not name_is_verbatim(resolved):
            raise ValueError("Cannot advertise with pattern name")
        t = self._topic_ensure(resolved)
        t.pub_count += 1
        t.sync_implicit()
        pub = Publisher(self, t)
        self._ensure_topic_writer(t)
        self._schedule_gossip(t)
        return pub

    def subscribe(self, name: str, *, reordering_window: float | None = None) -> Subscriber:
        resolved = name_resolve(name, self._namespace, self._home)
        is_pattern = not name_is_verbatim(resolved)

        # Get or create subscriber root
        root = self._subscribers_by_name.get(resolved)
        if root is None:
            root = _SubscriberRoot(resolved, is_pattern)
            self._subscribers_by_name[resolved] = root
            if is_pattern:
                self._subscribers_by_pattern[resolved] = root

        sub = Subscriber(self, root, reordering_window)
        root.subscribers.append(sub)

        if is_pattern:
            # Attach to all existing matching topics
            for t in list(self._topics_by_hash.values()):
                subs = name_match(resolved, t.name)
                if subs is not None:
                    existing = any(c.root is root for c in t.couplings)
                    if not existing:
                        t.couplings.append(_TopicCoupling(root=root, substitutions=subs))
                        t.sync_implicit()
                        self._ensure_topic_listener(t)
            # Send scout
            if root.needs_scouting:
                root.needs_scouting = False
                self._send_scout(resolved)
        else:
            # Verbatim subscription -- ensure topic exists
            t = self._topic_ensure(resolved)
            existing = any(c.root is root for c in t.couplings)
            if not existing:
                t.couplings.append(_TopicCoupling(root=root, substitutions=[]))
                t.sync_implicit()
            self._ensure_topic_listener(t)
            self._schedule_gossip(t)

        return sub

    # -----------------------------------------------------------------------------------------------------------------
    # Topic management
    # -----------------------------------------------------------------------------------------------------------------

    def _topic_ensure(self, resolved_name: str) -> _Topic:
        t = self._topics_by_name.get(resolved_name)
        if t is not None:
            return t
        h = compute_topic_hash(resolved_name)
        return self._topic_new(resolved_name, h, 0, LAGE_MIN)

    def _topic_new(self, resolved_name: str, h: int, evictions: int, lage: int) -> _Topic:
        t = _Topic(resolved_name, h, evictions, lage, self)
        self._topics_by_hash[h] = t
        self._topics_by_name[resolved_name] = t
        if not is_pinned(h):
            self._topic_allocate(t, evictions)
        self._ensure_gossip_shard(t)
        return t

    def _topic_allocate(self, topic: _Topic, new_evictions: int) -> None:
        """CRDT topic allocation with collision resolution."""
        modulus = self._transport.subject_id_modulus
        # Remove from subject-ID index only if this topic owns the slot (identity check like cavl2_remove_if)
        old_sid = topic.subject_id()
        if self._topics_by_subject_id.get(old_sid) is topic:
            del self._topics_by_subject_id[old_sid]

        # Iterate until we find a free slot or win arbitration
        evictions = new_evictions
        while True:
            new_sid = topic_subject_id(topic.hash, evictions, modulus)
            incumbent = self._topics_by_subject_id.get(new_sid)
            if incumbent is None:
                # Free slot
                topic.evictions = evictions
                self._topics_by_subject_id[new_sid] = topic
                self._schedule_gossip_urgent(topic)
                return
            # Collision -- arbitrate
            my_lage = topic.lage()
            their_lage = incumbent.lage()
            if left_wins(my_lage, topic.hash, their_lage, incumbent.hash):
                # We win, displace incumbent
                self._topics_by_subject_id.pop(new_sid, None)
                topic.evictions = evictions
                self._topics_by_subject_id[new_sid] = topic
                # Re-close old resources
                if topic.pub_writer is not None:
                    topic.pub_writer.close()
                    topic.pub_writer = None
                if topic.sub_listener is not None:
                    topic.sub_listener.close()
                    topic.sub_listener = None
                self._schedule_gossip_urgent(topic)
                # Displace incumbent
                self._topic_allocate(incumbent, incumbent.evictions + 1)
                return
            else:
                # We lose, try next slot
                evictions += 1

    def _maybe_retire_topic(self, topic: _Topic) -> None:
        """Remove topic if it's implicit with no couplings or publishers."""
        if topic.is_implicit and not topic.couplings and topic.pub_count == 0:
            self._topic_destroy(topic)

    def _topic_destroy(self, topic: _Topic) -> None:
        if topic.gossip_task is not None:
            topic.gossip_task.cancel()
            topic.gossip_task = None
        if topic.pub_writer is not None:
            topic.pub_writer.close()
            topic.pub_writer = None
        if topic.sub_listener is not None:
            topic.sub_listener.close()
            topic.sub_listener = None
        self._topics_by_hash.pop(topic.hash, None)
        self._topics_by_name.pop(topic.name, None)
        if not is_pinned(topic.hash):
            sid = topic.subject_id()
            if self._topics_by_subject_id.get(sid) is topic:
                self._topics_by_subject_id.pop(sid, None)

    def _cleanup_subscriber_root(self, root: _SubscriberRoot) -> None:
        """Remove root and its couplings from all topics when no subscribers remain."""
        self._subscribers_by_name.pop(root.name, None)
        if root.is_pattern:
            self._subscribers_by_pattern.pop(root.name, None)
        # Remove couplings from all topics
        for t in list(self._topics_by_hash.values()):
            t.couplings = [c for c in t.couplings if c.root is not root]
            t.sync_implicit()
            if not t.couplings and t.sub_listener is not None:
                t.sub_listener.close()
                t.sub_listener = None
            if t.is_implicit and not t.couplings and t.pub_count == 0:
                self._topic_destroy(t)

    # -----------------------------------------------------------------------------------------------------------------
    # Transport I/O
    # -----------------------------------------------------------------------------------------------------------------

    def _ensure_topic_writer(self, t: _Topic) -> None:
        if t.pub_writer is None:
            t.pub_writer = self._transport.subject_advertise(t.subject_id())

    def _ensure_topic_listener(self, t: _Topic) -> None:
        if t.sub_listener is None:
            sid = t.subject_id()
            t.sub_listener = self._transport.subject_listen(
                sid, lambda arrival, _t=t: self._on_subject_arrival(_t, arrival)
            )

    def _ensure_gossip_shard(self, t: _Topic) -> None:
        if is_pinned(t.hash):
            return
        shard_sid = gossip_shard_subject_id(t.hash, self._transport.subject_id_modulus)
        if shard_sid not in self._shard_writers:
            self._shard_writers[shard_sid] = self._transport.subject_advertise(shard_sid)
            self._shard_listeners[shard_sid] = self._transport.subject_listen(
                shard_sid, lambda arr, _sid=shard_sid: self._on_shard_arrival(_sid, arr)
            )

    async def _send_on_topic(self, t: _Topic, deadline: Instant, priority: Priority, payload: bytes) -> None:
        self._ensure_topic_writer(t)
        assert t.pub_writer is not None
        try:
            await t.pub_writer(deadline, priority, payload)
        except Exception as e:
            raise SendError(str(e)) from e

    async def _publish_reliable(
        self,
        pub: Publisher,
        t: _Topic,
        tag: int,
        payload: bytes,
        deadline: Instant,
        original_message: bytes | memoryview,
    ) -> None:
        """Reliable publish with retransmission and ack collection."""
        ack_future: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
        ack_key = (t.hash, tag)
        self._pub_ack_futures[ack_key] = ack_future

        ack_timeout = _derive_ack_timeout(self._ack_baseline_timeout, pub.priority)

        try:
            # Initial send
            await self._send_on_topic(t, deadline, pub.priority, payload)

            while True:
                now = Instant.now()
                remaining = deadline.s - now.s
                if remaining <= 0:
                    raise DeliveryError("Reliable publish deadline exceeded")
                wait_time = min(ack_timeout, remaining)
                try:
                    positive = await asyncio.wait_for(asyncio.shield(ack_future), timeout=wait_time)
                    if positive:
                        return
                    # Ack received but negative -- still a valid response in some contexts
                    return
                except asyncio.TimeoutError:
                    pass
                now = Instant.now()
                if now.s >= deadline.s:
                    raise DeliveryError("Reliable publish deadline exceeded")
                ack_timeout *= 2
                # Retransmit
                try:
                    await self._send_on_topic(t, deadline, pub.priority, payload)
                except Exception:
                    pass  # transient
        finally:
            self._pub_ack_futures.pop(ack_key, None)

    # -----------------------------------------------------------------------------------------------------------------
    # Gossip
    # -----------------------------------------------------------------------------------------------------------------

    def _schedule_gossip(self, t: _Topic) -> None:
        if is_pinned(t.hash) or t.is_implicit:
            return
        if t.gossip_task is not None:
            return
        t.gossip_task = asyncio.ensure_future(self._gossip_loop(t))

    def _schedule_gossip_urgent(self, t: _Topic) -> None:
        if is_pinned(t.hash):
            return
        # Cancel existing and restart with short delay
        if t.gossip_task is not None:
            t.gossip_task.cancel()
        t.gossip_counter = 0
        t.gossip_task = asyncio.ensure_future(self._gossip_urgent(t))

    async def _gossip_urgent(self, t: _Topic) -> None:
        try:
            delay = random.uniform(0, self._gossip_urgent_delay_max)
            await asyncio.sleep(delay)
            await self._send_gossip_broadcast(t)
            t.gossip_task = None
            self._schedule_gossip(t)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _logger.warning("Gossip urgent error: %s", e)
            t.gossip_task = None
            self._schedule_gossip(t)

    async def _gossip_loop(self, t: _Topic) -> None:
        try:
            while not self._closed and t.hash in self._topics_by_hash:
                dither = self._gossip_period / _GOSSIP_PERIOD_DITHER_RATIO
                if t.gossip_counter < self._broadcast_ratio:
                    delay = random.uniform((self._gossip_period - dither) / 16, self._gossip_period + dither)
                else:
                    delay = random.uniform(self._gossip_period - dither, self._gossip_period + dither)

                await asyncio.sleep(delay)

                broadcast = t.gossip_counter < self._broadcast_ratio or (t.gossip_counter % self._broadcast_ratio) == 0
                t.gossip_counter += 1

                if broadcast:
                    await self._send_gossip_broadcast(t)
                else:
                    await self._send_gossip_shard(t)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _logger.warning("Gossip loop error: %s", e)
        finally:
            t.gossip_task = None

    async def _send_gossip_broadcast(self, t: _Topic) -> None:
        name_bytes = t.name.encode()
        header = pack_gossip_header(t.lage(), t.hash, t.evictions, len(name_bytes))
        payload = header + name_bytes
        try:
            await self._broadcast_writer(Instant.now() + _ACK_TX_TIMEOUT, Priority.NOMINAL, payload)
        except Exception as e:
            _logger.warning("Gossip broadcast send error: %s", e)

    async def _send_gossip_shard(self, t: _Topic) -> None:
        shard_sid = gossip_shard_subject_id(t.hash, self._transport.subject_id_modulus)
        writer = self._shard_writers.get(shard_sid)
        if writer is None:
            return
        name_bytes = t.name.encode()
        header = pack_gossip_header(t.lage(), t.hash, t.evictions, len(name_bytes))
        payload = header + name_bytes
        try:
            await writer(Instant.now() + _ACK_TX_TIMEOUT, Priority.NOMINAL, payload)
        except Exception as e:
            _logger.warning("Gossip shard send error: %s", e)

    async def _send_gossip_unicast(self, t: _Topic, remote_id: int) -> None:
        name_bytes = t.name.encode()
        header = pack_gossip_header(t.lage(), t.hash, t.evictions, len(name_bytes))
        payload = header + name_bytes
        try:
            await self._transport.unicast(Instant.now() + _ACK_TX_TIMEOUT, Priority.NOMINAL, remote_id, payload)
        except Exception as e:
            _logger.warning("Gossip unicast send error: %s", e)

    def _send_scout(self, pattern: str) -> None:
        pattern_bytes = pattern.encode()
        header = pack_scout_header(len(pattern_bytes))
        payload = header + pattern_bytes
        asyncio.ensure_future(self._send_scout_async(payload))

    async def _send_scout_async(self, payload: bytes) -> None:
        try:
            await self._broadcast_writer(Instant.now() + _ACK_TX_TIMEOUT, Priority.NOMINAL, payload)
        except Exception as e:
            _logger.warning("Scout send error: %s", e)

    # -----------------------------------------------------------------------------------------------------------------
    # Message dispatch
    # -----------------------------------------------------------------------------------------------------------------

    def _on_broadcast_arrival(self, arrival: TransportArrival) -> None:
        self._dispatch_message(arrival, subject_id=self._broadcast_subject)

    def _on_shard_arrival(self, shard_sid: int, arrival: TransportArrival) -> None:
        self._dispatch_message(arrival, subject_id=shard_sid)

    def _on_subject_arrival(self, topic: _Topic, arrival: TransportArrival) -> None:
        self._dispatch_message(arrival, subject_id=topic.subject_id())

    def _on_unicast_arrival(self, arrival: TransportArrival) -> None:
        self._dispatch_message(arrival, subject_id=None)

    def _dispatch_message(self, arrival: TransportArrival, *, subject_id: int | None) -> None:
        """Central handler for all incoming messages."""
        data = arrival.message
        if len(data) < HEADER_SIZE:
            return
        try:
            hdr = unpack_header(data[:HEADER_SIZE])
        except (ValueError, KeyError):
            return
        payload = data[HEADER_SIZE:]
        msg_type = hdr["type"]
        ts = arrival.timestamp
        remote_id = arrival.remote_id
        priority = arrival.priority

        modulus = self._transport.subject_id_modulus
        unicast = subject_id is None
        multicast = not unicast and subject_id is not None and subject_id <= subject_id_max(modulus)
        is_broadcast = not unicast and subject_id == self._broadcast_subject

        if msg_type in (HeaderType.MSG_BE, HeaderType.MSG_REL):
            incompatibility = hdr.get("incompatibility", 0)
            lage = hdr["lage"]
            if incompatibility != 0 or lage < LAGE_MIN or lage > LAGE_MAX:
                return
            evictions = hdr["evictions"]
            h = hdr["hash"]
            tag = hdr["tag"]
            if is_pinned(h) and evictions != 0:
                return
            reliable = msg_type == HeaderType.MSG_REL

            if multicast:
                expected_sid = topic_subject_id(h, evictions, modulus)
                if expected_sid != subject_id:
                    return

            topic = self._topics_by_hash.get(h)
            accepted = False
            if topic is not None:
                # Inline CRDT gossip
                self._on_gossip_known_topic(topic, ts, evictions, lage, "inline")
                accepted = self._on_message(topic, tag, ts, payload, reliable, remote_id, priority)
            else:
                self._on_gossip_unknown_topic(ts, h, evictions, lage)

            has_subscribers = topic is not None and bool(topic.couplings)
            if reliable and accepted:
                self._send_message_ack(remote_id, tag, h, ts, True, priority)
            elif reliable and unicast and not has_subscribers:
                self._send_message_ack(remote_id, tag, h, ts, False, priority)

        elif msg_type in (HeaderType.MSG_ACK, HeaderType.MSG_NACK):
            if not unicast:
                return
            incompatibility = hdr.get("incompatibility", 0)
            if incompatibility != 0:
                return
            h = hdr["hash"]
            tag = hdr["tag"]
            positive = msg_type == HeaderType.MSG_ACK
            topic = self._topics_by_hash.get(h)
            if topic is not None:
                self._on_message_ack(topic, tag, remote_id, positive)

        elif msg_type in (HeaderType.RSP_BE, HeaderType.RSP_REL):
            if not unicast:
                return
            reliable = msg_type == HeaderType.RSP_REL
            rsp_tag = hdr["tag"]
            seqno = hdr["seqno"]
            h = hdr["hash"]
            message_tag = hdr["message_tag"]
            ack = False
            stream_key = (h, message_tag)
            stream = self._request_streams.get(stream_key)
            if stream is not None:
                response = Response(timestamp=ts, remote_id=remote_id, seqno=seqno, message=payload)
                stream._deliver(response)
                ack = True
            if reliable:
                self._send_response_ack(remote_id, message_tag, seqno, rsp_tag, h, ack, priority)

        elif msg_type in (HeaderType.RSP_ACK, HeaderType.RSP_NACK):
            if not unicast:
                return
            rsp_tag = hdr["tag"]
            seqno = hdr["seqno"]
            h = hdr["hash"]
            message_tag = hdr["message_tag"]
            key = _respond_key(remote_id, message_tag, h, seqno, rsp_tag)
            fut = self._respond_futures.get(key)
            if fut is not None and not fut.done():
                fut.set_result(msg_type == HeaderType.RSP_ACK)

        elif msg_type == HeaderType.GOSSIP:
            incompatibility = hdr.get("incompatibility", 0)
            if incompatibility != 0:
                return
            lage = hdr["lage"]
            h = hdr["hash"]
            evictions = hdr["evictions"]
            name_len = hdr["name_len"]
            if lage < LAGE_MIN or lage > LAGE_MAX:
                return
            if is_pinned(h) and evictions != 0:
                return
            gossip_name = payload[:name_len].decode(errors="replace") if name_len > 0 else ""
            scope = "unicast" if unicast else ("broadcast" if is_broadcast else "sharded")
            self._on_gossip(ts, h, evictions, lage, gossip_name, scope, remote_id)

        elif msg_type == HeaderType.SCOUT:
            incompatibility = hdr.get("incompatibility", 0)
            incompatibility1 = hdr.get("incompatibility1", 0)
            if incompatibility != 0 or incompatibility1 != 0:
                return
            pattern_len = hdr["pattern_len"]
            if pattern_len == 0:
                return
            scout_pattern = payload[:pattern_len].decode(errors="replace")
            self._on_scout(ts, scout_pattern, remote_id)

    # -----------------------------------------------------------------------------------------------------------------
    # Message handling
    # -----------------------------------------------------------------------------------------------------------------

    def _on_message(
        self,
        topic: _Topic,
        tag: int,
        ts: Instant,
        payload: bytes,
        reliable: bool,
        remote_id: int,
        priority: Priority,
    ) -> bool:
        """Process incoming message, return True if accepted."""
        topic.animate()

        if not topic.couplings:
            # No subscribers -- check dedup for possible retransmit ack
            if reliable:
                dd = topic.dedup_by_remote.get(remote_id)
                return dd is not None and dd.check(tag)
            return False

        if reliable:
            dd = topic.dedup_by_remote.get(remote_id)
            if dd is None:
                dd = _DedupState(remote_id=remote_id, last_tag=tag)
                topic.dedup_by_remote[remote_id] = dd
            if dd.update(tag, ts.s):
                return True  # duplicate, ack but don't deliver

        # Deliver to all coupled subscribers
        accepted = False
        bc = Breadcrumb(self, remote_id, topic.hash, tag, priority)
        arr = Arrival(timestamp=ts, breadcrumb=bc, message=payload)
        for cpl in list(topic.couplings):
            for sub in list(cpl.root.subscribers):
                if sub._closed:
                    continue
                sub._deliver(arr)
                accepted = True

        # Update associations for reliable delivery tracking
        if reliable and accepted:
            assoc = topic.associations.get(remote_id)
            if assoc is None:
                assoc = _Association(remote_id=remote_id)
                topic.associations[remote_id] = assoc
            assoc.last_seen = ts.s
            assoc.slack = 0

        return accepted

    def _on_message_ack(self, topic: _Topic, tag: int, remote_id: int, positive: bool) -> None:
        ack_key = (topic.hash, tag)
        fut = self._pub_ack_futures.get(ack_key)
        if fut is not None and not fut.done():
            fut.set_result(positive)
        # Update association
        assoc = topic.associations.get(remote_id)
        if assoc is None:
            assoc = _Association(remote_id=remote_id)
            topic.associations[remote_id] = assoc
        assoc.last_seen = time.monotonic()
        assoc.slack = 0

    def _send_message_ack(
        self, remote_id: int, tag: int, h: int, ts: Instant, positive: bool, priority: Priority
    ) -> None:
        ack_type = HeaderType.MSG_ACK if positive else HeaderType.MSG_NACK
        header = pack_ack_header(ack_type, h, tag)
        asyncio.ensure_future(self._transport.unicast(ts + _ACK_TX_TIMEOUT, priority, remote_id, header))

    def _send_response_ack(
        self,
        remote_id: int,
        message_tag: int,
        seqno: int,
        rsp_tag: int,
        h: int,
        positive: bool,
        priority: Priority,
    ) -> None:
        ack_type = HeaderType.RSP_ACK if positive else HeaderType.RSP_NACK
        header = pack_rsp_ack_header(ack_type, rsp_tag, seqno, h, message_tag)
        asyncio.ensure_future(self._transport.unicast(Instant.now() + _ACK_TX_TIMEOUT, priority, remote_id, header))

    # -----------------------------------------------------------------------------------------------------------------
    # Gossip handling
    # -----------------------------------------------------------------------------------------------------------------

    def _on_gossip(
        self,
        ts: Instant,
        h: int,
        evictions: int,
        lage: int,
        name: str,
        scope: str,
        remote_id: int,
    ) -> None:
        mine = self._topics_by_hash.get(h)
        if mine is None and name and scope in ("broadcast", "unicast"):
            mine = self._topic_subscribe_if_matching(name, h, evictions, lage)
        if mine is not None:
            self._on_gossip_known_topic(mine, ts, evictions, lage, scope)
        else:
            self._on_gossip_unknown_topic(ts, h, evictions, lage)

    def _on_gossip_known_topic(self, mine: _Topic, ts: Instant, evictions: int, lage: int, scope: str) -> None:
        mine.animate()
        mine_lage = mine.lage()
        now_s = time.monotonic()

        if mine.evictions != evictions:
            win = mine_lage > lage or (mine_lage == lage and mine.evictions > evictions)
            # Merge log-age
            self._merge_lage(mine, lage, now_s)
            if win:
                self._schedule_gossip_urgent(mine)
            else:
                self._topic_allocate(mine, evictions)
                if mine.evictions == evictions:
                    self._schedule_gossip(mine)
        else:
            self._merge_lage(mine, lage, now_s)
            self._ensure_topic_listener(mine)

    def _on_gossip_unknown_topic(self, ts: Instant, h: int, evictions: int, lage: int) -> None:
        modulus = self._transport.subject_id_modulus
        sid = topic_subject_id(h, evictions, modulus)
        mine = self._topics_by_subject_id.get(sid)
        if mine is None:
            return
        mine_lage = mine.lage()
        win = left_wins(mine_lage, mine.hash, lage, h)
        if win:
            self._schedule_gossip_urgent(mine)
        else:
            self._topic_allocate(mine, mine.evictions + 1)

    def _topic_subscribe_if_matching(self, name: str, h: int, evictions: int, lage: int) -> _Topic | None:
        if not name or compute_topic_hash(name) != h:
            return None
        # Check if any pattern matches
        matched = False
        for pattern, root in self._subscribers_by_pattern.items():
            subs = name_match(pattern, name)
            if subs is not None:
                matched = True
                break
        if not matched:
            return None
        # Create topic
        t = self._topic_new(name, h, evictions, lage)
        # Attach all matching pattern subscriptions
        for pattern, root in list(self._subscribers_by_pattern.items()):
            subs = name_match(pattern, name)
            if subs is not None:
                existing = any(c.root is root for c in t.couplings)
                if not existing:
                    t.couplings.append(_TopicCoupling(root=root, substitutions=subs))
        t.sync_implicit()
        self._ensure_topic_listener(t)
        return t

    def _merge_lage(self, topic: _Topic, remote_lage: int, now_s: float) -> None:
        """Merge remote log-age into local topic, keeping the older (larger) origin."""
        if remote_lage > LAGE_MAX:
            return
        remote_origin = now_s - (2.0**remote_lage) if remote_lage >= 0 else now_s
        if remote_origin < topic.ts_origin:
            topic.ts_origin = remote_origin

    def _on_scout(self, ts: Instant, pattern: str, remote_id: int) -> None:
        """Respond to scout with unicast gossips for matching topics."""
        for t in list(self._topics_by_hash.values()):
            subs = name_match(pattern, t.name)
            if subs is not None:
                asyncio.ensure_future(self._send_gossip_unicast(t, remote_id))
