from __future__ import annotations

import asyncio
from collections import OrderedDict
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable

from ._hash import rapidhash
from ._header import (
    HEADER_SIZE,
    GossipHeader,
    LAGE_MAX,
    MsgAckHeader,
    MsgBeHeader,
    MsgNackHeader,
    MsgRelHeader,
    RspAckHeader,
    RspBeHeader,
    RspNackHeader,
    RspRelHeader,
    ScoutHeader,
    deserialize_header,
)
from ._transport import SubjectWriter, Transport, TransportArrival
from ._api import Topic, Node, Publisher, Subscriber, Breadcrumb, Closable, Instant, Priority, SendError
from ._api import SUBJECT_ID_PINNED_MAX

if TYPE_CHECKING:
    from ._publisher import ResponseStreamImpl
    from ._subscriber import RespondTracker

_logger = logging.getLogger(__name__)

# =====================================================================================================================
# Constants
# =====================================================================================================================

TOPIC_NAME_MAX = 200
EVICTIONS_PINNED_MIN = 0xFFFFE000
GOSSIP_PERIOD = 5.0
GOSSIP_URGENT_DELAY_MAX = 0.01
GOSSIP_BROADCAST_RATIO = 10
GOSSIP_PERIOD_DITHER_RATIO = 8
ACK_BASELINE_DEFAULT_TIMEOUT = 0.016
ACK_TX_TIMEOUT = 1.0
SESSION_LIFETIME = 60.0
IMPLICIT_TOPIC_TIMEOUT = 600.0
REORDERING_CAPACITY = 16
ASSOC_SLACK_LIMIT = 2
DEDUP_HISTORY = 512
ACK_SEQNO_MAX_LAG = 100000
U64_MASK = (1 << 64) - 1


class GossipScope(Enum):
    UNICAST = auto()
    BROADCAST = auto()
    SHARDED = auto()
    INLINE = auto()


# =====================================================================================================================
# Name Resolution
# =====================================================================================================================


def _name_normalize(name: str) -> str:
    """Collapse separators, strip leading/trailing separators."""
    parts: list[str] = []
    for seg in name.split("/"):
        if seg:
            parts.append(seg)
    return "/".join(parts)


def _name_consume_pin_suffix(name: str) -> tuple[str, int | None]:
    """Extract pin suffix like 'foo#123' -> ('foo', 123). Returns (name, None) if no valid pin."""
    hash_pos = -1
    for i in range(len(name) - 1, -1, -1):
        ch = name[i]
        if ch == "#":
            hash_pos = i
            break
        if not ch.isdigit():
            return (name, None)
    if hash_pos < 0:
        return (name, None)
    digits = name[hash_pos + 1 :]
    if len(digits) == 0:
        return (name, None)
    if len(digits) > 1 and digits[0] == "0":
        return (name, None)  # leading zeros not allowed
    pin = int(digits)
    if pin > SUBJECT_ID_PINNED_MAX:
        return (name, None)
    return (name[:hash_pos], pin)


def _name_join(left: str, right: str) -> str:
    """Join two name parts with separator, normalizing the result."""
    left = _name_normalize(left)
    right = _name_normalize(right)
    if left and right:
        return left + "/" + right
    return left or right


def _name_is_homeful(name: str) -> bool:
    return name == "~" or name.startswith("~/")


def resolve_name(
    name: str, home: str, namespace: str, remaps: dict[str, str] | None = None
) -> tuple[str, int | None, bool]:
    """
    Resolve a topic name to (resolved_name, pin_or_None, is_verbatim).
    Raises ValueError on invalid names.
    """
    # REFERENCE PARITY: Python-only ergonomic deviation -- outer whitespace is trimmed before validation.
    # The reference resolver rejects such names because spaces are invalid topic-name characters.
    name = name.strip()
    if not name:
        raise ValueError("Empty name")

    # Strip pin suffix first.
    name, pin = _name_consume_pin_suffix(name)

    # Apply remapping: lookup on normalized pin-free name; matched rule replaces both name and pin.
    if remaps:
        lookup = _name_normalize(name)
        if lookup in remaps:
            name = remaps[lookup]
            name, pin = _name_consume_pin_suffix(name)

    # Classify and construct.
    if name.startswith("/"):
        resolved = _name_normalize(name)
    elif _name_is_homeful(name):
        tail = name[1:].lstrip("/") if len(name) > 1 else ""
        resolved = _name_join(home, tail)
    else:
        if _name_is_homeful(namespace):
            ns_tail = namespace[1:].lstrip("/") if len(namespace) > 1 else ""
            expanded_ns = _name_join(home, ns_tail)
        else:
            expanded_ns = namespace
        resolved = _name_join(expanded_ns, name)

    if not resolved:
        raise ValueError("Name resolves to empty string")
    if len(resolved) > TOPIC_NAME_MAX:
        raise ValueError(f"Resolved name exceeds {TOPIC_NAME_MAX} characters")
    # Validate characters: ASCII 33-126 and '/' only.
    for ch in resolved:
        o = ord(ch)
        if o < 33 or o > 126:
            raise ValueError(f"Invalid character in name: {ch!r}")

    verbatim = "*" not in resolved and ">" not in resolved
    if pin is not None and not verbatim:
        raise ValueError("Pattern names cannot be pinned")
    return resolved, pin, verbatim


# =====================================================================================================================
# Pattern Matching
# =====================================================================================================================


def match_pattern(pattern: str, name: str) -> list[tuple[str, int]] | None:
    """
    Match a pattern against a topic name.
    Returns substitutions list on match, None on no match.
    Empty list for verbatim match (pattern == name).

    REFERENCE PARITY: Intentional deviation from the current C reference -- only a terminal '>' acts as an
    any-segment wildcard. Non-terminal '>' is treated literally until the reference behavior converges.
    """
    if pattern == name:
        return []
    p_parts = pattern.split("/")
    n_parts = name.split("/")
    subs: list[tuple[str, int]] = []
    for i, pp in enumerate(p_parts):
        if pp == ">" and i == (len(p_parts) - 1):
            subs.append(("/".join(n_parts[i:]), i))
            return subs
        if i >= len(n_parts):
            return None
        if pp == "*":
            subs.append((n_parts[i], i))
        elif pp != n_parts[i]:
            return None
    if len(p_parts) != len(n_parts):
        return None
    return subs


# =====================================================================================================================
# Subject-ID Computation
# =====================================================================================================================


def compute_subject_id(topic_hash: int, evictions: int, modulus: int) -> int:
    """Compute the subject-ID for a topic given its hash, evictions, and subject-ID modulus."""
    if evictions >= EVICTIONS_PINNED_MIN:
        return 0xFFFFFFFF - evictions
    return SUBJECT_ID_PINNED_MAX + 1 + ((topic_hash + (evictions * evictions)) % modulus)


# =====================================================================================================================
# Internal Data Structures
# =====================================================================================================================


@dataclass
class Association:
    """Tracks a known remote subscriber for reliable delivery ACK tracking."""

    remote_id: int
    last_seen: float
    slack: int = 0
    seqno_witness: int = 0
    pending_count: int = 0


@dataclass
class DedupState:
    """Per-remote deduplication state for reliable messages."""

    tag_frontier: int = 0
    bitmap: int = 0
    last_active: float = 0.0

    def check(self, tag: int) -> bool:
        rev = (self.tag_frontier - tag) & U64_MASK
        return rev < DEDUP_HISTORY and bool((self.bitmap >> rev) & 1)

    def check_and_record(self, tag: int, now: float) -> bool:
        """Returns True if this is a new (non-duplicate) tag."""
        if (now - self.last_active) > SESSION_LIFETIME:
            self.tag_frontier = tag
            self.bitmap = 0
        self.last_active = now
        fwd = (tag - self.tag_frontier) & U64_MASK
        rev = (self.tag_frontier - tag) & U64_MASK
        if rev < DEDUP_HISTORY:
            mask = 1 << rev
            if self.bitmap & mask:
                return False
            self.bitmap |= mask
            return True
        if fwd < DEDUP_HISTORY:
            self.bitmap = (self.bitmap << fwd) & ((1 << DEDUP_HISTORY) - 1)
        else:
            self.bitmap = 0
        self.tag_frontier = tag
        self.bitmap |= 1
        return True


@dataclass
class SubscriberRoot:
    """Groups subscribers sharing the same subscription name/pattern."""

    name: str
    is_pattern: bool
    subscribers: list[Any] = field(default_factory=list)  # list[SubscriberImpl]
    needs_scouting: bool = False
    scout_task: asyncio.Task[None] | None = None


@dataclass
class Coupling:
    """Links a topic to a subscriber root with pattern substitutions."""

    root: SubscriberRoot
    substitutions: list[tuple[str, int]]


@dataclass
class SharedSubjectListener:
    """One transport listener shared by all topics bound to the same subject-ID."""

    handle: Closable
    owners: set[Topic] = field(default_factory=set)


@dataclass
class SharedSubjectWriter:
    """One transport writer shared by all topics bound to the same subject-ID."""

    handle: SubjectWriter
    owners: set[Topic] = field(default_factory=set)


@dataclass(frozen=True)
class _TopicFlyweight(Topic):
    """Short-lived topic view for unknown gossip."""

    _topic_hash: int
    _name: str

    @property
    def hash(self) -> int:
        return self._topic_hash

    @property
    def name(self) -> str:
        return self._name

    def match(self, pattern: str) -> list[tuple[str, int]] | None:
        return match_pattern(pattern, self._name)


@dataclass
class _MonitorHandle(Closable):
    _node: NodeImpl | None
    _callback_id: int

    def close(self) -> None:
        node = self._node
        if node is None:
            return
        node.monitor_unregister(self._callback_id)
        self._node = None


@dataclass
class PublishTracker:
    """Tracks a pending reliable publication awaiting ACKs."""

    tag: int
    deadline_ns: int
    ack_event: asyncio.Event
    acknowledged: bool = False
    data: bytes | None = None
    ack_timeout: float = ACK_BASELINE_DEFAULT_TIMEOUT
    compromised: bool = False
    remaining: set[int] = field(default_factory=set)
    associations: list[Association] = field(default_factory=list)

    def on_ack(self, remote_id: int, positive: bool) -> None:
        self.remaining.discard(remote_id)
        self.acknowledged = self.acknowledged or positive
        if not self.remaining and self.acknowledged:
            self.ack_event.set()


# =====================================================================================================================
# Topic
# =====================================================================================================================


class TopicImpl(Topic):

    def __init__(self, node: NodeImpl, name: str, evictions: int, now: float) -> None:
        self._node = node
        self._name = name
        self._topic_hash = rapidhash(name)
        self.evictions = evictions
        self.ts_origin = now
        self.ts_animated = now
        self._pub_tag_baseline = int.from_bytes(os.urandom(8), "little")
        self._pub_seqno = 0
        self.pub_count = 0
        self.pub_writer: SubjectWriter | None = None
        self.sub_listener: Closable | None = None
        self.couplings: list[Coupling] = []
        self.is_implicit = True
        self.associations: dict[int, Association] = {}
        self.dedup: dict[int, DedupState] = {}
        self.publish_futures: dict[int, PublishTracker] = {}
        self.request_futures: dict[int, ResponseStreamImpl] = {}  # tag -> ResponseStreamImpl
        self.gossip_task: asyncio.Task[None] | None = None
        self.gossip_deadline: float | None = None
        self.gossip_task_is_periodic = False
        self.gossip_counter = 0

    # -- Topic ABC --
    @property
    def hash(self) -> int:
        return self._topic_hash

    @property
    def name(self) -> str:
        return self._name

    def match(self, pattern: str) -> list[tuple[str, int]] | None:
        return match_pattern(pattern, self._name)

    # -- Internal --
    @property
    def subject_id(self) -> int:
        return compute_subject_id(self._topic_hash, self.evictions, self._node.transport.subject_id_modulus)

    def lage(self, now: float) -> int:
        return log_age(self.ts_origin, now)

    def merge_lage(self, now: float, remote_lage: int) -> None:
        """Shift ts_origin backward if the remote claims an older origin."""
        self.ts_origin = min(self.ts_origin, now - lage_to_seconds(remote_lage))

    def animate(self, ts: float) -> None:
        self.ts_animated = ts
        if self.is_implicit:
            self._node.touch_implicit_topic(self)

    def next_tag(self) -> int:
        tag = (self._pub_tag_baseline + self._pub_seqno) & ((1 << 64) - 1)
        self._pub_seqno += 1
        return tag

    @property
    def pub_seqno(self) -> int:
        return self._pub_seqno

    def tag_seqno(self, tag: int) -> int:
        return (tag - self._pub_tag_baseline) & U64_MASK

    def ensure_writer(self) -> SubjectWriter:
        if self.pub_writer is None:
            sid = self.subject_id
            self.pub_writer = self._node.acquire_subject_writer(self, sid)
            _logger.info("Writer acquired for '%s' sid=%d", self._name, sid)
        return self.pub_writer

    def ensure_listener(self) -> None:
        if self.sub_listener is None and self.couplings:
            sid = self.subject_id
            self.sub_listener = self._node.acquire_subject_listener(self, sid)
            _logger.info("Listener acquired for '%s' sid=%d", self._name, sid)

    def sync_listener(self) -> None:
        if self.couplings:
            self.ensure_listener()
        elif self.sub_listener is not None:
            self._node.release_subject_listener(self, self.subject_id)
            self.sub_listener = None
            _logger.info("Listener released for '%s'", self._name)

    def release_transport_handles(self) -> None:
        sid = self.subject_id
        if self.pub_writer is not None:
            self._node.release_subject_writer(self, sid)
            self.pub_writer = None
        if self.sub_listener is not None:
            self._node.release_subject_listener(self, sid)
            self.sub_listener = None

    def compute_is_implicit(self) -> bool:
        has_verbatim_sub = any(not c.root.is_pattern for c in self.couplings)
        return self.pub_count == 0 and not has_verbatim_sub

    def sync_implicit(self) -> None:
        """Sync implicitness and transport state with the reference state machine."""
        self._node.sync_topic_lifecycle(self)


def log_age(origin: float, now: float) -> int:
    diff = int(now - origin)
    if diff <= 0:
        return -1
    return int(math.log2(diff))


def lage_to_seconds(lage: int) -> float:
    if lage < 0:
        return 0.0
    return float(1 << min(LAGE_MAX, lage))


def left_wins(l_lage: int, l_hash: int, r_lage: int, r_hash: int) -> bool:
    return l_lage > r_lage if l_lage != r_lage else l_hash < r_hash


# =====================================================================================================================
# Node
# =====================================================================================================================


class NodeImpl(Node):
    def __init__(self, transport: Transport, *, home: str, namespace: str) -> None:
        self.transport = transport
        self._home = home
        self._namespace = namespace
        self._remaps: dict[str, str] = {}
        self._closed = False
        self.loop = asyncio.get_running_loop()
        self._now_mono = time.monotonic()
        self._monitor_callbacks: dict[int, Callable[[Topic], None]] = {}
        self._next_monitor_callback_id = 0

        # Topic indexes.
        self.topics_by_name: dict[str, TopicImpl] = {}
        self.topics_by_hash: dict[int, TopicImpl] = {}
        self.topics_by_subject_id: dict[int, TopicImpl] = {}  # non-pinned only

        # Subscriber roots.
        self.sub_roots_verbatim: dict[str, SubscriberRoot] = {}
        self.sub_roots_pattern: dict[str, SubscriberRoot] = {}

        # Respond futures for reliable responses.
        self.respond_futures: dict[tuple[int, ...], RespondTracker] = {}

        # Compute broadcast and gossip shard subject IDs.
        modulus = transport.subject_id_modulus
        sid_max = SUBJECT_ID_PINNED_MAX + modulus
        self.broadcast_subject_id = (1 << (int(math.log2(sid_max)) + 1)) - 1
        self.gossip_shard_count = self.broadcast_subject_id - (sid_max + 1)
        assert self.gossip_shard_count > 0

        # Set up broadcast writer and listener.
        self.broadcast_writer = transport.subject_advertise(self.broadcast_subject_id)

        def broadcast_handler(arrival: TransportArrival) -> None:
            self.on_subject_arrival(self.broadcast_subject_id, arrival)

        self.broadcast_listener = transport.subject_listen(self.broadcast_subject_id, broadcast_handler)

        # Gossip shard state: lazily created per shard.
        self.gossip_shard_writers: dict[int, SubjectWriter] = {}
        self.gossip_shard_listeners: dict[int, Closable] = {}
        self.shared_subject_writers: dict[int, SharedSubjectWriter] = {}
        self.shared_subject_listeners: dict[int, SharedSubjectListener] = {}

        # Register unicast handler.
        transport.unicast_listen(self.on_unicast_arrival)

        # Implicit topic GC task, driven by the earliest implicit-topic expiry.
        self._implicit_topics: OrderedDict[TopicImpl, None] = OrderedDict()
        self._implicit_gc_wakeup = asyncio.Event()
        self._gc_task = self.loop.create_task(self.implicit_gc_loop())

        _logger.info(
            "Node init home='%s' ns='%s' broadcast_sid=%d shards=%d",
            home,
            namespace,
            self.broadcast_subject_id,
            self.gossip_shard_count,
        )

    # -- Node ABC --
    @property
    def home(self) -> str:
        return self._home

    @property
    def namespace(self) -> str:
        return self._namespace

    def remap(self, spec: str | dict[str, str]) -> None:
        if isinstance(spec, str):
            spec = dict(x.split("=", 1) for x in spec.split() if "=" in x)
        assert isinstance(spec, dict)
        for from_name, to_name in spec.items():
            if key := _name_normalize(from_name):
                self._remaps[key] = to_name

    def advertise(self, name: str) -> Publisher:
        from ._publisher import PublisherImpl

        resolved, pin, verbatim = resolve_name(name, self._home, self._namespace, self._remaps)
        if not verbatim:
            raise ValueError("Cannot advertise on a pattern name")
        topic = self.topic_ensure(resolved, pin)
        topic.pub_count += 1
        topic.sync_implicit()
        topic.ensure_writer()
        _logger.info("Advertise '%s' -> '%s' sid=%d", name, resolved, topic.subject_id)
        return PublisherImpl(self, topic)

    def subscribe(self, name: str, *, reordering_window: float | None = None) -> Subscriber:
        from ._subscriber import SubscriberImpl

        resolved, pin, verbatim = resolve_name(name, self._home, self._namespace, self._remaps)
        if pin is not None and not verbatim:
            raise ValueError("Pattern names cannot be pinned")

        # Ensure subscriber root.
        if verbatim:
            root = self.sub_roots_verbatim.get(resolved)
            if root is None:
                root = SubscriberRoot(name=resolved, is_pattern=False)
                self.sub_roots_verbatim[resolved] = root
        else:
            root = self.sub_roots_pattern.get(resolved)
            if root is None:
                root = SubscriberRoot(name=resolved, is_pattern=True, needs_scouting=True)
                self.sub_roots_pattern[resolved] = root

        subscriber = SubscriberImpl(self, root, resolved, verbatim, reordering_window)
        root.subscribers.append(subscriber)

        if verbatim:
            # Ensure topic exists and couple.
            topic = self.topic_ensure(resolved, pin)
            self.couple_topic_root(topic, root)
            topic.sync_implicit()
        else:
            # Pattern subscriber: couple with all existing matching topics and scout once per root.
            for topic in list(self.topics_by_name.values()):
                self.couple_topic_root(topic, root)
                topic.sync_implicit()
            self._ensure_root_scouting(root)

        _logger.info("Subscribe '%s' -> '%s' verbatim=%s", name, resolved, verbatim)
        return subscriber

    def monitor(self, callback: Callable[[Topic], None]) -> Closable:
        callback_id = self._next_monitor_callback_id
        self._next_monitor_callback_id += 1
        self._monitor_callbacks[callback_id] = callback
        return _MonitorHandle(self, callback_id)

    def monitor_unregister(self, callback_id: int) -> None:
        self._monitor_callbacks.pop(callback_id, None)

    def _notify_monitors(self, topic: Topic) -> None:
        for callback in list(self._monitor_callbacks.values()):
            try:
                callback(topic)
            except Exception:
                _logger.exception("monitor() callback failed for %s", topic)

    async def scout(self, pattern: str) -> None:
        resolved, pin, _ = resolve_name(pattern, self._home, self._namespace, self._remaps)
        if pin is not None:
            raise ValueError("Cannot scout a pinned name/pattern")
        try:
            await self._transmit_scout(resolved)
        except SendError:
            raise
        except Exception as ex:
            raise SendError(f"Scout send failed for '{resolved}'") from ex

    # -- Topic Management --

    def topic_ensure(self, name: str, pin: int | None) -> TopicImpl:
        """Get or create a topic by resolved name."""
        topic = self.topics_by_name.get(name)
        if topic is not None:
            return topic
        now = time.monotonic()
        evictions = 0
        if pin is not None:
            evictions = 0xFFFFFFFF - pin
        topic = TopicImpl(self, name, evictions, now)
        self.topics_by_name[name] = topic
        self.topics_by_hash[topic.hash] = topic
        self.ensure_gossip_shard(self.gossip_shard_subject_id(topic.hash))
        self.touch_implicit_topic(topic)
        self.topic_allocate(topic, evictions, now)
        # Couple with existing pattern subscriber roots.
        for root in self.sub_roots_pattern.values():
            self.couple_topic_root(topic, root)
        topic.sync_listener()
        self.notify_implicit_gc()
        _logger.info("Topic created '%s' hash=%016x sid=%d", name, topic.hash, topic.subject_id)
        return topic

    def topic_allocate(self, topic: TopicImpl, new_evictions: int, now: float) -> None:
        """Iterative subject-ID allocation with collision resolution. Mirrors topic_allocate() in cy.c."""
        # Work queue: list of (topic, new_evictions) pairs to process.
        work: list[tuple[TopicImpl, int]] = [(topic, new_evictions)]
        while work:
            t, ev = work.pop(0)
            # Remove from subject-ID index first.
            old_sid = t.subject_id
            if old_sid in self.topics_by_subject_id and self.topics_by_subject_id[old_sid] is t:
                del self.topics_by_subject_id[old_sid]

            if ev >= EVICTIONS_PINNED_MIN:
                # Pinned topic: no collision detection, shared subject-IDs are fine.
                t.release_transport_handles()
                t.evictions = ev
                t.sync_listener()
                self.schedule_gossip_urgent(t)
                continue

            modulus = self.transport.subject_id_modulus
            new_sid = compute_subject_id(t.hash, ev, modulus)
            collider = self.topics_by_subject_id.get(new_sid)

            if collider is not None and collider is t:
                collider = None  # same topic, no real collision

            if collider is None:
                # No collision, install.
                t.release_transport_handles()
                t.evictions = ev
                self.topics_by_subject_id[new_sid] = t
                t.sync_listener()
                self.schedule_gossip_urgent(t)
            elif left_wins(t.lage(now), t.hash, collider.lage(now), collider.hash):
                # Our topic wins: take the slot, evict the collider.
                t.release_transport_handles()
                t.evictions = ev
                del self.topics_by_subject_id[new_sid]
                self.topics_by_subject_id[new_sid] = t
                if collider.pub_writer is not None:
                    t.pub_writer = self.acquire_subject_writer(t, new_sid)
                t.sync_listener()
                self.schedule_gossip_urgent(t)
                # Schedule collider for reallocation.
                collider.release_transport_handles()
                work.append((collider, collider.evictions + 1))
            else:
                # Our topic loses: increment evictions and retry.
                work.append((t, ev + 1))

    def sync_topic_lifecycle(self, topic: TopicImpl) -> None:
        implicit = topic.compute_is_implicit()
        if implicit != topic.is_implicit:
            topic.is_implicit = implicit
            if implicit:
                self.touch_implicit_topic(topic)
                self._cancel_gossip(topic)
            else:
                self.discard_implicit_topic(topic)
                self.schedule_gossip_urgent(topic)
        elif (not implicit) and (topic.gossip_task is None):
            self.schedule_gossip(topic)
        topic.sync_listener()
        self.notify_implicit_gc()

    def touch_implicit_topic(self, topic: TopicImpl) -> None:
        self._implicit_topics[topic] = None
        self._implicit_topics.move_to_end(topic, last=False)
        self.notify_implicit_gc()

    def discard_implicit_topic(self, topic: TopicImpl) -> None:
        if topic in self._implicit_topics:
            del self._implicit_topics[topic]
            self.notify_implicit_gc()

    def decouple_topic_root(
        self, topic: TopicImpl, root: SubscriberRoot, *, silenced: bool = True, sync_lifecycle: bool = True
    ) -> None:
        from ._subscriber import SubscriberImpl

        topic.couplings = [c for c in topic.couplings if c.root is not root]
        for sub in root.subscribers:
            if isinstance(sub, SubscriberImpl):
                sub.forget_topic_reordering(topic.hash, silenced=silenced)
        if sync_lifecycle:
            self.sync_topic_lifecycle(topic)

    @staticmethod
    def forget_association(topic: TopicImpl, assoc: Association) -> None:
        current = topic.associations.get(assoc.remote_id)
        if current is assoc:
            del topic.associations[assoc.remote_id]

    @staticmethod
    def publish_tracker_release(topic: TopicImpl, tracker: PublishTracker) -> None:
        seqno = topic.tag_seqno(tracker.tag)
        for assoc in tracker.associations:
            if assoc.remote_id in tracker.remaining and seqno >= assoc.seqno_witness and not tracker.compromised:
                assoc.slack += 1
            if assoc.pending_count > 0:
                assoc.pending_count -= 1
            if assoc.slack >= ASSOC_SLACK_LIMIT and assoc.pending_count == 0:
                NodeImpl.forget_association(topic, assoc)
        tracker.associations.clear()
        tracker.remaining.clear()

    @staticmethod
    def prepare_publish_tracker(topic: TopicImpl, tag: int, deadline_ns: int, data: bytes) -> PublishTracker:
        tracker = PublishTracker(
            tag=tag,
            deadline_ns=deadline_ns,
            ack_event=asyncio.Event(),
            data=data,
        )
        tracker.ack_timeout = ACK_BASELINE_DEFAULT_TIMEOUT
        for assoc in sorted(topic.associations.values(), key=lambda x: x.remote_id):
            if assoc.slack < ASSOC_SLACK_LIMIT:
                tracker.associations.append(assoc)
                tracker.remaining.add(assoc.remote_id)
                assoc.pending_count += 1
        return tracker

    @staticmethod
    def couple_topic_root(topic: TopicImpl, root: SubscriberRoot) -> None:
        """Create a coupling between a topic and a subscriber root if not already coupled."""
        for c in topic.couplings:
            if c.root is root:
                return  # already coupled
        subs = match_pattern(root.name, topic.name) if root.is_pattern else ([] if root.name == topic.name else None)
        if subs is not None:
            topic.couplings.append(Coupling(root=root, substitutions=subs))
            _logger.debug("Coupled '%s' <-> root '%s'", topic.name, root.name)

    # -- Gossip --

    def gossip_shard_subject_id(self, topic_hash: int) -> int:
        modulus = self.transport.subject_id_modulus
        sid_max = SUBJECT_ID_PINNED_MAX + modulus
        shard_index = topic_hash % self.gossip_shard_count
        return sid_max + 1 + shard_index

    def ensure_gossip_shard(self, shard_sid: int) -> SubjectWriter:
        writer = self.gossip_shard_writers.get(shard_sid)
        if writer is None:
            writer = self.transport.subject_advertise(shard_sid)
            self.gossip_shard_writers[shard_sid] = writer

            def handler(arrival: TransportArrival) -> None:
                self.on_subject_arrival(shard_sid, arrival)

            self.gossip_shard_listeners[shard_sid] = self.transport.subject_listen(shard_sid, handler)
            _logger.debug("Gossip shard writer/listener for sid=%d", shard_sid)
        return writer

    def acquire_subject_writer(self, topic: TopicImpl, subject_id: int) -> SubjectWriter:
        entry = self.shared_subject_writers.get(subject_id)
        if entry is None:
            entry = SharedSubjectWriter(handle=self.transport.subject_advertise(subject_id))
            self.shared_subject_writers[subject_id] = entry
            _logger.debug("Shared subject writer created sid=%d", subject_id)
        entry.owners.add(topic)
        return entry.handle

    def release_subject_writer(self, topic: TopicImpl, subject_id: int) -> None:
        entry = self.shared_subject_writers.get(subject_id)
        if entry is None:
            return
        entry.owners.discard(topic)
        if not entry.owners:
            entry.handle.close()
            del self.shared_subject_writers[subject_id]
            _logger.debug("Shared subject writer released sid=%d", subject_id)

    def acquire_subject_listener(self, topic: TopicImpl, subject_id: int) -> Closable:
        entry = self.shared_subject_listeners.get(subject_id)
        if entry is None:

            def handler(arrival: TransportArrival) -> None:
                self.on_subject_arrival(subject_id, arrival)

            entry = SharedSubjectListener(handle=self.transport.subject_listen(subject_id, handler))
            self.shared_subject_listeners[subject_id] = entry
            _logger.debug("Shared subject listener created sid=%d", subject_id)
        entry.owners.add(topic)
        return entry.handle

    def release_subject_listener(self, topic: TopicImpl, subject_id: int) -> None:
        entry = self.shared_subject_listeners.get(subject_id)
        if entry is None:
            return
        entry.owners.discard(topic)
        if not entry.owners:
            entry.handle.close()
            del self.shared_subject_listeners[subject_id]
            _logger.debug("Shared subject listener released sid=%d", subject_id)

    def schedule_gossip(self, topic: TopicImpl) -> None:
        """Start periodic gossip for an explicit topic."""
        if topic.gossip_task is not None:
            return  # already scheduled
        self._reschedule_gossip_periodic(topic, suppressed=False)

    @staticmethod
    def _cancel_gossip(topic: TopicImpl) -> None:
        if topic.gossip_task is not None:
            topic.gossip_task.cancel()
            topic.gossip_task = None
        topic.gossip_deadline = None

    def _schedule_gossip_task(self, topic: TopicImpl, deadline: float, *, periodic: bool) -> None:
        self._cancel_gossip(topic)
        topic.gossip_task_is_periodic = periodic
        topic.gossip_deadline = deadline
        topic.gossip_task = self.loop.create_task(self._gossip_wait(topic, deadline))

    def _reschedule_gossip_periodic(self, topic: TopicImpl, *, suppressed: bool) -> None:
        if topic.is_implicit:
            self._cancel_gossip(topic)
            return
        dither = GOSSIP_PERIOD / GOSSIP_PERIOD_DITHER_RATIO
        if suppressed:
            delay_min = GOSSIP_PERIOD + dither
            delay_max = GOSSIP_PERIOD * 3
        else:
            delay_min = GOSSIP_PERIOD - dither
            delay_max = GOSSIP_PERIOD + dither
            if topic.gossip_counter < GOSSIP_BROADCAST_RATIO:
                delay_min /= 16
        delay = random.uniform(max(0.0, delay_min), max(delay_min, delay_max))
        self._schedule_gossip_task(topic, time.monotonic() + delay, periodic=True)

    def schedule_gossip_urgent(self, topic: TopicImpl) -> None:
        """Schedule an urgent gossip, preserving an earlier pending deadline when possible."""
        at = time.monotonic() + (random.random() * GOSSIP_URGENT_DELAY_MAX)
        if (topic.gossip_task is None) or (topic.gossip_deadline is None) or (at < topic.gossip_deadline):
            self._schedule_gossip_task(topic, at, periodic=False)
        else:
            topic.gossip_task_is_periodic = False

    async def _gossip_wait(self, topic: TopicImpl, deadline: float) -> None:
        try:
            await asyncio.sleep(max(0.0, deadline - time.monotonic()))
        except asyncio.CancelledError:
            return
        if topic.gossip_task is not asyncio.current_task():
            return
        topic.gossip_task = None
        topic.gossip_deadline = None
        if self._closed:
            return
        if topic.gossip_task_is_periodic:
            await self._gossip_event_periodic(topic)
        else:
            await self._gossip_event_urgent(topic)

    async def _gossip_event_urgent(self, topic: TopicImpl) -> None:
        self._reschedule_gossip_periodic(topic, suppressed=False)
        topic.gossip_counter = 0
        await self.send_gossip(topic, broadcast=True)

    async def _gossip_event_periodic(self, topic: TopicImpl) -> None:
        self._reschedule_gossip_periodic(topic, suppressed=False)
        broadcast = (topic.gossip_counter < GOSSIP_BROADCAST_RATIO) or (
            (topic.gossip_counter % GOSSIP_BROADCAST_RATIO) == 0
        )
        topic.gossip_counter += 1
        await self.send_gossip(topic, broadcast=broadcast)

    async def send_gossip(self, topic: TopicImpl, *, broadcast: bool = False) -> None:
        now = time.monotonic()
        lage = topic.lage(now)
        name_bytes = topic.name.encode("utf-8")
        hdr = GossipHeader(
            topic_log_age=lage,
            topic_hash=topic.hash,
            topic_evictions=topic.evictions,
            name_len=len(name_bytes),
        )
        payload = hdr.serialize() + name_bytes
        deadline = Instant.now() + 1.0
        try:
            if broadcast:
                await self.broadcast_writer(deadline, Priority.NOMINAL, payload)
            else:
                shard_sid = self.gossip_shard_subject_id(topic.hash)
                writer = self.ensure_gossip_shard(shard_sid)
                await writer(deadline, Priority.NOMINAL, payload)
            _logger.debug("Gossip sent '%s' broadcast=%s", topic.name, broadcast)
        except (SendError, OSError) as e:
            _logger.warning("Gossip send failed for '%s': %s", topic.name, e)

    async def send_gossip_unicast(
        self,
        topic: TopicImpl,
        remote_id: int,
        priority: Priority = Priority.NOMINAL,
    ) -> None:
        now = time.monotonic()
        lage = topic.lage(now)
        name_bytes = topic.name.encode("utf-8")
        hdr = GossipHeader(
            topic_log_age=lage,
            topic_hash=topic.hash,
            topic_evictions=topic.evictions,
            name_len=len(name_bytes),
        )
        payload = hdr.serialize() + name_bytes
        deadline = Instant.now() + 1.0
        try:
            await self.transport.unicast(deadline, priority, remote_id, payload)
        except (SendError, OSError) as e:
            _logger.warning("Gossip unicast send failed for '%s': %s", topic.name, e)

    # -- Scout --

    async def _transmit_scout(self, pattern: str) -> None:
        pattern_bytes = pattern.encode("utf-8")
        hdr = ScoutHeader(pattern_len=len(pattern_bytes))
        payload = hdr.serialize() + pattern_bytes
        deadline = Instant.now() + 1.0
        await self.broadcast_writer(deadline, Priority.NOMINAL, payload)
        _logger.debug("Scout sent for pattern '%s'", pattern)

    async def _send_scout_once(self, pattern: str) -> bool:
        try:
            await self._transmit_scout(pattern)
        except Exception as e:
            _logger.warning("Scout send failed for '%s': %s", pattern, e)
            return False
        return True

    def _ensure_root_scouting(self, root: SubscriberRoot) -> None:
        if (not root.is_pattern) or (not root.needs_scouting) or (root.scout_task is not None):
            return

        async def do_send() -> None:
            try:
                root.needs_scouting = not await self._send_scout_once(root.name)
            finally:
                root.scout_task = None

        root.scout_task = self.loop.create_task(do_send())

    def send_scout(self, pattern: str) -> None:
        """Send a scout message to discover topics matching a pattern."""

        async def do_send() -> None:
            await self._send_scout_once(pattern)

        self.loop.create_task(do_send())

    # -- Message Dispatch --

    def on_subject_arrival(self, subject_id: int, arrival: TransportArrival) -> None:
        """Handle an arrival on a subject (multicast)."""
        self.dispatch_arrival(arrival, subject_id=subject_id, unicast=False)

    def on_unicast_arrival(self, arrival: TransportArrival) -> None:
        """Handle an arrival via unicast."""
        self.dispatch_arrival(arrival, subject_id=None, unicast=True)

    def dispatch_arrival(self, arrival: TransportArrival, *, subject_id: int | None, unicast: bool) -> None:
        msg = arrival.message
        if len(msg) < HEADER_SIZE:
            _logger.debug("Drop short msg len=%d", len(msg))
            return
        hdr = deserialize_header(msg[:HEADER_SIZE])
        if hdr is None:
            _logger.debug("Drop bad header")
            return
        payload = msg[HEADER_SIZE:]

        if isinstance(hdr, (MsgBeHeader, MsgRelHeader)):
            self.on_msg(arrival, hdr, payload, subject_id=subject_id, unicast=unicast)
        elif isinstance(hdr, (MsgAckHeader, MsgNackHeader)):
            if unicast:
                self.on_msg_ack(arrival, hdr)
        elif isinstance(hdr, (RspBeHeader, RspRelHeader)):
            if unicast:
                self.on_rsp(arrival, hdr, payload)
        elif isinstance(hdr, (RspAckHeader, RspNackHeader)):
            if unicast:
                self.on_rsp_ack(arrival, hdr)
        elif isinstance(hdr, GossipHeader):
            if hdr.name_len > TOPIC_NAME_MAX or len(payload) < hdr.name_len:
                return
            scope = (
                GossipScope.UNICAST
                if unicast
                else GossipScope.BROADCAST if subject_id == self.broadcast_subject_id else GossipScope.SHARDED
            )
            self.on_gossip(arrival.timestamp.s, hdr, payload, scope)
        elif isinstance(hdr, ScoutHeader):
            self.on_scout(arrival, hdr, payload)

    def on_msg(
        self,
        arrival: TransportArrival,
        hdr: MsgBeHeader | MsgRelHeader,
        payload: bytes,
        *,
        subject_id: int | None,
        unicast: bool,
    ) -> None:
        if (
            (not unicast)
            and (subject_id is not None)
            and (subject_id <= (SUBJECT_ID_PINNED_MAX + self.transport.subject_id_modulus))
            and (
                compute_subject_id(hdr.topic_hash, hdr.topic_evictions, self.transport.subject_id_modulus) != subject_id
            )
        ):
            _logger.debug("MSG drop subject mismatch sid=%d hash=%016x", subject_id, hdr.topic_hash)
            return
        topic = self.topics_by_hash.get(hdr.topic_hash)
        reliable = isinstance(hdr, MsgRelHeader)
        accepted = False
        if topic is not None:
            self.on_gossip_known(topic, hdr.topic_evictions, hdr.topic_log_age, arrival.timestamp.s, GossipScope.INLINE)
            accepted = self.accept_message(topic, arrival, hdr.tag, payload, reliable)
        else:
            self.on_gossip_unknown(hdr.topic_hash, hdr.topic_evictions, hdr.topic_log_age, arrival.timestamp.s)
            _logger.debug("MSG drop unknown hash=%016x", hdr.topic_hash)

        has_subscribers = (topic is not None) and bool(topic.couplings)
        if reliable and (accepted or (unicast and not has_subscribers)):
            self.send_msg_ack(arrival.remote_id, hdr.topic_hash, hdr.tag, arrival.timestamp, arrival.priority, accepted)

    def accept_message(
        self,
        topic: TopicImpl,
        arrival: TransportArrival,
        tag: int,
        payload: bytes,
        reliable: bool,
    ) -> bool:
        topic.animate(arrival.timestamp.s)
        if not topic.couplings:
            if reliable:
                dedup = topic.dedup.get(arrival.remote_id)
                if dedup is not None and (arrival.timestamp.s - dedup.last_active) > SESSION_LIFETIME:
                    del topic.dedup[arrival.remote_id]
                    dedup = None
                return dedup.check(tag) if dedup is not None else False
            return False

        if reliable:
            dedup = topic.dedup.get(arrival.remote_id)
            if dedup is not None and (arrival.timestamp.s - dedup.last_active) > SESSION_LIFETIME:
                del topic.dedup[arrival.remote_id]
                dedup = None
            if dedup is None:
                dedup = DedupState(tag_frontier=tag)
                topic.dedup[arrival.remote_id] = dedup
            if not dedup.check_and_record(tag, arrival.timestamp.s):
                _logger.debug("MSG dedup drop hash=%016x tag=%d", topic.hash, tag)
                return True

        from ._subscriber import BreadcrumbImpl

        breadcrumb = BreadcrumbImpl(
            node=self,
            remote_id=arrival.remote_id,
            topic=topic,
            message_tag=tag,
            initial_priority=arrival.priority,
        )
        return self.deliver_to_subscribers(topic, arrival, breadcrumb, payload, tag)

    @staticmethod
    def deliver_to_subscribers(
        topic: TopicImpl,
        arrival: TransportArrival,
        breadcrumb: Breadcrumb,
        payload: bytes,
        tag: int,
    ) -> bool:
        from ._api import Arrival
        from ._subscriber import SubscriberImpl

        arr = Arrival(
            timestamp=arrival.timestamp,
            breadcrumb=breadcrumb,
            message=payload,
        )
        accepted = False
        for coupling in topic.couplings:
            for sub in coupling.root.subscribers:
                if isinstance(sub, SubscriberImpl) and not sub.closed:
                    accepted = sub.deliver(arr, tag, arrival.remote_id) or accepted
        return accepted

    def send_msg_ack(
        self,
        remote_id: int,
        topic_hash: int,
        tag: int,
        ts: Instant,
        priority: Priority,
        positive: bool,
    ) -> None:
        hdr: MsgAckHeader | MsgNackHeader
        hdr = (
            MsgAckHeader(topic_hash=topic_hash, tag=tag) if positive else MsgNackHeader(topic_hash=topic_hash, tag=tag)
        )
        payload = hdr.serialize()
        deadline = ts + ACK_TX_TIMEOUT

        async def do_send() -> None:
            try:
                await self.transport.unicast(deadline, priority, remote_id, payload)
            except (SendError, OSError) as e:
                _logger.debug("ACK send failed: %s", e)

        self.loop.create_task(do_send())

    def on_msg_ack(self, arrival: TransportArrival, hdr: MsgAckHeader | MsgNackHeader) -> None:
        topic = self.topics_by_hash.get(hdr.topic_hash)
        if topic is None:
            return
        seqno = topic.tag_seqno(hdr.tag)
        if seqno >= topic.pub_seqno or (topic.pub_seqno - seqno) > ACK_SEQNO_MAX_LAG:
            return
        positive = isinstance(hdr, MsgAckHeader)
        remote_id = arrival.remote_id

        assoc = topic.associations.get(remote_id)
        if assoc is None:
            if not positive:
                return
            assoc = Association(remote_id=remote_id, last_seen=arrival.timestamp.s)
            topic.associations[remote_id] = assoc
        assoc.last_seen = arrival.timestamp.s
        if seqno >= assoc.seqno_witness:
            assoc.slack = 0 if positive else ASSOC_SLACK_LIMIT
            assoc.seqno_witness = seqno
            if (not positive) and assoc.pending_count == 0:
                assoc.slack = 0
                self.forget_association(topic, assoc)
                return

        tracker = topic.publish_futures.get(hdr.tag)
        if tracker is not None:
            tracker.on_ack(remote_id, positive)

    def on_rsp(self, arrival: TransportArrival, hdr: RspBeHeader | RspRelHeader, payload: bytes) -> None:
        """Handle a response message (for RPC)."""
        ack = False
        topic = self.topics_by_hash.get(hdr.topic_hash)
        if topic is not None:
            stream = topic.request_futures.get(hdr.message_tag)
            if stream is not None:
                ack = stream.on_response(arrival, hdr, payload)
        if not ack and not isinstance(hdr, RspBeHeader):
            _logger.debug("RSP drop no matching request tag=%d", hdr.message_tag)
        elif topic is None or hdr.message_tag not in topic.request_futures:
            _logger.debug("RSP drop no matching request tag=%d", hdr.message_tag)
        if isinstance(hdr, RspRelHeader):
            self.send_rsp_ack(
                arrival.remote_id,
                hdr.message_tag,
                hdr.seqno,
                hdr.tag,
                hdr.topic_hash,
                arrival.timestamp,
                arrival.priority,
                ack,
            )

    def on_rsp_ack(self, arrival: TransportArrival, hdr: RspAckHeader | RspNackHeader) -> None:
        """Handle a response ACK/NACK."""
        key = (arrival.remote_id, hdr.message_tag, hdr.topic_hash, hdr.seqno, hdr.tag)
        future = self.respond_futures.get(key)
        if future is not None:
            positive = isinstance(hdr, RspAckHeader)
            future.on_ack(positive)

    def send_rsp_ack(
        self,
        remote_id: int,
        message_tag: int,
        seqno: int,
        tag: int,
        topic_hash: int,
        ts: Instant,
        priority: Priority,
        positive: bool,
    ) -> None:
        hdr: RspAckHeader | RspNackHeader
        if positive:
            hdr = RspAckHeader(tag=tag, seqno=seqno, topic_hash=topic_hash, message_tag=message_tag)
        else:
            hdr = RspNackHeader(tag=tag, seqno=seqno, topic_hash=topic_hash, message_tag=message_tag)
        payload = hdr.serialize()
        deadline = ts + ACK_TX_TIMEOUT

        async def do_send() -> None:
            try:
                await self.transport.unicast(deadline, priority, remote_id, payload)
            except (SendError, OSError) as e:
                _logger.debug("RSP ACK send failed: %s", e)

        self.loop.create_task(do_send())

    def on_gossip(
        self,
        ts: float,
        hdr: GossipHeader,
        payload: bytes,
        scope: GossipScope,
    ) -> None:
        name = ""
        if hdr.name_len > 0:
            name = payload[: hdr.name_len].decode("utf-8", errors="replace")

        topic = self.topics_by_hash.get(hdr.topic_hash)

        # If unknown topic with a name, check for pattern subscriber matches.
        if topic is None and name:
            if scope in {GossipScope.UNICAST, GossipScope.BROADCAST}:
                topic = self.topic_subscribe_if_matching(
                    name, hdr.topic_hash, hdr.topic_evictions, hdr.topic_log_age, ts
                )
        if topic is not None:
            self.on_gossip_known(topic, hdr.topic_evictions, hdr.topic_log_age, ts, scope)
            self._notify_monitors(topic)
        else:
            self.on_gossip_unknown(hdr.topic_hash, hdr.topic_evictions, hdr.topic_log_age, ts)
            self._notify_monitors(_TopicFlyweight(hdr.topic_hash, name))

    def on_gossip_known(
        self,
        topic: TopicImpl,
        evictions: int,
        lage: int,
        now: float,
        scope: GossipScope,
    ) -> None:
        topic.animate(now)
        my_lage = topic.lage(now)
        if topic.evictions != evictions:
            win = my_lage > lage or (my_lage == lage and topic.evictions > evictions)
            topic.merge_lage(now, lage)
            if win:
                self.schedule_gossip_urgent(topic)
            else:
                self.topic_allocate(topic, evictions, now)
                if topic.evictions == evictions:
                    self._reschedule_gossip_periodic(topic, suppressed=True)
        else:
            topic.merge_lage(now, lage)
            suppress = (
                (scope in {GossipScope.BROADCAST, GossipScope.SHARDED})
                and (topic.lage(now) == lage)
                and (topic.gossip_task_is_periodic or scope == GossipScope.BROADCAST)
            )
            if suppress:
                self._reschedule_gossip_periodic(topic, suppressed=True)
            topic.sync_listener()

    def on_gossip_unknown(self, topic_hash: int, evictions: int, lage: int, now: float) -> None:
        modulus = self.transport.subject_id_modulus
        remote_sid = compute_subject_id(topic_hash, evictions, modulus)
        mine = self.topics_by_subject_id.get(remote_sid)
        if mine is None:
            return
        win = left_wins(mine.lage(now), mine.hash, lage, topic_hash)
        if win:
            self.schedule_gossip_urgent(mine)
        else:
            self.topic_allocate(mine, mine.evictions + 1, now)

    def topic_subscribe_if_matching(
        self,
        name: str,
        topic_hash: int,
        evictions: int,
        lage: int,
        now: float,
    ) -> TopicImpl | None:
        """Create an implicit topic if any pattern subscriber matches the name."""
        # Validate that the hash matches the name to prevent corrupt gossip from creating inconsistencies.
        if rapidhash(name) != topic_hash:
            _logger.debug("Gossip hash mismatch for '%s': got %016x, expected %016x", name, topic_hash, rapidhash(name))
            return None
        matches = [root for pattern, root in self.sub_roots_pattern.items() if match_pattern(pattern, name) is not None]
        if matches:
            topic = TopicImpl(self, name, evictions, now)
            topic.ts_origin = now - lage_to_seconds(lage)
            self.topics_by_name[name] = topic
            self.topics_by_hash[topic_hash] = topic
            self.ensure_gossip_shard(self.gossip_shard_subject_id(topic.hash))
            self.touch_implicit_topic(topic)
            self.topic_allocate(topic, evictions, now)
            for root in matches:
                self.couple_topic_root(topic, root)
            topic.sync_listener()
            self.notify_implicit_gc()
            _logger.info("Implicit topic '%s' created from gossip", name)
            return topic
        return None

    def on_scout(self, arrival: TransportArrival, hdr: ScoutHeader, payload: bytes) -> None:
        if hdr.pattern_len == 0 or hdr.pattern_len > TOPIC_NAME_MAX or len(payload) < hdr.pattern_len:
            return
        pattern = payload[: hdr.pattern_len].decode("utf-8", errors="replace")
        _logger.debug("Scout received pattern='%s' from %016x", pattern, arrival.remote_id)
        for topic in list(self.topics_by_name.values()):
            subs = match_pattern(pattern, topic.name)
            if subs is not None:
                self.loop.create_task(self.send_gossip_unicast(topic, arrival.remote_id, arrival.priority))

    # -- Implicit Topic GC --

    def notify_implicit_gc(self) -> None:
        if not self._closed:
            self._implicit_gc_wakeup.set()

    def _next_implicit_gc_delay(self, now: float | None = None) -> float | None:
        now = time.monotonic() if now is None else now
        if not self._implicit_topics:
            return None
        oldest = next(reversed(self._implicit_topics))
        return max(0.0, (oldest.ts_animated + IMPLICIT_TOPIC_TIMEOUT) - now)

    def _retire_one_expired_implicit_topic(self, now: float) -> bool:
        if not self._implicit_topics:
            return False
        oldest = next(reversed(self._implicit_topics))
        if (oldest.ts_animated + IMPLICIT_TOPIC_TIMEOUT) >= now:
            return False
        self.destroy_topic(oldest.name)
        _logger.info("GC removed implicit topic '%s'", oldest.name)
        return True

    async def implicit_gc_loop(self) -> None:
        try:
            while not self._closed:
                self._implicit_gc_wakeup.clear()
                delay = self._next_implicit_gc_delay()
                if delay is None:
                    await self._implicit_gc_wakeup.wait()
                    continue
                if delay > 0:
                    try:
                        await asyncio.wait_for(self._implicit_gc_wakeup.wait(), timeout=delay)
                        continue
                    except asyncio.TimeoutError:
                        pass
                self._retire_one_expired_implicit_topic(time.monotonic())
        except asyncio.CancelledError:
            pass

    def destroy_topic(self, name: str) -> None:
        topic = self.topics_by_name.get(name)
        if topic is None:
            return
        if topic.gossip_task is not None:
            self._cancel_gossip(topic)
        self.discard_implicit_topic(topic)
        topic.release_transport_handles()
        while topic.couplings:
            self.decouple_topic_root(topic, topic.couplings[0].root, sync_lifecycle=False)
        self.topics_by_name.pop(name, None)
        self.topics_by_hash.pop(topic.hash, None)
        sid = topic.subject_id
        if self.topics_by_subject_id.get(sid) is topic:
            del self.topics_by_subject_id[sid]
        topic.associations.clear()
        topic.dedup.clear()
        topic.publish_futures.clear()
        self.notify_implicit_gc()
        _logger.info("Topic destroyed '%s'", name)

    # -- Cleanup --

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _logger.info("Node closing home='%s'", self._home)
        self._gc_task.cancel()
        for root in list(self.sub_roots_pattern.values()):
            if root.scout_task is not None:
                root.scout_task.cancel()
                root.scout_task = None
        for topic in list(self.topics_by_name.values()):
            if topic.gossip_task is not None:
                self._cancel_gossip(topic)
            topic.release_transport_handles()
        self.broadcast_writer.close()
        self.broadcast_listener.close()
        for shared_writer in list(self.shared_subject_writers.values()):
            shared_writer.handle.close()
        self.shared_subject_writers.clear()
        for shared_listener in list(self.shared_subject_listeners.values()):
            shared_listener.handle.close()
        self.shared_subject_listeners.clear()
        for w in self.gossip_shard_writers.values():
            w.close()
        for gossip_listener in self.gossip_shard_listeners.values():
            gossip_listener.close()
        self._monitor_callbacks.clear()
        self._implicit_topics.clear()
        self.transport.close()
