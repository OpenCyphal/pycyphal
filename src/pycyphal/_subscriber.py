from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field

from ._api import DeliveryError, Instant, LivenessError, NackError, Priority, SendError
from ._api import Subscriber, Breadcrumb, Topic, Arrival
from ._header import SEQNO48_MASK, RspBeHeader, RspRelHeader
from ._node import (
    ACK_BASELINE_DEFAULT_TIMEOUT,
    REORDERING_CAPACITY,
    SESSION_LIFETIME,
    NodeImpl,
    SubscriberRoot,
    TopicImpl,
    match_pattern,
)

_logger = logging.getLogger(__name__)
REORDERING_WINDOW_MAX = SESSION_LIFETIME / 2


# =====================================================================================================================
# Reordering
# =====================================================================================================================


@dataclass
class InternedMsg:
    arrival: Arrival
    tag: int
    remote_id: int
    lin_tag: int


@dataclass
class ReorderingState:
    """Per (remote_id, topic_hash) reordering state for ordered subscriptions."""

    tag_baseline: int = 0
    last_ejected_lin_tag: int = 0
    last_active_at: float = 0.0
    interned: dict[int, InternedMsg] = field(default_factory=dict)  # lin_tag -> msg
    timeout_handle: asyncio.TimerHandle | None = None


class SubscriberImpl(Subscriber):
    def __init__(
        self,
        node: NodeImpl,
        root: SubscriberRoot,
        pattern: str,
        verbatim: bool,
        reordering_window: float | None,
    ) -> None:
        self._node = node
        self._root = root
        self._pattern = pattern
        self._verbatim = verbatim
        self._timeout = float("inf")
        self._reordering_window = self._normalize_reordering_window(reordering_window)
        self.queue: asyncio.Queue[Arrival | BaseException] = asyncio.Queue()
        self._reordering: dict[tuple[int, int], ReorderingState] = {}  # (remote_id, topic_hash)
        self.closed = False

    @staticmethod
    def _normalize_reordering_window(reordering_window: float | None) -> float | None:
        if reordering_window is None:
            return None
        out = float(reordering_window)
        if (out < 0.0) or (not math.isfinite(out)):
            raise ValueError("Reordering window must be a finite non-negative duration")
        if out > REORDERING_WINDOW_MAX:
            raise ValueError(f"Reordering window is too large")
        return out

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
        return match_pattern(self._pattern, topic.name)

    def __aiter__(self) -> SubscriberImpl:
        return self

    async def __anext__(self) -> Arrival:
        if self.closed:
            raise StopAsyncIteration
        timeout = self._timeout if self._timeout != float("inf") else None
        try:
            item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise LivenessError("No message received within timeout")
        if isinstance(item, StopAsyncIteration):
            raise item
        if isinstance(item, BaseException):
            raise item
        return item

    def deliver(self, arrival: Arrival, tag: int, remote_id: int) -> bool:
        """Called by the node to deliver a message to this subscriber."""
        if self.closed:
            return False
        if self._reordering_window is None:
            self.queue.put_nowait(arrival)
            return True
        # Reordering enabled.
        self._drop_stale_reordering(arrival.timestamp.s)
        topic_hash = arrival.breadcrumb.topic.hash
        key = (remote_id, topic_hash)
        state = self._reordering.get(key)
        if state is None:
            state = ReorderingState(
                tag_baseline=tag - (REORDERING_CAPACITY // 2),
                last_ejected_lin_tag=0,
                last_active_at=arrival.timestamp.s,
            )
            self._reordering[key] = state
        state.last_active_at = arrival.timestamp.s
        lin_tag = (tag - state.tag_baseline) & ((1 << 64) - 1)

        # Detect wraparound / very late messages.
        if lin_tag > ((1 << 63) - 1):
            _logger.debug("Reorder drop late tag=%d lin=%d", tag, lin_tag)
            return False
        if lin_tag <= state.last_ejected_lin_tag:
            _logger.debug("Reorder drop dup/late tag=%d lin=%d last=%d", tag, lin_tag, state.last_ejected_lin_tag)
            return False

        while state.interned and lin_tag > (state.last_ejected_lin_tag + REORDERING_CAPACITY):
            self._scan_reordering(state, force_first=True)

        expected = state.last_ejected_lin_tag + 1
        if lin_tag == expected:
            # In-order: eject immediately and scan for consecutive.
            self.queue.put_nowait(arrival)
            state.last_ejected_lin_tag = lin_tag
            self._scan_reordering(state, force_first=False)
            return True

        if lin_tag > (state.last_ejected_lin_tag + REORDERING_CAPACITY):
            state.tag_baseline = tag - (REORDERING_CAPACITY // 2)
            state.last_ejected_lin_tag = 0
            lin_tag = (tag - state.tag_baseline) & ((1 << 64) - 1)
            _logger.debug("Reorder resequence tag=%d lin=%d", tag, lin_tag)

        # Out-of-order but within capacity: intern.
        if lin_tag in state.interned:
            return True
        state.interned[lin_tag] = InternedMsg(arrival=arrival, tag=tag, remote_id=remote_id, lin_tag=lin_tag)
        self._rearm_reorder_timeout(state)
        return True

    def _scan_reordering(self, state: ReorderingState, force_first: bool) -> None:
        while True:
            if not state.interned:
                if state.timeout_handle is not None:
                    state.timeout_handle.cancel()
                    state.timeout_handle = None
                break

            lin_tag = min(state.interned)
            if force_first or ((state.last_ejected_lin_tag + 1) == lin_tag):
                force_first = False
                interned = state.interned.pop(lin_tag)
                self.queue.put_nowait(interned.arrival)
                state.last_ejected_lin_tag = lin_tag
                continue

            self._rearm_reorder_timeout(state)
            break

    def _force_eject_all(self, state: ReorderingState, *, silenced: bool = False) -> None:
        """Force-eject all interned messages in tag order."""
        while state.interned:
            lin_tag = min(state.interned)
            interned = state.interned.pop(lin_tag)
            state.last_ejected_lin_tag = lin_tag
            if not silenced:
                self.queue.put_nowait(interned.arrival)
        if state.timeout_handle is not None:
            state.timeout_handle.cancel()
            state.timeout_handle = None

    def _rearm_reorder_timeout(self, state: ReorderingState) -> None:
        """Arm or rearm the reordering timeout against the current head-of-line slot."""
        if self._reordering_window is None:
            return
        if not state.interned:
            if state.timeout_handle is not None:
                state.timeout_handle.cancel()
                state.timeout_handle = None
            return

        lin_tag = min(state.interned)
        delay = max(0.0, (state.interned[lin_tag].arrival.timestamp.s + self._reordering_window) - Instant.now().s)

        loop = self._node.loop
        if state.timeout_handle is not None:
            state.timeout_handle.cancel()

        def on_timeout() -> None:
            state.timeout_handle = None
            self._scan_reordering(state, force_first=True)

        state.timeout_handle = loop.call_later(delay, on_timeout)

    def _arm_reorder_timeout(self, state: ReorderingState) -> None:
        self._rearm_reorder_timeout(state)

    def _drop_stale_reordering(self, now: float) -> None:
        stale = [key for key, state in self._reordering.items() if (state.last_active_at + SESSION_LIFETIME) < now]
        for key in stale:
            state = self._reordering.pop(key)
            self._force_eject_all(state)

    def forget_topic_reordering(self, topic_hash: int, *, silenced: bool = True) -> None:
        keys = [key for key in self._reordering if key[1] == topic_hash]
        for key in keys:
            state = self._reordering.pop(key)
            self._force_eject_all(state, silenced=silenced)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for state in self._reordering.values():
            self._force_eject_all(state)
        self._reordering.clear()
        if self in self._root.subscribers:
            self._root.subscribers.remove(self)
        if not self._root.subscribers:
            if self._root.scout_task is not None:
                self._root.scout_task.cancel()
                self._root.scout_task = None
            if self._root.is_pattern:
                self._node.sub_roots_pattern.pop(self._root.name, None)
            else:
                self._node.sub_roots_verbatim.pop(self._root.name, None)
            for topic in list(self._node.topics_by_name.values()):
                self._node.decouple_topic_root(topic, self._root)
        self.queue.put_nowait(StopAsyncIteration())
        _logger.info("Subscriber closed for '%s'", self._pattern)


# =====================================================================================================================
# Breadcrumb
# =====================================================================================================================


class BreadcrumbImpl(Breadcrumb):
    def __init__(
        self,
        node: NodeImpl,
        remote_id: int,
        topic: TopicImpl,
        message_tag: int,
        initial_priority: Priority,
    ) -> None:
        self._node = node
        self._remote_id = remote_id
        self._topic = topic
        self._message_tag = message_tag
        self._priority = initial_priority
        self._seqno = 0

    @property
    def remote_id(self) -> int:
        return self._remote_id

    @property
    def topic(self) -> Topic:
        return self._topic

    @property
    def tag(self) -> int:
        return self._message_tag

    async def __call__(
        self,
        deadline: Instant,
        message: memoryview | bytes,
        *,
        reliable: bool = False,
    ) -> None:
        seqno = self._seqno & SEQNO48_MASK
        self._seqno += 1

        hdr: RspBeHeader | RspRelHeader
        if not reliable:
            hdr = RspBeHeader(
                tag=0xFF,
                seqno=seqno,
                topic_hash=self._topic.hash,
                message_tag=self._message_tag,
            )
        else:
            rsp_tag = self._allocate_response_tag(seqno)
            hdr = RspRelHeader(
                tag=rsp_tag,
                seqno=seqno,
                topic_hash=self._topic.hash,
                message_tag=self._message_tag,
            )

        data = hdr.serialize() + bytes(message)
        if not reliable:
            await self._node.transport.unicast(deadline, self._priority, self._remote_id, data)
            _logger.debug("Response BE sent seqno=%d to %016x", seqno, self._remote_id)
            return

        # Reliable response with retransmission.
        tracker = RespondTracker(
            remote_id=self._remote_id,
            message_tag=self._message_tag,
            topic_hash=self._topic.hash,
            seqno=seqno,
            tag=hdr.tag,
        )
        key = tracker.key
        self._node.respond_futures[key] = tracker

        ack_timeout = ACK_BASELINE_DEFAULT_TIMEOUT * (1 << int(self._priority))
        try:
            initial_window = _ack_window(deadline.ns, ack_timeout)
            if initial_window is None:
                raise DeliveryError("Reliable response not acknowledged before deadline")

            ack_deadline_ns, last_attempt = initial_window
            tracker.ack_event.clear()
            try:
                await self._node.transport.unicast(Instant(ns=ack_deadline_ns), self._priority, self._remote_id, data)
            except SendError:
                raise
            except OSError as ex:
                raise SendError("Reliable response initial send failed") from ex

            while True:
                if tracker.done:
                    if tracker.nacked:
                        raise NackError("Response NACK'd by remote")
                    return

                wait_until_ns = deadline.ns if last_attempt else ack_deadline_ns
                wait_time = max(0.0, (wait_until_ns - Instant.now().ns) * 1e-9)
                try:
                    await asyncio.wait_for(tracker.ack_event.wait(), timeout=wait_time)
                except asyncio.TimeoutError:
                    pass

                if tracker.done:
                    if tracker.nacked:
                        raise NackError("Response NACK'd by remote")
                    return

                if last_attempt:
                    break
                ack_timeout *= 2
                next_window = _ack_window(deadline.ns, ack_timeout)
                if next_window is None:
                    break
                ack_deadline_ns, last_attempt = next_window
                tracker.ack_event.clear()
                try:
                    await self._node.transport.unicast(
                        Instant(ns=ack_deadline_ns), self._priority, self._remote_id, data
                    )
                except (SendError, OSError):
                    pass

            if not tracker.done:
                raise DeliveryError("Reliable response not acknowledged before deadline")
        finally:
            self._node.respond_futures.pop(key, None)

    def _allocate_response_tag(self, seqno: int) -> int:
        for tag in range(256):
            key = (self._remote_id, self._message_tag, self._topic.hash, seqno, tag)
            if key not in self._node.respond_futures:
                return tag
        raise DeliveryError("Reliable response tag space exhausted")


class RespondTracker:
    """Tracks a pending reliable response awaiting ACK."""

    def __init__(self, remote_id: int, message_tag: int, topic_hash: int, seqno: int, tag: int) -> None:
        self.remote_id = remote_id
        self.message_tag = message_tag
        self.topic_hash = topic_hash
        self.seqno = seqno
        self.tag = tag
        self.key = (remote_id, message_tag, topic_hash, seqno, tag)
        self.ack_event = asyncio.Event()
        self.done = False
        self.nacked = False

    def on_ack(self, positive: bool) -> None:
        self.done = True
        self.nacked = not positive
        self.ack_event.set()


def _ack_is_last_attempt(current_ack_deadline_ns: int, current_ack_timeout: float, total_deadline_ns: int) -> bool:
    next_ack_timeout_ns = round(current_ack_timeout * 2 * 1e9)
    remaining_budget_ns = total_deadline_ns - current_ack_deadline_ns
    return remaining_budget_ns < next_ack_timeout_ns


def _ack_window(deadline_ns: int, ack_timeout: float) -> tuple[int, bool] | None:
    now_ns = Instant.now().ns
    if now_ns >= deadline_ns:
        return None
    ack_deadline_ns = min(deadline_ns, now_ns + round(ack_timeout * 1e9))
    return ack_deadline_ns, _ack_is_last_attempt(ack_deadline_ns, ack_timeout, deadline_ns)
