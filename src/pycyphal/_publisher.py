from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass

from ._api import DeliveryError, Instant, LivenessError, Priority, SendError
from ._api import Publisher, Topic, ResponseStream, Response
from ._header import MsgBeHeader, MsgRelHeader, RspBeHeader, RspRelHeader
from ._node import ACK_BASELINE_DEFAULT_TIMEOUT, NodeImpl, PublishTracker, SESSION_LIFETIME, TopicImpl
from ._transport import TransportArrival

_logger = logging.getLogger(__name__)

REQUEST_FUTURE_HISTORY = 192
REQUEST_FUTURE_HISTORY_MASK = (1 << REQUEST_FUTURE_HISTORY) - 1
ACK_TIMEOUT_MIN = 1e-6


@dataclass
class ResponseRemoteState:
    seqno_top: int
    seqno_acked: int = 1

    def accept(self, seqno: int) -> tuple[bool, bool]:
        if seqno > self.seqno_top:
            shift = seqno - self.seqno_top
            self.seqno_acked = (
                1
                if shift >= REQUEST_FUTURE_HISTORY
                else (((self.seqno_acked << shift) & REQUEST_FUTURE_HISTORY_MASK) | 1)
            )
            self.seqno_top = seqno
            return True, True
        dist = self.seqno_top - seqno
        if dist >= REQUEST_FUTURE_HISTORY:
            return False, False
        mask = 1 << dist
        if self.seqno_acked & mask:
            return True, False
        self.seqno_acked |= mask
        return True, True

    def accepted_earlier(self, seqno: int) -> bool:
        if seqno > self.seqno_top:
            return False
        dist = self.seqno_top - seqno
        return dist < REQUEST_FUTURE_HISTORY and bool(self.seqno_acked & (1 << dist))


class PublisherImpl(Publisher):
    def __init__(self, node: NodeImpl, topic: TopicImpl) -> None:
        self._node = node
        self._topic = topic
        self._priority = Priority.NOMINAL
        self._ack_timeout_baseline = ACK_BASELINE_DEFAULT_TIMEOUT
        self.closed = False

    @property
    def topic(self) -> Topic:
        return self._topic

    @property
    def priority(self) -> Priority:
        return self._priority

    @priority.setter
    def priority(self, priority: Priority) -> None:
        self._priority = priority

    @property
    def ack_timeout(self) -> float:
        return self._ack_timeout_baseline * (1 << int(self._priority))

    @ack_timeout.setter
    def ack_timeout(self, duration: float) -> None:
        duration = float(duration)
        if duration < ACK_TIMEOUT_MIN or not math.isfinite(duration):
            raise ValueError("ACK timeout must be a positive finite duration")
        if duration > SESSION_LIFETIME:
            raise ValueError(f"ACK timeout must be less than session lifetime")
        self._ack_timeout_baseline = duration / (1 << int(self._priority))

    async def __call__(
        self,
        deadline: Instant,
        message: memoryview | bytes,
        *,
        reliable: bool = False,
    ) -> None:
        if self.closed:
            raise SendError("Publisher closed")

        tag = self._topic.next_tag()
        payload = bytes(message)

        if not reliable:
            writer = self._topic.ensure_writer()
            await writer(deadline, self._priority, self._serialize_message(tag, payload, reliable=False))
            _logger.debug("Published BE tag=%d topic='%s'", tag, self._topic.name)
            return

        await self._reliable_publish(deadline, tag, payload)

    async def request(
        self,
        delivery_deadline: Instant,
        response_timeout: float,
        message: memoryview | bytes,
    ) -> ResponseStream:
        if self.closed:
            raise SendError("Publisher closed")

        tag = self._topic.next_tag()
        payload = bytes(message)

        # Create response stream before publishing so it's ready to receive.
        stream = ResponseStreamImpl(
            node=self._node,
            topic=self._topic,
            message_tag=tag,
            response_timeout=response_timeout,
        )
        self._topic.request_futures[tag] = stream

        tracker = self._prepare_reliable_publish_tracker(tag, delivery_deadline.ns, payload)
        try:
            initial_window = await self._reliable_publish_start(delivery_deadline, tag, payload, tracker)
        except asyncio.CancelledError:
            tracker.compromised = True
            self._topic.request_futures.pop(tag, None)
            self._release_reliable_publish_tracker(tag, tracker)
            raise
        except BaseException:
            self._topic.request_futures.pop(tag, None)
            self._release_reliable_publish_tracker(tag, tracker)
            raise

        task = self._node.loop.create_task(
            self._request_publish(delivery_deadline, tag, payload, stream, tracker, initial_window)
        )

        def on_done(done_task: asyncio.Task[None]) -> None:
            if done_task.cancelled() and self._topic.publish_futures.get(tag) is tracker:
                tracker.compromised = True
                self._release_reliable_publish_tracker(tag, tracker)

        task.add_done_callback(on_done)
        stream.set_publish_task(task)
        return stream

    async def _request_publish(
        self,
        deadline: Instant,
        tag: int,
        payload: bytes,
        stream: ResponseStreamImpl,
        tracker: PublishTracker,
        initial_window: tuple[int, bool],
    ) -> None:
        try:
            await self._reliable_publish_continue(deadline, tag, payload, tracker, initial_window)
        except asyncio.CancelledError:
            tracker.compromised = True
            raise
        except BaseException as ex:
            stream.on_publish_error(ex)
        finally:
            self._release_reliable_publish_tracker(tag, tracker)

    @staticmethod
    def _ack_is_last_attempt(current_ack_deadline_ns: int, current_ack_timeout: float, total_deadline_ns: int) -> bool:
        next_ack_timeout_ns = round(current_ack_timeout * 2 * 1e9)
        remaining_budget_ns = total_deadline_ns - current_ack_deadline_ns
        return remaining_budget_ns < next_ack_timeout_ns

    @staticmethod
    def _ack_window_is_compromised(deadline_ns: int, current_ack_timeout: float) -> bool:
        return Instant.now().ns >= (deadline_ns - round(current_ack_timeout * 1e9))

    def _serialize_message(self, tag: int, payload: bytes, *, reliable: bool) -> bytes:
        lage = self._topic.lage(Instant.now().s)
        hdr = (MsgRelHeader if reliable else MsgBeHeader)(
            topic_log_age=lage,
            topic_evictions=self._topic.evictions,
            topic_hash=self._topic.hash,
            tag=tag,
        )
        return hdr.serialize() + payload

    @staticmethod
    def _reliable_publish_window(deadline_ns: int, ack_timeout: float) -> tuple[int, bool] | None:
        now_ns = Instant.now().ns
        if now_ns >= deadline_ns:
            return None
        ack_deadline_ns = min(deadline_ns, now_ns + round(ack_timeout * 1e9))
        return ack_deadline_ns, PublisherImpl._ack_is_last_attempt(ack_deadline_ns, ack_timeout, deadline_ns)

    def _prepare_reliable_publish_tracker(self, tag: int, deadline_ns: int, payload: bytes) -> PublishTracker:
        tracker = self._node.prepare_publish_tracker(self._topic, tag, deadline_ns, payload)
        tracker.ack_timeout = self.ack_timeout
        self._topic.publish_futures[tag] = tracker
        return tracker

    def _release_reliable_publish_tracker(self, tag: int, tracker: PublishTracker) -> None:
        self._topic.publish_futures.pop(tag, None)
        self._node.publish_tracker_release(self._topic, tracker)

    async def _send_reliable_publish(
        self,
        deadline: Instant,
        tag: int,
        payload: bytes,
        tracker: PublishTracker,
        *,
        first_attempt: bool,
    ) -> None:
        data = self._serialize_message(tag, payload, reliable=True)
        if (not first_attempt) and (len(tracker.remaining) == 1):
            remote_id = next(iter(tracker.remaining))
            await self._node.transport.unicast(deadline, self._priority, remote_id, data)
        else:
            writer = self._topic.ensure_writer()
            await writer(deadline, self._priority, data)

    async def _reliable_publish_start(
        self,
        deadline: Instant,
        tag: int,
        payload: bytes,
        tracker: PublishTracker,
    ) -> tuple[int, bool]:
        initial_window = self._reliable_publish_window(deadline.ns, tracker.ack_timeout)
        if initial_window is None:
            raise DeliveryError("Reliable publish not acknowledged before deadline")
        ack_deadline_ns, _ = initial_window
        tracker.ack_event.clear()
        try:
            await self._send_reliable_publish(Instant(ns=ack_deadline_ns), tag, payload, tracker, first_attempt=True)
        except SendError:
            tracker.compromised = True
            raise
        except OSError as ex:
            tracker.compromised = True
            raise SendError("Reliable publish initial send failed") from ex
        return initial_window

    async def _reliable_publish_continue(
        self,
        deadline: Instant,
        tag: int,
        payload: bytes,
        tracker: PublishTracker,
        initial_window: tuple[int, bool],
    ) -> None:
        ack_deadline_ns, last_attempt = initial_window
        while True:
            if tracker.acknowledged and not tracker.remaining:
                _logger.debug("Reliable publish ACKed tag=%d topic='%s'", tag, self._topic.name)
                return

            wait_until_ns = deadline.ns if last_attempt else ack_deadline_ns
            wait_timeout = max(0.0, (wait_until_ns - Instant.now().ns) * 1e-9)
            if wait_timeout > 0:
                try:
                    await asyncio.wait_for(tracker.ack_event.wait(), timeout=wait_timeout)
                except asyncio.TimeoutError:
                    pass

            if (not last_attempt) and self._ack_window_is_compromised(deadline.ns, tracker.ack_timeout):
                tracker.compromised = True

            if tracker.acknowledged and not tracker.remaining:
                _logger.debug("Reliable publish ACKed tag=%d topic='%s'", tag, self._topic.name)
                return
            if last_attempt:
                break
            tracker.ack_timeout *= 2
            next_window = self._reliable_publish_window(deadline.ns, tracker.ack_timeout)
            if next_window is None:
                break
            ack_deadline_ns, last_attempt = next_window
            tracker.ack_event.clear()
            try:
                await self._send_reliable_publish(
                    Instant(ns=ack_deadline_ns), tag, payload, tracker, first_attempt=False
                )
            except (SendError, OSError):
                tracker.compromised = True

        raise DeliveryError("Reliable publish not acknowledged before deadline")

    async def _reliable_publish(self, deadline: Instant, tag: int, payload: bytes) -> None:
        tracker = self._prepare_reliable_publish_tracker(tag, deadline.ns, payload)
        try:
            initial_window = await self._reliable_publish_start(deadline, tag, payload, tracker)
            await self._reliable_publish_continue(deadline, tag, payload, tracker, initial_window)
        except asyncio.CancelledError:
            tracker.compromised = True
            raise
        finally:
            self._release_reliable_publish_tracker(tag, tracker)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._topic.pub_count -= 1
        self._topic.sync_implicit()
        _logger.info("Publisher closed for '%s'", self._topic.name)


# =====================================================================================================================
# Response Stream
# =====================================================================================================================


class ResponseStreamImpl(ResponseStream):
    def __init__(
        self,
        node: NodeImpl,
        topic: TopicImpl,
        message_tag: int,
        response_timeout: float,
    ) -> None:
        self._node = node
        self._topic = topic
        self._message_tag = message_tag
        self._response_timeout = response_timeout
        self.queue: asyncio.Queue[Response | BaseException] = asyncio.Queue()
        self.closed = False
        self._reliable_remote_by_id: dict[int, ResponseRemoteState] = {}
        self._publish_task: asyncio.Task[None] | None = None
        self._cleanup_handle: asyncio.TimerHandle | None = None

    def __aiter__(self) -> ResponseStreamImpl:
        return self

    async def __anext__(self) -> Response:
        if self.closed:
            raise StopAsyncIteration
        try:
            item = await asyncio.wait_for(self.queue.get(), timeout=self._response_timeout)
        except asyncio.TimeoutError:
            raise LivenessError("Response timeout")
        if isinstance(item, StopAsyncIteration):
            raise item
        if isinstance(item, BaseException):
            raise item
        return item

    def set_publish_task(self, task: asyncio.Task[None]) -> None:
        self._publish_task = task

    def on_publish_error(self, ex: BaseException) -> None:
        if self.closed or isinstance(ex, asyncio.CancelledError):
            return
        self.queue.put_nowait(ex)

    def _remove_from_topic(self) -> None:
        if self._cleanup_handle is not None:
            self._cleanup_handle.cancel()
            self._cleanup_handle = None
        if self._topic.request_futures.get(self._message_tag) is self:
            del self._topic.request_futures[self._message_tag]

    def _schedule_cleanup(self) -> None:
        if self._cleanup_handle is not None:
            return

        def cleanup() -> None:
            self._cleanup_handle = None
            self._remove_from_topic()

        self._cleanup_handle = self._node.loop.call_later(SESSION_LIFETIME / 2, cleanup)

    def on_response(
        self,
        arrival: TransportArrival,
        hdr: RspBeHeader | RspRelHeader,
        payload: bytes,
    ) -> bool:
        """Called by the node when a response arrives matching our message_tag."""
        reliable = isinstance(hdr, RspRelHeader)
        if self.closed:
            if not reliable:
                return False
            remote = self._reliable_remote_by_id.get(arrival.remote_id)
            return (remote is not None) and remote.accepted_earlier(hdr.seqno)

        if reliable:
            remote = self._reliable_remote_by_id.get(arrival.remote_id)
            if remote is None:
                remote = ResponseRemoteState(seqno_top=hdr.seqno)
                self._reliable_remote_by_id[arrival.remote_id] = remote
                unique = True
            else:
                accepted, unique = remote.accept(hdr.seqno)
                if not accepted:
                    return False
            if not unique:
                _logger.debug("RSP dedup drop remote=%016x seqno=%d", arrival.remote_id, hdr.seqno)
                return True

        response = Response(
            timestamp=arrival.timestamp,
            remote_id=arrival.remote_id,
            seqno=hdr.seqno,
            message=payload,
        )
        self.queue.put_nowait(response)
        return True

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._publish_task is not None:
            self._publish_task.cancel()
            self._publish_task = None
        if self._reliable_remote_by_id:
            self._schedule_cleanup()
        else:
            self._remove_from_topic()
        self.queue.put_nowait(StopAsyncIteration())
        _logger.debug("Response stream closed for tag=%d", self._message_tag)
