"""
Per-transfer TX completion tracking shared by the CAN backends.

Awaiting the future is observational: discarding or cancelling it never removes frames from the queue.
A frame's deadline is checked when it is popped, so higher-priority traffic can starve a transfer past its
deadline, as bus arbitration would; the caller is bounded by its own deadline in send_transfer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from .._api import Instant


@dataclass(eq=False)
class TxJob:
    """
    The frames of one transfer queued on one interface. The transfer state is independent of the future so
    that an observer cancelling it (as ``asyncio.wait_for`` does) cannot suppress the tail of the transfer.
    """

    future: asyncio.Future[None]
    remaining: int
    deadline: Instant
    aborted: bool = False

    @property
    def settled(self) -> bool:
        return self.aborted or self.remaining <= 0

    def abort(self, ex: BaseException) -> None:
        if self.settled:
            return
        self.aborted = True
        self._settle(ex)

    def complete_one(self) -> None:
        self.remaining -= 1
        # An abort landing while the last frame is in flight must not be overwritten by its success.
        if self.remaining <= 0 and not self.aborted:
            self._settle(None)

    def _settle(self, ex: BaseException | None) -> None:
        if self.future.done():
            return
        try:
            self.future.set_exception(ex) if ex is not None else self.future.set_result(None)
        except RuntimeError:  # pragma: no cover -- the loop is gone, so nobody can be awaiting.
            pass


@dataclass(order=True)
class TxEntry:
    # `seq` is unique, so ordering never reaches the payload or the non-comparable job; `compare=False`
    # makes that hold by construction rather than by accident.
    id: int
    seq: int
    payload: bytes = field(compare=False)
    job: TxJob = field(compare=False)


class TxQueue:
    """
    Priority-ordered frames plus the transfer of the last popped frame. That transfer is tracked because its
    frame has already left the queue; without it, :meth:`abort_all` would strand its awaiter. It is never
    cleared: :meth:`TxJob.abort` is idempotent and the next :meth:`pop` replaces it.
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[TxEntry] = asyncio.PriorityQueue()
        self._seq = 0
        self._inflight: TxJob | None = None

    def qsize(self) -> int:
        return self._queue.qsize()

    def push(self, id: int, chunks: Iterable[bytes], deadline: Instant) -> asyncio.Future[None]:
        chunks = tuple(chunks)
        job = new_tx_job(len(chunks), deadline)
        for chunk in chunks:
            self._seq += 1
            self._queue.put_nowait(TxEntry(id, self._seq, chunk, job))
        return job.future

    async def pop(self) -> TxEntry:
        entry = await self._queue.get()
        self._inflight = entry.job  # No await here, so the frame is never reachable from neither.
        return entry

    def requeue(self, entry: TxEntry) -> None:
        self._queue.put_nowait(entry)

    def abort_all(self, make_error: Callable[[], BaseException]) -> int:
        """Fail every pending transfer; return the number of frames dropped. A fresh error per transfer."""
        dropped = 0
        while True:
            try:
                entry = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not entry.job.settled:
                entry.job.abort(make_error())
            dropped += 1
        if self._inflight is not None and not self._inflight.settled:
            self._inflight.abort(make_error())
        return dropped


def new_tx_job(frame_count: int, deadline: Instant) -> TxJob:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    # Redundant transmission discards the losing interfaces' futures; retrieve their exceptions so that
    # asyncio does not log them as never retrieved.
    future.add_done_callback(_retrieve_exception)
    job = TxJob(future=future, remaining=frame_count, deadline=deadline)
    if frame_count <= 0:  # Otherwise the awaiter would never be resumed.
        future.set_result(None)
    return job


def _retrieve_exception(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()
