"""Unit tests for the TX completion primitives shared by the CAN backends."""

from __future__ import annotations

import asyncio
import random

import pytest

from pycyphal2 import ClosedError, Instant, SendError
from pycyphal2.can._tx import TxEntry, TxQueue, new_tx_job


def _deadline() -> Instant:
    return Instant.now() + 30.0


async def test_job_resolves_only_after_every_frame_is_handed_over() -> None:
    job = new_tx_job(3, _deadline())
    job.complete_one()
    job.complete_one()
    assert not job.settled
    job.complete_one()
    assert job.future.result() is None


async def test_empty_transfer_resolves_immediately() -> None:
    assert new_tx_job(0, _deadline()).future.result() is None


async def test_abort_is_idempotent_and_the_first_outcome_wins() -> None:
    job = new_tx_job(1, _deadline())
    job.abort(SendError("first"))
    job.abort(ClosedError("second"))
    assert job.settled
    with pytest.raises(SendError, match="first"):
        await job.future


async def test_completing_an_aborted_transfer_does_not_overwrite_the_failure() -> None:
    """The tail of an aborted transfer may still be in flight; its success must not mask the abort."""
    job = new_tx_job(1, _deadline())
    job.abort(SendError("purged"))
    job.complete_one()
    with pytest.raises(SendError, match="purged"):
        await job.future


async def test_entry_ordering_ignores_the_payload_and_the_job() -> None:
    """A TxJob is not comparable, so ordering must never reach it."""
    job_a, job_b = new_tx_job(1, _deadline()), new_tx_job(1, _deadline())
    assert TxEntry(1, 2, b"zzz", job_a) < TxEntry(2, 1, b"aaa", job_b)  # Lower ID wins arbitration.
    assert TxEntry(5, 1, b"zzz", job_a) < TxEntry(5, 2, b"aaa", job_b)  # Same ID: order within transfer.
    for job in (job_a, job_b):
        job.abort(SendError("unused"))


async def test_queue_preserves_arbitration_and_intra_transfer_order() -> None:
    queue = TxQueue()
    queue.push(0x20, [b"lo1", b"lo2"], _deadline())
    queue.push(0x10, [b"hi"], _deadline())

    first = await queue.pop()
    assert (first.id, first.payload) == (0x10, b"hi")  # Higher priority overtakes.
    second = await queue.pop()
    third = await queue.pop()
    assert [e.payload for e in (second, third)] == [b"lo1", b"lo2"]


async def test_abort_all_fails_the_queued_and_the_in_flight_transfers() -> None:
    queue = TxQueue()
    queued = queue.push(0x20, [b"a"], _deadline())
    inflight = queue.push(0x10, [b"b"], _deadline())

    entry = await queue.pop()  # Off the queue, reachable only as the in-flight transfer.
    assert entry.payload == b"b"
    assert queue.qsize() == 1

    dropped = queue.abort_all(lambda: ClosedError("closed"))
    assert dropped == 1  # Only the still-queued frame is counted.
    for fut in (queued, inflight):
        with pytest.raises(ClosedError):
            await fut


async def test_abort_all_builds_exactly_one_fresh_error_per_transfer() -> None:
    """Sharing one exception instance across transfers would accumulate tracebacks on it."""
    queue = TxQueue()
    a = queue.push(0x10, [b"a"], _deadline())
    b = queue.push(0x20, [b"b1", b"b2", b"b3"], _deadline())  # One transfer, three frames.

    built = 0

    def make_error() -> SendError:
        nonlocal built
        built += 1
        return SendError("purged")

    dropped = queue.abort_all(make_error)
    assert dropped == 4  # Frames dropped...
    assert built == 2  # ...but one error per transfer.

    errors = [fut.exception() for fut in (a, b)]
    assert all(isinstance(e, SendError) for e in errors)
    assert errors[0] is not errors[1]


async def test_expiry_between_frames_leaves_the_head_on_the_wire_but_never_the_tail() -> None:
    """Frames already handed to the media cannot be recalled; only the remaining ones are suppressed."""
    queue = TxQueue()
    future = queue.push(0x10, [b"f1", b"f2", b"f3"], Instant.now() + 0.03)
    emitted = []

    head = await queue.pop()
    emitted.append(head.payload)  # Reaches the media before the deadline.
    head.job.complete_one()

    await asyncio.sleep(0.05)  # The deadline expires mid-transfer.
    for _ in range(2):
        entry = await queue.pop()
        if entry.job.settled:
            continue
        if Instant.now().ns >= entry.job.deadline.ns:
            entry.job.abort(SendError("expired"))
            continue
        emitted.append(entry.payload)

    assert emitted == [b"f1"]
    with pytest.raises(SendError):
        await future


async def test_requeue_keeps_the_transfer_owed() -> None:
    queue = TxQueue()
    future = queue.push(0x10, [b"a"], _deadline())
    entry = await queue.pop()
    queue.requeue(entry)  # Transient media failure: the frame is still owed.
    assert not future.done()
    assert queue.qsize() == 1

    queue.abort_all(lambda: ClosedError("closed"))
    with pytest.raises(ClosedError):
        await future


async def test_abort_all_after_a_transfer_completed_is_a_no_op() -> None:
    """The last popped transfer stays referenced after it is sent; aborting it must not undo its success."""
    queue = TxQueue()
    future = queue.push(0x10, [b"a"], _deadline())
    entry = await queue.pop()
    entry.job.complete_one()
    assert future.result() is None

    assert queue.abort_all(lambda: ClosedError("closed")) == 0
    assert future.result() is None


async def test_discarded_failed_future_is_not_reported_as_unretrieved(caplog: pytest.LogCaptureFixture) -> None:
    queue = TxQueue()
    queue.push(0x10, [b"a"], _deadline())  # Nobody keeps the future.
    with caplog.at_level("ERROR", logger="asyncio"):
        queue.abort_all(lambda: SendError("purged"))
        await asyncio.sleep(0)
    assert not [r for r in caplog.records if "never retrieved" in r.getMessage()]


async def test_cancelled_future_does_not_break_the_retrieval_callback() -> None:
    job = new_tx_job(1, _deadline())
    job.future.cancel()  # The done-callback must tolerate cancellation.
    await asyncio.sleep(0)
    with pytest.raises(asyncio.CancelledError):
        await job.future


async def test_cancelling_the_future_does_not_settle_the_transfer() -> None:
    """The future is an observation channel, not the transfer's state."""
    job = new_tx_job(3, _deadline())
    job.future.cancel()
    await asyncio.sleep(0)
    assert not job.settled

    job.complete_one()
    job.complete_one()
    assert not job.settled
    job.complete_one()
    assert job.settled  # By transmission, not by the observer walking away.


async def test_a_lapsed_observer_never_tears_a_multi_frame_transfer() -> None:
    queue = TxQueue()
    future = queue.push(0x10, [b"f1", b"f2", b"f3"], _deadline())
    emitted = []

    entry = await queue.pop()
    emitted.append(entry.payload)
    entry.job.complete_one()

    with pytest.raises(asyncio.TimeoutError):  # wait_for cancels the future.
        await asyncio.wait_for(future, timeout=0.01)

    for _ in range(2):  # The TX loop keeps draining.
        entry = await queue.pop()
        if entry.job.settled:
            continue
        emitted.append(entry.payload)
        entry.job.complete_one()

    assert emitted == [b"f1", b"f2", b"f3"]


async def test_randomised_interleavings_preserve_the_queue_invariants() -> None:
    """
    Model the backend TX loop, including the two states that are easy to get wrong: a frame mid-transmission
    while purge()/close() runs, and an observer that walks away by cancelling its future. Both guards this
    exercises are load-bearing: dropping abort_all()'s in-flight abort strands a transfer, and dropping
    _settle()'s future.done() check raises InvalidStateError inside the TX loop.
    """
    for seed in range(500):
        rng = random.Random(seed)
        queue = TxQueue()
        futures: dict[int, asyncio.Future[None]] = {}
        emitted: dict[int, list[bytes]] = {}
        expected: dict[int, list[bytes]] = {}
        succeeded: set[int] = set()

        for t in range(rng.randint(1, 4)):
            chunks = [bytes([t, i]) for i in range(rng.randint(1, 4))]
            expected[t], emitted[t] = chunks, []
            futures[t] = queue.push(0x100 + t, chunks, _deadline())  # The CAN-ID encodes the transfer index.

        held: TxEntry | None = None  # The frame the TX loop has popped and is transmitting.
        for _ in range(rng.randint(1, 40)):
            choices = ["purge", "abandon"]
            choices += ["pop"] * 4 if held is None and queue.qsize() else []
            choices += ["send"] * 4 + ["requeue"] if held is not None else []
            op = rng.choice(choices)

            if op == "purge":  # May land while `held` is mid-transmission.
                queue.abort_all(lambda: SendError("purged"))
            elif op == "abandon":  # An observer gives up.
                futures[rng.randrange(len(futures))].cancel()
            elif op == "pop":
                held = await queue.pop()
                if held.job.settled:
                    held = None  # The loop skips it; the frame is never emitted.
            elif op == "requeue":
                assert held is not None
                queue.requeue(held)
                held = None
            else:
                assert held is not None
                index = held.id - 0x100
                was_failed = held.job.settled  # A purge may have landed while this frame was in flight.
                emitted[index].append(held.payload)
                held.job.complete_one()  # Must tolerate an already-failed or abandoned transfer.
                if was_failed:
                    assert held.job.aborted
                    resurrected = held.job.future.done() and not held.job.future.cancelled()
                    assert not (resurrected and held.job.future.exception() is None), "a failure became a success"
                elif held.job.settled:
                    succeeded.add(index)
                held = None

        queue.abort_all(lambda: ClosedError("closed"))
        for index, fut in futures.items():
            assert fut.done(), f"transfer {index} left pending after abort_all"
            if index in succeeded and not fut.cancelled():
                assert fut.exception() is None, f"transfer {index} succeeded, then was failed"
            if not fut.cancelled():
                fut.exception()
            assert emitted[index] == expected[index][: len(emitted[index])], "frames emitted out of order"
