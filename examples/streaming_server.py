#!/usr/bin/env python3
"""
Serve a tiny streaming RPC over Cyphal/UDP.
Usage:
    python examples/streaming_server.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

from pycyphal2 import Arrival, DeliveryError, Instant, NackError, Node, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
PERIOD_MIN = 0.1
RESPONSE_DEADLINE = 2.0


def _decode_request(payload: bytes) -> tuple[int, int] | None:
    try:
        obj = json.loads(payload.decode("utf8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(obj, dict):
        return None
    count = obj.get("count")
    period = obj.get("period")
    if type(count) is not int or type(period) is not float:
        return None
    if count <= 0:
        return None
    return count, max(period, PERIOD_MIN)


def _make_stream_id(arrival: Arrival) -> str:
    breadcrumb = arrival.breadcrumb
    return f"{breadcrumb.remote_id:016x}:{breadcrumb.topic.hash:016x}:{breadcrumb.tag:016x}"


async def _serve_stream(arrival: Arrival, count: int, period: float) -> None:
    stream_id = _make_stream_id(arrival)
    logging.info(
        "new stream: id=%s remote=%016x count=%d period=%f",
        stream_id,
        arrival.breadcrumb.remote_id,
        count,
        period,
    )
    for index in range(count):
        remaining = count - index - 1
        payload = json.dumps(
            {
                "stream_id": stream_id,
                "requested_count": count,
                "period": period,
                "remaining": remaining,
                "sent_at": round(time.time(), 6),
            }
        ).encode("utf8")
        try:
            await arrival.breadcrumb(Instant.now() + RESPONSE_DEADLINE, payload, reliable=True)
        except NackError:
            logging.info("client closed stream: id=%s sent=%d requested=%d", stream_id, index, count)
            return
        except DeliveryError:
            logging.info("client unreachable: id=%s sent=%d requested=%d", stream_id, index, count)
            return
        except SendError as ex:
            logging.warning("stream send failed: id=%s error=%s", stream_id, ex)
            return
        if remaining > 0:
            await asyncio.sleep(period)
    logging.info("stream completed: id=%s count=%d", stream_id, count)


def _on_stream_task_done(tasks: set[asyncio.Task[None]], task: asyncio.Task[None]) -> None:
    tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logging.error("stream task failed: %s", exc)


async def run() -> None:
    transport = UDPTransport.new()
    node = Node.new(transport, NAME)
    sub = node.subscribe("demo/stream")
    tasks: set[asyncio.Task[None]] = set()
    logging.info("streaming server ready via %s", transport)
    try:
        async for arrival in sub:
            request = _decode_request(arrival.message)
            if request is None:
                logging.warning("dropping malformed request from %016x", arrival.breadcrumb.remote_id)
                continue
            count, period = request
            task = asyncio.create_task(_serve_stream(arrival, count, period), name=f"stream:{_make_stream_id(arrival)}")
            tasks.add(task)
            task.add_done_callback(lambda t, task_set=tasks: _on_stream_task_done(task_set, t))
    finally:
        sub.close()
        for task in list(tasks):
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        node.close()
        transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
