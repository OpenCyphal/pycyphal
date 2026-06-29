#!/usr/bin/env python3
"""
Serve file chunks over a tiny Cyphal/UDP RPC.
Usage:
    python examples/file_server.py
"""

from __future__ import annotations

import asyncio
import errno
import logging
import struct
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from pycyphal2 import Arrival, DeliveryError, NackError, Node, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
TOPIC = "file/read"
PATH_MAX_LEN = 2048
DATA_MAX = 4096
RESPONSE_DEADLINE = 10.0
REQUEST_HEADER_FORMAT = "<QH"
REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_HEADER_FORMAT)
RESPONSE_HEADER_FORMAT = "<IH"

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileReadRequest:
    read_offset: int
    file_path: str


@dataclass(frozen=True)
class FileReadResponse:
    error: int
    data: bytes


def _decode_request(payload: bytes) -> FileReadRequest | None:
    if len(payload) < REQUEST_HEADER_SIZE:
        return None
    read_offset, path_len = struct.unpack_from(REQUEST_HEADER_FORMAT, payload)
    if path_len == 0 or path_len > PATH_MAX_LEN:
        return None
    path_end = REQUEST_HEADER_SIZE + path_len
    if len(payload) != path_end:
        return None
    try:
        file_path = payload[REQUEST_HEADER_SIZE:path_end].decode("utf8")
    except UnicodeDecodeError:
        return None
    return FileReadRequest(read_offset=read_offset, file_path=file_path)


def _encode_response(response: FileReadResponse) -> bytes:
    if len(response.data) > DATA_MAX:
        raise ValueError(f"Response data is too large: {len(response.data)}")
    return struct.pack(RESPONSE_HEADER_FORMAT, response.error, len(response.data)) + response.data


def _errno_from_exception(ex: BaseException) -> int:
    if isinstance(ex, OSError) and ex.errno is not None:
        return ex.errno
    if isinstance(ex, OverflowError):
        return getattr(errno, "EOVERFLOW", errno.EINVAL)
    return errno.EINVAL


def _read_chunk(file_path: str, offset: int) -> FileReadResponse:
    try:
        with open(file_path, "rb") as file:
            file.seek(offset)
            data = file.read(DATA_MAX)
    except (OSError, ValueError, OverflowError) as ex:
        return FileReadResponse(error=_errno_from_exception(ex), data=b"")
    return FileReadResponse(error=0, data=data)


async def _serve_request(arrival: Arrival, request: FileReadRequest) -> None:
    response = _read_chunk(request.file_path, request.read_offset)
    payload = _encode_response(response)
    _logger.info(
        "responding: file=%r offset=%d size=%d error=%d",
        request.file_path,
        request.read_offset,
        len(response.data),
        response.error,
    )
    try:
        await arrival.breadcrumb(arrival.timestamp + RESPONSE_DEADLINE, payload, reliable=True)
    except NackError:
        _logger.info("client rejected response: remote=%016x file=%r", arrival.breadcrumb.remote_id, request.file_path)
    except DeliveryError:
        _logger.info(
            "client did not acknowledge: remote=%016x file=%r", arrival.breadcrumb.remote_id, request.file_path
        )
    except SendError as ex:
        _logger.warning("response send failed: remote=%016x error=%s", arrival.breadcrumb.remote_id, ex)


def _on_task_done(tasks: set[asyncio.Task[None]], task: asyncio.Task[None]) -> None:
    tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _logger.error("file request task failed: %s", exc)


async def run() -> None:
    transport = UDPTransport.new()
    node = Node.new(transport, NAME)
    sub = node.subscribe(TOPIC)
    tasks: set[asyncio.Task[None]] = set()
    _logger.info("file server ready on %r via %s", TOPIC, transport)
    try:
        async for arrival in sub:
            request = _decode_request(arrival.message)
            if request is None:
                _logger.debug("dropping malformed request of size %d", len(arrival.message))
                continue
            task = asyncio.create_task(_serve_request(arrival, request), name=f"file:{arrival.breadcrumb.tag}")
            tasks.add(task)
            task.add_done_callback(partial(_on_task_done, tasks))
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
