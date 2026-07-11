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
import sys
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from examples._file_types import FileReadRequest, FileReadResponse

from pycyphal2 import Arrival, DeliveryError, NackError, Node, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
TOPIC = "file/read"
RESPONSE_DEADLINE = 10.0

_logger = logging.getLogger(__name__)


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
            data = file.read(FileReadResponse.data_capacity())
    except (OSError, ValueError, OverflowError) as ex:
        return FileReadResponse(_errno_from_exception(ex), b"")
    return FileReadResponse(0, data)


async def _serve_request(arrival: Arrival, request: FileReadRequest) -> None:
    response = _read_chunk(request.file_path, request.read_offset)
    payload = response.serialize()
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


async def run() -> None:
    transport = UDPTransport.new()
    node = Node.new(transport, NAME)
    sub = node.subscribe(TOPIC)
    _logger.info("file server ready on %r via %s", TOPIC, transport)
    try:
        async for arrival in sub:
            request = FileReadRequest.deserialize(arrival.message)
            if request is None:
                _logger.debug("dropping malformed request of size %d", len(arrival.message))
                continue
            await _serve_request(arrival, request)
    finally:
        sub.close()
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
