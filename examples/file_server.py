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
import os
from pathlib import Path

from _file_types import FileReadError, FileReadRequest, FileReadResponse

from pycyphal2 import Arrival, DeliveryError, NackError, Node, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
TOPIC = "file/read"
RESPONSE_DEADLINE = 10.0
_SEEK_MAX = (1 << 47) - 1

_logger = logging.getLogger(__name__)


def _error_from_exception(ex: BaseException) -> FileReadError:
    if isinstance(ex, FileNotFoundError):
        return FileReadError.ERROR_EXISTENCE
    if isinstance(ex, IsADirectoryError):
        return FileReadError.ERROR_KIND
    if isinstance(ex, PermissionError):
        return FileReadError.ERROR_PERMISSION
    if isinstance(ex, (ValueError, OverflowError)):
        return FileReadError.ERROR_SEEK
    if isinstance(ex, OSError):
        if ex.errno in (errno.EISDIR, errno.ENOTDIR):
            return FileReadError.ERROR_KIND
        if ex.errno in (errno.EINVAL, errno.ESPIPE):
            return FileReadError.ERROR_SEEK
    return FileReadError.ERROR_RUNTIME


def _read_chunk(file_path: str, seek: int, size: int) -> FileReadResponse:
    try:
        if os.path.isdir(file_path):
            return FileReadResponse(0, FileReadError.ERROR_KIND, False, b"")
        with open(file_path, "rb") as file:
            if seek < 0:
                file.seek(seek + 1, os.SEEK_END)
            else:
                file.seek(seek)
            resolved_seek = file.tell()
            if resolved_seek > _SEEK_MAX:
                return FileReadResponse(0, FileReadError.ERROR_CAPACITY, False, b"")
            data = file.read(size)
            file.seek(0, os.SEEK_END)
            end = file.tell() <= resolved_seek + len(data)
    except (OSError, ValueError, OverflowError) as ex:
        return FileReadResponse(0, _error_from_exception(ex), False, b"")
    return FileReadResponse(resolved_seek, FileReadError.ERROR_OK, end, data)


async def _serve_request(arrival: Arrival, request: FileReadRequest) -> None:
    response = _read_chunk(request.path, request.seek, request.size)
    payload = response.serialize()
    _logger.info(
        "responding: file=%r seek=%d size=%d error=%d",
        request.path,
        response.seek,
        len(response.data),
        response.error,
    )
    try:
        await arrival.breadcrumb(arrival.timestamp + RESPONSE_DEADLINE, payload, reliable=True)
    except NackError:
        _logger.info("client rejected response: remote=%016x file=%r", arrival.breadcrumb.remote_id, request.path)
    except DeliveryError:
        _logger.info("client did not acknowledge: remote=%016x file=%r", arrival.breadcrumb.remote_id, request.path)
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
