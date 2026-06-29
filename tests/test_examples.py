from __future__ import annotations

import asyncio
import errno
import struct

import pytest

from examples.file_client import _decode_response, _format_remote_error, _receive_valid_response
from examples.file_server import _decode_request, _read_chunk
from pycyphal2 import Instant, LivenessError, Response, ResponseStream


def test_file_example_decoders_reject_trailing_garbage() -> None:
    assert _decode_request(struct.pack("<QH", 0, 1) + b"x" + b"y") is None
    assert _decode_response(struct.pack("<IH", 0, 0) + b"x") is None


def test_file_server_read_chunk_maps_invalid_inputs_to_errors() -> None:
    assert _read_chunk("bad\0path", 0).error == errno.EINVAL
    assert _read_chunk("pyproject.toml", (1 << 64) - 1).error != 0


def test_file_client_formats_unknown_remote_error() -> None:
    assert "4294967295" in _format_remote_error(0xFFFFFFFF)


class _MalformedResponseStream(ResponseStream):
    def __init__(self) -> None:
        self._seqno = 0

    def __aiter__(self) -> _MalformedResponseStream:
        return self

    async def __anext__(self) -> Response:
        await asyncio.sleep(0)
        seqno = self._seqno
        self._seqno += 1
        return Response(timestamp=Instant.now(), remote_id=123, seqno=seqno, message=b"x")

    def close(self) -> None:
        pass


async def test_file_client_absolute_response_timeout() -> None:
    with pytest.raises(LivenessError):
        await _receive_valid_response(_MalformedResponseStream(), None, 0.01)
