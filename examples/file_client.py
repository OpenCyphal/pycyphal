#!/usr/bin/env python3
"""
Read a file from a Cyphal/UDP file server and write it to stdout.
Usage:
    python examples/file_client.py <file> > copy.bin
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from _file_types import FileReadError, FileReadRequest, FileReadResponse

from pycyphal2 import DeliveryError, Instant, LivenessError, Node, ResponseStream, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
TOPIC = "file/read"
RESPONSE_TIMEOUT = 30.0
REQUEST_DELIVERY_TIMEOUT = RESPONSE_TIMEOUT / 2.0
REQUEST_SIZE = 4096
_logger = logging.getLogger(__name__)


def _format_remote_error(error: int) -> str:
    try:
        name = FileReadError(error).name
    except ValueError:
        name = FileReadError.ERROR_OTHER.name
    return f"Remote error {error}: {name}"


async def _receive_response(stream: ResponseStream, expected_server_id: int | None) -> tuple[int, FileReadResponse]:
    async for response in stream:
        if expected_server_id is not None and response.remote_id != expected_server_id:
            _logger.info(
                "ignoring response from redundant server %016x, expected %016x",
                response.remote_id,
                expected_server_id,
            )
            continue
        decoded = FileReadResponse.deserialize(response.message)
        if decoded is None:
            _logger.debug("dropping malformed response from %016x seq=%d", response.remote_id, response.seqno)
            continue
        return response.remote_id, decoded
    raise LivenessError("Response stream closed")


async def _receive_valid_response(
    stream: ResponseStream, expected_server_id: int | None, response_timeout: float
) -> tuple[int, FileReadResponse]:
    try:
        return await asyncio.wait_for(_receive_response(stream, expected_server_id), timeout=response_timeout)
    except asyncio.TimeoutError as ex:
        raise LivenessError("Response timeout") from ex


async def run(file_path: str) -> int:
    transport = UDPTransport.new()
    node = Node.new(transport, NAME)
    pub = node.advertise(TOPIC)
    discovered_server_id: int | None = None
    read_offset = 0
    _logger.info("file client ready on %r via %s", TOPIC, transport)
    try:
        while True:
            _logger.info("requesting seek %d", read_offset)
            stream: ResponseStream | None = None
            try:
                request = FileReadRequest(read_offset, REQUEST_SIZE, file_path).serialize()
                stream = await pub.request(Instant.now() + REQUEST_DELIVERY_TIMEOUT, RESPONSE_TIMEOUT, request)
                server_id, response = await _receive_valid_response(stream, discovered_server_id, RESPONSE_TIMEOUT)
            except ValueError as ex:
                sys.stderr.write(f"{ex}\n")
                return 1
            except DeliveryError:
                sys.stderr.write("Request delivery failed\n")
                return 1
            except LivenessError:
                sys.stderr.write("Response timeout\n")
                return 1
            except SendError as ex:
                sys.stderr.write(f"Request send failed: {ex}\n")
                return 1
            finally:
                if stream is not None:
                    stream.close()

            if discovered_server_id is None:
                discovered_server_id = server_id
                _logger.info("discovered server UID: %016x", discovered_server_id)
            _logger.info("received response: seek %d", read_offset)

            if response.error != 0:
                sys.stderr.write(_format_remote_error(response.error) + "\n")
                return 1
            if response.seek != read_offset:
                sys.stderr.write(f"Invalid response seek: expected {read_offset}, got {response.seek}\n")
                return 1
            if len(response.data) > REQUEST_SIZE:
                sys.stderr.write(f"Invalid response size: {len(response.data)} exceeds {REQUEST_SIZE}\n")
                return 1
            if not response.end and not response.data:
                sys.stderr.write("Invalid response: an unfinished chunk is empty\n")
                return 1
            if response.data:
                sys.stdout.buffer.write(response.data)
                sys.stdout.buffer.flush()
                read_offset += len(response.data)
            if not response.end:
                continue

            _logger.info("finished transferring %d bytes", read_offset)
            return 0
    finally:
        pub.close()
        node.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Read a file from a Cyphal/UDP file server.")
    parser.add_argument("file", help="File path to read on the server")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s: %(message)s")
    try:
        raise SystemExit(asyncio.run(run(args.file)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
