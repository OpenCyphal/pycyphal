"""
Cyphal/Serial-over-TCP broker.

Cyphal/Serial uses COBS-encoded frames with a zero byte as frame delimiter. When
brokering a byte-stream ncat --broker does know about the frame delimiter and
might interleave frames from different clients.
This broker is similar in functionality to :code:`ncat --broker`, but reads the
whole frame before passing it on to other clients, avoiding interleaved frames
and potential frame/data loss.
"""

import argparse
import asyncio
import logging
import socket
import typing as t


class Client:
    """
    Represents a client connected to the broker, wrapping StreamReader and
    StreamWriter to conveniently read/write zero-terminated frames.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._buffer = bytearray()
        self._reader = reader
        self._writer = writer

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *_: t.Any) -> bool:
        self._writer.close()
        await self._writer.wait_closed()
        return True

    async def read(self) -> t.AsyncGenerator[bytes, None]:
        """
        async generator yielding complete frames, including terminating \x00.
        """
        buffer = bytearray()
        while not self._reader.at_eof():
            buffer += await self._reader.readuntil(separator=b"\x00")
            # don't pass on a leading zero-byte on its own.
            if len(buffer) == 1:
                continue
            yield buffer
            buffer = bytearray()

    def write(self, frame: bytes) -> None:
        """
        Writes a frame to the stream, unless the stream is closing.

        :param frame: Frame to send to this client.
        """
        if self._writer.is_closing():
            return
        self._writer.write(frame)

    async def drain(self) -> None:
        """
        Flushes the stream.
        """
        if self._writer.is_closing():
            return
        await self._writer.drain()


async def serve_forever(host: str, port: int) -> None:
    """
    pybroker core server loop.

    Accept clients on :code:`host`::code:`port` and broadcast any frame
    received from any client to all other clients.

    :param host: IP, where the broker will be reachable on.
    :param port: port, on which the broker will listen on.
    """
    clients: list[Client] = []
    list_lock = asyncio.Lock()

    async def _run_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with Client(reader, writer) as client:
            async with list_lock:
                logging.info("Client connected.")
                clients.append(client)
            try:
                async for frame in client.read():
                    logging.debug("Received frame %s", frame)
                    for c in clients:
                        if c != client:
                            c.write(frame)
                    async with list_lock:
                        # not sure if flushing is required.
                        for c in clients:
                            await c.drain()

            finally:
                async with list_lock:
                    clients.remove(client)
                    logging.info("Client disconnected.")

    logging.info("Broker started on %s:%s", host, port)
    reuse_port = hasattr(socket, "SO_REUSEPORT") and socket.SO_REUSEPORT
    await asyncio.start_server(
        _run_client,
        host,
        port,
        family=socket.AF_INET,
        reuse_address=True,
        reuse_port=reuse_port,
    )


def main() -> None:
    """
    TCP-broker which forwards complete, zero-terminated frames/datagrams among
    all connected clients.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--host", default="127.0.0.1", help="Interface to listen on for incoming connections.")
    parser.add_argument("-p", "--port", default=50905, help="Clients connect to this port.")
    parser.add_argument("--verbose", default=False, action="store_true", help="Increase logging verbosity.")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(serve_forever(args.host, args.port))
    loop.run_forever()
