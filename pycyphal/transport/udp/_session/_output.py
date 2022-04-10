# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import sys
import copy
import socket as socket_
import typing
import asyncio
import logging
import pycyphal
from pycyphal.transport import Timestamp, ServiceDataSpecifier
from .._frame import UDPFrame


_IGNORE_OS_ERROR_ON_SEND = sys.platform.startswith("win")
r"""
On Windows, multicast output sockets have a weird corner case.
If the output interface is set to the loopback adapter and there are no registered listeners for the specified
multicast group, an attempt to send data to that group will fail with a "network unreachable" error.
Here is an example::

    import socket, asyncio
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.1.2.3', 0))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton('127.1.2.3'))
    s.sendto(b'\xaa\xbb\xcc', ('127.5.5.5', 1234))          # Success
    s.sendto(b'\xaa\xbb\xcc', ('239.1.2.3', 1234))          # OSError
    # OSError: [WinError 10051] A socket operation was attempted to an unreachable network
    await loop.sock_sendall(s, b'abc')                      # OSError
    # OSError: [WinError 1231] The network location cannot be reached
"""

_logger = logging.getLogger(__name__)


class UDPFeedback(pycyphal.transport.Feedback):
    def __init__(self, original_transfer_timestamp: Timestamp, first_frame_transmission_timestamp: Timestamp):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> Timestamp:
        return self._first_frame_transmission_timestamp


class UDPOutputSession(pycyphal.transport.OutputSession):
    """
    The output session logic is extremely simple because most of the work is handled by the UDP/IP
    stack of the operating system.
    Here we just split the transfer into frames, encode the frames, and write them into the socket one by one.
    If the transfer multiplier is greater than one (for unreliable networks),
    we repeat that the required number of times.
    """

    def __init__(
        self,
        specifier: pycyphal.transport.OutputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        mtu: int,
        multiplier: int,
        sock: socket_.socket,
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly. Instead, use the factory method.
        Instances take ownership of the socket.
        """
        self._closed = False
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._mtu = int(mtu)
        self._multiplier = int(multiplier)
        self._sock = sock
        self._finalizer = finalizer
        self._feedback_handler: typing.Optional[typing.Callable[[pycyphal.transport.Feedback], None]] = None
        self._statistics = pycyphal.transport.SessionStatistics()
        if self._multiplier < 1:  # pragma: no cover
            raise ValueError(f"Invalid transfer multiplier: {self._multiplier}")
        assert (
            specifier.remote_node_id is None
            if isinstance(specifier.data_specifier, pycyphal.transport.MessageDataSpecifier)
            else True
        ), "Internal protocol violation: cannot unicast a message transfer"
        assert (
            specifier.remote_node_id is not None if isinstance(specifier.data_specifier, ServiceDataSpecifier) else True
        ), "Internal protocol violation: cannot broadcast a service transfer"

    async def send(self, transfer: pycyphal.transport.Transfer, monotonic_deadline: float) -> bool:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError(f"{self} is closed")

        def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> UDPFrame:
            return UDPFrame(
                priority=transfer.priority,
                transfer_id=transfer.transfer_id,
                index=index,
                end_of_transfer=end_of_transfer,
                payload=payload,
            )

        frames = [
            fr.compile_header_and_payload()
            for fr in pycyphal.transport.commons.high_overhead_transport.serialize_transfer(
                transfer.fragmented_payload, self._mtu, construct_frame
            )
        ]
        _logger.debug("%s: Sending transfer: %s; current stats: %s", self, transfer, self._statistics)
        tx_timestamp = await self._emit(frames, monotonic_deadline)
        if tx_timestamp is None:
            return False

        self._statistics.transfers += 1

        # Once we have transmitted at least one copy of a multiplied transfer, it's a success.
        # We don't care if redundant copies fail.
        for _ in range(self._multiplier - 1):
            if not await self._emit(frames, monotonic_deadline):
                break

        if self._feedback_handler is not None:
            try:
                self._feedback_handler(
                    UDPFeedback(
                        original_transfer_timestamp=transfer.timestamp, first_frame_transmission_timestamp=tx_timestamp
                    )
                )
            except Exception as ex:  # pragma: no cover
                _logger.exception(
                    "Unhandled exception in the output session feedback handler %s: %s", self._feedback_handler, ex
                )

        return True

    def enable_feedback(self, handler: typing.Callable[[pycyphal.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    @property
    def specifier(self) -> pycyphal.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pycyphal.transport.SessionStatistics:
        return copy.copy(self._statistics)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._sock.close()
            finally:
                self._finalizer()

    @property
    def socket(self) -> socket_.socket:
        """
        Provides access to the underlying UDP socket.
        """
        return self._sock

    async def _emit(
        self, header_payload_pairs: typing.Sequence[typing.Tuple[memoryview, memoryview]], monotonic_deadline: float
    ) -> typing.Optional[Timestamp]:
        """
        Returns the transmission timestamp of the first frame (which is the transfer timestamp) on success.
        Returns None if at least one frame could not be transmitted.
        """
        ts: typing.Optional[Timestamp] = None
        loop = asyncio.get_running_loop()
        for index, (header, payload) in enumerate(header_payload_pairs):
            try:
                # TODO: concatenation is inefficient. Use vectorized IO via sendmsg() instead!
                await asyncio.wait_for(
                    loop.sock_sendall(self._sock, b"".join((header, payload))),
                    timeout=monotonic_deadline - loop.time(),
                )

                # TODO: use socket timestamping when running on Linux (Windows does not support timestamping).
                # Depending on the chosen approach, timestamping on Linux may require us to launch a new thread
                # reading from the socket's error message queue and then matching the returned frames with a
                # pending loopback registry, kind of like it's done with CAN.
                ts = ts or Timestamp.now()

            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._statistics.drops += len(header_payload_pairs) - index
                return None
            except Exception as ex:
                if _IGNORE_OS_ERROR_ON_SEND and isinstance(ex, OSError) and self._sock.fileno() >= 0:
                    # Windows compatibility workaround -- if there are no registered multicast receivers on the
                    # loopback interface, send() may raise WinError 1231 or 10051. This error shall be suppressed.
                    _logger.debug(
                        "%r: Socket send error ignored (the likely cause is that there are no known receivers "
                        "on the other end of the link): %r",
                        self,
                        ex,
                    )
                    # To suppress the error properly, we have to pretend that the data was actually transmitted,
                    # so we populate the timestamp with a phony value anyway.
                    ts = ts or Timestamp.now()
                else:
                    self._statistics.errors += 1
                    raise

            self._statistics.frames += 1
            self._statistics.payload_bytes += len(payload)

        return ts
