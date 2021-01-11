# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import copy
import socket as socket_
import typing
import asyncio
import logging
import pyuavcan
from pyuavcan.transport import Timestamp, ServiceDataSpecifier
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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop.sock_sendall(s, b'abc'))   # OSError
    # OSError: [WinError 1231] The network location cannot be reached
"""

_logger = logging.getLogger(__name__)


class UDPFeedback(pyuavcan.transport.Feedback):
    def __init__(self, original_transfer_timestamp: Timestamp, first_frame_transmission_timestamp: Timestamp):
        self._original_transfer_timestamp = original_transfer_timestamp
        self._first_frame_transmission_timestamp = first_frame_transmission_timestamp

    @property
    def original_transfer_timestamp(self) -> Timestamp:
        return self._original_transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> Timestamp:
        return self._first_frame_transmission_timestamp


class UDPOutputSession(pyuavcan.transport.OutputSession):
    """
    The output session logic is extremely simple because most of the work is handled by the UDP/IP
    stack of the operating system.
    Here we just split the transfer into frames, encode the frames, and write them into the socket one by one.
    If the transfer multiplier is greater than one (for unreliable networks),
    we repeat that the required number of times.
    """

    def __init__(
        self,
        specifier: pyuavcan.transport.OutputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        mtu: int,
        multiplier: int,
        sock: socket_.socket,
        loop: asyncio.AbstractEventLoop,
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
        self._loop = loop
        self._finalizer = finalizer
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        self._statistics = pyuavcan.transport.SessionStatistics()
        if self._multiplier < 1:  # pragma: no cover
            raise ValueError(f"Invalid transfer multiplier: {self._multiplier}")

        assert (
            specifier.remote_node_id is None
            if isinstance(specifier.data_specifier, pyuavcan.transport.MessageDataSpecifier)
            else True
        ), "Internal protocol violation: cannot unicast a message transfer"
        assert (
            specifier.remote_node_id is not None if isinstance(specifier.data_specifier, ServiceDataSpecifier) else True
        ), "Internal protocol violation: cannot broadcast a service transfer"

    async def send(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f"{self} is closed")

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
            for fr in pyuavcan.transport.commons.high_overhead_transport.serialize_transfer(
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

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    @property
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
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
        for index, (header, payload) in enumerate(header_payload_pairs):
            try:
                # TODO: concatenation is inefficient. Use vectorized IO via sendmsg() instead!
                await asyncio.wait_for(
                    self._loop.sock_sendall(self._sock, b"".join((header, payload))),
                    timeout=monotonic_deadline - self._loop.time(),
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


def _unittest_output_session() -> None:
    from pytest import raises
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority
    from pyuavcan.transport import PayloadMetadata, SessionStatistics, Feedback, Transfer

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete
    finalized = False

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

    def check_timestamp(t: Timestamp) -> bool:
        now = Timestamp.now()
        s = ts.system_ns <= t.system_ns <= now.system_ns
        m = ts.monotonic_ns <= t.monotonic_ns <= now.system_ns
        return s and m

    destination_endpoint = "127.100.0.1", 25406

    sock_rx = socket_.socket(socket_.AF_INET, socket_.SOCK_DGRAM)
    sock_rx.bind(destination_endpoint)
    sock_rx.settimeout(1.0)

    def make_sock() -> socket_.socket:
        sock = socket_.socket(socket_.AF_INET, socket_.SOCK_DGRAM)
        sock.bind(("127.100.0.2", 0))
        sock.connect(destination_endpoint)
        sock.setblocking(False)
        return sock

    sos = UDPOutputSession(
        specifier=OutputSessionSpecifier(MessageDataSpecifier(3210), None),
        payload_metadata=PayloadMetadata(1024),
        mtu=11,
        multiplier=1,
        sock=make_sock(),
        loop=asyncio.get_event_loop(),
        finalizer=do_finalize,
    )

    assert sos.specifier == OutputSessionSpecifier(MessageDataSpecifier(3210), None)
    assert sos.destination_node_id is None
    assert sos.payload_metadata == PayloadMetadata(1024)
    assert sos.sample_statistics() == SessionStatistics()

    assert run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            loop.time() + 10.0,
        )
    )

    rx_data, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    assert rx_data == (
        b"\x00\x04\x00\x00\x00\x00\x00\x8040\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        + b"one"
        + b"two"
        + b"three"
    )
    with raises(socket_.timeout):
        sock_rx.recvfrom(1000)

    last_feedback: typing.Optional[Feedback] = None

    def feedback_handler(feedback: Feedback) -> None:
        nonlocal last_feedback
        last_feedback = feedback

    sos.enable_feedback(feedback_handler)

    assert last_feedback is None
    assert run_until_complete(
        sos.send(
            Transfer(timestamp=ts, priority=Priority.NOMINAL, transfer_id=12340, fragmented_payload=[]),
            loop.time() + 10.0,
        )
    )
    assert last_feedback is not None
    assert last_feedback.original_transfer_timestamp == ts
    assert check_timestamp(last_feedback.first_frame_transmission_timestamp)

    sos.disable_feedback()
    sos.disable_feedback()  # Idempotency check

    _, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    with raises(socket_.timeout):
        sock_rx.recvfrom(1000)

    assert sos.sample_statistics() == SessionStatistics(transfers=2, frames=2, payload_bytes=11, errors=0, drops=0)

    assert sos.socket.fileno() >= 0
    assert not finalized
    sos.close()
    assert finalized
    assert sos.socket.fileno() < 0  # The socket is supposed to be disposed of.
    finalized = False

    # Multi-frame with multiplication
    sos = UDPOutputSession(
        specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(1024),
        mtu=10,
        multiplier=2,
        sock=make_sock(),
        loop=asyncio.get_event_loop(),
        finalizer=do_finalize,
    )
    assert run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.OPTIONAL,
                transfer_id=54321,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            loop.time() + 10.0,
        )
    )
    data_main_a, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    data_main_b, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    data_redundant_a, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    data_redundant_b, endpoint = sock_rx.recvfrom(1000)
    assert endpoint[0] == "127.100.0.2"
    with raises(socket_.timeout):
        sock_rx.recvfrom(1000)

    print("data_main_a", data_main_a)
    print("data_main_b", data_main_b)
    print("data_redundant_a", data_redundant_a)
    print("data_redundant_b", data_redundant_b)

    assert data_main_a == data_redundant_a
    assert data_main_b == data_redundant_b
    assert data_main_a == (
        b"\x00\x07\x00\x00\x00\x00\x00\x001\xd4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        + b"one"
        + b"two"
        + b"three"[:-1]
    )
    assert data_main_b == (
        b"\x00\x07\x00\x00\x01\x00\x00\x801\xd4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        + b"e"
        + pyuavcan.transport.commons.crc.CRC32C.new(b"one", b"two", b"three").value_as_bytes
    )

    sos.socket.close()  # This is to prevent resource warning
    sos = UDPOutputSession(
        specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(1024),
        mtu=10,
        multiplier=1,
        sock=make_sock(),
        loop=asyncio.get_event_loop(),
        finalizer=do_finalize,
    )

    # Induced timeout
    assert not run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            loop.time() - 0.1,  # Expired on arrival
        )
    )

    assert sos.sample_statistics() == SessionStatistics(
        transfers=0, frames=0, payload_bytes=0, errors=0, drops=2  # Because multiframe
    )

    # Induced failure
    sos.socket.close()
    with raises(OSError):
        assert not run_until_complete(
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )

    assert sos.sample_statistics() == SessionStatistics(transfers=0, frames=0, payload_bytes=0, errors=1, drops=2)

    assert not finalized
    sos.close()
    assert finalized
    sos.close()  # Idempotency

    with raises(pyuavcan.transport.ResourceClosedError):
        run_until_complete(
            sos.send(
                Transfer(
                    timestamp=ts,
                    priority=Priority.NOMINAL,
                    transfer_id=12340,
                    fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
                ),
                loop.time() + 10.0,
            )
        )

    sock_rx.close()


def _unittest_output_session_no_listener() -> None:
    """
    Test the Windows-specific corner case. Should be handled identically on all platforms.
    """
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority
    from pyuavcan.transport import PayloadMetadata, Feedback, Transfer

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete

    def make_sock() -> socket_.socket:
        sock = socket_.socket(socket_.AF_INET, socket_.SOCK_DGRAM)
        sock.bind(("127.100.0.2", 0))
        sock.connect(("239.0.1.2", 33333))  # There is no listener on this endpoint.
        sock.setsockopt(socket_.IPPROTO_IP, socket_.IP_MULTICAST_IF, socket_.inet_aton("127.100.0.2"))
        sock.setblocking(False)
        return sock

    sos = UDPOutputSession(
        specifier=OutputSessionSpecifier(MessageDataSpecifier(3210), None),
        payload_metadata=PayloadMetadata(1024),
        mtu=11,
        multiplier=1,
        sock=make_sock(),
        loop=asyncio.get_event_loop(),
        finalizer=lambda: None,
    )
    assert run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.NOMINAL,
                transfer_id=12340,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            loop.time() + 10.0,
        )
    )
    sos.close()

    # Multi-frame with multiplication and feedback
    last_feedback: typing.Optional[Feedback] = None

    def feedback_handler(feedback: Feedback) -> None:
        nonlocal last_feedback
        assert last_feedback is None, "Unexpected feedback"
        last_feedback = feedback

    sos = UDPOutputSession(
        specifier=OutputSessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        payload_metadata=PayloadMetadata(1024),
        mtu=10,
        multiplier=2,
        sock=make_sock(),
        loop=asyncio.get_event_loop(),
        finalizer=lambda: None,
    )
    sos.enable_feedback(feedback_handler)
    assert last_feedback is None
    assert run_until_complete(
        sos.send(
            Transfer(
                timestamp=ts,
                priority=Priority.OPTIONAL,
                transfer_id=54321,
                fragmented_payload=[memoryview(b"one"), memoryview(b"two"), memoryview(b"three")],
            ),
            loop.time() + 10.0,
        )
    )
    print("last_feedback:", last_feedback)
    assert isinstance(last_feedback, UDPFeedback)
    # Ensure that the timestamp is populated even if the error suppression logic is activated.
    assert last_feedback.original_transfer_timestamp == ts
    assert Timestamp.now().monotonic >= last_feedback.first_frame_transmission_timestamp.monotonic >= ts.monotonic
    assert Timestamp.now().system >= last_feedback.first_frame_transmission_timestamp.system >= ts.system

    sos.close()
