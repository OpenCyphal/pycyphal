# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import asyncio
import socket as socket_
import typing
import pytest
import pyuavcan
from pyuavcan.transport import Timestamp, ServiceDataSpecifier
from pyuavcan.transport.udp._session._output import UDPOutputSession, UDPFeedback


pytestmark = pytest.mark.asyncio


async def _unittest_output_session() -> None:
    from pytest import raises
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority
    from pyuavcan.transport import PayloadMetadata, SessionStatistics, Feedback, Transfer

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
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
        finalizer=do_finalize,
    )

    assert sos.specifier == OutputSessionSpecifier(MessageDataSpecifier(3210), None)
    assert sos.destination_node_id is None
    assert sos.payload_metadata == PayloadMetadata(1024)
    assert sos.sample_statistics() == SessionStatistics()

    assert await (
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
    assert await (
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
        finalizer=do_finalize,
    )
    assert await (
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
        finalizer=do_finalize,
    )

    # Induced timeout
    assert not await (
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
        assert not await (
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
        await (
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


async def _unittest_output_session_no_listener() -> None:
    """
    Test the Windows-specific corner case. Should be handled identically on all platforms.
    """
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, Priority
    from pyuavcan.transport import PayloadMetadata, Feedback, Transfer

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()

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
        finalizer=lambda: None,
    )
    assert await (
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
        finalizer=lambda: None,
    )
    sos.enable_feedback(feedback_handler)
    assert last_feedback is None
    assert await (
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
