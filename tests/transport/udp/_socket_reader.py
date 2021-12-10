# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import pytest
from pytest import raises
from ipaddress import ip_address
from pyuavcan.transport.udp._socket_reader import *
from pyuavcan.transport.udp._socket_reader import _READ_TIMEOUT
from pyuavcan.transport import Priority

pytestmark = pytest.mark.asyncio


async def _unittest_socket_reader(caplog: typing.Any) -> None:
    destination_endpoint = "127.100.0.100", 58724

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()

    def check_timestamp(t: pyuavcan.transport.Timestamp) -> bool:
        now = pyuavcan.transport.Timestamp.now()
        s = ts.system_ns <= t.system_ns <= now.system_ns
        m = ts.monotonic_ns <= t.monotonic_ns <= now.system_ns
        return s and m

    sock_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_rx.bind(destination_endpoint)

    def make_sock_tx(source_ip_address: str) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((source_ip_address, 0))
        sock.connect(destination_endpoint)
        return sock

    stats = SocketReaderStatistics()
    srd = SocketReader(
        sock=sock_rx, local_ip_address=ip_address("127.100.4.210"), anonymous=False, statistics=stats  # 1234
    )
    assert not srd.has_listeners
    with raises(LookupError):
        srd.remove_listener(123)

    received_frames_promiscuous: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    received_frames_3: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []

    srd.add_listener(None, lambda t, i, f: received_frames_promiscuous.append((t, i, f)))
    assert srd.has_listeners
    srd.add_listener(3, lambda t, i, f: received_frames_3.append((t, i, f)))
    with raises(Exception):
        srd.add_listener(3, lambda t, i, f: received_frames_3.append((t, i, f)))
    assert srd.has_listeners

    sock_tx_1 = make_sock_tx("127.100.0.1")
    sock_tx_3 = make_sock_tx("127.100.0.3")
    sock_tx_9 = make_sock_tx("127.200.0.9")

    # FRAME FOR THE PROMISCUOUS LISTENER
    sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.HIGH,
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                payload=memoryview(b"HARDBASS"),
            ).compile_header_and_payload()
        )
    )
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1},
        dropped_datagrams={},
    )
    t, nid, rxf = received_frames_promiscuous.pop()
    assert rxf is not None
    assert nid == 1
    assert check_timestamp(t)
    assert bytes(rxf.payload) == b"HARDBASS"
    assert rxf.priority == Priority.HIGH
    assert rxf.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rxf.single_frame_transfer

    assert not received_frames_promiscuous
    assert not received_frames_3

    # FRAME FOR THE SELECTIVE AND THE PROMISCUOUS LISTENER
    sock_tx_3.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                transfer_id=0x_DEADBEEF_DEADBE,
                index=0,
                end_of_transfer=False,
                payload=memoryview(b"Oy blin!"),
            ).compile_header_and_payload()
        )
    )

    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 1},
        dropped_datagrams={},
    )
    t, nid, rxf = received_frames_promiscuous.pop()
    assert rxf is not None
    assert nid == 3
    assert check_timestamp(t)
    assert bytes(rxf.payload) == b"Oy blin!"
    assert rxf.priority == Priority.LOW
    assert rxf.transfer_id == 0x_DEADBEEF_DEADBE
    assert not rxf.single_frame_transfer

    assert (3, rxf) == received_frames_3.pop()[1:]  # Same exact frame in the other listener.

    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROP THE PROMISCUOUS LISTENER, ENSURE THE REMAINING SELECTIVE LISTENER WORKS
    srd.remove_listener(None)
    with raises(LookupError):
        srd.remove_listener(None)
    assert srd.has_listeners

    sock_tx_3.send(
        b"".join(
            UDPFrame(
                priority=Priority.HIGH,
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                payload=memoryview(b"HARDBASS"),
            ).compile_header_and_payload()
        )
    )
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={},
    )
    t, nid, rxf = received_frames_3.pop()
    assert rxf is not None
    assert nid == 3
    assert check_timestamp(t)
    assert bytes(rxf.payload) == b"HARDBASS"
    assert rxf.priority == Priority.HIGH
    assert rxf.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rxf.single_frame_transfer

    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROPPED DATAGRAM FROM VALID NODE-ID
    sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                transfer_id=0x_DEADBEEF_DEADBE,
                index=0,
                end_of_transfer=False,
                payload=memoryview(b"Oy blin!"),
            ).compile_header_and_payload()
        )
    )
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={1: 1},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROPPED DATAGRAM FROM AN UNMAPPED IP ADDRESS
    sock_tx_9.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                transfer_id=0x_DEADBEEF_DEADBE,
                index=0,
                end_of_transfer=False,
                payload=memoryview(b"Oy blin!"),
            ).compile_header_and_payload()
        )
    )
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={1: 1, ip_address("127.200.0.9"): 1},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM NODE
    sock_tx_3.send(b"abc")
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 3},
        dropped_datagrams={1: 1, ip_address("127.200.0.9"): 1},
    )
    assert received_frames_3.pop()[1:] == (3, None)
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM UNMAPPED IP ADDRESS
    sock_tx_9.send(b"abc")
    await (asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 3},
        dropped_datagrams={1: 1, ip_address("127.200.0.9"): 2},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # CLOSURE
    assert srd.has_listeners
    with raises(Exception):
        srd.close()
    srd.remove_listener(3)
    assert not srd.has_listeners
    srd.close()
    srd.close()  # Idempotency
    with raises(pyuavcan.transport.ResourceClosedError):
        srd.add_listener(3, lambda t, i, f: received_frames_3.append((t, i, f)))
    assert sock_rx.fileno() < 0, "The socket has not been closed"

    # SOCKET FAILURE
    with caplog.at_level(logging.CRITICAL, logger=__name__):
        sock_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_rx.bind(("127.100.0.100", 0))
        stats = SocketReaderStatistics()
        srd = SocketReader(
            sock=sock_rx,
            local_ip_address=ip_address("127.100.4.210"),  # 1234
            anonymous=False,
            statistics=stats,
        )
        srd._sock.close()  # pylint: disable=protected-access
        await (asyncio.sleep(_READ_TIMEOUT * 2))  # Wait for the reader thread to notice the problem.
        assert not srd._thread.is_alive()  # pylint: disable=protected-access
        srd._ctl_main.close()  # pylint: disable=protected-access
        srd._ctl_worker.close()  # pylint: disable=protected-access

    sock_tx_1.close()
    sock_tx_3.close()
    sock_tx_9.close()


async def _unittest_socket_reader_endpoint_reuse() -> None:
    """
    This test is designed to ensure that we can replace one socket reader with another without causing a socket FD
    reuse conflict. Here is the background:

    - https://stackoverflow.com/questions/3624365
    - https://stackoverflow.com/questions/3589723
    """
    destination_endpoint = "127.30.0.30", 9999

    loop = asyncio.get_event_loop()

    sock_tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_tx.bind(("127.30.0.10", 0))
    sock_tx.connect(destination_endpoint)
    listener_node_id = 10  # from 127.30.0.10

    async def send_and_wait() -> None:
        ts = Timestamp.now()
        sock_tx.send(
            b"".join(
                UDPFrame(
                    priority=Priority.HIGH,
                    transfer_id=0,
                    index=0,
                    end_of_transfer=True,
                    payload=memoryview(str(ts).encode()),
                ).compile_header_and_payload()
            )
        )
        await (asyncio.sleep(0.5))  # Let the handler run in the background.

    stats = SocketReaderStatistics()

    def make_reader(destination: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]]) -> SocketReader:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sys.platform.startswith("linux"):  # pragma: no branch
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(destination_endpoint)
        out = SocketReader(
            sock=sock,
            local_ip_address=ip_address(destination_endpoint[0]),
            anonymous=False,
            statistics=stats,
        )
        out.add_listener(listener_node_id, lambda *args: destination.append(args))  # type: ignore
        return out

    # Test the first instance. No conflict is possible here, it is just to prepare the foundation for the actual test.
    rxf_a: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    srd_a = make_reader(rxf_a)
    assert len(rxf_a) == 0
    await send_and_wait()
    assert len(rxf_a) == 1, rxf_a

    # Destroy the old instance and QUICKLY create the next one. Make sure the old one does not piggyback on the old FD.
    rxf_b: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    srd_a.remove_listener(listener_node_id)
    srd_a.close()
    srd_b = make_reader(rxf_b)
    assert len(rxf_a) == 1
    assert len(rxf_b) == 0
    await send_and_wait()
    assert len(rxf_a) == 1, rxf_a
    assert len(rxf_b) == 1, rxf_b

    # Just in case, repeat the above exercise checking for the conflict with the second instance.
    rxf_c: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    srd_b.remove_listener(listener_node_id)
    srd_b.close()
    srd_c = make_reader(rxf_c)
    assert len(rxf_a) == 1
    assert len(rxf_b) == 1
    assert len(rxf_c) == 0
    await send_and_wait()
    assert len(rxf_a) == 1, rxf_a
    assert len(rxf_b) == 1, rxf_b
    assert len(rxf_c) == 1, rxf_c

    # Ensure that the statistics do not show duplicate entries processed by FD-conflicting readers.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={
            10: 3,
        },
        dropped_datagrams={},
    )

    # Clean up to avoid warnings.
    srd_c.remove_listener(listener_node_id)
    srd_c.close()
    sock_tx.close()
