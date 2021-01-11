# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import time
import socket
import typing
import select
import asyncio
import logging
import threading
import ipaddress
import functools
import dataclasses
import pyuavcan
from pyuavcan.transport import Timestamp
from ._frame import UDPFrame
from ._ip import unicast_ip_to_node_id


_READ_SIZE = 0xFFFF  # Per libpcap documentation, this is to be sufficient always.
_READ_TIMEOUT = 1.0

_logger = logging.getLogger(__name__)

_parse_address = functools.lru_cache(None)(ipaddress.ip_address)

_IPAddress = typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


@dataclasses.dataclass
class SocketReaderStatistics:
    """
    Incoming UDP datagram statistics for an input socket.
    """

    accepted_datagrams: typing.Dict[int, int] = dataclasses.field(default_factory=dict)
    """
    Key is the remote node-ID; value is the number of datagrams received from that node.
    The counters are invariant to the validity of the frame contained in the datagram.
    """

    dropped_datagrams: typing.Dict[typing.Union[_IPAddress, int], int] = dataclasses.field(default_factory=dict)
    """
    Counters of datagrams received from IP addresses that cannot be mapped to a valid node-ID,
    and from nodes that no listener is registered for.
    In the former case, the key is the IP address; in the latter case, the key is the node-ID.
    The counters are invariant to the validity of the frame contained in the datagram.
    """


class SocketReader:
    """
    This class is the solution to the UDP demultiplexing problem.
    The objective is to read data from the supplied socket, parse it, and then forward it to interested listeners.

    Why can't we ask the operating system to do this for us? Because there is no portable way of doing this
    (except for multicast sockets).
    Even on GNU/Linux, there is a risk of race conditions, but I'll spare you the details.
    Those who care may read this: https://stackoverflow.com/a/54156768/1007777.

    The UDP transport is unable to detect a node-ID conflict because it has to discard traffic generated
    by itself in user space. To this transport, its own traffic and a node-ID conflict would look identical.
    """

    Listener = typing.Callable[[Timestamp, int, typing.Optional[UDPFrame]], None]
    """
    The callback is invoked with the timestamp, source node-ID, and the frame instance upon successful reception.
    Remember that on UDP there is no concept of "anonymous node", there is DHCP to handle that.
    If a UDP frame is received that does not contain a valid UAVCAN frame,
    the callback is invoked with None for error statistic collection purposes.
    """

    def __init__(
        self,
        sock: socket.socket,
        local_ip_address: _IPAddress,
        anonymous: bool,
        statistics: SocketReaderStatistics,
        loop: asyncio.AbstractEventLoop,
    ):
        """
        :param sock: The instance takes ownership of the socket; it will be closed when the instance is closed.
        :param local_ip_address: Needed for node-ID mapping.
        :param anonymous: If True, then packets originating from the local IP address will not be discarded.
        :param statistics: A reference to the external statistics object that will be updated by the instance.
        :param loop: The event loop. You know the drill.
        """
        self._sock = sock
        self._sock.setblocking(False)
        self._original_file_desc = self._sock.fileno()  # This is needed for repr() only.
        self._local_ip_address = local_ip_address
        self._anonymous = anonymous
        self._statistics = statistics
        self._loop = loop

        assert isinstance(self._local_ip_address, (ipaddress.IPv4Address, ipaddress.IPv6Address))
        assert isinstance(self._anonymous, bool)
        assert isinstance(self._statistics, SocketReaderStatistics)
        assert isinstance(self._loop, asyncio.AbstractEventLoop)

        self._listeners: typing.Dict[typing.Optional[int], SocketReader.Listener] = {}
        self._ctl_worker, self._ctl_main = socket.socketpair()  # For communicating with the worker thread.
        self._thread = threading.Thread(
            target=self._thread_entry_point, name=f"socket_reader_fd_{self._original_file_desc}", daemon=True
        )
        self._thread.start()

    def add_listener(self, source_node_id: typing.Optional[int], handler: Listener) -> None:
        """
        :param source_node_id: The listener will be invoked whenever a frame from this node-ID is received.
            If the value is None, the listener will be invoked for all source node-IDs (promiscuous).
            There shall be at most one listener per source node-ID value (incl. None, i.e., at most one
            promiscuous listener).
            If such listener already exists, a :class:`ValueError` will be raised.

        :param handler: The callable of type :attr:`Listener` that received frames will be passed to.
            If a frame is received that cannot be parsed, the callable will be invoked with None
            in order to let it update its error statistics.
        """
        if not self._thread.is_alive():
            raise pyuavcan.transport.ResourceClosedError(f"{self} is no longer operational")

        if source_node_id in self._listeners:
            raise ValueError(
                f"{self}: The listener for node-ID {source_node_id} is already registered "
                f"with handler {self._listeners[source_node_id]}"
            )
        _logger.debug(
            "%r: Adding listener %r for node-ID %r. Current stats: %s", self, handler, source_node_id, self._statistics
        )
        self._listeners[source_node_id] = handler

    def remove_listener(self, node_id: typing.Optional[int]) -> None:
        """
        Raises :class:`LookupError` if there is no such listener.
        """
        _logger.debug("%r: Removing listener for node-ID %r. Current stats: %s", self, node_id, self._statistics)
        del self._listeners[node_id]

    @property
    def has_listeners(self) -> bool:
        """
        If there are no listeners, the reader instance can be safely closed and destroyed.
        """
        return len(self._listeners) > 0

    def close(self) -> None:
        """
        Closes the instance and its socket, waits for the thread to terminate (which should happen instantly).

        This method is guaranteed to not return until the socket is closed and all calls that might have been
        blocked on it have been completed (particularly, the calls made by the worker thread).
        THIS IS EXTREMELY IMPORTANT because if the worker thread is left on a blocking read from a closed socket,
        the next created socket is likely to receive the same file descriptor and the worker thread would then
        inadvertently consume the data destined for another reader.
        Worse yet, this error may occur spuriously depending on the timing of the worker thread's access to the
        blocking read function, causing the problem to appear and disappear at random.
        I literally spent the whole day sifting through logs and Wireshark dumps trying to understand why the test
        (specifically, the node tracker test, which is an application-layer entity)
        sometimes fails to see a service response that is actually present on the wire.
        This case is now covered by a dedicated unit test.

        The lesson is to never close a file descriptor while there is a system call blocked on it. Never again.

        Once closed, new listeners can no longer be added.
        Raises :class:`RuntimeError` instead of closing if there is at least one active listener.
        """
        if self.has_listeners:
            raise RuntimeError("Refusing to close socket reader with active listeners. Call remove_listener first.")
        if self._sock.fileno() < 0:  # Ensure idempotency.
            return
        started_at = time.monotonic()
        try:
            _logger.debug("%r: Stopping the thread before closing the socket to avoid accidental fd reuse...", self)
            self._ctl_main.send(b"stop")  # The actual data is irrelevant, we just need it to unblock the select().
            self._thread.join(timeout=_READ_TIMEOUT)
        finally:
            self._sock.close()
            self._ctl_worker.close()
            self._ctl_main.close()
        _logger.debug("%r: Closed. Elapsed time: %.3f milliseconds", self, (time.monotonic() - started_at) * 1e3)

    def _dispatch_frame(
        self, timestamp: Timestamp, source_ip_address: _IPAddress, frame: typing.Optional[UDPFrame]
    ) -> None:
        # Do not accept datagrams emitted by the local node itself. Do not update the statistics either.
        external = self._anonymous or (source_ip_address != self._local_ip_address)
        if not external:
            return

        # Process the datagram. This is where the actual demultiplexing takes place.
        # The node-ID mapper will return None for datagrams coming from outside of our UAVCAN subnet.
        handled = False
        source_node_id = unicast_ip_to_node_id(self._local_ip_address, source_ip_address)
        if source_node_id is not None:
            # Each frame is sent to the promiscuous listener and to the selective listener.
            # We parse the frame before invoking the listener in order to avoid the double parsing workload.
            for key in (None, source_node_id):
                try:
                    callback = self._listeners[key]
                except LookupError:
                    pass
                else:
                    handled = True
                    try:
                        callback(timestamp, source_node_id, frame)
                    except Exception as ex:  # pragma: no cover
                        _logger.exception("%r: Unhandled exception in the listener for node-ID %r: %s", self, key, ex)

        # Update the statistics.
        if not handled:
            ip_nid: typing.Union[_IPAddress, int] = source_node_id if source_node_id is not None else source_ip_address
            try:
                self._statistics.dropped_datagrams[ip_nid] += 1
            except LookupError:
                self._statistics.dropped_datagrams[ip_nid] = 1
        else:
            assert source_node_id is not None
            try:
                self._statistics.accepted_datagrams[source_node_id] += 1
            except LookupError:
                self._statistics.accepted_datagrams[source_node_id] = 1

    def _thread_entry_point(self) -> None:
        while self._sock.fileno() >= 0:
            try:
                read_ready, _, _ = select.select([self._ctl_worker, self._sock], [], [], _READ_TIMEOUT)
                if self._sock in read_ready:
                    # TODO: use socket timestamping when running on GNU/Linux (Windows does not support timestamping).
                    ts = pyuavcan.transport.Timestamp.now()

                    # Notice that we MUST create a new buffer for each received datagram to avoid race conditions.
                    # Buffer memory cannot be shared because the rest of the stack is completely zero-copy;
                    # meaning that the data we allocate here, at the very bottom of the protocol stack,
                    # is likely to be carried all the way up to the application layer without being copied.
                    data, endpoint = self._sock.recvfrom(_READ_SIZE)
                    assert len(data) < _READ_SIZE, "Datagram might have been truncated"
                    source_ip = _parse_address(endpoint[0])

                    frame = UDPFrame.parse(memoryview(data))
                    _logger.debug(
                        "%r: Received UDP packet of %d bytes from %s containing frame: %s",
                        self,
                        len(data),
                        endpoint,
                        frame,
                    )
                    self._loop.call_soon_threadsafe(self._dispatch_frame, ts, source_ip, frame)

                if self._ctl_worker in read_ready:
                    cmd = self._ctl_worker.recv(_READ_SIZE)
                    if cmd:
                        _logger.debug("%r: Worker thread has received the stop signal: %r", self, cmd)
                        break
            except Exception as ex:  # pragma: no cover
                _logger.exception("%r: Worker thread error: %s; will continue after a short nap", self, ex)
                time.sleep(1)
        _logger.debug("%r: Worker thread is exiting, bye bye", self)

    def __repr__(self) -> str:
        """
        The instance remembers its original file descriptor and prints it for diagnostic purposes even if the socket
        is already closed.
        Also, the object ID is printed to differentiate instances sharing the same file descriptor
        (which is NOT permitted, of course, but there have been issues related to that so it was added for debugging).
        """
        return pyuavcan.util.repr_attributes_noexcept(
            self,
            id=hex(id(self)),
            original_fd=self._original_file_desc,
            socket=self._sock,
            remote_node_ids=list(self._listeners.keys()),
        )


def _unittest_socket_reader(caplog: typing.Any) -> None:
    from ipaddress import ip_address
    from pytest import raises
    from pyuavcan.transport import Priority

    destination_endpoint = "127.100.0.100", 58724

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete

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
        sock=sock_rx, local_ip_address=ip_address("127.100.4.210"), anonymous=False, statistics=stats, loop=loop  # 1234
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
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
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

    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
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
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
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
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
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
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={1: 1, ip_address("127.200.0.9"): 1},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM NODE
    sock_tx_3.send(b"abc")
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == SocketReaderStatistics(
        accepted_datagrams={1: 1, 3: 3},
        dropped_datagrams={1: 1, ip_address("127.200.0.9"): 1},
    )
    assert received_frames_3.pop()[1:] == (3, None)
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM UNMAPPED IP ADDRESS
    sock_tx_9.send(b"abc")
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
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
            loop=loop,
        )
        srd._sock.close()  # pylint: disable=protected-access
        run_until_complete(asyncio.sleep(_READ_TIMEOUT * 2))  # Wait for the reader thread to notice the problem.
        assert not srd._thread.is_alive()  # pylint: disable=protected-access
        srd._ctl_main.close()  # pylint: disable=protected-access
        srd._ctl_worker.close()  # pylint: disable=protected-access

    sock_tx_1.close()
    sock_tx_3.close()
    sock_tx_9.close()


def _unittest_socket_reader_endpoint_reuse() -> None:
    """
    This test is designed to ensure that we can replace one socket reader with another without causing a socket FD
    reuse conflict. Here is the background:

    - https://stackoverflow.com/questions/3624365
    - https://stackoverflow.com/questions/3589723
    """
    import sys
    from ipaddress import ip_address
    from pyuavcan.transport import Priority

    destination_endpoint = "127.30.0.30", 9999

    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete

    sock_tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_tx.bind(("127.30.0.10", 0))
    sock_tx.connect(destination_endpoint)
    listener_node_id = 10  # from 127.30.0.10

    def send_and_wait() -> None:
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
        run_until_complete(asyncio.sleep(0.5))  # Let the handler run in the background.

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
            loop=loop,
        )
        out.add_listener(listener_node_id, lambda *args: destination.append(args))  # type: ignore
        return out

    # Test the first instance. No conflict is possible here, it is just to prepare the foundation for the actual test.
    rxf_a: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    srd_a = make_reader(rxf_a)
    assert len(rxf_a) == 0
    send_and_wait()
    assert len(rxf_a) == 1, rxf_a

    # Destroy the old instance and QUICKLY create the next one. Make sure the old one does not piggyback on the old FD.
    rxf_b: typing.List[typing.Tuple[Timestamp, int, typing.Optional[UDPFrame]]] = []
    srd_a.remove_listener(listener_node_id)
    srd_a.close()
    srd_b = make_reader(rxf_b)
    assert len(rxf_a) == 1
    assert len(rxf_b) == 0
    send_and_wait()
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
    send_and_wait()
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
