#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import asyncio
import logging
import threading
import dataclasses
import socket
import pyuavcan
from ._frame import UDPFrame


_READ_TIMEOUT = 1.0

_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPDemultiplexerStatistics:
    """
    Incoming UDP datagram statistics for an input socket.
    """
    accepted_datagrams: typing.Dict[int, int] = dataclasses.field(default_factory=dict)
    """
    Key is the remote node-ID; value is the number of datagrams received from that node.
    The counters are invariant to the validity of the frame contained in the datagram.
    """

    dropped_datagrams: typing.Dict[typing.Union[str, int], int] = dataclasses.field(default_factory=dict)
    """
    Counters of datagrams received from IP addresses that cannot be mapped to a valid node-ID,
    and from nodes that no listener is registered for.
    In the former case, the key is the IP address as a string; in the latter case, the key is the node-ID.
    The counters are invariant to the validity of the frame contained in the datagram.
    """


class UDPDemultiplexer:
    """
    This class is the solution to the UDP demultiplexing problem, as you can probably figure out from reading its name.
    The objective is to read data from the supplied socket and then forward it to interested listeners.

    Why can't we ask the operating system to do this for us? Because there is no portable way of doing this.
    Even on GNU/Linux, there is a risk of race conditions, but I'll spare you the details.
    Those who care may read this: https://stackoverflow.com/a/54156768/1007777.

    The UDP transport is unable to detect a node-ID conflict because it has to discard broadcast traffic generated
    by itself in user space. To this transport, its own traffic and a node-ID conflict would look identical.
    """
    Listener = typing.Callable[[int, typing.Optional[UDPFrame]], None]
    """
    The callback is invoked with the source node-ID and the frame instance upon successful reception.
    Remember that on UDP there is no concept of "anonymous node", there is DHCP to handle that.
    If a UDP frame is received that does not contain a valid UAVCAN frame,
    the callback is invoked with None for error statistic collection purposes.
    """

    def __init__(self,
                 sock:           socket.socket,
                 udp_mtu:        int,
                 node_id_mapper: typing.Callable[[str], typing.Optional[int]],
                 local_node_id:  typing.Optional[int],
                 statistics:     UDPDemultiplexerStatistics,
                 loop:           asyncio.AbstractEventLoop):
        """
        :param sock: The instance takes ownership of the socket; it will be closed when the instance is closed.
        :param udp_mtu: The size of the socket read buffer. Make it large. If not sure, make it larger.
        :param node_id_mapper: A mapping: ``(ip_address) -> Optional[node_id]``.
        :param local_node_id: The node-ID of the local node or None. Needed to discard own-generated broadcast traffic.
        :param statistics: A reference to the external statistics object that will be updated by the instance.
        :param loop: The event loop. You know the drill.
        """
        self._sock = sock
        self._sock.settimeout(_READ_TIMEOUT)

        self._udp_mtu = int(udp_mtu)
        self._node_id_mapper = node_id_mapper
        self._local_node_id = local_node_id
        self._statistics = statistics
        self._loop = loop

        assert callable(self._node_id_mapper)
        assert isinstance(self._local_node_id, int) or self._local_node_id is None
        assert isinstance(self._statistics, UDPDemultiplexerStatistics)
        assert isinstance(self._loop, asyncio.AbstractEventLoop)

        self._closed = False
        self._listeners: typing.Dict[typing.Optional[int], UDPDemultiplexer.Listener] = {}

        self._thread = threading.Thread(target=self._thread_entry_point,
                                        name='demultiplexer_socket_reader',
                                        daemon=True)
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
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')

        if source_node_id in self._listeners:
            raise ValueError(f'{self}: The listener for node-ID {source_node_id} is already registered '
                             f'with handler {self._listeners[source_node_id]}')
        self._listeners[source_node_id] = handler
        _logger.debug('%r: Adding listener %r for node-ID %r', self, handler, source_node_id)

    def remove_listener(self, node_id: typing.Optional[int]) -> None:
        """
        Raises :class:`LookupError` if there is no such listener.
        """
        _logger.debug('%r: Removing listener for node-ID %r', self, node_id)
        del self._listeners[node_id]

    @property
    def has_listeners(self) -> bool:
        """
        If there are no listeners, the demultiplexer instance can be safely closed and destroyed.
        """
        return len(self._listeners) > 0

    def close(self) -> None:
        """
        Closes the instance and its socket.
        Once closed, new listeners can no longer be added.
        Raises :class:`RuntimeError` instead of closing if there is at least one active listener.
        """
        if self.has_listeners:
            raise RuntimeError('Do not close the demultiplexer with active listeners, suka!')
        self._closed = True
        self._sock.close()
        # We don't wait for the thread to join because who cares?

    def _dispatch_frame(self, source_ip: str, frame: typing.Optional[UDPFrame]) -> None:
        if self._closed:
            # A check for closure is mandatory here because there is a period of uncertainty between the point
            # when this method is invoked from the reader thread and the point where the event loop gets around
            # to calling it: between these two events the instance might be closed and the surrounding
            # infrastructure (such as the IP address mapper) may become unusable.
            return  # pragma: no cover

        # Process the datagram. This is where the actual demultiplexing takes place.
        # The node-ID mapper will return None for datagrams coming from outside of our UAVCAN subnet.
        handled = False
        source_node_id = self._node_id_mapper(source_ip)
        if source_node_id is not None and source_node_id != self._local_node_id:
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
                        callback(source_node_id, frame)
                    except Exception as ex:  # pragma: no cover
                        _logger.exception('%r: Unhandled exception in the listener for node-ID %r: %s', self, key, ex)

        # Update the statistics.
        if not handled:
            ip_nid: typing.Union[str, int] = source_node_id if source_node_id is not None else source_ip
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
        while not self._closed:
            try:
                # Notice that we MUST create a new buffer for each received datagram to avoid race conditions.
                # Buffer memory cannot be shared because the rest of the stack is completely zero-copy;
                # meaning that the data we allocate here, at the very bottom of the protocol stack,
                # is likely to be carried all the way up to the application layer without being copied.
                data, endpoint = self._sock.recvfrom(self._udp_mtu)
                source_ip = endpoint[0]
                assert isinstance(source_ip, str)

                # TODO: use socket timestamping when running on Linux (Windows does not support timestamping).
                ts = pyuavcan.transport.Timestamp.now()

                frame = UDPFrame.parse(memoryview(data), ts)
                self._loop.call_soon_threadsafe(self._dispatch_frame, source_ip, frame)

                if len(data) >= self._udp_mtu:  # pragma: no cover
                    _logger.warning('%r: A datagram from %r is %d bytes long which is not less than '
                                    'the size of the buffer, therefore it might have been truncated. '
                                    'Enlarge the read buffer to squelch this warning.',
                                    self, endpoint, len(data))

            except socket.timeout:
                # This is needed for checking the status of the closure flag periodically.
                # I don't actually expect this to be necessary because when the socket is closed we'll get an
                # exception anyway, but the socket API docs are unclear in this regard so this paranoia is justified.
                pass

            except Exception as ex:
                if self._closed:  # pragma: no cover
                    _logger.debug('%r: Ignoring exception %r because we have been commanded to stop', self, ex)

                elif self._sock.fileno() < 0:
                    self._closed = True
                    _logger.exception('%r: The socket has been closed unexpectedly! Terminating the instance.', self)

                else:  # pragma: no cover
                    _logger.exception('%r: Reader thread failure: %s; will continue after a short nap', self, ex)
                    time.sleep(1)

        _logger.debug('%r: The reader worker thread is exiting, bye bye', self)
        assert self._closed

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(self, self._sock, remote_node_ids=list(self._listeners.keys()))


def _unittest_demultiplexer(caplog: typing.Any) -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, Timestamp

    destination_endpoint = '127.100.0.100', 58724

    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    run_until_complete = loop.run_until_complete

    def check_timestamp(t: pyuavcan.transport.Timestamp) -> bool:
        now = pyuavcan.transport.Timestamp.now()
        s = ts.system_ns <= t.system_ns <= now.system_ns
        m = ts.monotonic_ns <= t.monotonic_ns <= now.system_ns
        return s and m

    # This is a simplified mapping; good enough for testing.
    node_id_map = {
        '127.100.0.1': 1,
        '127.100.0.2': 2,
        '127.100.0.3': 3,
    }

    sock_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_rx.bind(destination_endpoint)

    def make_sock_tx(source_ip_address: str) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((source_ip_address, 0))
        sock.connect(destination_endpoint)
        return sock

    stats = UDPDemultiplexerStatistics()
    demux = UDPDemultiplexer(sock=sock_rx,
                             udp_mtu=10240,
                             node_id_mapper=node_id_map.get,
                             local_node_id=1234,
                             statistics=stats,
                             loop=loop)
    assert not demux.has_listeners
    with raises(LookupError):
        demux.remove_listener(123)

    received_frames_promiscuous: typing.List[typing.Tuple[int, typing.Optional[UDPFrame]]] = []
    received_frames_3: typing.List[typing.Tuple[int, typing.Optional[UDPFrame]]] = []

    demux.add_listener(None, lambda i, f: received_frames_promiscuous.append((i, f)))
    assert demux.has_listeners
    demux.add_listener(3, lambda i, f: received_frames_3.append((i, f)))
    with raises(Exception):
        demux.add_listener(3, lambda i, f: received_frames_3.append((i, f)))
    assert demux.has_listeners

    sock_tx_1 = make_sock_tx('127.100.0.1')
    sock_tx_3 = make_sock_tx('127.100.0.3')
    sock_tx_9 = make_sock_tx('127.100.0.9')

    # FRAME FOR THE PROMISCUOUS LISTENER
    sock_tx_1.send(b''.join(
        UDPFrame(timestamp=Timestamp.now(),
                 priority=Priority.HIGH,
                 transfer_id=0x_dead_beef_c0ffee,
                 index=0,
                 end_of_transfer=True,
                 payload=memoryview(b'HARDBASS')).compile_header_and_payload()
    ))
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1},
        dropped_datagrams={},
    )
    nid, rxf = received_frames_promiscuous.pop()
    assert rxf is not None
    assert nid == 1
    assert check_timestamp(rxf.timestamp)
    assert bytes(rxf.payload) == b'HARDBASS'
    assert rxf.priority == Priority.HIGH
    assert rxf.transfer_id == 0x_dead_beef_c0ffee
    assert rxf.single_frame_transfer

    assert not received_frames_promiscuous
    assert not received_frames_3

    # FRAME FOR THE SELECTIVE AND THE PROMISCUOUS LISTENER
    sock_tx_3.send(b''.join(
        UDPFrame(timestamp=Timestamp.now(),
                 priority=Priority.LOW,
                 transfer_id=0x_deadbeef_deadbe,
                 index=0,
                 end_of_transfer=False,
                 payload=memoryview(b'Oy blin!')).compile_header_and_payload()
    ))

    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 1},
        dropped_datagrams={},
    )
    nid, rxf = received_frames_promiscuous.pop()
    assert rxf is not None
    assert nid == 3
    assert check_timestamp(rxf.timestamp)
    assert bytes(rxf.payload) == b'Oy blin!'
    assert rxf.priority == Priority.LOW
    assert rxf.transfer_id == 0x_deadbeef_deadbe
    assert not rxf.single_frame_transfer

    assert (3, rxf) == received_frames_3.pop()   # Same exact frame in the other listener.

    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROP THE PROMISCUOUS LISTENER, ENSURE THE REMAINING SELECTIVE LISTENER WORKS
    demux.remove_listener(None)
    with raises(LookupError):
        demux.remove_listener(None)
    assert demux.has_listeners

    sock_tx_3.send(b''.join(
        UDPFrame(timestamp=Timestamp.now(),
                 priority=Priority.HIGH,
                 transfer_id=0x_dead_beef_c0ffee,
                 index=0,
                 end_of_transfer=True,
                 payload=memoryview(b'HARDBASS')).compile_header_and_payload()
    ))
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={},
    )
    nid, rxf = received_frames_3.pop()
    assert rxf is not None
    assert nid == 3
    assert check_timestamp(rxf.timestamp)
    assert bytes(rxf.payload) == b'HARDBASS'
    assert rxf.priority == Priority.HIGH
    assert rxf.transfer_id == 0x_dead_beef_c0ffee
    assert rxf.single_frame_transfer

    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROPPED DATAGRAM FROM VALID NODE-ID
    sock_tx_1.send(b''.join(
        UDPFrame(timestamp=Timestamp.now(),
                 priority=Priority.LOW,
                 transfer_id=0x_deadbeef_deadbe,
                 index=0,
                 end_of_transfer=False,
                 payload=memoryview(b'Oy blin!')).compile_header_and_payload()
    ))
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={1: 1},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # DROPPED DATAGRAM FROM AN UNMAPPED IP ADDRESS
    sock_tx_9.send(b''.join(
        UDPFrame(timestamp=Timestamp.now(),
                 priority=Priority.LOW,
                 transfer_id=0x_deadbeef_deadbe,
                 index=0,
                 end_of_transfer=False,
                 payload=memoryview(b'Oy blin!')).compile_header_and_payload()
    ))
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 2},
        dropped_datagrams={1: 1, '127.100.0.9': 1},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM NODE
    sock_tx_3.send(b'abc')
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 3},
        dropped_datagrams={1: 1, '127.100.0.9': 1},
    )
    assert received_frames_3.pop() == (3, None)
    assert not received_frames_promiscuous
    assert not received_frames_3

    # INVALID FRAME FROM UNMAPPED IP ADDRESS
    sock_tx_9.send(b'abc')
    run_until_complete(asyncio.sleep(1.1))  # Let the handler run in the background.
    assert stats == UDPDemultiplexerStatistics(
        accepted_datagrams={1: 1, 3: 3},
        dropped_datagrams={1: 1, '127.100.0.9': 2},
    )
    assert not received_frames_promiscuous
    assert not received_frames_3

    # CLOSURE
    assert demux.has_listeners
    with raises(Exception):
        demux.close()
    demux.remove_listener(3)
    assert not demux.has_listeners
    demux.close()
    demux.close()   # Idempotency
    with raises(pyuavcan.transport.ResourceClosedError):
        demux.add_listener(3, lambda i, f: received_frames_3.append((i, f)))
    assert sock_rx.fileno() < 0, 'The socket has not been closed'

    # SOCKET FAILURE
    with caplog.at_level(logging.CRITICAL, logger=__name__):
        sock_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_rx.bind(('127.100.0.100', 0))
        stats = UDPDemultiplexerStatistics()
        demux = UDPDemultiplexer(sock=sock_rx,
                                 udp_mtu=10240,
                                 node_id_mapper=node_id_map.get,
                                 local_node_id=1234,
                                 statistics=stats,
                                 loop=loop)
        # noinspection PyProtectedMember
        demux._sock.close()
        run_until_complete(asyncio.sleep(_READ_TIMEOUT * 2))  # Wait for the reader thread to notice the problem.
        # noinspection PyProtectedMember
        assert demux._closed
