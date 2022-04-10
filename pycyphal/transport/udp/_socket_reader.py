# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

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
import pycyphal
from pycyphal.transport import Timestamp
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
    If a UDP frame is received that does not contain a valid Cyphal frame,
    the callback is invoked with None for error statistic collection purposes.
    """

    def __init__(
        self,
        sock: socket.socket,
        local_ip_address: _IPAddress,
        anonymous: bool,
        statistics: SocketReaderStatistics,
    ):
        """
        :param sock: The instance takes ownership of the socket; it will be closed when the instance is closed.
        :param local_ip_address: Needed for node-ID mapping.
        :param anonymous: If True, then packets originating from the local IP address will not be discarded.
        :param statistics: A reference to the external statistics object that will be updated by the instance.
        """
        self._sock = sock
        self._sock.setblocking(False)
        self._original_file_desc = self._sock.fileno()  # This is needed for repr() only.
        self._local_ip_address = local_ip_address
        self._anonymous = anonymous
        self._statistics = statistics

        assert isinstance(self._local_ip_address, (ipaddress.IPv4Address, ipaddress.IPv6Address))
        assert isinstance(self._anonymous, bool)
        assert isinstance(self._statistics, SocketReaderStatistics)

        self._listeners: typing.Dict[typing.Optional[int], SocketReader.Listener] = {}
        self._ctl_worker, self._ctl_main = socket.socketpair()  # For communicating with the worker thread.
        self._thread = threading.Thread(
            target=self._thread_entry_point,
            args=(asyncio.get_event_loop(),),
            name=f"socket_reader_fd_{self._original_file_desc}",
            daemon=True,
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
            raise pycyphal.transport.ResourceClosedError(f"{self} is no longer operational")

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
        # The node-ID mapper will return None for datagrams coming from outside of our Cyphal subnet.
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

    def _thread_entry_point(self, loop: asyncio.AbstractEventLoop) -> None:
        while self._sock.fileno() >= 0:
            try:
                read_ready, _, _ = select.select([self._ctl_worker, self._sock], [], [], _READ_TIMEOUT)
                if self._sock in read_ready:
                    # TODO: use socket timestamping when running on GNU/Linux (Windows does not support timestamping).
                    ts = pycyphal.transport.Timestamp.now()

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
                    loop.call_soon_threadsafe(self._dispatch_frame, ts, source_ip, frame)

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
        return pycyphal.util.repr_attributes_noexcept(
            self,
            id=hex(id(self)),
            original_fd=self._original_file_desc,
            socket=self._sock,
            remote_node_ids=list(self._listeners.keys()),
        )
