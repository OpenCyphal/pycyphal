# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import typing
import socket
import logging
import ipaddress
import pycyphal.util
import pycyphal.transport
from ._link_layer import LinkLayerCapture

_logger = logging.getLogger(__name__)


class SocketFactory(abc.ABC):
    """
    The factory encapsulates the mapping logic between data specifiers and UDP endpoints.
    Additionally, it also provides an abstract interface for constructing IP-version-specific sniffers.

    May be related:

    - https://stackoverflow.com/a/26988214/1007777
    - https://stackoverflow.com/a/14388707/1007777
    - https://tldp.org/HOWTO/Multicast-HOWTO-6.html
    - https://habr.com/ru/post/141021/
    - https://habr.com/ru/company/cbs/blog/309486/
    - https://stackoverflow.com/a/58118503/1007777
    - http://www.enderunix.org/docs/en/rawipspoof/
    - https://docs.oracle.com/cd/E19683-01/816-5042/sockets-5/index.html
    """

    MULTICAST_TTL = 16
    """
    RFC 1112 dictates that the default TTL for multicast sockets is 1.
    This is not acceptable so we use a larger default.
    """

    @staticmethod
    def new(
        local_ip_addr: typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
    ) -> SocketFactory:
        """
        Use this factory factory to create new instances.
        """
        ipv6_addr = isinstance(local_ip_addr, ipaddress.IPv6Address)
        if not ipv6_addr:
            from ._v4 import IPv4SocketFactory

            return IPv4SocketFactory(local_ip_addr)

        else:
            raise NotImplementedError("Sorry, IPv6 is not yet supported by this implementation.")

    @property
    @abc.abstractmethod
    def max_nodes(self) -> int:
        """
        The maximum number of nodes per subnet may be a function of the protocol version.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_output_socket(
        self, remote_node_id: typing.Optional[int], data_specifier: pycyphal.transport.DataSpecifier
    ) -> socket.socket:
        """
        Make a new non-blocking output socket connected to the appropriate endpoint
        (multicast for both message data specifiers and service data specifiers).
        The socket will be bound to an ephemeral port at the configured local network address.

        The required options will be set up as needed automatically.
        Timestamping will need to be enabled separately if needed.

        WARNING: on Windows, multicast output sockets have a weird corner case.
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
            await  loop.sock_sendall(s, b'abc')                     # OSError
            # OSError: [WinError 1231] The network location cannot be reached
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_input_socket(
        self, remote_node_id: typing.Optional[int], data_specifier: pycyphal.transport.DataSpecifier
    ) -> socket.socket:
        r"""
        Makes a new non-blocking input socket bound to the correct endpoint
        (multicast for both message data specifiers and service data specifiers).

        The required socket options will be set up as needed automatically;
        specifically, ``SO_REUSEADDR``, ``SO_REUSEPORT`` (if available), maybe others as needed.
        Timestamping will need to be enabled separately if needed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_sniffer(self, handler: typing.Callable[[LinkLayerCapture], None]) -> Sniffer:
        """
        Launch a new network sniffer based on a raw socket (usually this requires special permissions).
        The sniffer will run in a separate thread, invoking the handler *directly from the worker thread*
        whenever a UDP packet from the specified subnet is received.

        Packets whose origin does not belong to the current Cyphal/UDP subnet are dropped (not reported).
        This is critical because there may be multiple Cyphal/UDP transport networks running on the same
        physical IP network, which may also be shared with other protocols.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self)


class Sniffer(abc.ABC):
    """
    Network sniffer is responsible for managing the raw socket and parsing and filtering the raw IP packets.
    """

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError
