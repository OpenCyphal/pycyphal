#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import socket
import logging
import ipaddress
import pyuavcan.util
import pyuavcan.transport
from ._endpoint_mapping import IPAddress
from ._packet import UDPIPPacket


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
    """

    MULTICAST_TTL = 16
    """
    RFC 1112 dictates that the default TTL for multicast sockets is 1.
    This is not acceptable so we use a larger default.
    """

    @staticmethod
    def new(local_ip_address: IPAddress) -> SocketFactory:
        """
        Use this factory factory to create new instances.
        """
        if isinstance(local_ip_address, ipaddress.IPv4Address):
            from ._v4 import SocketFactoryIPv4
            return SocketFactoryIPv4(local_ip_address)
        elif isinstance(local_ip_address, ipaddress.IPv6Address):
            from ._v6 import SocketFactoryIPv6
            return SocketFactoryIPv6(local_ip_address)
        else:
            raise TypeError(f'Invalid local IP address: {local_ip_address!r}')

    @property
    @abc.abstractmethod
    def max_nodes(self) -> int:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_ip_address(self) -> IPAddress:
        raise NotImplementedError

    @abc.abstractmethod
    def make_output_socket(self,
                           remote_node_id: typing.Optional[int],
                           data_specifier: pyuavcan.transport.DataSpecifier) -> socket.socket:
        """
        Make a new non-blocking output socket connected to the appropriate endpoint
        (unicast for service data specifiers, multicast for message data specifiers).
        The socket will be bound to an ephemeral port at the configured local network address.

        The required options will be set up as needed automatically.
        Timestamping will need to be enabled separately if needed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_input_socket(self, data_specifier: pyuavcan.transport.DataSpecifier) -> socket.socket:
        r"""
        Makes a new non-blocking input socket bound to the correct endpoint
        (unicast for service data specifiers, multicast for message data specifiers).

        The required socket options will be set up as needed automatically;
        specifically, ``SO_REUSEADDR``, ``SO_REUSEPORT`` (if available), maybe others as needed.
        Timestamping will need to be enabled separately if needed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_sniffer(self, handler: typing.Callable[[pyuavcan.transport.Timestamp, UDPIPPacket], None]) -> Sniffer:
        """
        Launch a new network sniffer based on a raw socket (usually this requires special permissions).
        The sniffer will run in a separate thread, invoking the handler *directly from the worker thread*
        whenever a UDP packet from the specified subnet is received.

        Packets whose origin does not belong to the current UAVCAN/UDP subnet are dropped (not reported).
        This is critical because there may be multiple UAVCAN/UDP transport networks running on the same
        physical IP network, which may also be shared with other protocols.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, local_ip_address=self.local_ip_address)


class Sniffer(abc.ABC):
    """
    Network sniffer is responsible for managing the raw socket and parsing and filtering the raw IP packets.
    """
    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError
