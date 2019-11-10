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
import pyuavcan.util


_logger = logging.getLogger(__name__)


class NetworkMap(abc.ABC):
    """
    This class encapsulates the logic of mapping between node-ID values and IP addresses.
    The principle is that the node-ID is represented as a contiguous set of least significant bits of
    the node's IP address.

    If the IP address of the local node does not belong to the subnet
    (e.g., if the local IP address is the same as the broadcast address of the subnet,
    or if the value of the host bits does not belong to the set of valid node-ID values),
    the local node is considered to be anonymous.
    Anonymous UDP/IP nodes cannot initiate network exchanges of any kind, they can only listen.

    If none of the available network interfaces have the supplied IP address, the constructor will raise
    :class:`pyuavcan.transport.InvalidMediaConfigurationError`.
    """

    NODE_ID_BIT_LENGTH = 12
    """
    The maximum theoretical number of nodes on the network is determined by raising 2 into this power.
    A node-ID is the set of this many least significant bits of the IP address of the node.
    """

    @staticmethod
    def new(ip_address: str) -> NetworkMap:
        """
        Use this factory to create new instances.
        """
        if ':' in ip_address:
            from ._ipv6 import NetworkMapIPv6
            return NetworkMapIPv6(ip_address)
        else:
            from ._ipv4 import NetworkMapIPv4
            return NetworkMapIPv4(ip_address)

    @property
    @abc.abstractmethod
    def max_nodes(self) -> int:
        """
        The maximum number of nodes that the network can accommodate.
        This is a function of the network prefix capped by the node-ID range limit.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> typing.Optional[int]:
        """
        The node-ID of the local node derived from its IP address;
        None if the local IP address configuration dictates that the node shall be anonymous.
        If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def map_ip_address_to_node_id(self, ip: str) -> typing.Optional[int]:
        """
        Attempts to convert the IP address into a valid node-ID.
        Returns None if the supplied IP address is outside of the node-ID-mapped range within the network
        or belongs to a different subnet.
        This method is intended to be invoked frequently, approx. once per received frame.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_output_socket(self, remote_node_id: typing.Optional[int], remote_port: int) -> socket.socket:
        """
        Make a new non-blocking output socket connected to the specified port at the specified node or broadcast.
        The socket will be bound to an ephemeral port at the configured local network address.
        The required options (such as ``SO_BROADCAST`` etc) will be set up as needed automatically.
        Timestamping will need to be enabled separately.
        Raises :class:`pyuavcan.transport.OperationNotDefinedForAnonymousNodeError` if the local node is anonymous.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_input_socket(self, local_port: int, expect_broadcast: bool) -> socket.socket:
        r"""
        Makes a new non-blocking input socket bound to the specified port.
        The required socket options will be set up as needed automatically.
        Timestamping will need to be enabled separately.

        If the "expect broadcast" parameter is True, the bind address may be INADDR_ANY
        in order to allow reception of broadcast datagrams,
        so the user of the socket will have to filter out packets manually in user space;
        the background is explained here: https://stackoverflow.com/a/58118503/1007777.
        If that parameter is False, the socket may be bound to the local IP address,
        which on some OS will make it reject all broadcast traffic.
        The positive side-effect of binding to a specific address instead of INADDR_ANY
        is that on GNU/Linux it will allow multiple processes to bind to and receive unicast
        datagrams from the same port.

        The above distinction between broadcast-capable and not broadcast-capable sockets is
        important for GNU/Linux because sockets bound to INADDR_ANY can receive both broadcast
        and unicast datagrams, but if there is more than one socket on the same port and
        the same interface, only the last bound one will receive unicast traffic::

            dgram to 127.255.255.255:1234   --------------->    INADDR_ANY:1234 (socket A, OK)
                                                    \
                                                     ------>    INADDR_ANY:1234 (socket B, OK)

            dgram to 127.255.255.255:1234   ------------X       127.0.0.11:1234 (socket A, data lost)
                                                    \
                                                     ---X       127.0.0.22:1234 (socket B, data lost)

            dgram to 127.0.0.11:1234        ------------X       INADDR_ANY:1234 (socket A, data lost)
                                                    \
                                                     ------>    INADDR_ANY:1234 (socket B, OK)

        The expect-broadcast option allows us to run more than one node on localhost side-by-side,
        which is convenient for testing. Otherwise, each node interested in the same UAVCAN service
        would bind to the same UDP port at INADDR_ANY, and on GNU/Linux the result would be that
        only the latest bound node would be able to receive unicast traffic directed to that port.
        To an outside observer it would appear as if the other nodes just ignore the packets.
        On other operating systems this problem may not exist though.

        :param local_port: The UDP port to bind to (function of the data specifier).
        :param expect_broadcast: If True, the socket shall be able to accept broadcast datagrams.
            If False, acceptance of broadcast datagrams is possible but not guaranteed.
            This option enables certain optimizations depending on the underlying OS.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Canonical notation: address plus the netmask width.
        For example: ``192.168.1.200/24``.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, str(self), max_nodes=self.max_nodes)
