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

    If none of the available network interfaces have the supplied IP address, the constructor will raise
    :class:`pyuavcan.transport.InvalidMediaConfigurationError`.
    """

    #: The maximum theoretical number of nodes on the network is determined by raising 2 into this power.
    #: A node-ID is the set of this many least significant bits of the IP address of the node.
    NODE_ID_BIT_LENGTH = 12

    @staticmethod
    def new(ip_address: str) -> NetworkMap:
        """
        Use this factory to create new instances.
        """
        if ':' in ip_address:
            from ._ipv6 import NetworkMapIPv6
            return NetworkMapIPv6(ip_address)  # type: ignore
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
    def local_node_id(self) -> int:
        """
        The concept of anonymous node is not defined for UDP/IP; in this transport, every node always has a node-ID.
        If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def map_ip_address_to_node_id(self, ip: str) -> typing.Optional[int]:
        """
        Attempts to convert the IP address into a valid node-ID.
        Returns None if the supplied IP address is outside of the node-ID-mapped range within the network
        or belongs to a different subnet.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_output_socket(self, remote_node_id: typing.Optional[int], remote_port: int) -> socket.socket:
        """
        Make a new non-blocking output socket connected to the specified port at the specified node or broadcast.
        The socket will be bound to an ephemeral port at the configured local network address.
        The required options (such as ``SO_BROADCAST`` etc) will be set up as needed automatically.
        Timestamping will need to be enabled separately.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def make_input_socket(self, local_port: int) -> socket.socket:
        """
        Makes a new non-blocking input socket bound to the specified port and the configured network address.
        By virtue of being bound, the socket will only receive data from the target subnet.
        The required socket options will be set up as needed automatically.
        Timestamping will need to be enabled separately.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Canonical subnet notation.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, str(self), max_nodes=self.max_nodes)
