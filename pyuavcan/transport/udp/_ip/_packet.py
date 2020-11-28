#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses


class IPAddress(abc.ABC):
    """
    This interface is implemented per IP version.
    It models the IP address of a particular host along with the subnet it is contained in,
    and also implements the IP address to node-ID mapping logic.
    For example:

    - 192.168.1.200/24:
        - Host:      192.168.1.200
        - Network:   192.168.1.0
        - Broadcast: 192.168.1.255
        - Node-ID:   200
    """

    @property
    @abc.abstractmethod
    def node_id(self) -> int:
        """
        Maps the IP address to the UAVCAN Node-ID by clearing out the network mask bits.
        Note that if the address belongs to a different subnet, the result will be nonsensical.
        E.g., 42 for ``192.168.1.42/24`` or ``192.168.222.42/24`` or ``192.168.0.42/16``.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def netmask_width(self) -> int:
        """
        A netmask that equals the address bit length (32 bits for IPv4, 128 bits for IPv6) represents a
        specific host address rather than a subnet.
        E.g., 24 for ``192.168.1.42/24``.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def subnet_address(self) -> IPAddress:
        """
        E.g., ``192.168.1.0/24`` for ``192.168.1.42/24``.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def broadcast_address(self) -> IPAddress:
        """
        E.g., ``192.168.1.255/24`` for ``192.168.1.42/24``.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __contains__(self, item: typing.Union[int, IPAddress]) -> bool:
        """
        Returns True if the provided address falls into the subnet of the current instance,
        including its subnet and broadcast addresses.
        The subnet of the provided address (if any) is ignored.
        It follows that this method works as an equality check if netmask width equals address width.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __eq__(self, other: object) -> bool:
        """
        If the operand is an integer, returns True if it equals the host address (netmask ignored).
        If the operand is another instance of same type, returns True if the host address and the netmask are equal.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __int__(self) -> int:
        """
        Host address as an integer, netmask discarded.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Returns canonical address representation.
        A netmask that equals the address bit length (32 bits for IPv4, 128 bits for IPv6) represents a
        specific host address rather than a subnet.
        Host addresses are converted to string without the trailing netmask specifier.
        Example:
        - ``192.168.1.42/24``
        - ``192.168.1.42``  (``/32`` implied)
        """
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class IPHeader:  # The IPv6 implementation may subclass this to add flow info and scope ID.
    """
    Raw IP packet header used to represent sniffed packets.
    The addresses are specialized per protocol version.
    The :class:`IPAddress` class also implements the mapping logic between IP addresses and UAVCAN node-ID.
    """
    source:      IPAddress
    destination: IPAddress


@dataclasses.dataclass(frozen=True)
class UDPHeader:
    """
    Raw UDP packet header used to represent sniffed packets.
    """
    source_port:      int
    destination_port: int


@dataclasses.dataclass(frozen=True)
class UDPIPPacket:
    """
    Raw UDP/IP sniffed packet picked up from the network.
    This may not be a valid UAVCAN/UDP transport frame.
    """
    ip_header:   IPHeader
    udp_header:  UDPHeader
    udp_payload: memoryview


class Sniffer(abc.ABC):
    """
    Network sniffer is responsible for managing the raw socket and parsing and filtering the raw IP packets.
    """
    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError
