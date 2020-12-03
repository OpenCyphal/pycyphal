#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import dataclasses
from ._endpoint_mapping import IPAddress


@dataclasses.dataclass(frozen=True)
class IPHeader:  # The IPv6 implementation may subclass this to add flow info and scope ID.
    """
    Raw IP packet header used to represent sniffed packets.
    The addresses are specialized per protocol version.
    """
    source:      IPAddress
    destination: IPAddress

    def __post_init__(self) -> None:
        if self.source.is_multicast:
            raise ValueError(f'Source IP address cannot be a multicast group address')


@dataclasses.dataclass(frozen=True)
class UDPHeader:
    """
    Raw UDP packet header used to represent sniffed packets.
    """
    source_port:      int
    destination_port: int

    def __post_init__(self) -> None:
        if not (0 <= self.source_port <= 0xFFFF):
            raise ValueError(f'Invalid source port: {self.source_port}')
        if not (0 <= self.destination_port <= 0xFFFF):
            raise ValueError(f'Invalid destination port: {self.destination_port}')


@dataclasses.dataclass(frozen=True)
class UDPIPPacket:
    """
    Raw UDP/IP sniffed packet picked up from the network.
    This may or may not be a valid UAVCAN/UDP transport frame.

    +---------------+---------------+---------------+---------------+
    |**MAC header** | **IP header** |**UDP header** |**UDP payload**|
    +---------------+---------------+---------------+---------------+
    |               |          Layers modeled by this type          |
    +---------------+-----------------------------------------------+
    """
    ip_header:  IPHeader
    udp_header: UDPHeader
    payload:    memoryview
