# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses
import pyuavcan.util
from ._endpoint_mapping import IPAddress


@dataclasses.dataclass(frozen=True)
class MACHeader:
    """
    The link-layer header model.
    The source and the destination addresses are represented in the original, network byte order.
    Usually these are EUI-48 MAC addresses.
    """

    source: memoryview
    destination: memoryview

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(
            self, source=bytes(self.source).hex(), destination=bytes(self.destination).hex()
        )


@dataclasses.dataclass(frozen=True)
class IPHeader:  # The IPv6 implementation may subclass this to add flow info and scope ID.
    """
    Raw IP packet header used to represent captured packets.
    The addresses are specialized per protocol version.
    """

    source: IPAddress
    destination: IPAddress

    def __post_init__(self) -> None:
        if self.source.is_multicast:
            raise ValueError(f"Source IP address cannot be a multicast group address")

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, source=str(self.source), destination=str(self.destination))


@dataclasses.dataclass(frozen=True)
class UDPHeader:
    """
    Raw UDP packet header used to represent captured packets.
    """

    source_port: int
    destination_port: int

    def __post_init__(self) -> None:
        if not (0 <= self.source_port <= 0xFFFF):
            raise ValueError(f"Invalid source port: {self.source_port}")
        if not (0 <= self.destination_port <= 0xFFFF):
            raise ValueError(f"Invalid destination port: {self.destination_port}")


@dataclasses.dataclass(frozen=True)
class RawPacket:
    """
    Raw UDP/IP captured packet picked up from the network.
    This may or may not be a valid UAVCAN/UDP transport frame.
    This type models the entire protocol stack up to UDP (L4), inclusive:

    +---------------+---------------+---------------+---------------+
    |**MAC header** | **IP header** |**UDP header** |**UDP payload**|
    +---------------+---------------+---------------+---------------+
    """

    mac_header: MACHeader
    ip_header: IPHeader
    udp_header: UDPHeader
    udp_payload: memoryview

    def __repr__(self) -> str:
        """
        If the payload is large (ca. a hundred bytes), it may be truncated,
        in which case an ellipsis will be added at the end.
        """
        limit = 100
        if len(self.udp_payload) <= limit:
            pld = bytes(self.udp_payload).hex()
        else:
            pld = bytes(self.udp_payload[:limit]).hex() + "..."
        return pyuavcan.util.repr_attributes(
            self, mac_header=self.mac_header, ip_header=self.ip_header, udp_header=self.udp_header, udp_payload=pld
        )
