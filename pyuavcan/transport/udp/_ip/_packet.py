#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import dataclasses


@dataclasses.dataclass(frozen=True)
class IPHeader:  # The IPv6 implementation may subclass this to add flow info and scope ID.
    """
    Raw IP packet header used to represent sniffed packets.
    """
    source_address:      str
    destination_address: str


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
