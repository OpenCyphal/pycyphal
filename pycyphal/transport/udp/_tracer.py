# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import struct
import dataclasses
from ipaddress import IPv4Address, IPv6Address
import pycyphal
import pycyphal.transport.udp
from pycyphal.transport import Trace, TransferTrace, Capture, AlienSessionSpecifier, AlienTransferMetadata
from pycyphal.transport import AlienTransfer, TransferFrom, Timestamp
from pycyphal.transport.commons.high_overhead_transport import AlienTransferReassembler, TransferReassembler
from pycyphal.transport.commons.high_overhead_transport import TransferCRC
from ._frame import UDPFrame
from ._ip import LinkLayerPacket, CYPHAL_PORT


@dataclasses.dataclass(frozen=True)
class IPPacket:
    protocol: int
    payload: memoryview

    @property
    def source_destination(
        self,
    ) -> typing.Union[typing.Tuple[IPv4Address, IPv4Address], typing.Tuple[IPv6Address, IPv6Address]]:
        raise NotImplementedError

    @staticmethod
    def parse(link_layer_packet: LinkLayerPacket) -> typing.Optional[IPPacket]:
        import socket

        if link_layer_packet.protocol == socket.AF_INET:
            return IPv4Packet.parse_payload(link_layer_packet.payload)
        if link_layer_packet.protocol == socket.AF_INET6:
            return IPv6Packet.parse_payload(link_layer_packet.payload)
        return None


@dataclasses.dataclass(frozen=True)
class IPv4Packet(IPPacket):
    source: IPv4Address
    destination: IPv4Address

    _FORMAT = struct.Struct("!BB HHH BB H II")

    def __post_init__(self) -> None:
        if self.source.is_multicast:
            raise ValueError(f"Source IP address cannot be a multicast group address")

    @property
    def source_destination(self) -> typing.Tuple[IPv4Address, IPv4Address]:
        return self.source, self.destination

    @staticmethod
    def parse_payload(link_layer_payload: memoryview) -> typing.Optional[IPv4Packet]:
        try:
            (
                ver_ihl,
                _dscp_ecn,
                total_length,
                _ident,
                _flags_frag_off,
                _ttl,
                proto,
                _hdr_chk,
                src_adr,
                dst_adr,
            ) = IPv4Packet._FORMAT.unpack_from(link_layer_payload)
        except struct.error:
            return None
        ver, ihl = ver_ihl >> 4, ver_ihl & 0xF
        if ver == 4:
            payload = link_layer_payload[ihl * 4 : total_length]
            return IPv4Packet(
                protocol=proto,
                payload=payload,
                source=IPv4Address(src_adr),
                destination=IPv4Address(dst_adr),
            )
        return None


@dataclasses.dataclass(frozen=True)
class IPv6Packet(IPPacket):
    source: IPv6Address
    destination: IPv6Address

    @property
    def source_destination(self) -> typing.Tuple[IPv6Address, IPv6Address]:
        return self.source, self.destination

    @staticmethod
    def parse_payload(link_layer_payload: memoryview) -> typing.Optional[IPv6Packet]:
        raise NotImplementedError("Support for IPv6 is not implemented yet")


@dataclasses.dataclass(frozen=True)
class UDPIPPacket:
    source_port: int
    destination_port: int
    payload: memoryview

    _FORMAT = struct.Struct("!HH HH")

    def __post_init__(self) -> None:
        if not (0 <= self.source_port <= 0xFFFF):
            raise ValueError(f"Invalid source port: {self.source_port}")
        if self.destination_port != CYPHAL_PORT:
            raise ValueError(f"Invalid destination port: {self.destination_port}")

    @staticmethod
    def parse(ip_packet: IPPacket) -> typing.Optional[UDPIPPacket]:
        if ip_packet.protocol != 0x11:  # https://en.wikipedia.org/wiki/List_of_IP_protocol_numbers
            return None
        try:
            src_port, dst_port, total_length, _udp_chk = UDPIPPacket._FORMAT.unpack_from(ip_packet.payload)
        except struct.error:
            return None
        payload = ip_packet.payload[UDPIPPacket._FORMAT.size : total_length]
        return UDPIPPacket(source_port=src_port, destination_port=dst_port, payload=payload)


@dataclasses.dataclass(frozen=True)
class UDPCapture(Capture):
    """
    The UDP transport does not differentiate between sent and received packets.
    See :meth:`pycyphal.transport.udp.UDPTransport.begin_capture` for details.
    """

    link_layer_packet: LinkLayerPacket

    def parse(self) -> typing.Optional[typing.Tuple[pycyphal.transport.AlienSessionSpecifier, UDPFrame]]:
        """
        The parsed representation is only defined if the packet is a valid Cyphal/UDP frame.
        The source node-ID can be None in the case of anonymous messages.
        """
        ip_packet = IPPacket.parse(self.link_layer_packet)
        if ip_packet is None:
            return None

        udp_packet = UDPIPPacket.parse(ip_packet)
        if udp_packet is None:
            return None

        frame = UDPFrame.parse(udp_packet.payload)
        if frame is None:
            return None

        src_nid = frame.source_node_id
        dst_nid = frame.destination_node_id
        data_spec = frame.data_specifier
        ses_spec = pycyphal.transport.AlienSessionSpecifier(
            source_node_id=src_nid, destination_node_id=dst_nid, data_specifier=data_spec
        )
        return ses_spec, frame

    @staticmethod
    def get_transport_type() -> typing.Type[pycyphal.transport.udp.UDPTransport]:
        return pycyphal.transport.udp.UDPTransport


@dataclasses.dataclass(frozen=True)
class UDPErrorTrace(pycyphal.transport.ErrorTrace):
    error: TransferReassembler.Error


class UDPTracer(pycyphal.transport.Tracer):
    """
    This is like a Wireshark dissector but Cyphal-focused.
    Return types from :meth:`update`:

    - :class:`pycyphal.transport.TransferTrace`
    - :class:`UDPErrorTrace`
    """

    def __init__(self) -> None:
        self._sessions: typing.Dict[AlienSessionSpecifier, _AlienSession] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if not isinstance(cap, UDPCapture):
            return None

        parsed = cap.parse()
        if not parsed:
            return None

        spec, frame = parsed
        return self._get_session(spec).update(cap.timestamp, frame)

    def _get_session(self, specifier: AlienSessionSpecifier) -> _AlienSession:
        try:
            return self._sessions[specifier]
        except KeyError:
            self._sessions[specifier] = _AlienSession(specifier)
        return self._sessions[specifier]


class _AlienSession:
    def __init__(self, specifier: AlienSessionSpecifier) -> None:
        assert specifier.source_node_id is not None
        self._specifier = specifier
        self._reassembler = AlienTransferReassembler(specifier.source_node_id)

    def update(self, timestamp: Timestamp, frame: UDPFrame) -> typing.Optional[Trace]:
        tid_timeout = self._reassembler.transfer_id_timeout
        tr = self._reassembler.process_frame(timestamp, frame)
        if isinstance(tr, TransferReassembler.Error):
            return UDPErrorTrace(timestamp=timestamp, error=tr)
        if isinstance(tr, TransferFrom):
            meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
            return TransferTrace(timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)
        assert tr is None
        return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_tracer() -> None:
    import socket
    from pytest import approx
    from ipaddress import ip_address
    from pycyphal.transport import Priority, ServiceDataSpecifier
    from pycyphal.transport.udp import UDPTransport

    tr = UDPTransport.make_tracer()
    ts = Timestamp.now()
    ds = ServiceDataSpecifier(service_id=11, role=ServiceDataSpecifier.Role.REQUEST)

    # VALID SERVICE FRAME
    llp = LinkLayerPacket(
        protocol=socket.AF_INET,
        source=memoryview(b""),
        destination=memoryview(b""),
        payload=memoryview(
            b"".join(
                [
                    # IPv4
                    b"\x45\x00",
                    (20 + 8 + 24 + 12 + 4).to_bytes(2, "big"),  # Total length (incl. the 20 bytes of the IP header)
                    b"\x7e\x50\x40\x00\x40",  # ID, flags, fragment offset, TTL
                    b"\x11",  # Protocol (UDP)
                    b"\x00\x00",  # IP checksum (unset)
                    ip_address("127.0.0.1").packed,  # Source
                    ip_address("239.1.0.63").packed,  # Destination
                    # UDP/IP
                    CYPHAL_PORT.to_bytes(2, "big"),  # Source port
                    CYPHAL_PORT.to_bytes(2, "big"),  # Destination port
                    (8 + 24 + 12 + 4).to_bytes(2, "big"),  # Total length (incl. the 8 bytes of the UDP header)
                    b"\x00\x00",  # UDP checksum (unset)
                    # Cyphal/UDP
                    b"".join(
                        UDPFrame(
                            priority=Priority.SLOW,
                            source_node_id=42,
                            destination_node_id=63,
                            data_specifier=ds,
                            transfer_id=1234567890,
                            index=0,
                            end_of_transfer=True,
                            user_data=0,
                            payload=memoryview(b"Hello world!" + TransferCRC.new(b"Hello world!").value_as_bytes),
                        ).compile_header_and_payload()
                    ),
                ]
            )
        ),
    )

    ip_packet = IPPacket.parse(llp)
    assert ip_packet is not None
    assert ip_packet.source_destination == (ip_address("127.0.0.1"), ip_address("239.1.0.63"))
    assert ip_packet.protocol == 0x11
    udp_packet = UDPIPPacket.parse(ip_packet)
    assert udp_packet is not None
    assert udp_packet.source_port == CYPHAL_PORT
    assert udp_packet.destination_port == CYPHAL_PORT
    trace = tr.update(UDPCapture(ts, llp))
    assert isinstance(trace, TransferTrace)
    assert trace.timestamp == ts
    assert trace.transfer_id_timeout == approx(2.0)  # Initial value.
    assert trace.transfer.metadata.transfer_id == 1234567890
    assert trace.transfer.metadata.priority == Priority.SLOW
    assert trace.transfer.metadata.session_specifier.source_node_id == 42
    assert trace.transfer.metadata.session_specifier.destination_node_id == 63
    assert trace.transfer.metadata.session_specifier.data_specifier == ds
    assert trace.transfer.fragmented_payload == [memoryview(b"Hello world!")]

    # ANOTHER TRANSPORT, IGNORED
    assert None is tr.update(pycyphal.transport.Capture(ts))

    # MALFORMED - Cyphal/UDP IS EMPTY
    llp = LinkLayerPacket(
        protocol=socket.AF_INET,
        source=memoryview(b""),
        destination=memoryview(b""),
        payload=memoryview(
            b"".join(
                [
                    # IPv4
                    b"\x45\x00",
                    (20 + 8 + 24 + 12).to_bytes(2, "big"),  # Total length (incl. the 20 bytes of the IP header)
                    b"\x7e\x50\x40\x00\x40",  # ID, flags, fragment offset, TTL
                    b"\x11",  # Protocol (UDP)
                    b"\x00\x00",  # IP checksum (unset)
                    ip_address("127.0.0.42").packed,  # Source
                    ip_address("239.1.0.63").packed,  # Destination
                    # UDP/IP
                    CYPHAL_PORT.to_bytes(2, "big"),  # Source port
                    CYPHAL_PORT.to_bytes(2, "big"),  # Destination port
                    (8).to_bytes(2, "big"),  # Total length (incl. the 8 bytes of the UDP header)
                    b"\x00\x00",  # UDP checksum (unset)
                    # Cyphal/UDP is missing
                ]
            )
        ),
    )
    ip_packet = IPPacket.parse(llp)
    assert ip_packet is not None
    assert ip_packet.source_destination == (ip_address("127.0.0.42"), ip_address("239.1.0.63"))
    assert ip_packet.protocol == 0x11
    udp_packet = UDPIPPacket.parse(ip_packet)
    assert udp_packet is not None
    assert udp_packet.source_port == CYPHAL_PORT
    assert udp_packet.destination_port == CYPHAL_PORT
    assert None is tr.update(UDPCapture(ts, llp))
