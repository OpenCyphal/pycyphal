# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan
from pyuavcan.transport import Trace, TransferTrace, Capture, AlienSessionSpecifier, AlienTransferMetadata
from pyuavcan.transport import AlienTransfer, TransferFrom, Timestamp
from pyuavcan.transport.commons.high_overhead_transport import AlienTransferReassembler, TransferReassembler
from ._frame import UDPFrame
from ._ip import RawPacket, SUBJECT_PORT
from ._ip import unicast_ip_to_node_id, udp_port_to_service_data_specifier, multicast_group_to_message_data_specifier


@dataclasses.dataclass(frozen=True)
class UDPCapture(pyuavcan.transport.Capture):
    """
    See :meth:`pyuavcan.transport.udp.UDPTransport.begin_capture` for details.
    """

    packet: RawPacket

    def parse(self) -> typing.Optional[typing.Tuple[pyuavcan.transport.AlienSessionSpecifier, UDPFrame]]:
        """
        The parsed representation is only defined if the packet is a valid UAVCAN/UDP frame.
        The source node-ID is never None.
        """
        ip_header = self.packet.ip_header

        dst_nid: typing.Optional[int]
        data_spec: typing.Optional[pyuavcan.transport.DataSpecifier]
        if ip_header.destination.is_multicast:
            if self.packet.udp_header.destination_port != SUBJECT_PORT:
                return None
            dst_nid = None  # Broadcast
            data_spec = multicast_group_to_message_data_specifier(ip_header.source, ip_header.destination)
        else:
            dst_nid = unicast_ip_to_node_id(ip_header.source, ip_header.destination)
            if dst_nid is None:  # The packet crosses the UAVCAN/UDP subnet boundary, invalid.
                return None
            data_spec = udp_port_to_service_data_specifier(self.packet.udp_header.destination_port)

        if data_spec is None:
            return None

        frame = UDPFrame.parse(self.packet.udp_payload)
        if frame is None:
            return None

        src_nid = unicast_ip_to_node_id(ip_header.source, ip_header.source)
        assert src_nid is not None
        ses_spec = pyuavcan.transport.AlienSessionSpecifier(
            source_node_id=src_nid, destination_node_id=dst_nid, data_specifier=data_spec
        )
        return ses_spec, frame


@dataclasses.dataclass(frozen=True)
class UDPErrorTrace(pyuavcan.transport.ErrorTrace):
    error: TransferReassembler.Error


class UDPTracer(pyuavcan.transport.Tracer):
    """
    This is like a Wireshark dissector but UAVCAN-focused.
    Return types from :meth:`update`:

    - :class:`pyuavcan.transport.TransferTrace`
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
        elif isinstance(tr, TransferFrom):
            meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
            return TransferTrace(timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)
        else:
            assert tr is None
        return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_tracer() -> None:
    from pytest import raises, approx
    from ipaddress import ip_address
    from pyuavcan.transport import Priority, ServiceDataSpecifier
    from pyuavcan.transport.udp import UDPTransport
    from ._ip import MACHeader, IPHeader, UDPHeader, service_data_specifier_to_udp_port

    tr = UDPTransport.make_tracer()
    ts = Timestamp.now()

    ds = ServiceDataSpecifier(11, ServiceDataSpecifier.Role.RESPONSE)
    trace = tr.update(
        UDPCapture(
            ts,
            RawPacket(
                MACHeader(memoryview(b""), memoryview(b"")),
                IPHeader(ip_address("127.0.0.42"), ip_address("127.0.0.63")),
                UDPHeader(12345, service_data_specifier_to_udp_port(ds)),
                memoryview(
                    b"".join(
                        UDPFrame(
                            priority=Priority.SLOW,
                            transfer_id=1234567890,
                            index=0,
                            end_of_transfer=True,
                            payload=memoryview(b"Hello world!"),
                        ).compile_header_and_payload()
                    )
                ),
            ),
        )
    )
    assert isinstance(trace, TransferTrace)
    assert trace.timestamp == ts
    assert trace.transfer_id_timeout == approx(AlienTransferReassembler.MAX_TRANSFER_ID_TIMEOUT)  # Initial value.
    assert trace.transfer.metadata.transfer_id == 1234567890
    assert trace.transfer.metadata.priority == Priority.SLOW
    assert trace.transfer.metadata.session_specifier.source_node_id == 42
    assert trace.transfer.metadata.session_specifier.destination_node_id == 63
    assert trace.transfer.metadata.session_specifier.data_specifier == ds
    assert trace.transfer.fragmented_payload == [memoryview(b"Hello world!")]

    assert None is tr.update(pyuavcan.transport.Capture(ts))  # Another transport, ignored.

    assert None is tr.update(
        UDPCapture(  # Malformed frame.
            ts,
            RawPacket(
                MACHeader(memoryview(b""), memoryview(b"")),
                IPHeader(ip_address("127.0.0.42"), ip_address("127.1.0.63")),
                UDPHeader(1, 1),
                memoryview(b""),
            ),
        )
    )
