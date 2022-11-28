# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# pylint: disable=protected-access

from __future__ import annotations
import sys
import time
import typing
import socket
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier, UnsupportedSessionConfigurationError
from pycyphal.transport import InvalidMediaConfigurationError, Timestamp
from pycyphal.transport.udp._ip._socket_factory import SocketFactory
from pycyphal.transport.udp._ip._endpoint_mapping import SUBJECT_PORT, service_data_specifier_to_udp_port
from pycyphal.transport.udp._ip._v4 import SnifferIPv4, IPv4SocketFactory
from pycyphal.transport.udp._ip import LinkLayerCapture
from pycyphal.transport.udp import IPPacket, LinkLayerPacket, UDPIPPacket


def _unittest_socket_factory() -> None:
    from pytest import raises
    from ipaddress import IPv4Address

    is_linux = sys.platform.startswith("linux")

    fac = SocketFactory.new(IPv4Address("127.0.0.1"), subnet_id=13)
    assert fac.max_nodes == 0xFFFF
    assert str(fac.local_ip_address) == "127.0.0.1"
    assert fac.subnet_id == 13

    # SERVICE SOCKET TEST
    ds = ServiceDataSpecifier(100, ServiceDataSpecifier.Role.REQUEST)
    test_srv_i = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_srv_i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    test_srv_i.bind(("239.53.1.200" * is_linux, service_data_specifier_to_udp_port(ds)))
    test_srv_i.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.53.1.200") + socket.inet_aton("127.0.0.1")
    )

    srv_o = fac.make_output_socket(456, ds)
    srv_o.send(b"Goose")
    rx = test_srv_i.recvfrom(1024)
    assert rx[0] == b"Goose"
    assert rx[1][0] == "127.0.0.1"

    test_srv_o = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_srv_o.bind(("127.0.0.1", 0))
    test_srv_o.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))

    srv_i = fac.make_input_socket(456, ds)
    test_srv_o.sendto(b"Duck", ("239.53.1.200", service_data_specifier_to_udp_port(ds)))
    time.sleep(1)
    rx = srv_i.recvfrom(1024)
    assert rx[0] == b"Duck"
    assert rx[1][0] == "127.0.0.1"

    # MESSAGE SOCKET TEST (multicast)
    # Note that Windows does not permit using the same socket for both sending to and receiving from a unicast group
    # because in order to specify a particular output interface the socket must be bound to a unicast address.
    # So we set up separate sockets for input and output.
    test_msg_i = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_msg_i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    test_msg_i.bind(("239.52.2.100" * is_linux, SUBJECT_PORT))
    test_msg_i.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.52.2.100") + socket.inet_aton("127.0.0.1")
    )

    msg_o = fac.make_output_socket(None, MessageDataSpecifier(612))
    msg_o.send(b"Eagle")
    rx = test_msg_i.recvfrom(1024)
    assert rx[0] == b"Eagle"
    assert rx[1][0] == "127.0.0.1"

    test_msg_o = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_msg_o.bind(("127.0.0.1", 0))
    test_msg_o.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))

    msg_i = fac.make_input_socket(None, MessageDataSpecifier(612))
    test_msg_o.sendto(b"Seagull", ("239.52.2.100", SUBJECT_PORT))
    time.sleep(1)
    rx = msg_i.recvfrom(1024)
    assert rx[0] == b"Seagull"
    assert rx[1][0] == "127.0.0.1"  # Same address we just bound to.

    # ERRORS
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4"), 13).make_input_socket(
            456, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4"), 13).make_input_socket(None, MessageDataSpecifier(0))
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4"), 13).make_output_socket(
            1, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4"), 13).make_output_socket(1, MessageDataSpecifier(0))

    with raises(UnsupportedSessionConfigurationError):
        fac.make_output_socket(1, MessageDataSpecifier(0))
    with raises(UnsupportedSessionConfigurationError):
        fac.make_output_socket(None, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE))

    # CLEAN UP
    # test_u.close()
    test_srv_i.close()
    test_srv_o.close()
    test_msg_i.close()
    test_msg_o.close()
    srv_o.close()
    srv_i.close()
    msg_o.close()
    msg_i.close()


def _unittest_sniffer() -> None:
    from ipaddress import ip_address

    def parse_ip(ll_pkt: LinkLayerPacket) -> IPPacket:
        ip_pkt = IPPacket.parse(ll_pkt)
        assert ip_pkt is not None
        return ip_pkt

    def parse_udp(ll_pkt: LinkLayerPacket) -> UDPIPPacket:
        udp_pkt = UDPIPPacket.parse(parse_ip(ll_pkt))
        assert udp_pkt is not None
        return udp_pkt

    # The sniffer is expected to drop all traffic except from 127.66.0.0/16
    fac = SocketFactory.new(ip_address("127.66.1.200"))

    ts_last = Timestamp.now()
    sniffs: typing.List[LinkLayerCapture] = []

    def sniff_sniff(cap: LinkLayerCapture) -> None:
        nonlocal ts_last
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= cap.timestamp.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= cap.timestamp.system_ns <= now.system_ns
        ts_last = cap.timestamp
        # Make sure that all traffic from foreign networks is filtered out by the sniffer.
        assert (int(parse_ip(cap.packet).source_destination[0]) & 0x_FFFF_0000) == (
            int(fac.local_ip_address) & 0x_FFFF_0000
        )
        sniffs.append(cap)

    sniffer = fac.make_sniffer(sniff_sniff)
    assert isinstance(sniffer, SnifferIPv4)
    assert sniffer._link_layer._filter_expr == "udp and src net 127.64.0.0/14"

    # The sink socket is needed for compatibility with Windows. On Windows, an attempt to transmit to a loopback
    # multicast group for which there are no receivers may fail with the following errors:
    #   OSError: [WinError 10051]   A socket operation was attempted to an unreachable network
    #   OSError: [WinError 1231]    The network location cannot be reached. For information about network
    #                               troubleshooting, see Windows Help
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sink.bind(("", 4444))
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.66.1.200") + socket.inet_aton("127.42.0.123")
    )

    outside = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    outside.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.42.0.123"))
    outside.bind(("127.42.0.123", 0))  # Some random noise on an adjacent subnet.
    for i in range(10):
        outside.sendto(f"{i:04x}".encode(), ("127.66.1.200", 3333))  # Ignored unicast
        outside.sendto(f"{i:04x}".encode(), ("239.66.1.200", 4444))  # Ignored multicast
        time.sleep(0.1)

    time.sleep(1)
    assert sniffs == []  # Make sure we are not picking up any noise.

    inside = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    inside.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.66.33.44"))
    inside.bind(("127.66.33.44", 0))  # This one is on our local subnet, it should be heard.
    inside.sendto(b"\xAA\xAA\xAA\xAA", ("127.66.1.200", 1234))  # Accepted unicast inside subnet
    inside.sendto(b"\xBB\xBB\xBB\xBB", ("127.33.1.200", 5678))  # Accepted unicast outside subnet
    inside.sendto(b"\xCC\xCC\xCC\xCC", ("239.66.1.200", 4444))  # Accepted multicast

    outside.sendto(b"x", ("127.66.1.200", 5555))  # Ignored unicast
    outside.sendto(b"y", ("239.66.1.200", 4444))  # Ignored multicast

    time.sleep(3)

    # Validate the received callbacks.
    print(sniffs[0])
    print(sniffs[1])
    print(sniffs[2])
    assert len(sniffs) == 3

    # The MAC address length may be either 6 bytes (Ethernet encapsulation) or 0 bytes (null/loopback encapsulation)
    assert len(sniffs[0].packet.source) == len(sniffs[0].packet.destination)
    assert len(sniffs[1].packet.source) == len(sniffs[1].packet.destination)
    assert len(sniffs[2].packet.source) == len(sniffs[2].packet.destination)

    assert parse_ip(sniffs[0].packet).source_destination == (ip_address("127.66.33.44"), ip_address("127.66.1.200"))
    assert parse_ip(sniffs[1].packet).source_destination == (ip_address("127.66.33.44"), ip_address("127.33.1.200"))
    assert parse_ip(sniffs[2].packet).source_destination == (ip_address("127.66.33.44"), ip_address("239.66.1.200"))

    assert parse_udp(sniffs[0].packet).destination_port == 1234
    assert parse_udp(sniffs[1].packet).destination_port == 5678
    assert parse_udp(sniffs[2].packet).destination_port == 4444

    assert bytes(parse_udp(sniffs[0].packet).payload) == b"\xAA\xAA\xAA\xAA"
    assert bytes(parse_udp(sniffs[1].packet).payload) == b"\xBB\xBB\xBB\xBB"
    assert bytes(parse_udp(sniffs[2].packet).payload) == b"\xCC\xCC\xCC\xCC"

    sniffs.clear()

    # CLOSE and make sure we don't get any additional callbacks.
    sniffer.close()
    time.sleep(2)
    inside.sendto(b"d", ("127.66.1.100", SUBJECT_PORT))
    time.sleep(1)
    assert sniffs == []  # Should be terminated.

    # DISPOSE OF THE RESOURCES
    sniffer.close()
    outside.close()
    inside.close()
    sink.close()
