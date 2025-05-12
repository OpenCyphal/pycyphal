# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# pylint: disable=protected-access

from __future__ import annotations
import sys
import time
import typing
import socket
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier
from pycyphal.transport import InvalidMediaConfigurationError, Timestamp
from pycyphal.transport.udp._ip._socket_factory import SocketFactory
from pycyphal.transport.udp._ip._endpoint_mapping import CYPHAL_PORT
from pycyphal.transport.udp._ip._v4 import SnifferIPv4, IPv4SocketFactory
from pycyphal.transport.udp._ip import LinkLayerCapture
from pycyphal.transport.udp import IPPacket, LinkLayerPacket, UDPIPPacket


def _unittest_socket_factory() -> None:
    from pytest import raises
    from ipaddress import IPv4Address

    is_linux = sys.platform.startswith("linux") or sys.platform.startswith("darwin")

    fac = SocketFactory.new(IPv4Address("127.0.0.1"))
    assert isinstance(fac, IPv4SocketFactory)
    assert fac.max_nodes == 0xFFFF
    assert str(fac.local_ip_address) == "127.0.0.1"

    # SERVICE SOCKET TEST
    ds = ServiceDataSpecifier(100, ServiceDataSpecifier.Role.REQUEST)
    test_srv_i = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_srv_i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    test_srv_i.bind(("239.1.1.200" * is_linux, CYPHAL_PORT))
    test_srv_i.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.1.1.200") + socket.inet_aton("127.0.0.1")
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
    test_srv_o.sendto(b"Duck", ("239.1.1.200", CYPHAL_PORT))
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
    test_msg_i.bind(("239.0.2.100" * is_linux, CYPHAL_PORT))
    test_msg_i.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.0.2.100") + socket.inet_aton("127.0.0.1")
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
    test_msg_o.sendto(b"Seagull", ("239.0.2.100", CYPHAL_PORT))
    time.sleep(1)
    rx = msg_i.recvfrom(1024)
    assert rx[0] == b"Seagull"
    assert rx[1][0] == "127.0.0.1"  # Same address we just bound to.

    # ERRORS
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4")).make_input_socket(
            456, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4")).make_input_socket(None, MessageDataSpecifier(0))
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4")).make_output_socket(
            1, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(IPv4Address("1.2.3.4")).make_output_socket(1, MessageDataSpecifier(0))

    with raises(AssertionError):
        fac.make_output_socket(1, MessageDataSpecifier(0))

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

    is_linux = sys.platform.startswith("linux") or sys.platform.startswith("darwin")

    fac = SocketFactory.new(ip_address("127.0.0.1"))
    assert isinstance(fac, IPv4SocketFactory)

    ts_last = Timestamp.now()
    sniffs: typing.List[LinkLayerCapture] = []

    def sniff_sniff(cap: LinkLayerCapture) -> None:
        nonlocal ts_last
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= cap.timestamp.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= cap.timestamp.system_ns <= now.system_ns
        ts_last = cap.timestamp
        # Make sure that all traffic from foreign networks is filtered out by the sniffer.
        assert isinstance(fac, IPv4SocketFactory)
        assert (int(parse_ip(cap.packet).source_destination[0]) & 0x_FFFE_0000) == (
            int(fac.local_ip_address) & 0x_FFFE_0000
        )
        sniffs.append(cap)

    # The sniffer is expected to drop all traffic except from 239.0.0.0/15.
    sniffer = fac.make_sniffer(sniff_sniff)
    assert isinstance(sniffer, SnifferIPv4)
    assert sniffer._link_layer._filter_expr == "udp and dst net 239.0.0.0/15"

    # The sink socket is needed for compatibility with Windows. On Windows, an attempt to transmit to a loopback
    # multicast group for which there are no receivers may fail with the following errors:
    #   OSError: [WinError 10051]   A socket operation was attempted to an unreachable network
    #   OSError: [WinError 1231]    The network location cannot be reached. For information about network
    #                               troubleshooting, see Windows Help
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sink.bind(("239.0.1.200" * is_linux, CYPHAL_PORT))
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.2.1.200") + socket.inet_aton("127.0.0.1")
    )
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.0.1.199") + socket.inet_aton("127.0.0.1")
    )
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.0.1.200") + socket.inet_aton("127.0.0.1")
    )
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.0.1.201") + socket.inet_aton("127.0.0.1")
    )

    outside = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    outside.bind(("127.0.0.1", 0))
    outside.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))
    for i in range(10):
        outside.sendto(f"{i:04x}".encode(), ("239.2.1.200", CYPHAL_PORT))  # Ignored multicast
        time.sleep(0.1)

    time.sleep(1)
    assert sniffs == []  # Make sure we are not picking up any noise.

    inside = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    inside.bind(("127.0.0.1", 0))
    inside.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))
    inside.sendto(b"\xaa\xaa\xaa\xaa", ("239.0.1.199", CYPHAL_PORT))  # Accepted multicast
    inside.sendto(b"\xbb\xbb\xbb\xbb", ("239.0.1.200", CYPHAL_PORT))  # Accepted multicast
    inside.sendto(b"\xcc\xcc\xcc\xcc", ("239.0.1.201", CYPHAL_PORT))  # Accepted multicast

    outside.sendto(b"y", ("239.2.1.200", CYPHAL_PORT))  # Ignored multicast

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

    assert parse_ip(sniffs[0].packet).source_destination == (ip_address("127.0.0.1"), ip_address("239.0.1.199"))
    assert parse_ip(sniffs[1].packet).source_destination == (ip_address("127.0.0.1"), ip_address("239.0.1.200"))
    assert parse_ip(sniffs[2].packet).source_destination == (ip_address("127.0.0.1"), ip_address("239.0.1.201"))

    assert parse_udp(sniffs[0].packet).destination_port == CYPHAL_PORT
    assert parse_udp(sniffs[1].packet).destination_port == CYPHAL_PORT
    assert parse_udp(sniffs[2].packet).destination_port == CYPHAL_PORT

    assert bytes(parse_udp(sniffs[0].packet).payload) == b"\xaa\xaa\xaa\xaa"
    assert bytes(parse_udp(sniffs[1].packet).payload) == b"\xbb\xbb\xbb\xbb"
    assert bytes(parse_udp(sniffs[2].packet).payload) == b"\xcc\xcc\xcc\xcc"

    sniffs.clear()

    # CLOSE and make sure we don't get any additional callbacks.
    sniffer.close()
    time.sleep(2)
    inside.sendto(b"d", ("239.0.1.200", CYPHAL_PORT))
    time.sleep(1)
    assert sniffs == []  # Should be terminated.

    # DISPOSE OF THE RESOURCES
    sniffer.close()
    outside.close()
    inside.close()
    sink.close()
