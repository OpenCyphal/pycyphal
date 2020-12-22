# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import time
import errno
import typing
import socket
import struct
import logging
import pyuavcan
from ipaddress import IPv4Address
from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, UnsupportedSessionConfigurationError
from pyuavcan.transport import InvalidMediaConfigurationError, Timestamp
from ._socket_factory import SocketFactory, Sniffer
from ._packet import RawPacket, MACHeader, IPHeader, UDPHeader
from ._endpoint_mapping import SUBJECT_PORT, IP_ADDRESS_NODE_ID_MASK, service_data_specifier_to_udp_port
from ._endpoint_mapping import node_id_to_unicast_ip, message_data_specifier_to_multicast_group
from ._link_layer import LinkLayerCapture, LinkLayerSniffer, LinkLayerPacket


_logger = logging.getLogger(__name__)


class IPv4SocketFactory(SocketFactory):
    """
    In IPv4 networks, the node-ID of zero may not be usable because it represents the subnet address;
    a node-ID that maps to the broadcast address for the subnet is unavailable.
    """

    def __init__(self, local_ip_address: IPv4Address):
        if not isinstance(local_ip_address, IPv4Address):  # pragma: no cover
            raise TypeError(f"Unexpected IP address type: {type(local_ip_address)}")
        self._local = local_ip_address

    @property
    def max_nodes(self) -> int:
        return IP_ADDRESS_NODE_ID_MASK  # The maximum may not be available because it may be the broadcast address.

    @property
    def local_ip_address(self) -> IPv4Address:
        return self._local

    def make_output_socket(
        self, remote_node_id: typing.Optional[int], data_specifier: pyuavcan.transport.DataSpecifier
    ) -> socket.socket:
        _logger.debug(
            "%r: Constructing new output socket for remote node %s and %s", self, remote_node_id, data_specifier
        )
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        try:
            # Output sockets shall be bound, too, in order to ensure that outgoing packets have the correct
            # source IP address specified. This is particularly important for localhost; an unbound socket
            # there emits all packets from 127.0.0.1 which is certainly not what we need.
            s.bind((str(self._local), 0))  # Bind to an ephemeral port.
        except OSError as ex:
            s.close()
            if ex.errno == errno.EADDRNOTAVAIL:
                raise InvalidMediaConfigurationError(
                    f"Bad IP configuration: cannot bind output socket to {self._local} [{errno.errorcode[ex.errno]}]"
                ) from None
            raise  # pragma: no cover

        if isinstance(data_specifier, MessageDataSpecifier):
            if remote_node_id is not None:
                s.close()
                raise UnsupportedSessionConfigurationError("Unicast message transfers are not defined.")
            # Merely binding is not enough for multicast sockets. We also have to configure IP_MULTICAST_IF.
            # https://tldp.org/HOWTO/Multicast-HOWTO-6.html
            # https://stackoverflow.com/a/26988214/1007777
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, self._local.packed)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, IPv4SocketFactory.MULTICAST_TTL)
            remote_ip = message_data_specifier_to_multicast_group(self._local, data_specifier)
            remote_port = SUBJECT_PORT
        elif isinstance(data_specifier, ServiceDataSpecifier):
            if remote_node_id is None:
                s.close()
                raise UnsupportedSessionConfigurationError("Broadcast service transfers are not defined.")
            remote_ip = node_id_to_unicast_ip(self._local, remote_node_id)
            remote_port = service_data_specifier_to_udp_port(data_specifier)
        else:
            assert False

        s.connect((str(remote_ip), remote_port))
        _logger.debug("%r: New output %r connected to remote node %r", self, s, remote_node_id)
        return s

    def make_input_socket(self, data_specifier: pyuavcan.transport.DataSpecifier) -> socket.socket:
        _logger.debug("%r: Constructing new input socket for %s", self, data_specifier)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        # Allow other applications to use the same UAVCAN port as well.
        # These options shall be set before the socket is bound.
        # https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ/14388707#14388707
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sys.platform.startswith("linux"):  # pragma: no branch
            # This is expected to be useful for unicast inputs only.
            # https://stackoverflow.com/a/14388707/1007777
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        if isinstance(data_specifier, MessageDataSpecifier):
            multicast_ip = message_data_specifier_to_multicast_group(self._local, data_specifier)
            multicast_port = SUBJECT_PORT
            if sys.platform.startswith("linux"):
                # Binding to the multicast group address is necessary on GNU/Linux: https://habr.com/ru/post/141021/
                s.bind((str(multicast_ip), multicast_port))
            else:
                # Binding to a multicast address is not allowed on Windows, and it is not necessary there. Error is:
                #   OSError: [WinError 10049] The requested address is not valid in its context
                s.bind(("", multicast_port))
            try:
                # Note that using INADDR_ANY in IP_ADD_MEMBERSHIP doesn't actually mean "any",
                # it means "choose one automatically"; see https://tldp.org/HOWTO/Multicast-HOWTO-6.html
                # This is why we have to specify the interface explicitly here.
                s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_ip.packed + self._local.packed)
            except OSError as ex:
                s.close()
                if ex.errno in (errno.EADDRNOTAVAIL, errno.ENODEV):
                    raise InvalidMediaConfigurationError(
                        f"Could not register multicast group membership {multicast_ip} via {self._local} using {s} "
                        f"[{errno.errorcode[ex.errno]}]"
                    ) from None
                raise  # pragma: no cover
        elif isinstance(data_specifier, ServiceDataSpecifier):
            local_port = service_data_specifier_to_udp_port(data_specifier)
            try:
                s.bind((str(self._local), local_port))
            except OSError as ex:
                s.close()
                if ex.errno in (errno.EADDRNOTAVAIL, errno.ENODEV):
                    raise InvalidMediaConfigurationError(
                        f"Could not bind input service socket to {self._local}:{local_port} "
                        f"[{errno.errorcode[ex.errno]}]"
                    ) from None
                raise  # pragma: no cover
        else:
            assert False
        _logger.debug("%r: New input %r", self, s)
        return s

    def make_sniffer(self, handler: typing.Callable[[Timestamp, RawPacket], None]) -> SnifferIPv4:
        return SnifferIPv4(self._local, handler)


class SnifferIPv4(Sniffer):
    _IP_V4_FORMAT = struct.Struct("!BB HHH BB H II")
    _UDP_V4_FORMAT = struct.Struct("!HH HH")
    _PROTO_UDP = 0x11

    def __init__(self, local_ip_address: IPv4Address, handler: typing.Callable[[Timestamp, RawPacket], None]) -> None:
        from ipaddress import IPV4LENGTH, ip_network

        netmask_width = IPV4LENGTH - IP_ADDRESS_NODE_ID_MASK.bit_length()
        subnet = ip_network(f"{local_ip_address}/{netmask_width}", strict=False)
        filter_expression = f"udp and src net {subnet}"
        _logger.debug("Constructed BPF filter expression: %r", filter_expression)
        self._link_layer = LinkLayerSniffer(filter_expression, self._callback)
        self._local = local_ip_address
        self._handler = handler

    def close(self) -> None:
        self._link_layer.close()

    def _callback(self, lls: LinkLayerCapture) -> None:
        rp = self._try_parse(lls.packet)
        if rp is not None:
            self._handler(lls.timestamp, rp)

    @staticmethod
    def _try_parse(llp: LinkLayerPacket) -> typing.Optional[RawPacket]:
        if llp.protocol != socket.AddressFamily.AF_INET:
            return None
        data = llp.payload
        (
            ver_ihl,
            dscp_ecn,
            ip_length,
            ident,
            flags_frag_off,
            ttl,
            proto,
            hdr_chk,
            src_adr,
            dst_adr,
        ) = SnifferIPv4._IP_V4_FORMAT.unpack_from(data)
        ver, ihl = ver_ihl >> 4, ver_ihl & 0xF
        ip_header_size = ihl * 4
        udp_ip_header_size = ip_header_size + SnifferIPv4._UDP_V4_FORMAT.size
        if ver != 4 or proto != SnifferIPv4._PROTO_UDP or len(data) < udp_ip_header_size:
            return None
        src_port, dst_port, udp_length, udp_chk = SnifferIPv4._UDP_V4_FORMAT.unpack_from(data, offset=ip_header_size)
        return RawPacket(
            mac_header=MACHeader(source=llp.source, destination=llp.destination),
            ip_header=IPHeader(source=IPv4Address(src_adr), destination=IPv4Address(dst_adr)),
            udp_header=UDPHeader(source_port=src_port, destination_port=dst_port),
            udp_payload=data[udp_ip_header_size:],
        )

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, local_ip_address=str(self._local), link_layer=self._link_layer)


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_socket_factory() -> None:
    from pytest import raises
    from ipaddress import ip_address

    is_linux = sys.platform.startswith("linux")

    fac = SocketFactory.new(ip_address("127.42.1.200"))
    assert fac.max_nodes == 0xFFFF
    assert str(fac.local_ip_address) == "127.42.1.200"

    # SERVICE SOCKET TEST (unicast)
    ds = ServiceDataSpecifier(100, ServiceDataSpecifier.Role.REQUEST)
    test_u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_u.bind(("127.42.0.123", service_data_specifier_to_udp_port(ds)))

    srv_o = fac.make_output_socket(123, ds)
    srv_o.send(b"Goose")
    rx = test_u.recvfrom(1024)
    assert rx[0] == b"Goose"
    assert rx[1][0] == "127.42.1.200"

    srv_i = fac.make_input_socket(ds)
    test_u.sendto(b"Duck", ("127.42.1.200", service_data_specifier_to_udp_port(ds)))
    rx = srv_i.recvfrom(1024)
    assert rx[0] == b"Duck"
    assert rx[1][0] == "127.42.0.123"
    test_u.close()

    # MESSAGE SOCKET TEST (multicast)
    # Note that Windows does not permit using the same socket for both sending to and receiving from a unicast group
    # because in order to specify a particular output interface the socket must be bound to a unicast address.
    # So we set up separate sockets for input and output.
    test_i = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_i.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    test_i.bind(("239.42.2.100" * is_linux, SUBJECT_PORT))
    test_i.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.42.2.100") + socket.inet_aton("127.42.0.123")
    )

    msg_o = fac.make_output_socket(None, MessageDataSpecifier(612))  # 612 = (2 << 8) + 100
    msg_o.send(b"Eagle")
    rx = test_i.recvfrom(1024)
    assert rx[0] == b"Eagle"
    assert rx[1][0] == "127.42.1.200"

    test_o = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    test_o.bind(("127.42.0.123", 0))
    test_o.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.42.0.123"))
    msg_i = fac.make_input_socket(MessageDataSpecifier(612))
    test_o.sendto(b"Seagull", ("239.42.2.100", SUBJECT_PORT))
    rx = msg_i.recvfrom(1024)
    assert rx[0] == b"Seagull"
    assert rx[1][0] == "127.42.0.123"  # Same address we just bound to.

    # ERRORS
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(ip_address("1.2.3.4")).make_input_socket(
            ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(ip_address("1.2.3.4")).make_input_socket(MessageDataSpecifier(0))
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(ip_address("1.2.3.4")).make_output_socket(
            1, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE)
        )
    with raises(InvalidMediaConfigurationError):
        IPv4SocketFactory(ip_address("1.2.3.4")).make_output_socket(1, MessageDataSpecifier(0))

    with raises(UnsupportedSessionConfigurationError):
        fac.make_output_socket(1, MessageDataSpecifier(0))
    with raises(UnsupportedSessionConfigurationError):
        fac.make_output_socket(None, ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE))

    # CLEAN UP
    test_u.close()
    test_i.close()
    test_o.close()
    srv_o.close()
    srv_i.close()
    msg_o.close()
    msg_i.close()


def _unittest_sniffer() -> None:
    from ipaddress import ip_address

    # The sniffer is expected to drop all traffic except from 127.66.0.0/16
    fac = SocketFactory.new(ip_address("127.66.1.200"))

    ts_last = Timestamp.now()
    sniffs: typing.List[RawPacket] = []

    def sniff_sniff(ts: Timestamp, pack: RawPacket) -> None:
        nonlocal ts_last
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= ts.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= ts.system_ns <= now.system_ns
        ts_last = ts
        # Make sure that all traffic from foreign networks is filtered out by the sniffer.
        assert (int(pack.ip_header.source) & 0x_FFFF_0000) == (int(fac.local_ip_address) & 0x_FFFF_0000)
        sniffs.append(pack)

    sniffer = fac.make_sniffer(sniff_sniff)
    assert isinstance(sniffer, SnifferIPv4)
    # noinspection PyProtectedMember
    assert sniffer._link_layer._filter_expr == "udp and src net 127.66.0.0/16"

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
    assert len(sniffs[0].mac_header.source) == len(sniffs[0].mac_header.destination)
    assert len(sniffs[1].mac_header.source) == len(sniffs[1].mac_header.destination)
    assert len(sniffs[2].mac_header.source) == len(sniffs[2].mac_header.destination)

    assert sniffs[0].ip_header.source == ip_address("127.66.33.44")
    assert sniffs[1].ip_header.source == ip_address("127.66.33.44")
    assert sniffs[2].ip_header.source == ip_address("127.66.33.44")

    assert sniffs[0].ip_header.destination == ip_address("127.66.1.200")
    assert sniffs[1].ip_header.destination == ip_address("127.33.1.200")
    assert sniffs[2].ip_header.destination == ip_address("239.66.1.200")

    assert sniffs[0].udp_header.destination_port == 1234
    assert sniffs[1].udp_header.destination_port == 5678
    assert sniffs[2].udp_header.destination_port == 4444

    assert bytes(sniffs[0].udp_payload) == b"\xAA\xAA\xAA\xAA"
    assert bytes(sniffs[1].udp_payload) == b"\xBB\xBB\xBB\xBB"
    assert bytes(sniffs[2].udp_payload) == b"\xCC\xCC\xCC\xCC"

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
