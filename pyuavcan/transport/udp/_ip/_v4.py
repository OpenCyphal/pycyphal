#
# Copyright (c) 2019-2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import sys
import time
import errno
import typing
import socket
import struct
import logging
import threading
import pyuavcan
from ipaddress import IPv4Address
from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, UnsupportedSessionConfigurationError
from pyuavcan.transport import InvalidMediaConfigurationError
from ._socket_factory import SocketFactory, Sniffer
from ._packet import UDPIPPacket, IPHeader, UDPHeader
from ._endpoint_mapping import SUBJECT_PORT, IP_ADDRESS_NODE_ID_MASK, service_data_specifier_to_udp_port
from ._endpoint_mapping import node_id_to_unicast_ip, message_data_specifier_to_multicast_group


_logger = logging.getLogger(__name__)


class SocketFactoryIPv4(SocketFactory):
    """
    In IPv4 networks, the node-ID of zero may not be usable because it represents the subnet address;
    a node-ID that maps to the broadcast address for the subnet is unavailable.
    """

    def __init__(self, local_ip_address: IPv4Address):
        if not isinstance(local_ip_address, IPv4Address):  # pragma: no cover
            raise TypeError(f'Unexpected IP address type: {type(local_ip_address)}')
        self._local = local_ip_address

    @property
    def max_nodes(self) -> int:
        return IP_ADDRESS_NODE_ID_MASK  # The maximum may not be available because it may be the broadcast address.

    @property
    def local_ip_address(self) -> IPv4Address:
        return self._local

    def make_output_socket(self,
                           remote_node_id: typing.Optional[int],
                           data_specifier: pyuavcan.transport.DataSpecifier) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        try:
            # Output sockets shall be bound, too, in order to ensure that outgoing packets have the correct
            # source IP address specified. This is particularly important for localhost; an unbound socket
            # there emits all packets from 127.0.0.1 which is certainly not what we need.
            s.bind((str(self._local), 0))  # Bind to an ephemeral port.
        except OSError as ex:
            if ex.errno == errno.EADDRNOTAVAIL:
                raise InvalidMediaConfigurationError(
                    f'Bad IP configuration: cannot bind output socket to {self._local} [{errno.errorcode[ex.errno]}]'
                ) from None
            raise  # pragma: no cover

        if isinstance(data_specifier, MessageDataSpecifier):
            if remote_node_id is not None:
                raise UnsupportedSessionConfigurationError('Unicast message transfers are not defined.')
            # Merely binding is not enough for multicast sockets. We also have to configure IP_MULTICAST_IF.
            # https://tldp.org/HOWTO/Multicast-HOWTO-6.html
            # https://stackoverflow.com/a/26988214/1007777
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, self._local.packed)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, SocketFactoryIPv4.MULTICAST_TTL)
            remote_ip = message_data_specifier_to_multicast_group(self._local, data_specifier)
            remote_port = SUBJECT_PORT

        elif isinstance(data_specifier, ServiceDataSpecifier):
            if remote_node_id is None:
                raise UnsupportedSessionConfigurationError('Broadcast service transfers are not defined.')
            remote_ip = node_id_to_unicast_ip(self._local, remote_node_id)
            remote_port = service_data_specifier_to_udp_port(data_specifier)

        else:
            assert False

        s.connect((str(remote_ip), remote_port))
        _logger.debug('%r: New output %r connected to remote node %r', self, s, remote_node_id)
        return s

    def make_input_socket(self, data_specifier: pyuavcan.transport.DataSpecifier) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        # Allow other applications to use the same UAVCAN port as well.
        # These options shall be set before the socket is bound.
        # https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ/14388707#14388707
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sys.platform.startswith('linux'):
            # This is expected to be useful for unicast inputs only.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        try:
            if isinstance(data_specifier, MessageDataSpecifier):
                multicast_ip = message_data_specifier_to_multicast_group(self._local, data_specifier)
                multicast_port = SUBJECT_PORT
                # Binding to the multicast group address is necessary on GNU/Linux: https://habr.com/ru/post/141021/
                s.bind((str(multicast_ip), multicast_port))
                # Note that using INADDR_ANY in IP_ADD_MEMBERSHIP doesn't actually mean "any",
                # it means "choose one automatically"; see https://tldp.org/HOWTO/Multicast-HOWTO-6.html
                # This is why we have to specify the interface explicitly here.
                s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_ip.packed + self._local.packed)
            elif isinstance(data_specifier, ServiceDataSpecifier):
                local_port = service_data_specifier_to_udp_port(data_specifier)
                s.bind((str(self._local), local_port))
            else:
                assert False
        except OSError as ex:
            if ex.errno == errno.EADDRNOTAVAIL:
                raise InvalidMediaConfigurationError(
                    f'Bad IP configuration: cannot bind input socket to {self._local} [{errno.errorcode[ex.errno]}]'
                ) from None
            raise  # pragma: no cover
        _logger.debug('%r: New input %r', self, s)
        return s

    def make_sniffer(self, handler: typing.Callable[[pyuavcan.transport.Timestamp, UDPIPPacket], None]) -> SnifferIPv4:
        # http://www.enderunix.org/docs/en/rawipspoof/
        s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_UDP)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)       # Include raw IP headers with received data.
        if sys.platform.startswith('win32'):
            s.ioctl(socket.SIO_RCVALL, socket.RCVALL_ON)            # Enable promiscuous mode on the interface.
        return SnifferIPv4(s, self._local, handler)


class SnifferIPv4(Sniffer):
    """
    A very basic background worker continuously reading IP packets from the raw socket, filtering the UDP ones,
    dropping those that do not originate from the specified subnet,
    and emitting them via the callback (yup, right from the worker thread).
    """

    _MTU = 2 ** 16

    _IP_V4_FORMAT = struct.Struct('!BB HHH BB H II')
    _UDP_V4_FORMAT = struct.Struct('!HH HH')

    _PROTO_UDP = 0x11

    def __init__(self,
                 sock:             socket.socket,
                 local_ip_address: IPv4Address,
                 handler:          typing.Callable[[pyuavcan.transport.Timestamp, UDPIPPacket], None]) -> None:
        self._sock = sock
        self._local = local_ip_address
        self._handler = handler
        self._keep_going = True
        self._thread = threading.Thread(target=self._thread_function, name='udp_ipv4_sniffer', daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._keep_going = False
        self._sock.close()

    @staticmethod
    def _try_parse(data: memoryview) -> typing.Optional[UDPIPPacket]:
        ver_ihl, dscp_ecn, ip_length, ident, flags_frag_off, ttl, proto, hdr_chk, src_adr, dst_adr = \
            SnifferIPv4._IP_V4_FORMAT.unpack_from(data)
        ver, ihl = ver_ihl >> 4, ver_ihl & 0xF
        ip_header_size = ihl * 4
        udp_ip_header_size = ip_header_size + SnifferIPv4._UDP_V4_FORMAT.size
        if ver != 4 or proto != SnifferIPv4._PROTO_UDP or len(data) < udp_ip_header_size:
            return None
        src_port, dst_port, udp_length, udp_chk = SnifferIPv4._UDP_V4_FORMAT.unpack_from(data, offset=ip_header_size)
        return UDPIPPacket(
            ip_header=IPHeader(source=IPv4Address(src_adr), destination=IPv4Address(dst_adr)),
            udp_header=UDPHeader(source_port=src_port, destination_port=dst_port),
            payload=data[udp_ip_header_size:],
        )

    def _thread_function(self) -> None:
        _logger.debug('%s: worker thread started', self)
        local_prefix = IP_ADDRESS_NODE_ID_MASK | int(self._local)
        while self._keep_going:
            try:
                while self._keep_going:
                    data, _addr = self._sock.recvfrom(self._MTU)
                    ts = pyuavcan.transport.Timestamp.now()         # TODO: use accurate timestamping.
                    pkt = self._try_parse(memoryview(data))
                    if pkt is not None and local_prefix == (IP_ADDRESS_NODE_ID_MASK | int(pkt.ip_header.source)):
                        try:
                            self._handler(ts, pkt)
                        except Exception as ex:
                            _logger.exception('%s: exception in the sniffer handler: %s', self, ex)
            except Exception as ex:
                if (self._sock.fileno() < 0) or (isinstance(ex, OSError) and ex.errno == errno.EBADF) or \
                        not self._keep_going:
                    _logger.debug('%s: stopping because the socket is closed', self)
                    self._keep_going = False
                    break
                # This is probably inadequate, reconsider later.
                _logger.exception('%s: exception in the worker thread: %s', self, ex)
                time.sleep(1)
