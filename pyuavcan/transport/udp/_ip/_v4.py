# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import errno
import typing
import socket
import logging
from ipaddress import IPv4Address, IPV4LENGTH, ip_network
import pyuavcan
from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, UnsupportedSessionConfigurationError
from pyuavcan.transport import InvalidMediaConfigurationError
from ._socket_factory import SocketFactory, Sniffer
from ._endpoint_mapping import SUBJECT_PORT, IP_ADDRESS_NODE_ID_MASK, service_data_specifier_to_udp_port
from ._endpoint_mapping import node_id_to_unicast_ip, message_data_specifier_to_multicast_group
from ._link_layer import LinkLayerCapture, LinkLayerSniffer


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

    def make_sniffer(self, handler: typing.Callable[[LinkLayerCapture], None]) -> SnifferIPv4:
        return SnifferIPv4(self._local, handler)


class SnifferIPv4(Sniffer):
    def __init__(self, local_ip_address: IPv4Address, handler: typing.Callable[[LinkLayerCapture], None]) -> None:
        netmask_width = IPV4LENGTH - IP_ADDRESS_NODE_ID_MASK.bit_length()
        subnet = ip_network(f"{local_ip_address}/{netmask_width}", strict=False)
        filter_expression = f"udp and src net {subnet}"
        _logger.debug("Constructed BPF filter expression: %r", filter_expression)
        self._link_layer = LinkLayerSniffer(filter_expression, handler)

    def close(self) -> None:
        self._link_layer.close()

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self._link_layer)
