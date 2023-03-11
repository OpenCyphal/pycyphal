# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import sys
import errno
import typing
import socket
import logging
import ipaddress
from ipaddress import IPV4LENGTH, ip_network
import pycyphal
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier
from pycyphal.transport import InvalidMediaConfigurationError
from ._socket_factory import SocketFactory, Sniffer

from ._endpoint_mapping import CYPHAL_PORT
from ._endpoint_mapping import DESTINATION_NODE_ID_MASK
from ._endpoint_mapping import MULTICAST_PREFIX
from ._endpoint_mapping import service_node_id_to_multicast_group, message_data_specifier_to_multicast_group

from ._link_layer import LinkLayerCapture, LinkLayerSniffer

_logger = logging.getLogger(__name__)


class IPv4SocketFactory(SocketFactory):
    """
    In IPv4 networks, the node-ID of zero may not be usable because it represents the subnet address;
    a node-ID that maps to the broadcast address for the subnet is unavailable.
    """

    def __init__(self, local_ip_address: ipaddress.IPv4Address):
        self._local_ip_address = local_ip_address

    @property
    def max_nodes(self) -> int:
        return DESTINATION_NODE_ID_MASK

    @property
    def local_ip_address(self) -> ipaddress.IPv4Address:
        return self._local_ip_address

    def make_output_socket(
        self, remote_node_id: typing.Optional[int], data_specifier: pycyphal.transport.DataSpecifier
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
            s.bind((str(self._local_ip_address), 0))  # Bind to an ephemeral port.
        except OSError as ex:
            s.close()
            if ex.errno == errno.EADDRNOTAVAIL:
                raise InvalidMediaConfigurationError(
                    f"Bad IP configuration: cannot bind output socket to {self._local_ip_address} [{errno.errorcode[ex.errno]}]"
                ) from None
            raise  # pragma: no cover

        if isinstance(data_specifier, MessageDataSpecifier):
            assert remote_node_id is None  # Message transfers don't require a remote_node_id.
            # Merely binding is not enough for multicast sockets. We also have to configure IP_MULTICAST_IF.
            # https://tldp.org/HOWTO/Multicast-HOWTO-6.html
            # https://stackoverflow.com/a/26988214/1007777
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, self._local_ip_address.packed)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, IPv4SocketFactory.MULTICAST_TTL)
            remote_ip = message_data_specifier_to_multicast_group(data_specifier)
            remote_port = CYPHAL_PORT
        elif isinstance(data_specifier, ServiceDataSpecifier):
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, self._local_ip_address.packed)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, IPv4SocketFactory.MULTICAST_TTL)
            remote_ip = service_node_id_to_multicast_group(remote_node_id)
            remote_port = CYPHAL_PORT
        else:
            assert False

        s.connect((str(remote_ip), remote_port))
        _logger.debug("%r: New output %r connected to remote node %r", self, s, remote_node_id)
        return s

    def make_input_socket(
        self, remote_node_id: typing.Optional[int], data_specifier: pycyphal.transport.DataSpecifier
    ) -> socket.socket:
        ## TODO: Add check for remote_node_id is None or not (like in make_output_socket above)
        _logger.debug("%r: Constructing new input socket for %s", self, data_specifier)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setblocking(False)
        # Allow other applications to use the same Cyphal port as well.
        # These options shall be set before the socket is bound.
        # https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ/14388707#14388707
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):  # pragma: no branch
            # This is expected to be useful for unicast inputs only.
            # https://stackoverflow.com/a/14388707/1007777
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        if isinstance(data_specifier, MessageDataSpecifier):
            multicast_ip = message_data_specifier_to_multicast_group(data_specifier)
            multicast_port = CYPHAL_PORT
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
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
                s.setsockopt(
                    socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_ip.packed + self._local_ip_address.packed
                )
            except OSError as ex:
                s.close()
                if ex.errno in (errno.EADDRNOTAVAIL, errno.ENODEV):
                    raise InvalidMediaConfigurationError(
                        f"Could not register multicast group membership {multicast_ip} via {self._local_ip_address} using {s} "
                        f"[{errno.errorcode[ex.errno]}]"
                    ) from None
                raise  # pragma: no cover
        elif isinstance(data_specifier, ServiceDataSpecifier):
            multicast_ip = service_node_id_to_multicast_group(remote_node_id)
            multicast_port = CYPHAL_PORT
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                s.bind((str(multicast_ip), multicast_port))
            else:
                s.bind(("", multicast_port))
            try:
                s.setsockopt(
                    socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, multicast_ip.packed + self._local_ip_address.packed
                )
            except OSError as ex:
                s.close()
                if ex.errno in (errno.EADDRNOTAVAIL, errno.ENODEV):
                    raise InvalidMediaConfigurationError(
                        f"Could not register multicast group membership {multicast_ip} via {self._local_ip_address} using {s} "
                        f"[{errno.errorcode[ex.errno]}]"
                    ) from None
                raise  # pragma: no cover
        else:
            assert False
        _logger.debug("%r: New input %r", self, s)
        return s

    def make_sniffer(self, handler: typing.Callable[[LinkLayerCapture], None]) -> SnifferIPv4:
        return SnifferIPv4(handler)


class SnifferIPv4(Sniffer):
    def __init__(self, handler: typing.Callable[[LinkLayerCapture], None]) -> None:
        netmask_width = IPV4LENGTH - DESTINATION_NODE_ID_MASK.bit_length() - 1  # -1 for the snm bit
        fix = MULTICAST_PREFIX
        subnet_ip = ipaddress.IPv4Address(fix)
        subnet = ip_network(f"{subnet_ip}/{netmask_width}", strict=False)
        filter_expression = f"udp and dst net {subnet}"
        _logger.debug("Constructed BPF filter expression: %r", filter_expression)
        self._link_layer = LinkLayerSniffer(filter_expression, handler)

    def close(self) -> None:
        self._link_layer.close()

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(self, self._link_layer)


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_socket_factory_v4() -> None:
    sock_fac = IPv4SocketFactory(local_ip_address=ipaddress.IPv4Address("127.0.0.1"))
    assert sock_fac.local_ip_address == ipaddress.IPv4Address("127.0.0.1")

    is_linux = sys.platform.startswith("linux") or sys.platform.startswith("darwin")

    msg_output_socket = sock_fac.make_output_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(456))
    assert "239.0.1.200" == msg_output_socket.getpeername()[0]
    assert CYPHAL_PORT == msg_output_socket.getpeername()[1]

    srvc_output_socket = sock_fac.make_output_socket(
        remote_node_id=123, data_specifier=ServiceDataSpecifier(456, ServiceDataSpecifier.Role.RESPONSE)
    )
    assert "239.1.0.123" == srvc_output_socket.getpeername()[0]
    assert CYPHAL_PORT == srvc_output_socket.getpeername()[1]

    broadcast_srvc_output_socket = sock_fac.make_output_socket(
        remote_node_id=None, data_specifier=ServiceDataSpecifier(456, ServiceDataSpecifier.Role.RESPONSE)
    )
    assert "239.1.255.255" == broadcast_srvc_output_socket.getpeername()[0]
    assert CYPHAL_PORT == broadcast_srvc_output_socket.getpeername()[1]

    msg_input_socket = sock_fac.make_input_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(456))
    if is_linux:
        assert "239.0.1.200" == msg_input_socket.getsockname()[0]
    assert CYPHAL_PORT == msg_input_socket.getsockname()[1]

    srvc_input_socket = sock_fac.make_input_socket(
        remote_node_id=123, data_specifier=ServiceDataSpecifier(456, ServiceDataSpecifier.Role.REQUEST)
    )
    if is_linux:
        assert "239.1.0.123" == srvc_input_socket.getsockname()[0]
    assert CYPHAL_PORT == srvc_input_socket.getsockname()[1]

    broadcast_srvc_input_socket = sock_fac.make_input_socket(
        remote_node_id=None, data_specifier=ServiceDataSpecifier(456, ServiceDataSpecifier.Role.REQUEST)
    )
    if is_linux:
        assert "239.1.255.255" == broadcast_srvc_input_socket.getsockname()[0]
    assert CYPHAL_PORT == broadcast_srvc_input_socket.getsockname()[1]

    sniffer = SnifferIPv4(handler=lambda x: None)
    assert "udp and dst net 239.0.0.0/15" == sniffer._link_layer._filter_expr  # pylint: disable=protected-access
