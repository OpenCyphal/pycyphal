#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import socket
import logging
from ._network_map import NetworkMap


_logger = logging.getLogger(__name__)


class NetworkMapIPv6(NetworkMap):
    def __init__(self, network: str):
        assert network
        raise NotImplementedError('Sorry, IPv6 is not yet supported. Please submit patches!')

    @property
    def max_nodes(self) -> int:
        raise NotImplementedError

    @property
    def local_node_id(self) -> int:
        raise NotImplementedError

    def map_ip_address_to_node_id(self, ip: str) -> typing.Optional[int]:
        raise NotImplementedError

    def make_output_socket(self, remote_node_id: typing.Optional[int], remote_port: int) -> socket.socket:
        raise NotImplementedError

    def make_input_socket(self, local_port: int, expect_broadcast: bool) -> socket.socket:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError
