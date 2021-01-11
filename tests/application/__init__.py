# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import pyuavcan


def get_transport(node_id: typing.Optional[int]) -> pyuavcan.transport.Transport:
    from pyuavcan.transport.udp import UDPTransport

    return UDPTransport(f"127.0.0.{node_id}") if node_id is not None else UDPTransport("127.0.0.1", anonymous=True)
