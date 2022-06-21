# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import pycyphal


def get_transport(node_id: typing.Optional[int]) -> pycyphal.transport.Transport:
    from pycyphal.transport.udp import UDPTransport

    return UDPTransport("127.42.0.1", local_node_id=node_id)
