#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing


#: This factory constructs arguments for the CLI instructing it to use a particular transport configuration.
#: The factory takes one argument - the node-ID - which can be None (anonymous).
TransportFactory = typing.Callable[[typing.Optional[int]], typing.Sequence[str]]


def _make_transport_factories_for_cli() -> typing.Iterable[TransportFactory]:
    """
    Sensible transport configurations supported by the CLI to test against.
    Don't forget to extend when adding support for new transports.
    """
    if sys.platform == 'linux':
        # CAN via SocketCAN
        yield lambda nid: (f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan0",64),local_node_id={nid})', )

    # Serial via TCP/IP tunnel (emulation)
    from tests.transport.serial import VIRTUAL_BUS_URI
    yield lambda nid: (f'--tr=Serial("{VIRTUAL_BUS_URI}",local_node_id={nid})', )

    # UDP/IP on localhost (anonymous nodes not supported)
    yield lambda nid: ((f'--tr=UDP("127.0.0.{nid}/8")', )
                       if nid is not None and nid > 0 else
                       ())


TRANSPORT_FACTORIES = list(_make_transport_factories_for_cli())
