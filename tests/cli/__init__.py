#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
import dataclasses


@dataclasses.dataclass(frozen=True)
class TransportConfig:
    cli_args:     typing.Sequence[str]
    can_transmit: bool


#: This factory constructs arguments for the CLI instructing it to use a particular transport configuration.
#: The factory takes one argument - the node-ID - which can be None (anonymous).
TransportFactory = typing.Callable[[typing.Optional[int]], TransportConfig]


def _make_transport_factories_for_cli() -> typing.Iterable[TransportFactory]:
    """
    Sensible transport configurations supported by the CLI to test against.
    Don't forget to extend when adding support for new transports.
    """
    if sys.platform == 'linux':
        # CAN via SocketCAN
        yield lambda nid: TransportConfig(
            cli_args=(f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan0",64),local_node_id={nid})', ),
            can_transmit=True,
        )

        # Redundant CAN via SocketCAN
        yield lambda nid: TransportConfig(
            cli_args=(
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan0",8),local_node_id={nid})',
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan1",32),local_node_id={nid})',
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan2",64),local_node_id={nid})',
            ),
            can_transmit=True,
        )

    # Serial via TCP/IP tunnel (emulation)
    from tests.transport.serial import VIRTUAL_BUS_URI
    yield lambda nid: TransportConfig(
        cli_args=(f'--tr=Serial("{VIRTUAL_BUS_URI}",local_node_id={nid})', ),
        can_transmit=True,
    )

    # UDP/IP on localhost (cannot transmit if anonymous)
    yield lambda nid: TransportConfig(
        cli_args=(f'--tr=UDP("127.0.0.{nid}/8")', ),
        can_transmit=True,
    ) if nid is not None else TransportConfig(
        cli_args=('--tr=UDP("127.255.255.255/8")', ),
        can_transmit=False,
    )

    # Redundant UDP+Serial. The UDP transport does not support anonymous transfers.
    yield lambda nid: TransportConfig(
        cli_args=(
            f'--tr=Serial("{VIRTUAL_BUS_URI}",local_node_id={nid})',
            (f'--tr=UDP("127.0.0.{nid}/8")' if nid is not None else '--tr=UDP("127.255.255.255/8")'),
        ),
        can_transmit=nid is not None,
    )


TRANSPORT_FACTORIES = list(_make_transport_factories_for_cli())
