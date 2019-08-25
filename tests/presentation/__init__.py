#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


TransportPair = typing.Tuple[pyuavcan.transport.Transport, pyuavcan.transport.Transport]

#: The factory yields two new transports connected to the same (virtual) bus so that they can intercommunicate.
TransportFactory = typing.Callable[[], TransportPair]


def _make_transport_can() -> TransportPair:
    from pyuavcan.transport.can import CANTransport
    from tests.transport.can.media.mock import MockMedia
    bus: typing.Set[MockMedia] = set()
    media_a = MockMedia(bus, 8, 1)
    media_b = MockMedia(bus, 64, 2)      # Heterogeneous setup
    assert bus == {media_a, media_b}
    return CANTransport(media_a), CANTransport(media_b)


def _make_transport_serial() -> TransportPair:
    from pyuavcan.transport.serial import SerialTransport
    from tests.transport.serial import VIRTUAL_BUS_URI
    return SerialTransport(VIRTUAL_BUS_URI), SerialTransport(VIRTUAL_BUS_URI)


TRANSPORT_FACTORIES = [
    _make_transport_can,
    _make_transport_serial,
]
