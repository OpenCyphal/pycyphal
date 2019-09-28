#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


TransportPair = typing.Tuple[pyuavcan.transport.Transport, pyuavcan.transport.Transport]

#: The factory yields two new transports connected to the same (virtual) bus so that they can intercommunicate.
TransportFactory = typing.Callable[[typing.Optional[int], typing.Optional[int]], TransportPair]


class UntestableTransportConfigurationError(ValueError):
    """
    Raised from a factory to inform the test case that the requested configuration should not be tested.
    We don't use pytest.skip() because it leaves notifications about skipped tests and also it is not
    well compatible with pytest.asyncio leaving terrible stack traces in the log saying that the exception
    was never retrieved.
    """
    pass


def _make_transport_can(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPair:
    from pyuavcan.transport.can import CANTransport
    from tests.transport.can.media.mock import MockMedia
    bus: typing.Set[MockMedia] = set()
    media_a = MockMedia(bus, 8, 1)
    media_b = MockMedia(bus, 64, 2)      # Heterogeneous setup
    assert bus == {media_a, media_b}
    return CANTransport(media_a, node_id_a), CANTransport(media_b, node_id_b)


def _make_transport_serial(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPair:
    from pyuavcan.transport.serial import SerialTransport
    from tests.transport.serial import VIRTUAL_BUS_URI
    return SerialTransport(VIRTUAL_BUS_URI, node_id_a), SerialTransport(VIRTUAL_BUS_URI, node_id_b)


def _make_transport_udp(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPair:
    from pyuavcan.transport.udp import UDPTransport
    if None in (node_id_a, node_id_b):
        raise UntestableTransportConfigurationError('Anonymous nodes are not defined on the UDP/IP transport.')
    return UDPTransport(f'127.0.0.{node_id_a}/8'), UDPTransport(f'127.0.0.{node_id_b}/8')


TRANSPORT_FACTORIES: typing.Sequence[TransportFactory] = [
    _make_transport_can,
    _make_transport_serial,
    _make_transport_udp,
]
