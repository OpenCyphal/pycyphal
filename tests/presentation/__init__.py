#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan


TransportPack = typing.Tuple[pyuavcan.transport.Transport, pyuavcan.transport.Transport, bool]

#: The factory yields two new transports connected to the same (virtual) bus so that they can intercommunicate.
#: The boolean flag is True if the transports are capable of sending anonymous transfers.
TransportFactory = typing.Callable[[typing.Optional[int], typing.Optional[int]], TransportPack]


def _make_transport_can(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPack:
    from pyuavcan.transport.can import CANTransport
    from tests.transport.can.media.mock import MockMedia
    bus: typing.Set[MockMedia] = set()
    media_a = MockMedia(bus, 8, 1)
    media_b = MockMedia(bus, 64, 2)      # Heterogeneous setup
    assert bus == {media_a, media_b}
    return CANTransport(media_a, node_id_a), CANTransport(media_b, node_id_b), True


def _make_transport_serial(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPack:
    from pyuavcan.transport.serial import SerialTransport
    from tests.transport.serial import VIRTUAL_BUS_URI
    return SerialTransport(VIRTUAL_BUS_URI, node_id_a), SerialTransport(VIRTUAL_BUS_URI, node_id_b), True


def _make_transport_udp(node_id_a: typing.Optional[int], node_id_b: typing.Optional[int]) -> TransportPack:
    from pyuavcan.transport.udp import UDPTransport

    def one(nid: typing.Optional[int]) -> UDPTransport:
        return UDPTransport(f'127.0.0.{nid}/8') if nid is not None else UDPTransport('127.255.255.255/8')

    return one(node_id_a), one(node_id_b), False


def _make_transport_redundant_udp_serial(node_id_a: typing.Optional[int],
                                         node_id_b: typing.Optional[int]) -> TransportPack:
    from pyuavcan.transport.redundant import RedundantTransport
    from pyuavcan.transport.udp import UDPTransport
    from pyuavcan.transport.serial import SerialTransport
    from tests.transport.serial import VIRTUAL_BUS_URI

    def one(nid: typing.Optional[int]) -> RedundantTransport:
        red = RedundantTransport()
        red.attach_inferior(UDPTransport(f'127.0.0.{nid}/8') if nid is not None else UDPTransport('127.255.255.255/8'))
        red.attach_inferior(SerialTransport(VIRTUAL_BUS_URI, nid))
        print('REDUNDANT TRANSPORT UDP+SERIAL:', red)
        return red

    return one(node_id_a), one(node_id_b), False


def _make_transport_redundant_can_can_can(node_id_a: typing.Optional[int],
                                          node_id_b: typing.Optional[int]) -> TransportPack:
    from pyuavcan.transport.redundant import RedundantTransport
    from pyuavcan.transport.can import CANTransport
    from tests.transport.can.media.mock import MockMedia

    bus_0: typing.Set[MockMedia] = set()
    bus_1: typing.Set[MockMedia] = set()
    bus_2: typing.Set[MockMedia] = set()

    def one(nid: typing.Optional[int]) -> RedundantTransport:
        # Triply redundant CAN bus.
        red = RedundantTransport()
        red.attach_inferior(CANTransport(MockMedia(bus_0, 8, 1), nid))      # Heterogeneous setup (CAN classic)
        red.attach_inferior(CANTransport(MockMedia(bus_1, 32, 1), nid))     # Heterogeneous setup (CAN FD)
        red.attach_inferior(CANTransport(MockMedia(bus_2, 64, 1), nid))     # Heterogeneous setup (CAN FD)
        print('REDUNDANT TRANSPORT CANx3:', red)
        return red

    return one(node_id_a), one(node_id_b), True


TRANSPORT_FACTORIES: typing.Sequence[TransportFactory] = [
    _make_transport_can,
    _make_transport_serial,
    _make_transport_udp,
    _make_transport_redundant_udp_serial,
    _make_transport_redundant_can_can_can,
]
