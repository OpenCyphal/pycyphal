# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import sys
import typing
import pytest
import pycyphal


TransportPack = typing.Tuple[pycyphal.transport.Transport, pycyphal.transport.Transport, bool]

TransportFactory = typing.Callable[[typing.Optional[int], typing.Optional[int]], TransportPack]
"""
The factory yields two new transports connected to the same (virtual) bus so that they can intercommunicate.
The boolean flag is True if the transports are capable of sending anonymous transfers.
"""


def _generate() -> typing.Iterator[typing.Callable[[], typing.Iterator[TransportFactory]]]:
    """
    We use the unwieldy generator syntax to leverage the setup/teardown functionality provided by PyTest.
    """

    def can_mock_media() -> typing.Iterator[TransportFactory]:
        """
        The mock media fixture allows us to test configurations with limited acceptance filter configurations.
        Also, the mock media allows Classic CAN and CAN FD nodes to co-exist easily,
        whereas the virtual bus emulated by SocketCAN has certain limitations there
        (a frame with BRS set cannot be received by receiver for which FD is not enabled).
        """
        from pycyphal.transport.can import CANTransport
        from tests.transport.can.media.mock import MockMedia

        def fact(nid_a: typing.Optional[int], nid_b: typing.Optional[int]) -> TransportPack:
            bus: typing.Set[MockMedia] = set()
            media_a = MockMedia(bus, 8, 1)
            media_b = MockMedia(bus, 64, 2)  # Heterogeneous setup
            assert bus == {media_a, media_b}
            return CANTransport(media_a, nid_a), CANTransport(media_b, nid_b), True

        yield fact

    yield can_mock_media

    def can_mock_media_triply_redundant() -> typing.Iterator[TransportFactory]:
        from pycyphal.transport.redundant import RedundantTransport
        from pycyphal.transport.can import CANTransport
        from tests.transport.can.media.mock import MockMedia

        def factory(nid_a: typing.Optional[int], nid_b: typing.Optional[int]) -> TransportPack:
            bus_0: typing.Set[MockMedia] = set()
            bus_1: typing.Set[MockMedia] = set()
            bus_2: typing.Set[MockMedia] = set()

            def one(nid: typing.Optional[int]) -> RedundantTransport:
                red = RedundantTransport()
                red.attach_inferior(CANTransport(MockMedia(bus_0, 8, 1), nid))  # Heterogeneous setup (CAN classic)
                red.attach_inferior(CANTransport(MockMedia(bus_1, 32, 2), nid))  # Heterogeneous setup (CAN FD)
                red.attach_inferior(CANTransport(MockMedia(bus_2, 64, 3), nid))  # Heterogeneous setup (CAN FD)
                return red

            return one(nid_a), one(nid_b), True

        yield factory

    yield can_mock_media_triply_redundant

    if sys.platform.startswith("linux"):

        def can_socketcan_vcan0() -> typing.Iterator[TransportFactory]:
            from pycyphal.transport.can import CANTransport
            from pycyphal.transport.can.media.socketcan import SocketCANMedia

            yield lambda nid_a, nid_b: (
                CANTransport(SocketCANMedia("vcan0", 16), nid_a),
                CANTransport(SocketCANMedia("vcan0", 64), nid_b),
                True,
            )

        yield can_socketcan_vcan0

        def can_socketcan_vcan0_vcan1() -> typing.Iterator[TransportFactory]:
            from pycyphal.transport.redundant import RedundantTransport
            from pycyphal.transport.can import CANTransport
            from pycyphal.transport.can.media.socketcan import SocketCANMedia

            def one(nid: typing.Optional[int]) -> RedundantTransport:
                red = RedundantTransport()
                red.attach_inferior(CANTransport(SocketCANMedia("vcan0", 64), nid))
                red.attach_inferior(CANTransport(SocketCANMedia("vcan1", 32), nid))
                return red

            yield lambda nid_a, nid_b: (one(nid_a), one(nid_b), True)

        yield can_socketcan_vcan0_vcan1

    def serial_tunneled_via_tcp() -> typing.Iterator[TransportFactory]:
        from pycyphal.transport.serial import SerialTransport
        from tests.transport.serial import VIRTUAL_BUS_URI

        yield lambda nid_a, nid_b: (
            SerialTransport(VIRTUAL_BUS_URI, nid_a),
            SerialTransport(VIRTUAL_BUS_URI, nid_b),
            True,
        )

    yield serial_tunneled_via_tcp

    def udp_loopback() -> typing.Iterator[TransportFactory]:
        from pycyphal.transport.udp import UDPTransport

        def one(nid: typing.Optional[int]) -> UDPTransport:
            return UDPTransport("127.0.0.1", local_node_id=nid)

        yield lambda nid_a, nid_b: (one(nid_a), one(nid_b), True)

    yield udp_loopback

    def heterogeneous_udp_serial() -> typing.Iterator[TransportFactory]:
        from pycyphal.transport.redundant import RedundantTransport
        from pycyphal.transport.udp import UDPTransport
        from pycyphal.transport.serial import SerialTransport
        from tests.transport.serial import VIRTUAL_BUS_URI

        def one(nid: typing.Optional[int]) -> RedundantTransport:
            red = RedundantTransport()
            red.attach_inferior(UDPTransport("127.0.0.1", local_node_id=nid))
            red.attach_inferior(SerialTransport(VIRTUAL_BUS_URI, nid))
            print("UDP+SERIAL:", red)
            return red

        yield lambda nid_a, nid_b: (one(nid_a), one(nid_b), True)

    yield heterogeneous_udp_serial


@pytest.fixture(params=list(_generate()))
def transport_factory(request: typing.Any) -> typing.Iterable[TransportFactory]:
    """
    This parametrized fixture generates multiple transport factories to run the test against different transports.
    """
    yield from request.param()
