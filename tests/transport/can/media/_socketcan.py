# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import typing
import asyncio
import pytest


if sys.platform != "linux":  # pragma: no cover
    pytest.skip("SocketCAN test skipped because the system is not GNU/Linux", allow_module_level=True)


# noinspection PyProtectedMember
@pytest.mark.asyncio  # type: ignore
async def _unittest_can_socketcan() -> None:
    from pyuavcan.transport import Timestamp
    from pyuavcan.transport.can.media import Envelope, DataFrame, FrameFormat, FilterConfiguration

    from pyuavcan.transport.can.media.socketcan import SocketCANMedia

    available = SocketCANMedia.list_available_interface_names()
    print("Available SocketCAN ifaces:", available)
    assert "vcan0" in available, (
        "Either the interface listing method is not working or the environment is not configured correctly. "
        'Please ensure that the virtual SocketCAN interface "vcan0" is available, and its MTU is set to 64+8.'
    )

    media_a = SocketCANMedia("vcan0", 12)
    media_b = SocketCANMedia("vcan0", 64)

    assert media_a.mtu == 12
    assert media_b.mtu == 64
    assert media_a.interface_name == "vcan0"
    assert media_b.interface_name == "vcan0"
    assert media_a.number_of_acceptance_filters == media_b.number_of_acceptance_filters
    assert media_a._maybe_thread is None  # pylint: disable=protected-access
    assert media_b._maybe_thread is None  # pylint: disable=protected-access

    media_a.configure_acceptance_filters([FilterConfiguration.new_promiscuous()])
    media_b.configure_acceptance_filters([FilterConfiguration.new_promiscuous()])

    rx_a: typing.List[typing.Tuple[Timestamp, Envelope]] = []

    def on_rx_a(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        nonlocal rx_a
        frames = list(frames)
        print("RX A:", frames)
        rx_a += frames

    def on_rx_b(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        frames = list(frames)
        print("RX B:", frames)
        asyncio.ensure_future(media_b.send((e for _, e in frames), asyncio.get_event_loop().time() + 1.0))

    media_a.start(on_rx_a, False)
    media_b.start(on_rx_b, True)

    assert media_a._maybe_thread is not None  # pylint: disable=protected-access
    assert media_b._maybe_thread is not None  # pylint: disable=protected-access

    await asyncio.sleep(2.0)  # This wait is needed to ensure that the RX thread handles select() timeout properly

    ts_begin = Timestamp.now()
    await media_a.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 0xBADC0FE, bytearray(range(8))), loopback=True),
            Envelope(DataFrame(FrameFormat.EXTENDED, 0x12345678, bytearray(range(0))), loopback=False),
            Envelope(DataFrame(FrameFormat.BASE, 0x123, bytearray(range(6))), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    await asyncio.sleep(0.1)
    ts_end = Timestamp.now()

    print("rx_a:", rx_a)
    # Three sent back from the other end, two loopback
    assert len(rx_a) == 5
    for t, _ in rx_a:
        assert ts_begin.monotonic_ns <= t.monotonic_ns <= ts_end.monotonic_ns
        assert ts_begin.system_ns <= t.system_ns <= ts_end.system_ns

    rx_loopback = [e.frame for t, e in rx_a if e.loopback]
    rx_external = [e.frame for t, e in rx_a if not e.loopback]
    assert len(rx_loopback) == 2 and len(rx_external) == 3

    assert rx_loopback[0].identifier == 0xBADC0FE
    assert rx_loopback[0].data == bytearray(range(8))
    assert rx_loopback[0].format == FrameFormat.EXTENDED

    assert rx_loopback[1].identifier == 0x123
    assert rx_loopback[1].data == bytearray(range(6))
    assert rx_loopback[1].format == FrameFormat.BASE

    assert rx_external[0].identifier == 0xBADC0FE
    assert rx_external[0].data == bytearray(range(8))
    assert rx_external[0].format == FrameFormat.EXTENDED

    assert rx_external[1].identifier == 0x12345678
    assert rx_external[1].data == bytearray(range(0))
    assert rx_external[1].format == FrameFormat.EXTENDED

    assert rx_external[2].identifier == 0x123
    assert rx_external[2].data == bytearray(range(6))
    assert rx_external[2].format == FrameFormat.BASE

    media_a.close()
    media_b.close()

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.
