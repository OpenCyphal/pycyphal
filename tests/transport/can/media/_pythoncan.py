# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Alex Kiselev <a.kiselev@volz-servos.com>, Pavel Kirienko <pavel@uavcan.org>

# pylint: disable=protected-access

import typing
import asyncio
import pytest


# noinspection PyProtectedMember
@pytest.mark.asyncio  # type: ignore
async def _unittest_can_pythoncan() -> None:
    from pyuavcan.transport import Timestamp
    from pyuavcan.transport.can.media import Envelope, DataFrame, FrameFormat
    from pyuavcan.transport.can.media.pythoncan import PythonCANMedia

    media_a = PythonCANMedia("virtual:0", 500000, 8)
    media_b = PythonCANMedia("virtual:0", 500000, 8)

    assert media_a.mtu == 8
    assert media_b.mtu == 8
    assert media_a.interface_name == "virtual:0"
    assert media_b.interface_name == "virtual:0"
    assert media_a.number_of_acceptance_filters == media_b.number_of_acceptance_filters
    assert media_a._maybe_thread is None
    assert media_b._maybe_thread is None

    rx_a: typing.List[typing.Tuple[Timestamp, Envelope]] = []
    rx_b: typing.List[typing.Tuple[Timestamp, Envelope]] = []

    def on_rx_a(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        nonlocal rx_a
        frames = list(frames)
        print("RX A:", frames)
        rx_a += frames

    def on_rx_b(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        nonlocal rx_b
        frames = list(frames)
        print("RX B:", frames)
        rx_b += frames

    media_a.start(on_rx_a, False)
    media_b.start(on_rx_b, False)

    assert media_a._maybe_thread is not None
    assert media_b._maybe_thread is not None

    await asyncio.sleep(2.0)  # This wait is needed to ensure that the RX thread handles read timeout properly

    ts_begin = Timestamp.now()
    await media_b.send(
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
    # Three received from another part
    assert len(rx_a) == 3
    for ts, _f in rx_a:
        assert ts_begin.monotonic_ns <= ts.monotonic_ns <= ts_end.monotonic_ns
        assert ts_begin.system_ns <= ts.system_ns <= ts_end.system_ns

    rx_external = list(filter(lambda x: True, rx_a))

    assert rx_external[0][1].frame.identifier == 0xBADC0FE
    assert rx_external[0][1].frame.data == bytearray(range(8))
    assert rx_external[0][1].frame.format == FrameFormat.EXTENDED

    assert rx_external[1][1].frame.identifier == 0x12345678
    assert rx_external[1][1].frame.data == bytearray(range(0))
    assert rx_external[1][1].frame.format == FrameFormat.EXTENDED

    assert rx_external[2][1].frame.identifier == 0x123
    assert rx_external[2][1].frame.data == bytearray(range(6))
    assert rx_external[2][1].frame.format == FrameFormat.BASE

    print("rx_b:", rx_b)
    # Two messages are loopback and were copied
    assert len(rx_b) == 2

    rx_loopback = list(filter(lambda x: True, rx_b))

    assert rx_loopback[0][1].frame.identifier == 0xBADC0FE
    assert rx_loopback[0][1].frame.data == bytearray(range(8))
    assert rx_loopback[0][1].frame.format == FrameFormat.EXTENDED

    assert rx_loopback[1][1].frame.identifier == 0x123
    assert rx_loopback[1][1].frame.data == bytearray(range(6))
    assert rx_loopback[1][1].frame.format == FrameFormat.BASE

    media_a.close()
    media_b.close()
