#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
import asyncio

import pytest


# noinspection PyProtectedMember
@pytest.mark.asyncio    # type: ignore
async def _unittest_can_pythoncan() -> None:
    from pyuavcan.transport import Timestamp
    from pyuavcan.transport.can.media import TimestampedDataFrame, DataFrame, FrameFormat, FilterConfiguration

#    if sys.platform != 'linux':  # pragma: no cover
#        pytest.skip('SocketCAN test skipped because we do not seem to be on a GNU/Linux-based system')

    from pyuavcan.transport.can.media.pythoncan import PythonCANMedia
    available = PythonCANMedia.list_available_interface_names()
#    print('Available SocketCAN ifaces:', available)
#    assert 'vcan0' in available, \
#        'Either the interface listing method is not working or the environment is not configured correctly. ' \
#        'Please ensure that the virtual SocketCAN interface "vcan0" is available, and its MTU is set to 64+8.'

    media_a = PythonCANMedia('virtual:0', 500000, 8)
    media_b = PythonCANMedia('virtual:0', 500000, 8)

    assert media_a.mtu == 8
    assert media_b.mtu == 8
    assert media_a.interface_name == 'virtual:0'
    assert media_b.interface_name == 'virtual:0'
    assert media_a.number_of_acceptance_filters == media_b.number_of_acceptance_filters
    assert media_a._maybe_thread is None
    assert media_b._maybe_thread is None

    rx_a: typing.List[TimestampedDataFrame] = []
    rx_b: typing.List[TimestampedDataFrame] = []

    def on_rx_a(frames: typing.Iterable[TimestampedDataFrame]) -> None:
        nonlocal rx_a
        frames = list(frames)
        print('RX A:', frames)
        rx_a += frames

    def on_rx_b(frames: typing.Iterable[TimestampedDataFrame]) -> None:
        nonlocal rx_b
        frames = list(frames)
        print('RX B:', frames)
        rx_b += frames
        #asyncio.ensure_future(media_b.send_until(frames, asyncio.get_event_loop().time() + 1.0))

    media_a.start(on_rx_a, False)
    media_b.start(on_rx_b, False)

    assert media_a._maybe_thread is not None
    assert media_b._maybe_thread is not None

    await asyncio.sleep(2.0)    # This wait is needed to ensure that the RX thread handles select() timeout properly

    ts_begin = Timestamp.now()
    await media_b.send_until([
        DataFrame(identifier=0xbadc0fe,
                  data=bytearray(range(8)),
                  format=FrameFormat.EXTENDED,
                  loopback=True),
        DataFrame(identifier=0x12345678,
                  data=bytearray(range(0)),
                  format=FrameFormat.EXTENDED,
                  loopback=False),
        DataFrame(identifier=0x123,
                  data=bytearray(range(6)),
                  format=FrameFormat.BASE,
                  loopback=True),
    ], asyncio.get_event_loop().time() + 1.0)
    await asyncio.sleep(0.1)
    ts_end = Timestamp.now()
    
    

    print('rx_a:', rx_a)
    # Three sent back from the other end, two loopback
    assert len(rx_a) == 3
    '''for f in rx_a:
        assert ts_begin.monotonic_ns <= f.timestamp.monotonic_ns <= ts_end.monotonic_ns
        assert ts_begin.system_ns <= f.timestamp.system_ns <= ts_end.system_ns

    rx_loopback = list(filter(lambda x: x.loopback, rx_a))
    rx_external = list(filter(lambda x: not x.loopback, rx_a))
    assert len(rx_loopback) == 2 and len(rx_external) == 3'''
    
    rx_external = list(filter(lambda x: True, rx_a))

    assert rx_external[0].identifier == 0xbadc0fe
    assert rx_external[0].data == bytearray(range(8))
    assert rx_external[0].format == FrameFormat.EXTENDED

    assert rx_external[1].identifier == 0x12345678
    assert rx_external[1].data == bytearray(range(0))
    assert rx_external[1].format == FrameFormat.EXTENDED

    assert rx_external[2].identifier == 0x123
    assert rx_external[2].data == bytearray(range(6))
    assert rx_external[2].format == FrameFormat.BASE
    
    print('rx_b:', rx_b)
    assert len(rx_b) == 2
    
    rx_loopback = list(filter(lambda x: True, rx_b))
    
    assert rx_loopback[0].identifier == 0xbadc0fe
    assert rx_loopback[0].data == bytearray(range(8))
    assert rx_loopback[0].format == FrameFormat.EXTENDED

    assert rx_loopback[1].identifier == 0x123
    assert rx_loopback[1].data == bytearray(range(6))
    assert rx_loopback[1].format == FrameFormat.BASE    

    #media_a.close()
    #media_b.close()
