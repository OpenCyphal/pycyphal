# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import asyncio
from decimal import Decimal
from pathlib import Path
import pytest
import pycyphal


pytestmark = pytest.mark.asyncio


async def _unittest_slow_make_transport_candump(
    compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo],
    tmp_path: Path,
) -> None:
    from pycyphal.application import make_transport, make_registry
    from pycyphal.transport import Capture
    from pycyphal.transport.can import CANCapture

    asyncio.get_running_loop().slow_callback_duration = 3.0
    assert compiled
    candump_file = tmp_path / "candump.log"
    candump_file.write_text(_CANDUMP_TEST_DATA)

    registry = make_registry(None, {})  # type: ignore
    registry["uavcan.can.iface"] = "candump:" + str(candump_file)

    tr = make_transport(registry)
    print("Transport:", tr)
    assert tr

    captures: list[CANCapture] = []

    def handle_capture(cap: Capture) -> None:
        assert isinstance(cap, CANCapture)
        print(cap)
        captures.append(cap)

    tr.begin_capture(handle_capture)
    await asyncio.sleep(5.0)
    tr.close()

    assert len(captures) == 4

    assert captures[0].timestamp.system == Decimal("1657800496.359233")
    assert captures[0].frame.identifier == 0x0C60647D
    assert captures[0].frame.format == pycyphal.transport.can.media.FrameFormat.EXTENDED
    assert captures[0].frame.data == bytes.fromhex("020000FB")

    assert captures[1].timestamp.system == Decimal("1657800496.360136")
    assert captures[1].frame.identifier == 0x10606E7D
    assert captures[1].frame.format == pycyphal.transport.can.media.FrameFormat.EXTENDED
    assert captures[1].frame.data == bytes.fromhex("00000000000000BB")

    assert captures[2].timestamp.system == Decimal("1657800496.360152")
    assert captures[2].frame.identifier == 0x10606E7D
    assert captures[2].frame.format == pycyphal.transport.can.media.FrameFormat.EXTENDED
    assert captures[2].frame.data == bytes.fromhex("000000000000003B")

    assert captures[3].timestamp.system == Decimal("1657800496.360317")
    assert captures[3].frame.identifier == 0x1060787D
    assert captures[3].frame.format == pycyphal.transport.can.media.FrameFormat.EXTENDED
    assert captures[3].frame.data == bytes.fromhex("0000C07F147CB71B")


_CANDUMP_TEST_DATA = """
(1657800496.359233) slcan0 0C60647D#020000FB
(1657800496.360136) slcan0 10606E7D#00000000000000BB
(1657800496.360149) slcan1 10606E7D#000000000000001B
(1657800496.360152) slcan0 10606E7D#000000000000003B
(1657800496.360305) slcan2 1060787D#00000000000000BB
(1657800496.360317) slcan0 1060787D#0000C07F147CB71B
(1657800496.361011) slcan1 1060787D#412BCC7B
"""
