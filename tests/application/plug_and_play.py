#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import asyncio
import logging
import pathlib
import pyuavcan
from pyuavcan.transport.can import CANTransport
from pyuavcan.presentation import Presentation
from tests.transport.can.media.mock import MockMedia

_TABLE = pathlib.Path('allocation_table.sqlite.tmp')

_logger = logging.getLogger(__name__)


# noinspection PyProtectedMember
@pytest.mark.parametrize('mtu', [8, 16, 20, 64])  # type: ignore
@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_plug_and_play_centralized(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo],
                                                   caplog: typing.Any,
                                                   mtu: int) -> None:
    from pyuavcan.application.plug_and_play import CentralizedAllocator, Allocatee

    assert generated_packages
    assert caplog

    peers: typing.Set[MockMedia] = set()
    pres_client = Presentation(CANTransport(MockMedia(peers, mtu, 1), None))
    pres_server = Presentation(CANTransport(MockMedia(peers, mtu, 1), 123))

    cln_a = Allocatee(pres_client, _uid('00112233445566778899aabbccddeeff'), 42)
    assert cln_a.get_result() is None
    cln_a.start()
    await asyncio.sleep(2.0)
    assert cln_a.get_result() is None  # Nope, no response.

    try:
        _TABLE.unlink()
    except FileNotFoundError:
        pass
    with pytest.raises(ValueError, match='.*anonymous.*'):
        CentralizedAllocator(pres_client, _uid('deadbeefdeadbeefdeadbeefdeadbeef'), _TABLE)
    with pytest.raises(ValueError):
        CentralizedAllocator(pres_client, b'123', _TABLE)
    allocator = CentralizedAllocator(pres_server, _uid('deadbeefdeadbeefdeadbeefdeadbeef'), _TABLE)
    allocator.start()

    allocator.register_node(41, None)
    allocator.register_node(41, _uid('00000000000000000000000000000001'))   # Overwrites
    allocator.register_node(42, _uid('00000000000000000000000000000002'))
    allocator.register_node(42, None)                                       # Does not overwrite
    allocator.register_node(43, _uid('0000000000000000000000000000000F'))
    allocator.register_node(43, _uid('00000000000000000000000000000003'))   # Overwrites
    allocator.register_node(43, None)                                       # Does not overwrite

    use_v2 = mtu > cln_a._MTU_THRESHOLD
    await asyncio.sleep(2.0)
    assert cln_a.get_result() == (44 if use_v2 else 125)

    # Another request.
    cln_b = Allocatee(pres_client, _uid('aabbccddeeff00112233445566778899'))
    assert cln_b.get_result() is None
    cln_b.start()
    await asyncio.sleep(2.0)
    assert cln_b.get_result() == (125 if use_v2 else 124)

    # Re-request A and make sure we get the same response.
    cln_a = Allocatee(pres_client, _uid('00112233445566778899aabbccddeeff'), 42)
    assert cln_a.get_result() is None
    cln_a.start()
    await asyncio.sleep(2.0)
    assert cln_a.get_result() == (44 if use_v2 else 125)

    # C should be served from the manually added entries above.
    cln_c = Allocatee(pres_client, _uid('00000000000000000000000000000003'))
    assert cln_c.get_result() is None
    cln_c.start()
    await asyncio.sleep(2.0)
    assert cln_c.get_result() == 43

    # This one requires no allocation because the transport is not anonymous.
    cln_d = Allocatee(pres_server, _uid('00000000000000000000000000000009'), 100)
    assert cln_d.get_result() == 123
    cln_d.start()
    await asyncio.sleep(2.0)
    assert cln_d.get_result() == 123  # No change.

    # More test coverage needed.

    # Finalization.
    cln_a.close()
    cln_b.close()
    cln_c.close()
    cln_d.close()
    allocator.close()
    pres_client.close()
    pres_server.close()
    await asyncio.sleep(1.0)  # Let the tasks finalize properly.


@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_plug_and_play_allocatee(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo],
                                                 caplog: typing.Any) -> None:
    from pyuavcan.application.plug_and_play import Allocatee, NodeIDAllocationData_2, ID

    peers: typing.Set[MockMedia] = set()
    pres_client = Presentation(CANTransport(MockMedia(peers, 64, 1), None))
    pres_server = Presentation(CANTransport(MockMedia(peers, 64, 1), 123))
    allocatee = Allocatee(pres_client, _uid('00112233445566778899aabbccddeeff'), 42)
    allocatee.start()
    pub = pres_server.make_publisher_with_fixed_subject_id(NodeIDAllocationData_2)

    await pub.publish(NodeIDAllocationData_2(ID(10), unique_id=_uid('aabbccddeeff00112233445566778899')))  # Mismatch.
    await asyncio.sleep(1.0)
    assert allocatee.get_result() is None

    await pub.publish(NodeIDAllocationData_2(ID(999), unique_id=_uid('00112233445566778899aabbccddeeff')))  # Bad NID.
    await asyncio.sleep(1.0)
    assert allocatee.get_result() is None

    await pub.publish(NodeIDAllocationData_2(ID(0), unique_id=_uid('00112233445566778899aabbccddeeff')))  # Correct.
    await asyncio.sleep(1.0)
    assert allocatee.get_result() == 0

    allocatee.close()
    pub.close()
    pres_client.close()
    pres_server.close()
    await asyncio.sleep(1.0)  # Let the tasks finalize properly.


def _uid(as_hex: str) -> bytes:
    out = bytes.fromhex(as_hex)
    assert len(out) == 16
    return out
