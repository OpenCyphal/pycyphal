#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import pyuavcan.transport


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport() -> None:
    from .media.mock import MockMedia

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        pyuavcan.transport.can.CANTransport(MockMedia(set(), 64, 0))

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        pyuavcan.transport.can.CANTransport(MockMedia(set(), 7, 16))

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 64, 1000)

    tr = pyuavcan.transport.can.CANTransport(media)

    assert tr.protocol_parameters == pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        node_id_set_cardinality=128,
        single_frame_transfer_payload_capacity_bytes=63
    )
    assert tr.frame_payload_capacity == 63
    assert tr.local_node_id is None

    bs_ds = pyuavcan.transport.MessageDataSpecifier(1234)
    bs_meta = pyuavcan.transport.PayloadMetadata(0x_bad_c0ffee_0dd_f00d, 123)
    bco = await tr.get_broadcast_output(bs_ds, bs_meta)
