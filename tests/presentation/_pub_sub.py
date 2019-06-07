#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import pyuavcan
import pyuavcan.transport.can
import tests.transport.can


@pytest.mark.asyncio    # type: ignore
async def _unittest_slow_presentation_pub_sub(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) \
        -> None:
    assert generated_packages
    import uavcan.node
    import uavcan.diagnostic
    from pyuavcan.transport import Priority

    bus: typing.Set[tests.transport.can.media.mock.MockMedia] = set()
    media_a = tests.transport.can.media.mock.MockMedia(bus, 8, 1)
    media_b = tests.transport.can.media.mock.MockMedia(bus, 64, 2)      # Look, a heterogeneous setup!
    assert bus == {media_a, media_b}

    tran_a = pyuavcan.transport.can.CANTransport(media_a)
    tran_b = pyuavcan.transport.can.CANTransport(media_b)

    pres_a = pyuavcan.presentation.Presentation(tran_a)
    pres_b = pyuavcan.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    pub_heart = await pres_a.get_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    sub_heart = await pres_b.get_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)

    pub_record = await pres_b.get_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
    sub_record = await pres_a.get_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)

    assert pub_heart is await pres_a.get_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert sub_heart is await pres_b.get_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)

    pub_heart_old = pub_heart
    await pub_heart.close()
    pub_heart = await pres_a.get_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart is await pres_a.get_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart is not pub_heart_old

    assert pub_heart.transport_session.destination_node_id is None
    assert sub_heart.transport_session.specifier.data_specifier == pub_heart.transport_session.specifier.data_specifier
    assert pub_heart.port_id == pyuavcan.dsdl.get_fixed_port_id(uavcan.node.Heartbeat_1_0)
    assert sub_heart.dtype is uavcan.node.Heartbeat_1_0

    heart = uavcan.node.Heartbeat_1_0(uptime=123456,
                                      health=uavcan.node.Heartbeat_1_0.HEALTH_CAUTION,
                                      mode=uavcan.node.Heartbeat_1_0.MODE_OPERATIONAL,
                                      vendor_specific_status_code=0xc0fe)
    await pub_heart.publish(heart, Priority.SLOW)
    rx, transfer = await sub_heart.receive_with_transfer()
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id is None
    assert transfer.priority == Priority.SLOW
    assert transfer.transfer_id == 0
    assert sub_heart.deserialization_failure_count == 0

    await tran_a.set_local_node_id(123)
    await tran_b.set_local_node_id(42)

    pub_heart.transfer_id_counter.override(23)
    await pub_heart.publish(heart, Priority.SLOW)
    rx, transfer = await sub_heart.receive_with_transfer()
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id == 123
    assert transfer.priority == Priority.SLOW
    assert transfer.transfer_id == 23
    assert sub_heart.deserialization_failure_count == 0

    await sub_heart.close()
    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        await sub_heart.close()
