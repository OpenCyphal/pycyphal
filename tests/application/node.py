# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import pytest
import pyuavcan
from pyuavcan.transport.udp import UDPTransport
from pyuavcan.transport.redundant import RedundantTransport
from pyuavcan.presentation import Presentation


@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_node(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    from pyuavcan.application import Node
    from uavcan.node import Version_1_0, Heartbeat_1_0, GetInfo_1_0, Mode_1_0, Health_1_0

    asyncio.get_running_loop().slow_callback_duration = 3.0

    assert generated_packages
    remote_pres = Presentation(UDPTransport("127.1.1.1"))
    remote_hb_sub = remote_pres.make_subscriber_with_fixed_subject_id(Heartbeat_1_0)
    remote_info_cln = remote_pres.make_client_with_fixed_service_id(GetInfo_1_0, 258)

    trans = RedundantTransport()
    pres = Presentation(trans)
    try:
        info = GetInfo_1_0.Response(
            protocol_version=Version_1_0(*pyuavcan.UAVCAN_SPECIFICATION_VERSION),
            software_version=Version_1_0(*pyuavcan.__version_info__[:2]),
            name="org.uavcan.pyuavcan.test.node",
        )
        node = Node(pres, info, with_diagnostic_subscriber=True)
        print("node:", node)
        assert node.presentation is pres
        node.start()
        node.start()  # Idempotency

        node.heartbeat_publisher.priority = pyuavcan.transport.Priority.FAST
        node.heartbeat_publisher.period = 0.5
        node.heartbeat_publisher.mode = Mode_1_0.MAINTENANCE  # type: ignore
        node.heartbeat_publisher.health = Health_1_0.ADVISORY  # type: ignore
        node.heartbeat_publisher.vendor_specific_status_code = 93
        with pytest.raises(ValueError):
            node.heartbeat_publisher.period = 99.0
        with pytest.raises(ValueError):
            node.heartbeat_publisher.vendor_specific_status_code = -299

        assert node.heartbeat_publisher.priority == pyuavcan.transport.Priority.FAST
        assert node.heartbeat_publisher.period == pytest.approx(0.5)
        assert node.heartbeat_publisher.mode == Mode_1_0.MAINTENANCE
        assert node.heartbeat_publisher.health == Health_1_0.ADVISORY
        assert node.heartbeat_publisher.vendor_specific_status_code == 93

        assert None is await remote_hb_sub.receive_for(2.0)

        assert trans.local_node_id is None
        trans.attach_inferior(UDPTransport("127.1.1.2"))
        assert trans.local_node_id == 258

        for _ in range(2):
            hb_transfer = await remote_hb_sub.receive_for(2.0)
            assert hb_transfer is not None
            hb, transfer = hb_transfer
            assert transfer.source_node_id == 258
            assert transfer.priority == pyuavcan.transport.Priority.FAST
            assert 1 <= hb.uptime <= 9
            assert hb.mode.value == Mode_1_0.MAINTENANCE
            assert hb.health.value == Health_1_0.ADVISORY
            assert hb.vendor_specific_status_code == 93

        info_transfer = await remote_info_cln.call(GetInfo_1_0.Request())
        assert info_transfer is not None
        resp, transfer = info_transfer
        assert transfer.source_node_id == 258
        assert isinstance(resp, GetInfo_1_0.Response)
        assert resp.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node"
        assert resp.protocol_version.major == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
        assert resp.software_version.major == pyuavcan.__version_info__[0]

        trans.detach_inferior(trans.inferiors[0])
        assert trans.local_node_id is None

        assert None is await remote_hb_sub.receive_for(2.0)

        node.close()
        node.close()  # Idempotency
    finally:
        pres.close()
        remote_pres.close()
        await asyncio.sleep(1.0)  # Let the background tasks terminate.
