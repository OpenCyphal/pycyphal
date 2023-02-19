# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
from typing import Dict
import asyncio
import pytest
import pycyphal
from pycyphal.transport.udp import UDPTransport
from pycyphal.transport.redundant import RedundantTransport
from pycyphal.presentation import Presentation

pytestmark = pytest.mark.asyncio


async def _unittest_slow_node(compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from pycyphal.application import make_node, make_registry
    import uavcan.primitive
    from uavcan.node import Version_1, Heartbeat_1, GetInfo_1, Mode_1, Health_1

    asyncio.get_running_loop().slow_callback_duration = 3.0

    assert compiled
    remote_pres = Presentation(UDPTransport("127.1.1.1"))
    remote_hb_sub = remote_pres.make_subscriber_with_fixed_subject_id(Heartbeat_1)
    remote_info_cln = remote_pres.make_client_with_fixed_service_id(GetInfo_1, 258)

    trans = RedundantTransport()
    try:
        info = GetInfo_1.Response(
            protocol_version=Version_1(*pycyphal.CYPHAL_SPECIFICATION_VERSION),
            software_version=Version_1(*pycyphal.__version_info__[:2]),
            name="org.opencyphal.pycyphal.test.node",
        )
        node = make_node(info, make_registry(None, typing.cast(Dict[str, bytes], {})), transport=trans)
        print("node:", node)
        assert node.presentation.transport is trans
        node.start()
        node.start()  # Idempotency

        # Check port instantiation API for non-fixed-port-ID types.
        assert "uavcan.pub.optional.id" not in node.registry  # Nothing yet.
        with pytest.raises(KeyError, match=r".*uavcan\.pub\.optional\.id.*"):
            node.make_publisher(uavcan.primitive.Empty_1, "optional")
        assert 0xFFFF == int(node.registry["uavcan.pub.optional.id"])  # Created automatically!
        with pytest.raises(TypeError):
            node.make_publisher(uavcan.primitive.Empty_1)

        # Same but for fixed port-ID types.
        assert "uavcan.pub.atypical_heartbeat.id" not in node.registry  # Nothing yet.
        pub_port = node.make_publisher(uavcan.node.Heartbeat_1, "atypical_heartbeat")
        assert pub_port.port_id == pycyphal.dsdl.get_model(uavcan.node.Heartbeat_1).fixed_port_id
        pub_port.close()
        assert 0xFFFF == int(node.registry["uavcan.pub.atypical_heartbeat.id"])  # Created automatically!
        node.registry["uavcan.pub.atypical_heartbeat.id"] = 111  # Override the default.
        pub_port = node.make_publisher(uavcan.node.Heartbeat_1, "atypical_heartbeat")
        assert pub_port.port_id == 111
        pub_port.close()

        # Check direct assignment of port-ID.
        pub_port = node.make_publisher(uavcan.node.Heartbeat_1, 2222)
        assert pub_port.port_id == 2222
        pub_port.close()
        cln_port = node.make_client(uavcan.node.ExecuteCommand_1, 123, 333)
        assert cln_port.port_id == 333
        assert cln_port.output_transport_session.destination_node_id == 123
        cln_port.close()

        node.heartbeat_publisher.priority = pycyphal.transport.Priority.FAST
        node.heartbeat_publisher.period = 0.5
        node.heartbeat_publisher.mode = Mode_1.MAINTENANCE  # type: ignore
        node.heartbeat_publisher.health = Health_1.ADVISORY  # type: ignore
        node.heartbeat_publisher.vendor_specific_status_code = 93
        with pytest.raises(ValueError):
            node.heartbeat_publisher.period = 99.0
        with pytest.raises(ValueError):
            node.heartbeat_publisher.vendor_specific_status_code = -299

        assert node.heartbeat_publisher.priority == pycyphal.transport.Priority.FAST
        assert node.heartbeat_publisher.period == pytest.approx(0.5)
        assert node.heartbeat_publisher.mode == Mode_1.MAINTENANCE
        assert node.heartbeat_publisher.health == Health_1.ADVISORY
        assert node.heartbeat_publisher.vendor_specific_status_code == 93

        assert None is await remote_hb_sub.receive_for(2.0)

        assert trans.local_node_id is None
        trans.attach_inferior(UDPTransport("127.1.1.2", local_node_id=258))
        assert trans.local_node_id == 258

        for _ in range(2):
            hb_transfer = await remote_hb_sub.receive_for(2.0)
            assert hb_transfer is not None
            hb, transfer = hb_transfer
            assert transfer.source_node_id == 258
            assert transfer.priority == pycyphal.transport.Priority.FAST
            assert 1 <= hb.uptime <= 9
            assert hb.mode.value == Mode_1.MAINTENANCE
            assert hb.health.value == Health_1.ADVISORY
            assert hb.vendor_specific_status_code == 93

        info_transfer = await remote_info_cln.call(GetInfo_1.Request())
        assert info_transfer is not None
        resp, transfer = info_transfer
        assert transfer.source_node_id == 258
        assert isinstance(resp, GetInfo_1.Response)
        assert resp.name.tobytes().decode() == "org.opencyphal.pycyphal.test.node"
        assert resp.protocol_version.major == pycyphal.CYPHAL_SPECIFICATION_VERSION[0]
        assert resp.software_version.major == pycyphal.__version_info__[0]

        trans.detach_inferior(trans.inferiors[0])
        assert trans.local_node_id is None

        assert None is await remote_hb_sub.receive_for(2.0)

        node.close()
        node.close()  # Idempotency
    finally:
        trans.close()
        remote_pres.close()
        await asyncio.sleep(1.0)  # Let the background tasks terminate.
