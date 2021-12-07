# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import logging
import pytest
import pyuavcan

if typing.TYPE_CHECKING:
    import pyuavcan.application

_logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def _unittest_slow_node_tracker(compiled: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    from . import get_transport
    from uavcan.node import GetInfo_1_0
    from pyuavcan.application import make_node, NodeInfo
    from pyuavcan.application.node_tracker import NodeTracker, Entry

    assert compiled
    asyncio.get_running_loop().slow_callback_duration = 3.0

    n_a = make_node(NodeInfo(name="org.uavcan.pyuavcan.test.node_tracker.a"), transport=get_transport(0xA))
    n_b = make_node(NodeInfo(name="org.uavcan.pyuavcan.test.node_tracker.b"), transport=get_transport(0xB))
    n_c = make_node(NodeInfo(name="org.uavcan.pyuavcan.test.node_tracker.c"), transport=get_transport(0xC))
    n_trk = make_node(NodeInfo(name="org.uavcan.pyuavcan.test.node_tracker.trk"), transport=get_transport(None))

    try:
        last_update_args: typing.List[typing.Tuple[int, typing.Optional[Entry], typing.Optional[Entry]]] = []

        def simple_handler(node_id: int, old: typing.Optional[Entry], new: typing.Optional[Entry]) -> None:
            last_update_args.append((node_id, old, new))

        trk = NodeTracker(n_trk)

        assert not trk.registry
        assert pytest.approx(trk.get_info_timeout) == trk.DEFAULT_GET_INFO_TIMEOUT
        assert trk.get_info_attempts == trk.DEFAULT_GET_INFO_ATTEMPTS

        # Override the defaults to simplify and speed-up testing.
        trk.get_info_timeout = 1.0
        trk.get_info_attempts = 2
        assert pytest.approx(trk.get_info_timeout) == 1.0
        assert trk.get_info_attempts == 2

        trk.add_update_handler(simple_handler)

        n_trk.start()
        n_trk.start()  # Idempotency.

        await asyncio.sleep(9)
        assert not last_update_args
        assert not trk.registry

        # Bring the first node online and make sure it is detected and reported.
        n_a.heartbeat_publisher.vendor_specific_status_code = 0xDE
        n_a.start()
        await asyncio.sleep(9)
        assert len(last_update_args) == 1
        assert last_update_args[0][0] == 0xA
        assert last_update_args[0][1] is None
        assert last_update_args[0][2] is not None
        assert last_update_args[0][2].heartbeat.uptime == 0
        assert last_update_args[0][2].heartbeat.vendor_specific_status_code == 0xDE
        last_update_args.clear()
        assert list(trk.registry.keys()) == [0xA]
        assert 30 >= trk.registry[0xA].heartbeat.uptime >= 2
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is None

        # Bring the second node online and make sure it is detected and reported.
        n_b.heartbeat_publisher.vendor_specific_status_code = 0xBE
        n_b.start()
        await asyncio.sleep(9)
        assert len(last_update_args) == 1
        assert last_update_args[0][0] == 0xB
        assert last_update_args[0][1] is None
        assert last_update_args[0][2] is not None
        assert last_update_args[0][2].heartbeat.uptime == 0
        assert last_update_args[0][2].heartbeat.vendor_specific_status_code == 0xBE
        last_update_args.clear()
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 60 >= trk.registry[0xA].heartbeat.uptime >= 4
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is None
        assert 30 >= trk.registry[0xB].heartbeat.uptime >= 2
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xBE
        assert trk.registry[0xB].info is None

        await asyncio.sleep(9)
        assert not last_update_args
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 90 >= trk.registry[0xA].heartbeat.uptime >= 6
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is None
        assert 60 >= trk.registry[0xB].heartbeat.uptime >= 4
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xBE
        assert trk.registry[0xB].info is None

        # Create a new tracker, this time with a valid node-ID, and make sure node info is requested.
        # We are going to need a new handler for this.
        num_events_a = 0
        num_events_b = 0
        num_events_c = 0

        def validating_handler(node_id: int, old: typing.Optional[Entry], new: typing.Optional[Entry]) -> None:
            nonlocal num_events_a, num_events_b, num_events_c
            _logger.info("VALIDATING HANDLER %s %s %s", node_id, old, new)
            if node_id == 0xA:
                if num_events_a == 0:  # First detection
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xDE
                    assert new.info is None
                elif num_events_a == 1:  # Get info received
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xDE
                    assert new.heartbeat.vendor_specific_status_code == 0xDE
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"
                elif num_events_a == 2:  # Restart detected
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xDE
                    assert new.heartbeat.vendor_specific_status_code == 0xFE
                    assert old.info is not None
                    assert new.info is None
                elif num_events_a == 3:  # Get info after restart received
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xFE
                    assert new.heartbeat.vendor_specific_status_code == 0xFE
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"
                elif num_events_a == 4:  # Offline
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xFE
                    assert old.info is not None
                else:
                    assert False
                num_events_a += 1
            elif node_id == 0xB:
                if num_events_b == 0:
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xBE
                    assert new.info is None
                elif num_events_b == 1:
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xBE
                    assert new.heartbeat.vendor_specific_status_code == 0xBE
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.b"
                elif num_events_b == 2:
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xBE
                    assert old.info is not None
                else:
                    assert False
                num_events_b += 1
            elif node_id == 0xC:
                if num_events_c == 0:
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xF0
                    assert new.info is None
                elif num_events_c == 1:
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xF0
                    assert old.info is None
                else:
                    assert False
                num_events_c += 1
            else:
                assert False

        n_trk.close()
        n_trk.close()  # Idempotency
        n_trk = make_node(n_trk.info, transport=get_transport(0xDD))
        n_trk.start()
        trk = NodeTracker(n_trk)
        trk.add_update_handler(validating_handler)
        trk.get_info_timeout = 1.0
        trk.get_info_attempts = 2
        assert pytest.approx(trk.get_info_timeout) == 1.0
        assert trk.get_info_attempts == 2

        await asyncio.sleep(9)
        assert num_events_a == 2
        assert num_events_b == 2
        assert num_events_c == 0
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 60 >= trk.registry[0xA].heartbeat.uptime >= 8
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"
        assert 60 >= trk.registry[0xB].heartbeat.uptime >= 6
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xBE
        assert trk.registry[0xB].info is not None
        assert trk.registry[0xB].info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.b"

        # Node B goes offline.
        n_b.close()
        await asyncio.sleep(9)
        assert num_events_a == 2
        assert num_events_b == 3
        assert num_events_c == 0
        assert list(trk.registry.keys()) == [0xA]
        assert 90 >= trk.registry[0xA].heartbeat.uptime >= 12
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"

        # Node C appears online. It does not respond to GetInfo.
        n_c.heartbeat_publisher.vendor_specific_status_code = 0xF0
        n_c.start()
        # To make it not respond to GetInfo, get under the hood and break the transport session for this RPC-service.
        get_info_service_id = pyuavcan.dsdl.get_fixed_port_id(GetInfo_1_0)
        assert get_info_service_id
        for ses in n_c.presentation.transport.input_sessions:
            ds = ses.specifier.data_specifier
            if isinstance(ds, pyuavcan.transport.ServiceDataSpecifier) and ds.service_id == get_info_service_id:
                ses.close()
        await asyncio.sleep(9)
        assert num_events_a == 2
        assert num_events_b == 3
        assert num_events_c == 1
        assert list(trk.registry.keys()) == [0xA, 0xC]
        assert 180 >= trk.registry[0xA].heartbeat.uptime >= 17
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xDE
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"
        assert 30 >= trk.registry[0xC].heartbeat.uptime >= 5
        assert trk.registry[0xC].heartbeat.vendor_specific_status_code == 0xF0
        assert trk.registry[0xC].info is None

        # Node A is restarted. Node C goes offline.
        n_a.close()
        n_c.close()
        n_a = make_node(NodeInfo(name="org.uavcan.pyuavcan.test.node_tracker.a"), transport=get_transport(0xA))
        n_a.heartbeat_publisher.vendor_specific_status_code = 0xFE
        n_a.start()
        await asyncio.sleep(9)
        assert num_events_a == 4  # Two extra events: node restart detection, then get info reception.
        assert num_events_b == 3
        assert num_events_c == 2
        assert list(trk.registry.keys()) == [0xA]
        assert 30 >= trk.registry[0xA].heartbeat.uptime >= 5
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xFE
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == "org.uavcan.pyuavcan.test.node_tracker.a"

        # Node A goes offline. No online nodes are left standing.
        n_a.close()
        await asyncio.sleep(9)
        assert num_events_a == 5
        assert num_events_b == 3
        assert num_events_c == 2
        assert not trk.registry
    finally:
        for p in [n_a, n_b, n_c, n_trk]:
            p.close()
        await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.
