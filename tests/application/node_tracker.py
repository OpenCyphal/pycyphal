#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import asyncio
import logging
import pyuavcan


_logger = logging.getLogger(__name__)


# noinspection PyProtectedMember
@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_node_tracker(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo],
                                      caplog: typing.Any) -> None:
    from . import get_transport
    from pyuavcan.presentation import Presentation
    from pyuavcan.application.node_tracker import NodeTracker, Entry, GetInfo, Heartbeat

    assert generated_packages

    p_a = Presentation(get_transport(0xA))
    p_b = Presentation(get_transport(0xB))
    p_c = Presentation(get_transport(0xC))
    p_trk = Presentation(get_transport(None))

    try:
        last_update_args: typing.List[typing.Tuple[int, typing.Optional[Entry], typing.Optional[Entry]]] = []

        def simple_handler(node_id: int, old: typing.Optional[Entry], new: typing.Optional[Entry]) -> None:
            last_update_args.append((node_id, old, new))

        def faulty_handler(_node_id: int, _old: typing.Optional[Entry], _new: typing.Optional[Entry]) -> None:
            raise Exception('INTENDED EXCEPTION')

        trk = NodeTracker(p_trk)

        assert not trk.registry
        assert pytest.approx(trk.get_info_timeout) == trk.DEFAULT_GET_INFO_TIMEOUT
        assert trk.get_info_attempts == trk.DEFAULT_GET_INFO_ATTEMPTS

        # Override the defaults to simplify and speed-up testing.
        trk.get_info_timeout = 1.0
        trk.get_info_attempts = 2
        assert pytest.approx(trk.get_info_timeout) == 1.0
        assert trk.get_info_attempts == 2

        with caplog.at_level(logging.CRITICAL, logger=pyuavcan.application.node_tracker.__name__):
            trk.add_update_handler(faulty_handler)
            trk.add_update_handler(simple_handler)

            trk.start()
            trk.start()  # Idempotency

            await asyncio.sleep(1)
            assert not last_update_args
            assert not trk.registry

            # Bring the first node online and make sure it is detected and reported.
            hb_a = asyncio.create_task(_publish_heartbeat(p_a, 0xde))
            await asyncio.sleep(2.5)
            assert len(last_update_args) == 1
            assert last_update_args[0][0] == 0xA
            assert last_update_args[0][1] is None
            assert last_update_args[0][2] is not None
            assert last_update_args[0][2].heartbeat.uptime == 0
            assert last_update_args[0][2].heartbeat.vendor_specific_status_code == 0xde
            last_update_args.clear()
            assert list(trk.registry.keys()) == [0xA]
            assert 3 >= trk.registry[0xA].heartbeat.uptime >= 2
            assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
            assert trk.registry[0xA].info is None

            # Remove the faulty handler -- no point keeping the noise in the log.
            trk.remove_update_handler(faulty_handler)

        # Bring the second node online and make sure it is detected and reported.
        hb_b = asyncio.create_task(_publish_heartbeat(p_b, 0xbe))
        await asyncio.sleep(2.5)
        assert len(last_update_args) == 1
        assert last_update_args[0][0] == 0xB
        assert last_update_args[0][1] is None
        assert last_update_args[0][2] is not None
        assert last_update_args[0][2].heartbeat.uptime == 0
        assert last_update_args[0][2].heartbeat.vendor_specific_status_code == 0xbe
        last_update_args.clear()
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 6 >= trk.registry[0xA].heartbeat.uptime >= 4
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
        assert trk.registry[0xA].info is None
        assert 3 >= trk.registry[0xB].heartbeat.uptime >= 2
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xbe
        assert trk.registry[0xB].info is None

        # Enable get info servers. They will not be queried yet because the tracker node is anonymous.
        _serve_get_info(p_a, 'node-A')
        _serve_get_info(p_b, 'node-B')
        await asyncio.sleep(2.5)
        assert not last_update_args
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 9 >= trk.registry[0xA].heartbeat.uptime >= 6
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
        assert trk.registry[0xA].info is None
        assert 6 >= trk.registry[0xB].heartbeat.uptime >= 4
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xbe
        assert trk.registry[0xB].info is None

        # Create a new tracker, this time with a valid node-ID, and make sure node info is requested.
        # We are going to need a new handler for this.
        num_events_a = 0
        num_events_b = 0
        num_events_c = 0

        def validating_handler(node_id: int, old: typing.Optional[Entry], new: typing.Optional[Entry]) -> None:
            nonlocal num_events_a, num_events_b, num_events_c
            _logger.info('VALIDATING HANDLER %s %s %s', node_id, old, new)
            if node_id == 0xA:
                if num_events_a == 0:  # First detection
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xde
                    assert new.info is None
                elif num_events_a == 1:  # Get info received
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xde
                    assert new.heartbeat.vendor_specific_status_code == 0xde
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == 'node-A'
                elif num_events_a == 2:  # Restart detected
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xde
                    assert new.heartbeat.vendor_specific_status_code == 0xfe
                    assert old.info is not None
                    assert new.info is None
                elif num_events_a == 3:  # Get info after restart received
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xfe
                    assert new.heartbeat.vendor_specific_status_code == 0xfe
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == 'node-A'
                elif num_events_a == 4:  # Offline
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xfe
                    assert old.info is not None
                else:
                    assert False
                num_events_a += 1
            elif node_id == 0xB:
                if num_events_b == 0:
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xbe
                    assert new.info is None
                elif num_events_b == 1:
                    assert old is not None
                    assert new is not None
                    assert old.heartbeat.vendor_specific_status_code == 0xbe
                    assert new.heartbeat.vendor_specific_status_code == 0xbe
                    assert old.info is None
                    assert new.info is not None
                    assert new.info.name.tobytes().decode() == 'node-B'
                elif num_events_b == 2:
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xbe
                    assert old.info is not None
                else:
                    assert False
                num_events_b += 1
            elif node_id == 0xC:
                if num_events_c == 0:
                    assert old is None
                    assert new is not None
                    assert new.heartbeat.vendor_specific_status_code == 0xf0
                    assert new.info is None
                elif num_events_c == 1:
                    assert old is not None
                    assert new is None
                    assert old.heartbeat.vendor_specific_status_code == 0xf0
                    assert old.info is None
                else:
                    assert False
                num_events_c += 1
            else:
                assert False

        trk.close()
        trk.close()  # Idempotency
        p_trk = Presentation(get_transport(0xDD))
        trk = NodeTracker(p_trk)
        trk.add_update_handler(validating_handler)
        trk.start()
        trk.get_info_timeout = 1.0
        trk.get_info_attempts = 2
        assert pytest.approx(trk.get_info_timeout) == 1.0
        assert trk.get_info_attempts == 2

        await asyncio.sleep(2.5)
        assert num_events_a == 2
        assert num_events_b == 2
        assert num_events_c == 0
        assert list(trk.registry.keys()) == [0xA, 0xB]
        assert 12 >= trk.registry[0xA].heartbeat.uptime >= 8
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == 'node-A'
        assert 9 >= trk.registry[0xB].heartbeat.uptime >= 6
        assert trk.registry[0xB].heartbeat.vendor_specific_status_code == 0xbe
        assert trk.registry[0xB].info is not None
        assert trk.registry[0xB].info.name.tobytes().decode() == 'node-B'

        # Node B goes offline.
        hb_b.cancel()
        await asyncio.sleep(6)
        assert num_events_a == 2
        assert num_events_b == 3
        assert num_events_c == 0
        assert list(trk.registry.keys()) == [0xA]
        assert 20 >= trk.registry[0xA].heartbeat.uptime >= 12
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == 'node-A'

        # Node C appears online. It does not respond to GetInfo.
        hb_c = asyncio.create_task(_publish_heartbeat(p_c, 0xf0))
        await asyncio.sleep(6)
        assert num_events_a == 2
        assert num_events_b == 3
        assert num_events_c == 1
        assert list(trk.registry.keys()) == [0xA, 0xC]
        assert 28 >= trk.registry[0xA].heartbeat.uptime >= 17
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xde
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == 'node-A'
        assert 7 >= trk.registry[0xC].heartbeat.uptime >= 5
        assert trk.registry[0xC].heartbeat.vendor_specific_status_code == 0xf0
        assert trk.registry[0xC].info is None

        # Node A is restarted. Node C goes offline.
        hb_c.cancel()
        hb_a.cancel()
        hb_a = asyncio.create_task(_publish_heartbeat(p_a, 0xfe))
        await asyncio.sleep(6)
        assert num_events_a == 4  # Two extra events: node restart detection, then get info reception.
        assert num_events_b == 3
        assert num_events_c == 2
        assert list(trk.registry.keys()) == [0xA]
        assert 7 >= trk.registry[0xA].heartbeat.uptime >= 5
        assert trk.registry[0xA].heartbeat.vendor_specific_status_code == 0xfe
        assert trk.registry[0xA].info is not None
        assert trk.registry[0xA].info.name.tobytes().decode() == 'node-A'

        # Node A goes offline. No online nodes are left standing.
        hb_a.cancel()
        await asyncio.sleep(6)
        assert num_events_a == 5
        assert num_events_b == 3
        assert num_events_c == 2
        assert not trk.registry

        # Finalization.
        trk.close()
        trk.close()  # Idempotency
        for c in [hb_a, hb_b, hb_c]:
            c.cancel()
    finally:
        for p in [p_a, p_b, p_c, p_trk]:
            p.close()
        await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


async def _publish_heartbeat(pres: pyuavcan.presentation.Presentation, vssc: int) -> None:
    from pyuavcan.application.node_tracker import Heartbeat
    pub = pres.make_publisher_with_fixed_subject_id(Heartbeat)
    uptime = 0
    while True:
        msg = Heartbeat(uptime=uptime, vendor_specific_status_code=int(vssc))
        uptime += 1
        await pub.publish(msg)
        await asyncio.sleep(1)


def _serve_get_info(pres: pyuavcan.presentation.Presentation, name: str) -> None:
    from pyuavcan.application.node_tracker import GetInfo
    srv = pres.get_server_with_fixed_service_id(GetInfo)

    async def handler(req: GetInfo.Request, meta: pyuavcan.transport.TransferFrom) -> GetInfo.Response:
        resp = GetInfo.Response(
            name=name,
        )
        _logger.info(f'GetInfo request {req} metadata {meta} response {resp}')
        return resp

    srv.serve_in_background(handler)  # type: ignore
