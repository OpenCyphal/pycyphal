#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pytest
from ._subprocess import run_cli_tool, BackgroundChildProcess, CalledProcessError
from . import TRANSPORT_FACTORIES, TransportFactory


@pytest.mark.parametrize('transport_factory', TRANSPORT_FACTORIES)  # type: ignore
def _unittest_slow_cli_pick_nid(transport_factory: TransportFactory) -> None:
    # We spawn a lot of processes here, which might strain the test system a little, so beware. I've tested it
    # with 120 processes and it made my workstation (24 GB RAM ~4 GHz Core i7) struggle to the point of being
    # unable to maintain sufficiently real-time operation for the test to pass. Hm.
    used_node_ids = list(range(20))
    pubs = [
        BackgroundChildProcess.cli('pub', '--period=0.3', '--count=100', *transport_factory(idx).cli_args)
        for idx in used_node_ids
    ]
    result = run_cli_tool('-v', 'pick-nid', *transport_factory(None).cli_args, timeout=60.0)
    print('pick-nid result:', result)
    assert int(result) not in used_node_ids
    for p in pubs:
        p.wait(60.0, interrupt=True)


def _unittest_slow_cli_pick_nid_loopback() -> None:
    result = run_cli_tool('-v', 'pick-nid', '--tr=Loopback(None)', timeout=5.0)
    print('pick-nid result:', result)
    assert 0 <= int(result) < 2 ** 64


def _unittest_slow_cli_pick_nid_udp_localhost() -> None:
    from pyuavcan.transport.udp import UDPTransport
    result = run_cli_tool('-v', 'pick-nid', '--tr=UDP("127.255.255.255/8")', timeout=5.0)
    print('pick-nid result:', result)
    # Exclude zero from the set because an IP address with the host address of zero may cause complications.
    assert 1 <= int(result) < 2 ** UDPTransport.NODE_ID_BIT_LENGTH

    with pytest.raises(CalledProcessError):
        # Fails because the transport is not anonymous!
        run_cli_tool('-v', 'pick-nid', '--tr=UDP("127.0.0.123/8")', timeout=5.0)
