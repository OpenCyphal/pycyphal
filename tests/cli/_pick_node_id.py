#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from ._subprocess import run_process, BackgroundChildProcess
from ._common_args import make_iface_args


def _unittest_slow_cli_pick_nid() -> None:
    # We spawn a lot of processes here, which might strain the test system a little, so beware. I've tested it
    # with 120 processes and it made my workstation (24 GB RAM ~4 GHz Core i7) struggle to the point of being
    # unable to maintain sufficiently real-time operation for the test to pass. Hm.
    used_node_ids = list(range(20))
    pubs = [
        BackgroundChildProcess('pyuavcan', 'pub', '--period=0.3', '--count=100', f'--local-node-id={idx}',
                               *make_iface_args())
        for idx in used_node_ids
    ]

    result = run_process('pyuavcan', '-v', 'pick-nid', *make_iface_args(), timeout=60.0)
    print('pick-nid result:', result)

    assert int(result) not in used_node_ids

    for p in pubs:
        p.wait(10.0, interrupt=True)
