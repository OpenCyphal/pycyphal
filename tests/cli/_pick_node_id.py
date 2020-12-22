#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pytest
from ._subprocess import run_cli_tool, BackgroundChildProcess, CalledProcessError
from . import TRANSPORT_FACTORIES, TransportFactory


@pytest.mark.parametrize("transport_factory", TRANSPORT_FACTORIES)  # type: ignore
def _unittest_slow_cli_pick_nid(transport_factory: TransportFactory) -> None:
    # We spawn a lot of processes here, which might strain the test system a little, so beware. I've tested it
    # with 120 processes and it made my workstation (24 GB RAM ~4 GHz Core i7) struggle to the point of being
    # unable to maintain sufficiently real-time operation for the test to pass. Hm.
    used_node_ids = list(range(10))
    pubs = [
        BackgroundChildProcess.cli(
            "pub",
            "--period=0.4",
            "--count=200",
            # Construct an environment variable to ensure syntax equivalency with the `--transport=...` CLI args.
            environment_variables={
                "PYUAVCAN_CLI_TRANSPORT": (",".join(x.replace("--tr=", "") for x in transport_factory(idx).cli_args))
            },
        )
        for idx in used_node_ids
    ]
    result = run_cli_tool("-v", "pick-nid", *transport_factory(None).cli_args, timeout=100.0)
    print("pick-nid result:", result)
    assert int(result) not in used_node_ids
    for p in pubs:
        p.wait(100.0, interrupt=True)


def _unittest_slow_cli_pick_nid_loopback() -> None:
    result = run_cli_tool(
        "-v",
        "pick-nid",
        timeout=30.0,
        environment_variables={"PYUAVCAN_CLI_TRANSPORT": "[Loopback(None), Loopback(None)]"},
    )
    print("pick-nid result:", result)
    assert 0 <= int(result) < 2 ** 64


def _unittest_slow_cli_pick_nid_udp_localhost() -> None:
    result = run_cli_tool(
        "-v",
        "pick-nid",
        timeout=30.0,
        environment_variables={"PYUAVCAN_CLI_TRANSPORT": 'UDP("127.0.0.1",anonymous=True)'},
    )
    print("pick-nid result:", result)
    # Exclude zero from the set because an IP address with the host address of zero may cause complications.
    assert 1 <= int(result) <= 65534

    with pytest.raises(CalledProcessError):
        # Fails because the transport is not anonymous!
        run_cli_tool("-v", "pick-nid", '--tr=UDP("127.0.0.123")', timeout=30.0)
