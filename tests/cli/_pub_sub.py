# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import time
import json
import pathlib
import pytest
from tests.dsdl.conftest import PUBLIC_REGULATED_DATA_TYPES_DIR
from ._subprocess import run_cli_tool, BackgroundChildProcess
from . import TRANSPORT_FACTORIES, TransportFactory


@pytest.mark.parametrize("transport_factory", TRANSPORT_FACTORIES)  # type: ignore
def _unittest_slow_cli_pub_sub(transport_factory: TransportFactory) -> None:
    # Generate DSDL namespace "uavcan"
    if not pathlib.Path("uavcan").exists():
        run_cli_tool("dsdl-gen-pkg", str(PUBLIC_REGULATED_DATA_TYPES_DIR / "uavcan"))

    proc_sub_heartbeat = BackgroundChildProcess.cli(
        "sub",
        "uavcan.node.Heartbeat.1.0",
        "--format=json",  # Count unlimited
        "--with-metadata",
        *transport_factory(None).cli_args,
    )

    proc_sub_diagnostic = BackgroundChildProcess.cli(
        "sub",
        "4321.uavcan.diagnostic.Record.1.1",
        "--count=3",
        "--format=json",
        "--with-metadata",
        *transport_factory(None).cli_args,
    )

    proc_sub_diagnostic_wrong_pid = BackgroundChildProcess.cli(
        "sub",
        "uavcan.diagnostic.Record.1.1",
        "--count=3",
        "--format=yaml",
        "--with-metadata",
        *transport_factory(None).cli_args,
    )

    proc_sub_temperature = BackgroundChildProcess.cli(
        "sub",
        "555.uavcan.si.sample.temperature.Scalar.1.0",
        "--count=3",
        "--format=json",
        *transport_factory(None).cli_args,
    )

    time.sleep(1.0)  # Time to let the background processes finish initialization

    run_cli_tool(
        "-v",
        "pub",
        "4321.uavcan.diagnostic.Record.1.1",
        '{severity: {value: 6}, timestamp: {microsecond: 123456}, text: "Hello world!"}',
        "1234.uavcan.diagnostic.Record.1.1",
        '{text: "Goodbye world."}',
        "555.uavcan.si.sample.temperature.Scalar.1.0",
        "{kelvin: 123.456}",
        "--count=3",
        "--period=0.1",
        "--priority=slow",
        "--heartbeat-fields={vendor_specific_status_code: 54}",
        *transport_factory(51).cli_args,
        timeout=10.0,
    )

    time.sleep(1.0)  # Time to sync up

    out_sub_heartbeat = proc_sub_heartbeat.wait(1.0, interrupt=True)[1].splitlines()
    out_sub_diagnostic = proc_sub_diagnostic.wait(1.0, interrupt=True)[1].splitlines()
    out_sub_temperature = proc_sub_temperature.wait(1.0, interrupt=True)[1].splitlines()

    print("out_sub_heartbeat:", *out_sub_heartbeat, sep="\n\t")
    print("out_sub_diagnostic:", *out_sub_diagnostic, sep="\n\t")
    print("proc_sub_temperature:", *out_sub_temperature, sep="\n\t")

    heartbeats = list(map(json.loads, out_sub_heartbeat))
    diagnostics = list(map(json.loads, out_sub_diagnostic))
    temperatures = list(map(json.loads, out_sub_temperature))

    print("heartbeats:", *heartbeats, sep="\n\t")
    print("diagnostics:", *diagnostics, sep="\n\t")
    print("temperatures:", *temperatures, sep="\n\t")

    assert 2 <= len(heartbeats) <= 6
    for m in heartbeats:
        assert "slow" in m["7509"]["_metadata_"]["priority"].lower()
        assert m["7509"]["_metadata_"]["transfer_id"] >= 0
        assert m["7509"]["_metadata_"]["source_node_id"] == 51
        assert m["7509"]["uptime"] in (0, 1)
        assert m["7509"]["vendor_specific_status_code"] == 54

    assert len(diagnostics) == 3
    for m in diagnostics:
        assert "slow" in m["4321"]["_metadata_"]["priority"].lower()
        assert m["4321"]["_metadata_"]["transfer_id"] >= 0
        assert m["4321"]["_metadata_"]["source_node_id"] == 51
        assert m["4321"]["timestamp"]["microsecond"] == 123456
        assert m["4321"]["text"] == "Hello world!"

    assert len(temperatures) == 3
    assert all(map(lambda mt: mt["555"]["kelvin"] == pytest.approx(123.456), temperatures))

    assert proc_sub_diagnostic_wrong_pid.alive
    assert proc_sub_diagnostic_wrong_pid.wait(1.0, interrupt=True)[1].strip() == ""


@pytest.mark.parametrize("transport_factory", TRANSPORT_FACTORIES)  # type: ignore
def _unittest_slow_cli_pub_sub_anon(transport_factory: TransportFactory) -> None:
    # Generate DSDL namespace "uavcan"
    if not pathlib.Path("uavcan").exists():
        run_cli_tool("dsdl-gen-pkg", str(PUBLIC_REGULATED_DATA_TYPES_DIR / "uavcan"))

    proc_sub_heartbeat = BackgroundChildProcess.cli(
        "-v", "sub", "uavcan.node.Heartbeat.1.0", "--format=json", *transport_factory(None).cli_args  # Count unlimited
    )

    proc_sub_diagnostic_with_meta = BackgroundChildProcess.cli(
        "-v",
        "sub",
        "uavcan.diagnostic.Record.1.1",
        "--format=json",
        "--with-metadata",
        *transport_factory(None).cli_args,
    )

    proc_sub_diagnostic_no_meta = BackgroundChildProcess.cli(
        "-v",
        "sub",
        "uavcan.diagnostic.Record.1.1",
        "--format=json",
        *transport_factory(None).cli_args,
    )

    time.sleep(3.0)  # Time to let the background processes finish initialization

    if transport_factory(None).can_transmit:
        proc = BackgroundChildProcess.cli(
            "-v",
            "pub",
            "uavcan.diagnostic.Record.1.1",
            "{}",
            "--count=2",
            "--period=2",
            *transport_factory(None).cli_args,
        )
        proc.wait(timeout=8)

        time.sleep(2.0)  # Time to sync up

        assert (
            proc_sub_heartbeat.wait(1.0, interrupt=True)[1].strip() == ""
        ), "Anonymous nodes must not broadcast heartbeat"

        diagnostics = list(
            json.loads(s) for s in proc_sub_diagnostic_with_meta.wait(1.0, interrupt=True)[1].splitlines()
        )
        print("diagnostics:", diagnostics)
        # Remember that anonymous transfers over redundant transports are NOT deduplicated.
        # Hence, to support the case of redundant transports, we use 'greater or equal' here.
        assert len(diagnostics) >= 2
        for m in diagnostics:
            assert "nominal" in m["8184"]["_metadata_"]["priority"].lower()
            assert m["8184"]["_metadata_"]["transfer_id"] >= 0
            assert m["8184"]["_metadata_"]["source_node_id"] is None
            assert m["8184"]["timestamp"]["microsecond"] == 0
            assert m["8184"]["text"] == ""

        diagnostics = list(json.loads(s) for s in proc_sub_diagnostic_no_meta.wait(1.0, interrupt=True)[1].splitlines())
        print("diagnostics:", diagnostics)
        assert len(diagnostics) >= 2  # >= because see above
        for index, m in enumerate(diagnostics):
            assert m["8184"]["timestamp"]["microsecond"] == 0
            assert m["8184"]["text"] == ""
    else:
        proc = BackgroundChildProcess.cli(
            "-v",
            "pub",
            "uavcan.diagnostic.Record.1.1",
            "{}",
            "--count=2",
            "--period=2",
            *transport_factory(None).cli_args,
        )
        assert 0 < proc.wait(timeout=8)[0]
