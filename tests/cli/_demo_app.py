#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import re
import sys
import time
import json
import typing
import pytest
import pathlib
import dataclasses
import pyuavcan
from ._subprocess import run_cli_tool, BackgroundChildProcess, DEMO_DIR

# noinspection PyProtectedMember
from pyuavcan._cli import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL
from tests.dsdl.conftest import TEST_DATA_TYPES_DIR, PUBLIC_REGULATED_DATA_TYPES_DIR, generated_packages


@dataclasses.dataclass
class _IfaceOption:
    demo_env_vars: typing.Dict[str, str]
    make_cli_args: typing.Callable[[typing.Optional[int]], typing.Sequence[str]]


def _get_iface_options() -> typing.Iterable[_IfaceOption]:
    """
    Provides interface options to test the demo against.
    When adding new transports, add them to the demo and update this factory accordingly.
    Don't forget about redundant configurations, too.
    """
    if sys.platform == "linux":
        # CAN
        yield _IfaceOption(
            demo_env_vars={"DEMO_INTERFACE_KIND": "can"},
            make_cli_args=lambda nid: (  # The demo uses Classic CAN! SocketCAN does not support nonuniform MTU well.
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan0",8),local_node_id={nid})',
            ),
        )

        # TMR CAN
        yield _IfaceOption(
            demo_env_vars={"DEMO_INTERFACE_KIND": "can_can_can"},
            make_cli_args=lambda nid: (  # The MTU values are like in the demo otherwise SocketCAN may misbehave.
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan0",8),local_node_id={nid})',
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan1",32),local_node_id={nid})',
                f'--tr=CAN(can.media.socketcan.SocketCANMedia("vcan2",64),local_node_id={nid})',
            ),
        )

    # Serial
    yield _IfaceOption(
        demo_env_vars={"DEMO_INTERFACE_KIND": "serial"},
        make_cli_args=lambda nid: (f'--tr=Serial("socket://localhost:50905",local_node_id={nid})',),
    )

    # UDP
    yield _IfaceOption(
        demo_env_vars={"DEMO_INTERFACE_KIND": "udp"},
        make_cli_args=lambda nid: (
            (f'--tr=UDP("127.0.0.{nid}")',)  # Regular node
            if nid is not None
            else (f'--tr=UDP("127.0.0.1",anonymous=True)',)  # Anonymous node
        ),
    )

    # DMR UDP+Serial
    yield _IfaceOption(
        demo_env_vars={"DEMO_INTERFACE_KIND": "udp_serial"},
        make_cli_args=lambda nid: (
            (
                f'--tr=UDP("127.0.0.{nid}")'  # Regular node
                if nid is not None
                else f'--tr=UDP("127.0.0.1",anonymous=True)'  # Anonymous node
            ),
            f'--tr=Serial("socket://localhost:50905",local_node_id={nid})',
        ),
    )


@pytest.mark.parametrize("iface_option", _get_iface_options())  # type: ignore
def _unittest_slow_cli_demo_app(
    generated_packages: typing.Iterator[typing.List[pyuavcan.dsdl.GeneratedPackageInfo]], iface_option: _IfaceOption
) -> None:
    """
    This test is KINDA FRAGILE. It makes assumptions about particular data types and their port IDs and other
    aspects of the demo application. If you change things in the demo, this test will likely break.
    """
    import uavcan.node

    del generated_packages
    try:
        pathlib.Path("/tmp/dsdl-for-my-program").rmdir()  # Where the demo script puts its generated packages
    except OSError:
        pass

    # The demo may need to generate packages as well, so we launch it first.
    demo_proc_env_vars = iface_option.demo_env_vars.copy()
    demo_proc_env_vars["PYUAVCAN_LOGLEVEL"] = "DEBUG"
    demo_proc = BackgroundChildProcess(
        "python", str(DEMO_DIR / "demo_app.py"), environment_variables=demo_proc_env_vars
    )
    assert demo_proc.alive

    # Generate DSDL namespace "sirius_cyber_corp"
    if not pathlib.Path("sirius_cyber_corp").exists():
        run_cli_tool(
            "dsdl-gen-pkg",
            str(TEST_DATA_TYPES_DIR / "sirius_cyber_corp"),
            "--lookup",
            DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL,
        )

    # Generate DSDL namespace "test"
    if not pathlib.Path("test_dsdl_namespace").exists():
        run_cli_tool(
            "dsdl-gen-pkg",
            str(TEST_DATA_TYPES_DIR / "test_dsdl_namespace"),
            "--lookup",
            DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL,
        )

    # Generate DSDL namespace "uavcan"
    if not pathlib.Path("uavcan").exists():
        run_cli_tool("dsdl-gen-pkg", str(PUBLIC_REGULATED_DATA_TYPES_DIR / "uavcan"))

    # Time to let the background processes finish initialization.
    # The usage demo might take a long time to start because it may have to generate packages first.
    time.sleep(90)

    proc_sub_heartbeat = BackgroundChildProcess.cli(
        "sub",
        "uavcan.node.Heartbeat.1.0",
        "--format=json",  # Count unlimited
        "--with-metadata",
        *iface_option.make_cli_args(None),  # type: ignore
    )

    proc_sub_temperature = BackgroundChildProcess.cli(
        "sub",
        "2345.uavcan.si.sample.temperature.Scalar.1.0",
        "--count=3",
        "--format=json",
        "--with-metadata",
        *iface_option.make_cli_args(None),  # type: ignore
    )

    proc_sub_diagnostic = BackgroundChildProcess.cli(
        "sub",
        "uavcan.diagnostic.Record.1.1",
        "--count=3",
        "--format=json",
        "--with-metadata",
        *iface_option.make_cli_args(None),  # type: ignore
    )

    try:
        assert demo_proc.alive

        run_cli_tool(
            "-v",
            "pub",
            "2345.uavcan.si.sample.temperature.Scalar.1.0",
            "{kelvin: 321.5}",
            "--count=5",
            "--period=1",
            "--priority=slow",
            "--heartbeat-fields={vendor_specific_status_code: 123}",
            *iface_option.make_cli_args(1),  # type: ignore
            timeout=10.0,
        )

        time.sleep(1.0)  # Time to sync up

        out_sub_heartbeat = proc_sub_heartbeat.wait(2.0, interrupt=True)[1].splitlines()
        out_sub_temperature = proc_sub_temperature.wait(2.0, interrupt=True)[1].splitlines()
        out_sub_diagnostic = proc_sub_diagnostic.wait(2.0, interrupt=True)[1].splitlines()

        assert demo_proc.alive
        # Run service tests while the demo process is still running.
        node_info_text = run_cli_tool(
            "-v",
            "call",
            "42",
            "uavcan.node.GetInfo.1.0",
            "{}",
            "--format",
            "json",
            "--with-metadata",
            "--priority",
            "slow",
            "--timeout",
            "3.0",
            *iface_option.make_cli_args(123),  # type: ignore
            timeout=5.0,
        )
        assert demo_proc.alive
        print("node_info_text:", node_info_text)
        node_info = json.loads(node_info_text)
        assert node_info["430"]["_metadata_"]["source_node_id"] == 42
        assert node_info["430"]["_metadata_"]["transfer_id"] >= 0
        assert "slow" in node_info["430"]["_metadata_"]["priority"].lower()
        assert node_info["430"]["name"] == "org.uavcan.pyuavcan.demo.demo_app"
        assert node_info["430"]["protocol_version"]["major"] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
        assert node_info["430"]["protocol_version"]["minor"] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[1]

        assert demo_proc.alive
        command_response = json.loads(
            run_cli_tool(
                "-v",
                "call",
                "42",
                "uavcan.node.ExecuteCommand.1.1",
                f"{{command: {uavcan.node.ExecuteCommand_1_1.Request.COMMAND_STORE_PERSISTENT_STATES} }}",
                "--format",
                "json",
                *iface_option.make_cli_args(123),
                timeout=5.0,  # type: ignore
            )
        )
        assert command_response["435"]["status"] == uavcan.node.ExecuteCommand_1_1.Response.STATUS_BAD_COMMAND

        # Next request - this fails if the OUTPUT TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(
            run_cli_tool(
                "-v",
                "call",
                "42",
                "uavcan.node.ExecuteCommand.1.1",
                "{command: 23456}",
                "--format",
                "json",
                *iface_option.make_cli_args(123),
                timeout=5.0,  # type: ignore
            )
        )
        assert command_response["435"]["status"] == uavcan.node.ExecuteCommand_1_1.Response.STATUS_SUCCESS

        assert demo_proc.alive
        least_squares_response = json.loads(
            run_cli_tool(
                "-vv",
                "call",
                "42",
                "123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0",
                "{points: [{x: 1, y: 2}, {x: 10, y: 20}]}",
                "--timeout=5",
                "--format",
                "json",
                *iface_option.make_cli_args(123),
                timeout=6.0,  # type: ignore
            )
        )
        assert least_squares_response["123"]["slope"] == pytest.approx(2.0)
        assert least_squares_response["123"]["y_intercept"] == pytest.approx(0.0)

        assert demo_proc.alive
        # Next request - this fails if the OUTPUT TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(
            run_cli_tool(
                "-v",
                "call",
                "42",
                "uavcan.node.ExecuteCommand.1.1",
                f"{{command: {uavcan.node.ExecuteCommand_1_1.Request.COMMAND_POWER_OFF} }}",
                "--format",
                "json",
                *iface_option.make_cli_args(123),
                timeout=5.0,  # type: ignore
            )
        )
        assert command_response["435"]["status"] == uavcan.node.ExecuteCommand_1_1.Response.STATUS_SUCCESS

        # We've just asked the node to terminate, wait for it here.
        out_demo_proc = demo_proc.wait(10.0)[1].splitlines()

        print("out_demo_proc:", *out_demo_proc, sep="\n\t")
        print("out_sub_heartbeat:", *out_sub_heartbeat, sep="\n\t")
        print("out_sub_temperature:", *out_sub_temperature, sep="\n\t")
        print("out_sub_diagnostic:", *out_sub_diagnostic, sep="\n\t")

        assert out_demo_proc
        assert any(re.match(r"TEMPERATURE \d+\.\d+ C", s) for s in out_demo_proc)

        # We receive three heartbeats in order to eliminate possible edge cases due to timing jitter.
        # Sort by source node ID and eliminate the middle; thus we eliminate the uncertainty.
        heartbeats_ordered_by_nid = list(
            sorted(
                (json.loads(s) for s in out_sub_heartbeat), key=lambda x: int(x["7509"]["_metadata_"]["source_node_id"])
            )
        )
        print("heartbeats_ordered_by_nid:", heartbeats_ordered_by_nid)
        heartbeat_pub, heartbeat_demo = heartbeats_ordered_by_nid[0], heartbeats_ordered_by_nid[-1]
        print("heartbeat_pub :", heartbeat_pub)
        print("heartbeat_demo:", heartbeat_demo)

        assert "slow" in heartbeat_pub["7509"]["_metadata_"]["priority"].lower()
        assert heartbeat_pub["7509"]["_metadata_"]["transfer_id"] >= 0
        assert heartbeat_pub["7509"]["_metadata_"]["source_node_id"] == 1
        assert heartbeat_pub["7509"]["uptime"] in (0, 1)
        assert heartbeat_pub["7509"]["vendor_specific_status_code"] == 123

        assert "nominal" in heartbeat_demo["7509"]["_metadata_"]["priority"].lower()
        assert heartbeat_demo["7509"]["_metadata_"]["source_node_id"] == 42
        assert heartbeat_demo["7509"]["vendor_specific_status_code"] == demo_proc.pid % 100

        for parsed in (json.loads(s) for s in out_sub_temperature):
            assert "slow" in parsed["2345"]["_metadata_"]["priority"].lower()
            assert parsed["2345"]["_metadata_"]["transfer_id"] >= 0
            assert parsed["2345"]["_metadata_"]["source_node_id"] == 1
            assert parsed["2345"]["kelvin"] == pytest.approx(321.5)

        assert len(out_sub_diagnostic) >= 1
    finally:
        # It is important to get rid of processes even in the event of failure because if we fail to do so
        # the processes running in the background may fail the following tests, possibly making them very hard
        # to diagnose and debug.
        demo_proc.kill()
        proc_sub_heartbeat.kill()
        proc_sub_temperature.kill()
        proc_sub_diagnostic.kill()
