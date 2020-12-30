# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import re
import sys
import shutil
import typing
import pytest
import asyncio
import pathlib
import tempfile
import dataclasses
import pyuavcan
from ._subprocess import BackgroundChildProcess

# noinspection PyUnresolvedReferences
from tests.dsdl.conftest import generated_packages


DEMO_APP_NODE_ID = 42
DEMO_DIR = pathlib.Path(__file__).absolute().parent.parent.parent / "demo"


@dataclasses.dataclass
class RunConfig:
    demo_env_vars: typing.Dict[str, str]
    local_transport_factory: typing.Callable[[typing.Optional[int]], pyuavcan.transport.Transport]


def _get_run_configs() -> typing.Iterable[RunConfig]:
    """
    Provides interface options to test the demo against.
    When adding new transports, add them to the demo and update this factory accordingly.
    Don't forget about redundant configurations, too.
    """
    from pyuavcan.transport.redundant import RedundantTransport
    from pyuavcan.transport.serial import SerialTransport
    from pyuavcan.transport.udp import UDPTransport

    # UDP
    yield RunConfig(
        demo_env_vars={"DEMO_INTERFACE_KIND": "udp"},
        local_transport_factory=lambda nid: UDPTransport(f"127.0.0.{1 if nid is None else nid}", anonymous=nid is None),
    )

    # Serial
    yield RunConfig(
        demo_env_vars={"DEMO_INTERFACE_KIND": "serial"},
        local_transport_factory=lambda nid: SerialTransport("socket://localhost:50905", local_node_id=nid),
    )

    # DMR UDP+Serial
    def make_udp_serial(nid: typing.Optional[int]) -> pyuavcan.transport.Transport:
        tr = RedundantTransport()
        if nid is not None:
            tr.attach_inferior(UDPTransport(f"127.0.0.{nid}"))
        else:
            tr.attach_inferior(UDPTransport(f"127.0.0.1", anonymous=True))
        tr.attach_inferior(SerialTransport("socket://localhost:50905", local_node_id=nid))
        return tr

    yield RunConfig(
        demo_env_vars={"DEMO_INTERFACE_KIND": "udp_serial"},
        local_transport_factory=make_udp_serial,
    )

    if sys.platform.startswith("linux"):
        from pyuavcan.transport.can.media.socketcan import SocketCANMedia
        from pyuavcan.transport.can import CANTransport

        # CAN
        yield RunConfig(
            demo_env_vars={"DEMO_INTERFACE_KIND": "can"},
            # The demo uses Classic CAN! SocketCAN does not support nonuniform MTU well.
            local_transport_factory=lambda nid: CANTransport(SocketCANMedia("vcan0", 8), local_node_id=nid),
        )

        # TMR CAN
        def make_tmr_can(nid: typing.Optional[int]) -> pyuavcan.transport.Transport:
            from pyuavcan.transport.redundant import RedundantTransport

            tr = RedundantTransport()
            tr.attach_inferior(CANTransport(SocketCANMedia("vcan0", 8), local_node_id=nid))
            tr.attach_inferior(CANTransport(SocketCANMedia("vcan1", 32), local_node_id=nid))
            tr.attach_inferior(CANTransport(SocketCANMedia("vcan2", 64), local_node_id=nid))
            return tr

        yield RunConfig(
            demo_env_vars={"DEMO_INTERFACE_KIND": "can_can_can"},
            local_transport_factory=make_tmr_can,
        )


@pytest.mark.parametrize("parameters", [(idx == 0, rc) for idx, rc in enumerate(_get_run_configs())])  # type: ignore
@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_demo_app(
    generated_packages: typing.Iterator[typing.List[pyuavcan.dsdl.GeneratedPackageInfo]],
    parameters: typing.Tuple[bool, RunConfig],
) -> None:
    """
    This test is KINDA FRAGILE. It makes assumptions about particular data types and their port IDs and other
    aspects of the demo application. If you change things in the demo, this test will likely break.
    """
    import uavcan.node
    import uavcan.diagnostic
    import uavcan.si.sample.temperature
    import sirius_cyber_corp
    import pyuavcan.application

    asyncio.get_running_loop().slow_callback_duration = 3.0
    _ = generated_packages

    first_run, run_config = parameters
    if first_run:
        # At the first run, force the demo script to regenerate packages.
        # The following runs shall not force this behavior to save time and enhance branch coverage.
        print("FORCE DSDL RECOMPILATION")
        dsdl_output_path = pathlib.Path(tempfile.gettempdir(), "dsdl-for-my-program")
        if dsdl_output_path.exists():
            shutil.rmtree(dsdl_output_path)

    # The demo may need to generate packages as well, so we launch it first.
    demo_proc_env_vars = run_config.demo_env_vars.copy()
    demo_proc_env_vars.update(
        {
            "PYUAVCAN_LOGLEVEL": "INFO",
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),  # https://github.com/appveyor/ci/issues/1995
        }
    )
    demo_proc = BackgroundChildProcess(
        "python",
        "-m",
        "coverage",
        "run",
        str(DEMO_DIR / "demo_app.py"),
        environment_variables=demo_proc_env_vars,
    )
    assert demo_proc.alive
    print("DEMO APP STARTED WITH PID", demo_proc.pid, "FROM", pathlib.Path.cwd())

    # Initialize the local node for testing.
    try:
        transport = run_config.local_transport_factory(123)  # type: ignore
        presentation = pyuavcan.presentation.Presentation(transport)
    except Exception:
        demo_proc.kill()
        raise

    # Run the test and make sure to clean up at exit to avoid resource usage warnings in the test logs.
    try:
        local_node_info = uavcan.node.GetInfo_1_0.Response(
            protocol_version=uavcan.node.Version_1_0(*pyuavcan.UAVCAN_SPECIFICATION_VERSION),
            software_version=uavcan.node.Version_1_0(*pyuavcan.__version_info__[:2]),
            name="org.uavcan.pyuavcan.test.demo_app",
        )
        node = pyuavcan.application.Node(presentation, local_node_info, with_diagnostic_subscriber=True)

        # Construct the ports we will be using to interact with the demo application.
        sub_heartbeat = node.presentation.make_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
        sub_diagnostics = node.presentation.make_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_1)
        pub_temperature = node.presentation.make_publisher(uavcan.si.sample.temperature.Scalar_1_0, 2345)
        client_get_info = node.presentation.make_client_with_fixed_service_id(uavcan.node.GetInfo_1_0, DEMO_APP_NODE_ID)
        client_command = node.presentation.make_client_with_fixed_service_id(
            uavcan.node.ExecuteCommand_1_1, DEMO_APP_NODE_ID
        )
        client_least_squares = node.presentation.make_client(
            sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0, 123, DEMO_APP_NODE_ID
        )

        # At the first run, the usage demo might take a long time to start because it has to compile DSDL.
        # That's why we wait for it here to announce readiness by subscribing to the heartbeat.
        assert demo_proc.alive
        first_hb_transfer = await sub_heartbeat.receive_for(100.0)  # Pick a sensible start-up timeout.
        print("FIRST HEARTBEAT:", first_hb_transfer)
        assert first_hb_transfer
        assert first_hb_transfer[1].source_node_id == DEMO_APP_NODE_ID
        assert first_hb_transfer[1].transfer_id < 10  # We may have missed a couple but not too many!
        assert demo_proc.alive
        # Once the heartbeat is in, we know that the demo is ready for being tested.

        # Validate GetInfo.
        client_get_info.priority = pyuavcan.transport.Priority.EXCEPTIONAL
        client_get_info.transfer_id_counter.override(22)
        info_transfer = await client_get_info.call(uavcan.node.GetInfo_1_0.Request())
        print("GET INFO RESPONSE:", info_transfer)
        assert info_transfer
        info, transfer = info_transfer
        assert transfer.source_node_id == DEMO_APP_NODE_ID
        assert transfer.transfer_id == 22
        assert transfer.priority == pyuavcan.transport.Priority.EXCEPTIONAL
        assert isinstance(info, uavcan.node.GetInfo_1_0.Response)
        assert info.name.tobytes().decode() == "org.uavcan.pyuavcan.demo.demo_app"
        assert info.protocol_version.major == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
        assert info.protocol_version.minor == pyuavcan.UAVCAN_SPECIFICATION_VERSION[1]
        assert info.software_version.major == 1
        assert info.software_version.minor == 0

        # Test the linear regression service.
        solution_transfer = await client_least_squares.call(
            sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Request(
                points=[
                    sirius_cyber_corp.PointXY_1_0(x=1, y=2),
                    sirius_cyber_corp.PointXY_1_0(x=10, y=20),
                ]
            )
        )
        print("LINEAR REGRESSION RESPONSE:", info_transfer)
        assert solution_transfer
        solution, transfer = solution_transfer
        assert transfer.source_node_id == DEMO_APP_NODE_ID
        assert transfer.transfer_id == 0
        assert transfer.priority == pyuavcan.transport.Priority.NOMINAL
        assert isinstance(solution, sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response)
        assert solution.slope == pytest.approx(2.0)
        assert solution.y_intercept == pytest.approx(0.0)

        # Publish temperature. The result will be validated later.
        pub_temperature.priority = pyuavcan.transport.Priority.SLOW
        pub_temperature.publish_soon(uavcan.si.sample.temperature.Scalar_1_0(kelvin=321.5))

        # Test the command execution service.
        # Bad command.
        result_transfer = await client_command.call(
            uavcan.node.ExecuteCommand_1_1.Request(
                command=uavcan.node.ExecuteCommand_1_1.Request.COMMAND_STORE_PERSISTENT_STATES
            )
        )
        print("BAD COMMAND RESPONSE:", info_transfer)
        assert result_transfer
        result, transfer = result_transfer
        assert transfer.source_node_id == DEMO_APP_NODE_ID
        assert transfer.transfer_id == 0
        assert transfer.priority == pyuavcan.transport.Priority.NOMINAL
        assert isinstance(result, uavcan.node.ExecuteCommand_1_1.Response)
        assert result.status == result.STATUS_BAD_COMMAND
        # Good custom command.
        result_transfer = await client_command.call(
            uavcan.node.ExecuteCommand_1_1.Request(
                command=23456,
                parameter="This is my custom command parameter",
            )
        )
        print("CUSTOM COMMAND RESPONSE:", info_transfer)
        assert result_transfer
        result, transfer = result_transfer
        assert transfer.source_node_id == DEMO_APP_NODE_ID
        assert transfer.transfer_id == 1
        assert transfer.priority == pyuavcan.transport.Priority.NOMINAL
        assert isinstance(result, uavcan.node.ExecuteCommand_1_1.Response)
        assert result.status == result.STATUS_SUCCESS
        # FINAL COMMAND: ASK THE NODE TO TERMINATE ITSELF.
        assert demo_proc.alive, "Can't ask a dead node to kill itself, it's impolite."
        result_transfer = await client_command.call(
            uavcan.node.ExecuteCommand_1_1.Request(command=uavcan.node.ExecuteCommand_1_1.Request.COMMAND_POWER_OFF)
        )
        print("POWER OFF COMMAND RESPONSE:", info_transfer)
        assert result_transfer
        result, transfer = result_transfer
        assert transfer.source_node_id == DEMO_APP_NODE_ID
        assert transfer.transfer_id == 2
        assert transfer.priority == pyuavcan.transport.Priority.NOMINAL
        assert isinstance(result, uavcan.node.ExecuteCommand_1_1.Response)
        assert result.status == result.STATUS_SUCCESS

        # Validate the heartbeats (all of them) while waiting for the node to terminate.
        prev_hb_transfer = first_hb_transfer
        num_heartbeats = 0
        while True:
            # The timeout will get triggered at some point because the demo app has been asked to stop.
            hb_transfer = await sub_heartbeat.receive_for(1.0)
            if hb_transfer is None:
                break
            hb, transfer = hb_transfer
            assert num_heartbeats <= transfer.transfer_id <= 300
            assert transfer.priority == pyuavcan.transport.Priority.NOMINAL
            assert transfer.source_node_id == DEMO_APP_NODE_ID
            assert hb.health.value == hb.health.NOMINAL
            assert hb.mode.value == hb.mode.OPERATIONAL
            assert num_heartbeats <= hb.uptime <= 300
            assert hb.uptime == prev_hb_transfer[0].uptime + 1
            assert transfer.transfer_id == prev_hb_transfer[1].transfer_id + 1
            prev_hb_transfer = hb_transfer
            num_heartbeats += 1
        assert num_heartbeats > 0

        # Validate the diagnostic messages while waiting for the node to terminate.
        async def get_next_diagnostic() -> typing.Optional[str]:
            d = await sub_diagnostics.receive_for(1.0)
            if d:
                print("RECEIVED DIAGNOSTIC:", d)
                m, t = d
                assert t.source_node_id == DEMO_APP_NODE_ID
                assert t.priority == pyuavcan.transport.Priority.OPTIONAL
                assert isinstance(m, uavcan.diagnostic.Record_1_1)
                s = m.text.tobytes().decode()
                assert isinstance(s, str)
                return s
            return None

        assert re.match(rf"Least squares request from {transport.local_node_id}.*", await get_next_diagnostic() or "")
        assert re.match(r"Solution for .*: ", await get_next_diagnostic() or "")
        assert re.match(rf"Temperature .* from {transport.local_node_id}.*", await get_next_diagnostic() or "")
        assert not await get_next_diagnostic()

        # We've asked the node to terminate, wait for it here.
        out_demo_proc = demo_proc.wait(10.0)[1].splitlines()
        print("DEMO APP FINAL OUTPUT:", *out_demo_proc, sep="\n\t")
        assert out_demo_proc
        assert any(re.match(r"TEMPERATURE \d+\.\d+ C", s) for s in out_demo_proc)
        assert any(re.match(r"CUSTOM COMMAND PARAMETER: This is my custom command parameter", s) for s in out_demo_proc)
    finally:
        presentation.close()
        demo_proc.kill()
        await asyncio.sleep(2.0)  # Let coroutines terminate properly to avoid resource usage warnings.
