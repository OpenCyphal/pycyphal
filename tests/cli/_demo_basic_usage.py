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
    cli_arguments: typing.Sequence[str]


def _get_iface_options() -> typing.Iterable[_IfaceOption]:
    """
    Provides interface options to test the demo against.
    When adding new transports, add them to the demo and update this factory accordingly.
    Don't forget about redundant configurations, too.
    """
    # TODO: Add more transports when redundancy is supported.
    if sys.platform == 'linux':
        yield _IfaceOption(
            demo_env_vars={'DEMO_INTERFACE_KIND': 'can'},
            cli_arguments=[
                '--socketcan=vcan0,8',  # The demo uses CAN 2.0! SocketCAN does not support nonuniform MTU well.
            ],
        )

    yield _IfaceOption(
        demo_env_vars={},   # Defaults to this, no variable has to be specified. If the default is changed, update this.
        cli_arguments=[
            '--serial=socket://localhost:50905',
        ],
    )


@pytest.mark.parametrize('iface_option', _get_iface_options())  # type: ignore
def _unittest_slow_cli_demo_basic_usage(
        generated_packages: typing.Iterator[typing.List[pyuavcan.dsdl.GeneratedPackageInfo]],
        iface_option:       _IfaceOption) -> None:
    """
    This test is KINDA FRAGILE. It makes assumptions about particular data types and their port IDs and other
    aspects of the demo application. If you change things in the demo, this test will likely break.
    """
    import uavcan.node
    del generated_packages
    try:
        pathlib.Path('/tmp/dsdl-for-my-program').rmdir()    # Where the demo script puts its generated packages
    except OSError:
        pass

    # Generate DSDL namespace "sirius_cyber_corp"
    run_cli_tool('dsdl-gen-pkg', str(TEST_DATA_TYPES_DIR / 'sirius_cyber_corp'),
                 '--lookup', DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "test"
    run_cli_tool('dsdl-gen-pkg', str(TEST_DATA_TYPES_DIR / 'test'),
                 '--lookup', DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "uavcan"
    run_cli_tool('dsdl-gen-pkg', str(PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'))

    demo_proc_env_vars = iface_option.demo_env_vars.copy()
    demo_proc_env_vars['PYUAVCAN_LOGLEVEL'] = 'DEBUG'
    demo_proc = BackgroundChildProcess(
        'python', str(DEMO_DIR / 'basic_usage.py'),
        environment_variables=demo_proc_env_vars
    )
    assert demo_proc.alive

    proc_sub_heartbeat = BackgroundChildProcess.cli(
        'sub', 'uavcan.node.Heartbeat.1.0', '--format=JSON',    # Count unlimited
        '--with-metadata', *iface_option.cli_arguments
    )

    proc_sub_temperature = BackgroundChildProcess.cli(
        'sub', '12345.uavcan.si.sample.temperature.Scalar.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *iface_option.cli_arguments
    )

    proc_sub_diagnostic = BackgroundChildProcess.cli(
        'sub', 'uavcan.diagnostic.Record.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *iface_option.cli_arguments
    )

    try:
        # Time to let the background processes finish initialization.
        # The usage script might take a long time to start because it may have to generate packages first.
        assert demo_proc.alive
        time.sleep(10.0)
        assert demo_proc.alive

        run_cli_tool(
            '-v',
            'pub', '12345.uavcan.si.sample.temperature.Scalar.1.0', '{kelvin: 321.5}',
            '--count=3', '--period=0.1', '--priority=SLOW', '--local-node-id=0',
            '--heartbeat-fields={vendor_specific_status_code: 123456}',
            *iface_option.cli_arguments,
            timeout=5.0
        )

        time.sleep(1.0)     # Time to sync up

        out_sub_heartbeat = proc_sub_heartbeat.wait(1.0, interrupt=True)[1].splitlines()
        out_sub_temperature = proc_sub_temperature.wait(1.0, interrupt=True)[1].splitlines()
        out_sub_diagnostic = proc_sub_diagnostic.wait(1.0, interrupt=True)[1].splitlines()

        # Run service tests while the demo process is still running.
        node_info_text = run_cli_tool('-v', 'call', '42', 'uavcan.node.GetInfo.1.0', '{}',
                                      '--local-node-id', '123', '--format', 'JSON', '--with-metadata',
                                      '--priority', 'SLOW', '--timeout', '3.0',
                                      *iface_option.cli_arguments,
                                      timeout=5.0)
        print('node_info_text:', node_info_text)
        node_info = json.loads(node_info_text)
        assert node_info['430']['_metadata_']['source_node_id'] == 42
        assert node_info['430']['_metadata_']['transfer_id'] >= 0
        assert 'slow' in node_info['430']['_metadata_']['priority'].lower()
        assert node_info['430']['name'] == 'org.uavcan.pyuavcan.demo.basic_usage'
        assert node_info['430']['protocol_version']['major'] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
        assert node_info['430']['protocol_version']['minor'] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[1]

        command_response = json.loads(run_cli_tool(
            '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0',
            f'{{command: {uavcan.node.ExecuteCommand_1_0.Request.COMMAND_STORE_PERSISTENT_STATES} }}',
            '--local-node-id', '123', '--format', 'JSON', *iface_option.cli_arguments, timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_BAD_COMMAND

        # Next request - this fails if the EMITTED TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(run_cli_tool(
            '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0', '{command: 23456}',
            '--local-node-id', '123', '--format', 'JSON', *iface_option.cli_arguments, timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS

        least_squares_response = json.loads(run_cli_tool(
            '-vv', 'call', '42', '123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0',
            '{points: [{x: 1, y: 2}, {x: 10, y: 20}]}', '--timeout=5',
            '--local-node-id', '123', '--format', 'JSON', *iface_option.cli_arguments, timeout=6.0
        ))
        assert least_squares_response['123']['slope'] == pytest.approx(2.0)
        assert least_squares_response['123']['y_intercept'] == pytest.approx(0.0)

        # Next request - this fails if the EMITTED TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(run_cli_tool(
            '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0',
            f'{{command: {uavcan.node.ExecuteCommand_1_0.Request.COMMAND_POWER_OFF} }}',
            '--local-node-id', '123', '--format', 'JSON', *iface_option.cli_arguments, timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS

        # We've just asked the node to terminate, wait for it here.
        out_demo_proc = demo_proc.wait(2.0)[1].splitlines()

        print('out_demo_proc:', *out_demo_proc, sep='\n\t')
        print('out_sub_heartbeat:', *out_sub_heartbeat, sep='\n\t')
        print('out_sub_temperature:', *out_sub_temperature, sep='\n\t')
        print('out_sub_diagnostic:', *out_sub_diagnostic, sep='\n\t')

        assert out_demo_proc
        assert any(re.match(r'TEMPERATURE \d+\.\d+ C', s) for s in out_demo_proc)

        # We receive three heartbeats in order to eliminate possible edge cases due to timing jitter.
        # Sort by source node ID and eliminate the middle; thus we eliminate the uncertainty.
        heartbeats_ordered_by_nid = list(sorted((json.loads(s) for s in out_sub_heartbeat),
                                                key=lambda x: x['32085']['_metadata_']['source_node_id']))
        heartbeat_pub, heartbeat_demo = heartbeats_ordered_by_nid[0], heartbeats_ordered_by_nid[-1]
        print('heartbeat_pub :', heartbeat_pub)
        print('heartbeat_demo:', heartbeat_demo)

        assert 'slow' in heartbeat_pub['32085']['_metadata_']['priority'].lower()
        assert heartbeat_pub['32085']['_metadata_']['transfer_id'] >= 0
        assert heartbeat_pub['32085']['_metadata_']['source_node_id'] == 0
        assert heartbeat_pub['32085']['uptime'] in (0, 1)
        assert heartbeat_pub['32085']['vendor_specific_status_code'] == 123456

        assert 'nominal' in heartbeat_demo['32085']['_metadata_']['priority'].lower()
        assert heartbeat_demo['32085']['_metadata_']['source_node_id'] == 42
        assert heartbeat_demo['32085']['vendor_specific_status_code'] == demo_proc.pid

        for parsed in (json.loads(s) for s in out_sub_temperature):
            assert 'slow' in parsed['12345']['_metadata_']['priority'].lower()
            assert parsed['12345']['_metadata_']['transfer_id'] >= 0
            assert parsed['12345']['_metadata_']['source_node_id'] == 0
            assert parsed['12345']['kelvin'] == pytest.approx(321.5)

        assert len(out_sub_diagnostic) >= 1
    finally:
        # It is important to get rid of processes even in the event of failure because if we fail to do so
        # the processes running in the background may fail the following tests, possibly making them very hard
        # to diagnose and debug.
        demo_proc.kill()
        proc_sub_heartbeat.kill()
        proc_sub_temperature.kill()
        proc_sub_diagnostic.kill()
