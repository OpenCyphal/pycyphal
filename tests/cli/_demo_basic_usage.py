#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import re
import sys
import time
import json
import typing
import pytest
import pathlib
import pyuavcan
from ._subprocess import run_process, BackgroundChildProcess
# noinspection PyProtectedMember
from pyuavcan._cli import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL
from tests.dsdl.conftest import TEST_DATA_TYPES_DIR, PUBLIC_REGULATED_DATA_TYPES_DIR, generated_packages


def _unittest_slow_cli_demo_basic_usage(
        generated_packages: typing.Iterator[typing.List[pyuavcan.dsdl.GeneratedPackageInfo]]) -> None:
    """
    This test is KINDA FRAGILE. It makes assumptions about particular data types and their port IDs and other
    aspects of the demo application. If you change anything in the demo, this test may break, so please keep
    an eye out.
    """
    import uavcan.node
    del generated_packages
    try:
        pathlib.Path('/tmp/dsdl-for-my-program').rmdir()    # Where the demo script puts its generated packages
    except OSError:
        pass

    # Generate DSDL namespace "sirius_cyber_corp"
    run_process('pyuavcan', 'dsdl-gen-pkg',
                str(TEST_DATA_TYPES_DIR / 'sirius_cyber_corp'),
                '--lookup', DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "test"
    run_process('pyuavcan', 'dsdl-gen-pkg',
                str(TEST_DATA_TYPES_DIR / 'test'),
                '--lookup', DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "uavcan"
    run_process('pyuavcan', 'dsdl-gen-pkg', str(PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'))

    demo_proc = BackgroundChildProcess('basic_usage.py')

    proc_sub_heartbeat = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.node.Heartbeat.1.0', '--format=JSON',    # Count unlimited
        '--with-metadata', *_get_iface_args()
    )

    proc_sub_temperature = BackgroundChildProcess(
        'pyuavcan', 'sub', '12345.uavcan.si.temperature.Scalar.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *_get_iface_args()
    )

    proc_sub_diagnostic = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.diagnostic.Record.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *_get_iface_args()
    )

    try:
        # Time to let the background processes finish initialization.
        # The usage script might take a long time to start because it may have to generate packages first.
        time.sleep(10.0)

        run_process(
            'pyuavcan', '-v',
            'pub', '12345.uavcan.si.temperature.Scalar.1.0', '{kelvin: 321.5}',
            '--count=3', '--period=0.1', '--priority=SLOW', '--local-node-id=0',
            '--heartbeat-fields={vendor_specific_status_code: 123456}',
            *_get_iface_args(),
            timeout=3.0
        )

        time.sleep(1.0)     # Time to sync up

        out_sub_heartbeat = proc_sub_heartbeat.wait(1.0, interrupt=True)[1].splitlines()
        out_sub_temperature = proc_sub_temperature.wait(1.0, interrupt=True)[1].splitlines()
        out_sub_diagnostic = proc_sub_diagnostic.wait(1.0, interrupt=True)[1].splitlines()

        # Run service tests while the demo process is still running.
        node_info_text = run_process('pyuavcan', '-v', 'call', '42', 'uavcan.node.GetInfo.1.0', '{}',
                                     '--local-node-id', '123', '--format', 'JSON', '--with-metadata',
                                     '--priority', 'SLOW', '--timeout', '3.0',
                                     *_get_iface_args(),
                                     timeout=5.0)
        print('node_info_text:', node_info_text)
        node_info = json.loads(node_info_text)
        assert node_info['430']['_metadata_']['source_node_id'] == 42
        assert node_info['430']['_metadata_']['transfer_id'] >= 0
        assert 'slow' in node_info['430']['_metadata_']['priority'].lower()
        assert node_info['430']['name'] == 'org.uavcan.pyuavcan.demo.basic_usage'
        assert node_info['430']['protocol_version']['major'] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
        assert node_info['430']['protocol_version']['minor'] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[1]

        command_response = json.loads(run_process(
            'pyuavcan', '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0',
            f'{{command: {uavcan.node.ExecuteCommand_1_0.Request.COMMAND_STORE_PERSISTENT_STATES} }}',
            '--local-node-id', '123', '--format', 'JSON', *_get_iface_args(), timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_BAD_COMMAND

        # Next request - this fails if the EMITTED TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(run_process(
            'pyuavcan', '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0', '{command: 23456}',
            '--local-node-id', '123', '--format', 'JSON', *_get_iface_args(), timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS

        # Next request - this fails if the EMITTED TRANSFER-ID MAP save/restore logic is not working.
        command_response = json.loads(run_process(
            'pyuavcan', '-v', 'call', '42', 'uavcan.node.ExecuteCommand.1.0',
            f'{{command: {uavcan.node.ExecuteCommand_1_0.Request.COMMAND_POWER_OFF} }}',
            '--local-node-id', '123', '--format', 'JSON', *_get_iface_args(), timeout=5.0
        ))
        assert command_response['435']['status'] == uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS

        # Larger timeout is needed here because it tends to randomly fail on slower systems.
        # Oh, why our computers are so slow?
        least_squares_response = json.loads(run_process(
            'pyuavcan', '-v', 'call', '42', '123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0',
            '{points: [{x: 1, y: 2}, {x: 10, y: 20}]}', '--timeout=9',
            '--local-node-id', '123', '--format', 'JSON', *_get_iface_args(), timeout=10.0
        ))
        assert least_squares_response['123']['slope'] == pytest.approx(2.0)
        assert least_squares_response['123']['y_intercept'] == pytest.approx(0.0)

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


def _get_iface_args() -> typing.Sequence[str]:
    """
    Constructs the list of command-line arguments specifying which interfaces to use for testing.
    We could also add a random element here. It is crucial, however, to ensure that the demo script supports
    and uses those interfaces, so if you change the demo script, update this as well, please.
    """
    import pytest
    # Add more transports when redundancy is supported.
    if sys.platform == 'linux':
        if 0 != os.system('lsmod | grep -q vcan'):
            pytest.skip('Test skipped because the SocketCAN "vcan" module does not seem to be loaded. Please fix.')
        return '--socketcan=vcan0,8',   # The demo uses CAN 2.0!
    else:
        pytest.skip('CLI test skipped because it does not yet support non-GNU/Linux-based systems. Please fix.')
