#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import json
from tests.dsdl.conftest import PUBLIC_REGULATED_DATA_TYPES_DIR
from ._subprocess import run_process, BackgroundChildProcess
from ._common_args import make_iface_args


def _unittest_slow_cli_pub_sub_a() -> None:
    # Generate DSDL namespace "uavcan"
    run_process('pyuavcan', 'dsdl-gen-pkg', str(PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'))

    proc_sub_heartbeat = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.node.Heartbeat.1.0', '--format=JSON',    # Count unlimited
        '--with-metadata', *make_iface_args()
    )

    proc_sub_diagnostic = BackgroundChildProcess(
        'pyuavcan', 'sub', '4321.uavcan.diagnostic.Record.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *make_iface_args()
    )

    proc_sub_diagnostic_wrong_pid = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.diagnostic.Record.1.0', '--count=3', '--format=JSON',
        '--with-metadata', *make_iface_args()
    )

    time.sleep(1.0)     # Time to let the background processes finish initialization

    run_process(
        'pyuavcan', '-v',
        'pub', '4321.uavcan.diagnostic.Record.1.0',
        '{severity: {value: 6}, timestamp: {microsecond: 123456}, text: "Hello world!"}',
        '--count=3', '--period=0.1', '--priority=SLOW', '--local-node-id=51',
        '--transfer-id=123',    # Modulo 32: 27
        '--heartbeat-fields={vendor_specific_status_code: 54321}',
        *make_iface_args(),
        timeout=3.0
    )

    time.sleep(1.0)     # Time to sync up

    out_sub_heartbeat = proc_sub_heartbeat.wait(1.0, interrupt=True)[1].splitlines()
    out_sub_diagnostic = proc_sub_diagnostic.wait(1.0, interrupt=True)[1].splitlines()

    print('out_sub_heartbeat:', *out_sub_heartbeat, sep='\n\t')
    print('out_sub_diagnostic:', *out_sub_diagnostic, sep='\n\t')

    heartbeats = list(json.loads(s) for s in out_sub_heartbeat)
    diagnostics = list(json.loads(s) for s in out_sub_diagnostic)

    print('heartbeats:', *heartbeats, sep='\n\t')
    print('diagnostics:', *diagnostics, sep='\n\t')

    assert len(heartbeats) in (2, 3, 4)    # Fuzzy because the last one might be dropped
    for index, m in enumerate(heartbeats):
        assert 'slow' in m['32085']['_metadata_']['priority'].lower()
        assert m['32085']['_metadata_']['transfer_id'] == 27 + index
        assert m['32085']['_metadata_']['source_node_id'] == 51
        assert m['32085']['uptime'] in (0, 1)
        assert m['32085']['vendor_specific_status_code'] == 54321

    assert len(diagnostics) == 3
    for index, m in enumerate(diagnostics):
        assert 'slow' in m['4321']['_metadata_']['priority'].lower()
        assert m['4321']['_metadata_']['transfer_id'] == 27 + index
        assert m['4321']['_metadata_']['source_node_id'] == 51
        assert m['4321']['timestamp']['microsecond'] == 123456
        assert m['4321']['text'] == 'Hello world!'

    assert proc_sub_diagnostic_wrong_pid.alive
    assert proc_sub_diagnostic_wrong_pid.wait(1.0, interrupt=True)[1].strip() == ''


def _unittest_slow_cli_pub_sub_b() -> None:
    # Generate DSDL namespace "uavcan"
    run_process('pyuavcan', 'dsdl-gen-pkg', str(PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'))

    proc_sub_heartbeat = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.node.Heartbeat.1.0', '--format=JSON',    # Count unlimited
        *make_iface_args()
    )

    proc_sub_diagnostic_with_meta = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.diagnostic.Record.1.0', '--format=JSON', '--with-metadata',
        *make_iface_args()
    )

    proc_sub_diagnostic_no_meta = BackgroundChildProcess(
        'pyuavcan', 'sub', 'uavcan.diagnostic.Record.1.0', '--format=JSON',
        *make_iface_args()
    )

    time.sleep(1.0)     # Time to let the background processes finish initialization

    proc = BackgroundChildProcess(
        'pyuavcan', '-v', 'pub', 'uavcan.diagnostic.Record.1.0', '{}',
        '--count=2', '--period=2', *make_iface_args(),
    )
    proc.wait(timeout=8)

    time.sleep(1.0)     # Time to sync up

    assert proc_sub_heartbeat.wait(1.0, interrupt=True)[1].strip() == '', 'Anonymous nodes must not broadcast heartbeat'

    diagnostics = list(json.loads(s) for s in proc_sub_diagnostic_with_meta.wait(1.0, interrupt=True)[1].splitlines())
    assert len(diagnostics) == 2
    for index, m in enumerate(diagnostics):
        assert 'nominal' in m['32760']['_metadata_']['priority'].lower()
        assert m['32760']['_metadata_']['transfer_id'] == index
        assert m['32760']['_metadata_']['source_node_id'] is None
        assert m['32760']['timestamp']['microsecond'] == 0
        assert m['32760']['text'] == ''

    diagnostics = list(json.loads(s) for s in proc_sub_diagnostic_no_meta.wait(1.0, interrupt=True)[1].splitlines())
    assert len(diagnostics) == 2
    for index, m in enumerate(diagnostics):
        assert m['32760']['timestamp']['microsecond'] == 0
        assert m['32760']['text'] == ''
