# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import json
import pytest
import pathlib
import subprocess
import pyuavcan
from tests.dsdl.conftest import PUBLIC_REGULATED_DATA_TYPES_DIR
from ._subprocess import run_cli_tool


def _unittest_slow_cli_call_a() -> None:
    # Generate DSDL namespace "uavcan"
    if not pathlib.Path("uavcan").exists():
        run_cli_tool("dsdl-gen-pkg", str(PUBLIC_REGULATED_DATA_TYPES_DIR / "uavcan"))

    result_text = run_cli_tool(
        "-v", "call", "1234", "uavcan.node.GetInfo.1.0", "{}", "--tr=Loopback(1234)", "--format", "json"
    )
    result = json.loads(result_text)
    assert result["430"]["name"] == "org.uavcan.pyuavcan.cli.call"
    assert result["430"]["protocol_version"]["major"] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[0]
    assert result["430"]["protocol_version"]["minor"] == pyuavcan.UAVCAN_SPECIFICATION_VERSION[1]
    assert result["430"]["software_version"]["major"] == pyuavcan.__version_info__[0]
    assert result["430"]["software_version"]["minor"] == pyuavcan.__version_info__[1]

    with pytest.raises(subprocess.CalledProcessError):
        # Will time out because we're using a wrong service-ID
        run_cli_tool("-v", "call", "1234", "123.uavcan.node.GetInfo.1.0", "{}", "--tr=Loopback(1234)")
