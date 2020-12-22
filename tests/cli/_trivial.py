# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import pytest
import subprocess
from ._subprocess import run_cli_tool


_COMMANDS = ["dsdl-generate-packages", "call", "pick-node-id", "publish", "show-transport", "subscribe"]


def _unittest_cli_help() -> None:
    # Just make sure that the help can be displayed without issues.
    # I once encountered a super obscure failure where I added a line like "(PID % 100)" into a help string
    # and the option --help starting failing in the most obscure way possible because the part "% 100)" was
    # interpreted as a format specifier. The Python's built-in argparse library is unsuitable for complex
    # applications, debugging it is a pain.
    # Anyway, so here we just make sure that we can print help for every CLI command.
    run_cli_tool("--help", timeout=10.0)
    for cmd in _COMMANDS:
        run_cli_tool(cmd, "--help", timeout=10.0)


def _unittest_trivial() -> None:
    run_cli_tool("show-transport", timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool(timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool("invalid-command", timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool("dsdl-gen-pkg", "nonexistent/path", timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):  # Look-up of a nonexistent package requires large timeout
        run_cli_tool("pub", "nonexistent.data.Type.1.0", "{}", "--tr=Loopback(None)", timeout=5.0)
