#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pytest
import subprocess
from ._subprocess import run_cli_tool


def _unittest_trivial() -> None:
    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool(timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool('invalid-command', timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_cli_tool('dsdl-gen-pkg', 'nonexistent/path', timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):  # Look-up of a nonexistent package requires large timeout
        run_cli_tool('pub', 'nonexistent.data.Type.1.0', '{}', '--loopback', timeout=5.0)
