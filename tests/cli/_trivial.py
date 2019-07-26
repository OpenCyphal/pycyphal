#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pytest
import subprocess
from ._subprocess import run_process


def _unittest_trivial() -> None:
    with pytest.raises(subprocess.CalledProcessError):
        run_process('pyuavcan', timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_process('pyuavcan', 'invalid-command', timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):
        run_process('pyuavcan', 'dsdl-gen-pkg', 'nonexistent/path', timeout=2.0)

    with pytest.raises(subprocess.CalledProcessError):  # Look-up of a nonexistent package requires large timeout
        run_process('pyuavcan', 'pub', 'nonexistent.data.Type.1.0', '{}', '--socketcan=vcan0', timeout=5.0)
