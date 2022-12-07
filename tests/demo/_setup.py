# Copyright (c) 2021 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import os
from typing import Any
from ._subprocess import BackgroundChildProcess


def _unittest_slow_demo_setup_py(cd_to_demo: Any) -> None:
    _ = cd_to_demo
    proc = BackgroundChildProcess(
        "python",
        "setup.py",
        "build",
        environment_variables={
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),  # https://github.com/appveyor/ci/issues/1995
            # setup.py uses manual DSDL compilation so disable import hook instead of setting PYCYPHAL_PATH
            "PYCYPHAL_NO_IMPORT_HOOK": "True",
            "HOME": os.environ.get("HOME", ""),
            "USERPROFILE": os.environ.get("USERPROFILE", ""),
            "HOMEDRIVE": os.environ.get("HOMEDRIVE", ""),
            "HOMEPATH": os.environ.get("HOMEPATH", ""),
        },
    )
    exit_code, stdout = proc.wait(120)
    print(stdout)
    assert exit_code == 0
