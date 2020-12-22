# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os
import sys
import pathlib

OWN_PATH = pathlib.Path(__file__).absolute()


def detect_debugger() -> bool:
    if sys.gettrace() is not None:
        return True
    if (os.path.sep + "pydev") in sys.argv[0]:
        return True
    return False


def setup_coverage() -> None:
    try:
        import coverage  # The module may be missing during early stage setup, no need to abort everything.
    except ImportError as ex:
        print("COVERAGE NOT CONFIGURED:", ex, file=sys.stderr)
    else:
        # Coverage configuration; see https://coverage.readthedocs.io/en/coverage-4.2/subprocess.html
        # This is kind of a big gun because it makes us track coverage of everything we run, even doc generation,
        # but it's acceptable.
        os.environ["COVERAGE_PROCESS_START"] = str(OWN_PATH.parent / "setup.cfg")
        coverage.process_startup()


if detect_debugger():
    print("Debugger detected, coverage will not be tracked to avoid interference.")
else:
    print(f"Tracking coverage of {sys.argv[0]} with {OWN_PATH}", file=sys.stderr)
    setup_coverage()
