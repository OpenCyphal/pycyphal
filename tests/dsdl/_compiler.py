# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import logging
import pathlib
import tempfile
import pytest
import pycyphal.dsdl
from .conftest import DEMO_DIR


def _unittest_bad_usage() -> None:
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pycyphal.dsdl.compile("irrelevant", "irrelevant")  # type: ignore


def _unittest_module_import_path_usage_suggestion(caplog: typing.Any) -> None:
    caplog.set_level(logging.INFO)
    output_directory = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
    output_directory_name = pathlib.Path(output_directory.name).resolve()
    caplog.clear()
    pycyphal.dsdl.compile(DEMO_DIR / "public_regulated_data_types" / "uavcan", output_directory=output_directory.name)
    logs = caplog.record_tuples
    print("Captured log entries:", logs, sep="\n")
    for e in logs:
        if "dsdl" in e[0] and str(output_directory_name) in e[2]:
            assert e[1] == logging.INFO
            assert " path" in e[2]
            assert "Path(" not in e[2]  # Ensure decent formatting
            break
    else:
        assert False
    try:
        output_directory.cleanup()  # This may fail on Windows with Python 3.7, we don't care.
    except PermissionError:  # pragma: no cover
        pass


def _unittest_issue_133() -> None:
    with pytest.raises(ValueError, match=".*output directory.*"):
        pycyphal.dsdl.compile(pathlib.Path.cwd() / "irrelevant")
