# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import logging
import pathlib
import tempfile
import pytest
import pyuavcan.dsdl
from .conftest import DEMO_DIR


def _unittest_bad_usage() -> None:
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pyuavcan.dsdl.generate_package("irrelevant", "irrelevant")  # type: ignore


def _unittest_module_import_path_usage_suggestion(caplog: typing.Any) -> None:
    caplog.set_level(logging.INFO)
    with tempfile.TemporaryDirectory() as output_directory:
        output_directory_name = pathlib.Path(output_directory).resolve()
        caplog.clear()
        pyuavcan.dsdl.generate_package(
            DEMO_DIR / "public_regulated_data_types" / "uavcan",
            output_directory=output_directory,
        )
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


def _unittest_issue_133() -> None:
    with pytest.raises(ValueError, match=".*output directory.*"):
        pyuavcan.dsdl.generate_package(pathlib.Path.cwd() / "irrelevant")
