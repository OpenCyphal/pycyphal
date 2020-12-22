# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import logging
import pathlib
import tempfile
import pytest
import pyuavcan.dsdl
from .conftest import TEST_DATA_TYPES_DIR, PUBLIC_REGULATED_DATA_TYPES_DIR


def _unittest_bad_usage() -> None:
    with pytest.raises(TypeError):
        pyuavcan.dsdl.generate_package(TEST_DATA_TYPES_DIR, TEST_DATA_TYPES_DIR)  # type: ignore


def _unittest_module_import_path_usage_suggestion(caplog: typing.Any) -> None:
    caplog.set_level(logging.WARNING)
    with tempfile.TemporaryDirectory() as output_directory:
        output_directory_name = pathlib.Path(output_directory).resolve()
        caplog.clear()
        pyuavcan.dsdl.generate_package(
            PUBLIC_REGULATED_DATA_TYPES_DIR / "uavcan",
            output_directory=output_directory,
        )
        logs = caplog.record_tuples
    assert len(logs) == 1
    print("Captured warning log entry:", logs[0], sep="\n")
    assert "dsdl" in logs[0][0]
    assert logs[0][1] == logging.WARNING
    assert " path" in logs[0][2]
    assert "Path(" not in logs[0][2]  # Ensure decent formatting
    assert str(output_directory_name) in logs[0][2]
