# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>
import sys
import typing
import logging
import pathlib
import tempfile
import pytest
import pycyphal.dsdl
from pycyphal.dsdl._import_hook import DsdlMetaFinder
from pycyphal.dsdl import remove_import_hooks, add_import_hook

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


def _unittest_remove_import_hooks():
    original_meta_path = sys.meta_path.copy()
    try:
        old_hooks = [hook for hook in sys.meta_path.copy() if isinstance(hook, DsdlMetaFinder)]
        if not old_hooks:
            print("No DsdlMetaFinder hooks found; nothing to test.")
            return None

        # call remove_import_hooks, make sure they disappear
        remove_import_hooks()
        current_hooks = [meta_path for meta_path in sys.meta_path.copy() if isinstance(meta_path, DsdlMetaFinder)]

        assert not current_hooks, "Import hooks were not removed properly"

        # Re-add the original hooks
        add_import_hook()
        final_hooks = [meta_path for meta_path in sys.meta_path.copy() if isinstance(meta_path, DsdlMetaFinder)]

        assert old_hooks == final_hooks, "Hooks were not restored properly"
        return None
    finally:
        sys.meta_path = original_meta_path


def _unittest_issue_133() -> None:
    with pytest.raises(ValueError, match=".*output directory.*"):
        pycyphal.dsdl.compile(pathlib.Path.cwd() / "irrelevant")
