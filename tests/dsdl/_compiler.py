# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import random
import sys
import threading
import time
import pathlib
import tempfile
import pytest
import pycyphal.dsdl
from pycyphal.dsdl import remove_import_hooks, add_import_hook
from pycyphal.dsdl._lockfile import Locker
from .conftest import DEMO_DIR


def _unittest_bad_usage() -> None:
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pycyphal.dsdl.compile("irrelevant", "irrelevant")  # type: ignore


def _unittest_remove_import_hooks() -> None:
    from pycyphal.dsdl._import_hook import DsdlMetaFinder

    original_meta_path = sys.meta_path.copy()
    try:
        old_hooks = [hook for hook in sys.meta_path.copy() if isinstance(hook, DsdlMetaFinder)]
        assert old_hooks

        remove_import_hooks()
        current_hooks = [hook for hook in sys.meta_path.copy() if isinstance(hook, DsdlMetaFinder)]
        assert not current_hooks, "Import hooks were not removed properly"

        add_import_hook()
        final_hooks = [hook for hook in sys.meta_path.copy() if isinstance(hook, DsdlMetaFinder)]
        assert len(final_hooks) == 1
    finally:
        sys.meta_path = original_meta_path


def _unittest_issue_133() -> None:
    with pytest.raises(ValueError, match=".*output directory.*"):
        pycyphal.dsdl.compile(pathlib.Path.cwd() / "irrelevant")


def _unittest_lockfile_cant_be_recreated() -> None:
    output_directory = pathlib.Path(tempfile.gettempdir())
    root_namespace_name = str(random.getrandbits(64))

    lockfile1 = Locker(output_directory, root_namespace_name)
    lockfile2 = Locker(output_directory, root_namespace_name)

    assert lockfile1.create() is True

    def remove_lockfile1() -> None:
        time.sleep(5)
        lockfile1.remove()

    threading.Thread(target=remove_lockfile1).start()
    assert lockfile2.create() is False


def _unittest_lockfile_is_removed() -> None:
    output_directory = pathlib.Path(tempfile.gettempdir())

    pycyphal.dsdl.compile(DEMO_DIR / "public_regulated_data_types" / "uavcan", output_directory=output_directory.name)

    assert pathlib.Path.exists(output_directory / "uavcan.lock") is False
