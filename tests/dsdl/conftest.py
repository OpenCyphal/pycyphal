# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import sys
import pickle
import typing
import shutil
import logging
import functools
import importlib
from pathlib import Path
import pytest
import pycyphal.dsdl


# Please maintain these carefully if you're changing the project's directory structure.
SELF_DIR = Path(__file__).resolve().parent
LIBRARY_ROOT_DIR = SELF_DIR.parent.parent
DEMO_DIR = LIBRARY_ROOT_DIR / "demo"
DESTINATION_DIR = Path.cwd().resolve() / ".compiled"

_CACHE_FILE_NAME = "pydsdl_cache.pickle.tmp"


@functools.lru_cache()
def compile() -> typing.List[pycyphal.dsdl.GeneratedPackageInfo]:  # pylint: disable=redefined-builtin
    """
    Runs the DSDL package generator against the standard and test namespaces, emits a list of GeneratedPackageInfo.
    Automatically adds the path to the generated packages to sys path to make them importable.
    The output is cached permanently on disk in a file in the output directory because the workings of PyDSDL or
    Nunavut are outside of the scope of responsibilities of this test suite, yet generation takes a long time.
    To force regeneration, remove the generated package directories.
    """
    if str(DESTINATION_DIR) not in sys.path:  # pragma: no cover
        sys.path.insert(0, str(DESTINATION_DIR))
    importlib.invalidate_caches()
    cache_file = DESTINATION_DIR / _CACHE_FILE_NAME

    if DESTINATION_DIR.exists():  # pragma: no cover
        if cache_file.exists():
            with open(cache_file, "rb") as f:
                out = pickle.load(f)
            assert out and isinstance(out, list)
            assert all(map(lambda x: isinstance(x, pycyphal.dsdl.GeneratedPackageInfo), out))
            return out

        shutil.rmtree(DESTINATION_DIR, ignore_errors=True)
    DESTINATION_DIR.mkdir(parents=True, exist_ok=True)

    pydsdl_logger = logging.getLogger("pydsdl")
    pydsdl_logging_level = pydsdl_logger.level
    try:
        pydsdl_logger.setLevel(logging.INFO)
        out = pycyphal.dsdl.compile_all(
            [
                DEMO_DIR / "public_regulated_data_types" / "uavcan",
                DEMO_DIR / "custom_data_types" / "sirius_cyber_corp",
                SELF_DIR / "test_dsdl_namespace",
            ],
            DESTINATION_DIR,
        )
    finally:
        pydsdl_logger.setLevel(pydsdl_logging_level)

    with open(cache_file, "wb") as f:
        pickle.dump(out, f)

    assert out and isinstance(out, list)
    assert all(map(lambda x: isinstance(x, pycyphal.dsdl.GeneratedPackageInfo), out))
    return out

@functools.lru_cache()
def compile_no_cache() -> typing.List[pycyphal.dsdl.GeneratedPackageInfo]:  # pylint: disable=redefined-builtin
    """
    Runs the DSDL package generator against the standard and test namespaces, emits a list of GeneratedPackageInfo.
    Automatically adds the path to the generated packages to sys path to make them importable.
    The output is cached permanently on disk in a file in the output directory because the workings of PyDSDL or
    Nunavut are outside of the scope of responsibilities of this test suite, yet generation takes a long time.
    To force regeneration, remove the generated package directories.
    """
    if str(DESTINATION_DIR) not in sys.path:  # pragma: no cover
        sys.path.insert(0, str(DESTINATION_DIR))
    importlib.invalidate_caches()
    cache_file = DESTINATION_DIR / _CACHE_FILE_NAME

    if DESTINATION_DIR.exists():  # pragma: no cover
        if cache_file.exists():
            with open(cache_file, "rb") as f:
                out = pickle.load(f)
            assert out and isinstance(out, list)
            assert all(map(lambda x: isinstance(x, pycyphal.dsdl.GeneratedPackageInfo), out))
            return out

        shutil.rmtree(DESTINATION_DIR, ignore_errors=True)
    DESTINATION_DIR.mkdir(parents=True, exist_ok=True)

    pydsdl_logger = logging.getLogger("pydsdl")
    pydsdl_logging_level = pydsdl_logger.level
    try:
        pydsdl_logger.setLevel(logging.INFO)
        out = pycyphal.dsdl.compile_all(
            [
                DEMO_DIR / "public_regulated_data_types" / "uavcan",
                DEMO_DIR / "custom_data_types" / "sirius_cyber_corp",
                SELF_DIR / "test_dsdl_namespace",
            ],
            DESTINATION_DIR,
        )
    finally:
        pydsdl_logger.setLevel(pydsdl_logging_level)

    with open(cache_file, "wb") as f:
        pickle.dump(out, f)

    assert out and isinstance(out, list)
    assert all(map(lambda x: isinstance(x, pycyphal.dsdl.GeneratedPackageInfo), out))
    return out



compiled = pytest.fixture(scope="session")(compile)
