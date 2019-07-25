#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
import shutil
import pathlib
import logging
import functools
import importlib

import pytest

import pyuavcan.dsdl


# Please maintain these carefully if you're changing the project's directory structure.
TEST_ROOT_DIR = pathlib.Path(__file__).parent.parent
LIBRARY_ROOT_DIR = TEST_ROOT_DIR.parent
DESTINATION_DIR = LIBRARY_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
PUBLIC_REGULATED_DATA_TYPES_DIR = TEST_ROOT_DIR / 'public_regulated_data_types'
TEST_DATA_TYPES_DIR = pathlib.Path(__file__).parent / 'namespaces'


@pytest.fixture('session')  # type: ignore
def generated_packages() -> typing.List[pyuavcan.dsdl.GeneratedPackageInfo]:
    """
    https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions

    The implicitness of test fixtures and lack of type information makes the IDE emit bogus usage warnings,
    leads MyPy into emitting false positives, prevents developers from tracing the origins of used entities,
    and generally goes against one of the Python's core principles about explicit vs. implicit.
    I am not a big fan of this feature.
    """
    return generate_packages()


@functools.lru_cache()
def generate_packages() -> typing.List[pyuavcan.dsdl.GeneratedPackageInfo]:
    """
    Runs the DSDL package generator against the standard and test namespaces, emits a list of GeneratedPackageInfo.
    Automatically adds the path to the generated packages to sys path to make them importable.
    The output is cached permanently for the process' lifetime because the workings of PyDSDL or Nunavut are
    outside of the scope of responsibilities of this test suite, yet generation takes a long time.
    """
    if DESTINATION_DIR.exists():  # pragma: no cover
        shutil.rmtree(DESTINATION_DIR, ignore_errors=True)
    DESTINATION_DIR.mkdir(parents=True, exist_ok=True)

    pydsdl_logger = logging.getLogger('pydsdl')
    pydsdl_logging_level = pydsdl_logger.level
    try:
        pydsdl_logger.setLevel(logging.INFO)
        out = [
            pyuavcan.dsdl.generate_package(
                DESTINATION_DIR,
                PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan',
                []
            ),
            pyuavcan.dsdl.generate_package(
                DESTINATION_DIR,
                TEST_DATA_TYPES_DIR / 'test',
                [
                    PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'
                ]
            ),
            pyuavcan.dsdl.generate_package(
                DESTINATION_DIR,
                TEST_DATA_TYPES_DIR / 'sirius_cyber_corp',
                []
            ),
        ]
    finally:
        pydsdl_logger.setLevel(pydsdl_logging_level)

    sys.path.insert(0, str(DESTINATION_DIR))
    importlib.invalidate_caches()
    return out
