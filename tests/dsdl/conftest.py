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

import pytest

import pyuavcan.dsdl


# Please maintain these carefully if you're changing the project's directory structure.
TEST_ROOT_DIR = pathlib.Path(__file__).parent.parent
LIBRARY_ROOT_DIR = TEST_ROOT_DIR.parent
DESTINATION_DIR = LIBRARY_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
PUBLIC_REGULATED_DATA_TYPES_DIR = TEST_ROOT_DIR / 'public_regulated_data_types'
TEST_DATA_TYPES_DIR = pathlib.Path(__file__).parent / 'namespaces'


@pytest.fixture('session')  # type: ignore
def generated_packages() -> typing.Iterator[typing.List[pyuavcan.dsdl.GeneratedPackageInfo]]:
    """
    Runs the DSDL package generator against the standard and test namespaces, emits a list of GeneratedPackageInfo.
    Automatically adds the path to the generated packages to sys path to make them importable.
    https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions
    """
    original_sys_path = sys.path
    sys.path.insert(0, str(DESTINATION_DIR))
    logging.getLogger('pydsdl').setLevel(logging.WARNING)

    if DESTINATION_DIR.exists():  # pragma: no cover
        shutil.rmtree(DESTINATION_DIR, ignore_errors=True)
    DESTINATION_DIR.mkdir(parents=True, exist_ok=True)

    yield [
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

    logging.getLogger('pydsdl').setLevel(logging.DEBUG)
    sys.path = original_sys_path
