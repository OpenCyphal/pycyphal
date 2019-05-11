#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import pydsdl
import shutil
import pathlib
import logging
import importlib

import pyuavcan.dsdl


_PROJECT_ROOT_DIR = pathlib.Path(__file__).parent.parent
_DESTINATION_DIRECTORY = _PROJECT_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
_PUBLIC_REGULATED_DATA_TYPES = _PROJECT_ROOT_DIR / 'public_regulated_data_types.cache'


def _unittest_dsdl_compiler() -> None:
    logging.getLogger('pydsdl').setLevel(logging.WARNING)

    if _DESTINATION_DIRECTORY.exists():  # pragma: no cover
        shutil.rmtree(_DESTINATION_DIRECTORY, ignore_errors=True)
    _DESTINATION_DIRECTORY.mkdir(parents=True, exist_ok=True)

    uavcan_root = _PUBLIC_REGULATED_DATA_TYPES / 'uavcan'
    uavcan_info = pyuavcan.dsdl.generate_package_from_dsdl_namespace(_DESTINATION_DIRECTORY, uavcan_root, [])
    assert str(uavcan_info.path).endswith('uavcan')

    test_root = pathlib.Path(__file__).parent / 'dsdl_namespaces' / 'test'
    test_info = pyuavcan.dsdl.generate_package_from_dsdl_namespace(_DESTINATION_DIRECTORY, test_root, [uavcan_root])
    assert str(test_info.path).endswith('test')

    _test_package(uavcan_info)
    _test_package(test_info)


def _test_package(info: pyuavcan.dsdl.GeneratedPackageInfo) -> None:
    original_sys_path = sys.path
    sys.path.append(str(info.path.parent))
    mod = importlib.import_module(info.name)
    sys.path = original_sys_path

    def get_python_type(composite: pydsdl.CompositeType) -> pyuavcan.dsdl.CompositeObject:
        pass

    for t in info.types:
        pass
