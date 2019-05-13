#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import shutil
import pathlib
import logging

import pyuavcan.dsdl
from . import _serialization


_PROJECT_ROOT_DIR = pathlib.Path(__file__).parent.parent.parent
_DESTINATION_DIRECTORY = _PROJECT_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
_PUBLIC_REGULATED_DATA_TYPES = _PROJECT_ROOT_DIR / 'public_regulated_data_types.cache'


_NUM_RANDOM_SAMPLES = int(os.environ.get('PYUAVCAN_TEST_NUM_RANDOM_SAMPLES', 300))
assert _NUM_RANDOM_SAMPLES >= 20, 'Invalid configuration: low number of random samples may trigger a false-negative.'


def _unittest_dsdl_compiler() -> None:
    original_sys_path = sys.path
    sys.path.insert(0, str(_DESTINATION_DIRECTORY))

    logging.getLogger('pydsdl').setLevel(logging.WARNING)

    if _DESTINATION_DIRECTORY.exists():  # pragma: no cover
        shutil.rmtree(_DESTINATION_DIRECTORY, ignore_errors=True)
    _DESTINATION_DIRECTORY.mkdir(parents=True, exist_ok=True)

    uavcan_root = _PUBLIC_REGULATED_DATA_TYPES / 'uavcan'
    uavcan_info = pyuavcan.dsdl.generate_package_from_dsdl_namespace(_DESTINATION_DIRECTORY, uavcan_root, [])
    assert str(uavcan_info.path).endswith('uavcan')

    test_root = pathlib.Path(__file__).parent / 'namespaces' / 'test'
    test_info = pyuavcan.dsdl.generate_package_from_dsdl_namespace(_DESTINATION_DIRECTORY, test_root, [uavcan_root])
    assert str(test_info.path).endswith('test')

    _serialization.test_package(uavcan_info, _NUM_RANDOM_SAMPLES)
    _serialization.test_package(test_info, _NUM_RANDOM_SAMPLES)

    sys.path = original_sys_path
