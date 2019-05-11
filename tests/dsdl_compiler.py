#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing
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
    sys.path.insert(0, str(info.path.parent))
    mod = importlib.import_module(info.name)
    sys.path = original_sys_path

    for dsdl_type in info.types:
        python_type = _get_python_type(mod, dsdl_type)
        o = _make_random_instance(python_type)
        _test_roundtrip_serialization(o)


def _get_python_type(module: typing.Any, composite: pydsdl.CompositeType) -> typing.Type[pyuavcan.dsdl.CompositeObject]:
    obj = module
    # The first level is already reached, it's the package itself.
    # The final component is the short name, it requires special handling.
    for component in composite.name_components[1:-1]:
        # This will break on reserved identifiers because we append those with an underscore.
        obj = importlib.import_module(obj.__name__ + '.' + component)
    ref = '%s_%d_%d' % (composite.short_name, composite.version.major, composite.version.minor)
    obj = getattr(obj, ref)
    assert issubclass(obj, pyuavcan.dsdl.CompositeObject)
    return obj   # type: ignore


def _make_random_instance(t: typing.Type[pyuavcan.dsdl.CompositeObject]) -> pyuavcan.dsdl.CompositeObject:
    pass


def _test_roundtrip_serialization(o: pyuavcan.dsdl.CompositeObject) -> None:
    pass
