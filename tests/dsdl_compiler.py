#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import numpy
import typing
import pydsdl
import shutil
import pathlib
import logging
import importlib
from itertools import starmap

import pyuavcan.dsdl


_PROJECT_ROOT_DIR = pathlib.Path(__file__).parent.parent
_DESTINATION_DIRECTORY = _PROJECT_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
_PUBLIC_REGULATED_DATA_TYPES = _PROJECT_ROOT_DIR / 'public_regulated_data_types.cache'


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

    test_root = pathlib.Path(__file__).parent / 'dsdl_namespaces' / 'test'
    test_info = pyuavcan.dsdl.generate_package_from_dsdl_namespace(_DESTINATION_DIRECTORY, test_root, [uavcan_root])
    assert str(test_info.path).endswith('test')

    _test_package(uavcan_info)
    _test_package(test_info)

    sys.path = original_sys_path


# noinspection PyUnresolvedReferences
def _test_package(info: pyuavcan.dsdl.GeneratedPackageInfo) -> None:
    mod = importlib.import_module(info.name)
    for dsdl_type in info.types:
        python_type = _get_python_type(mod, dsdl_type)
        if issubclass(python_type, pyuavcan.dsdl.ServiceObject):
            _test_type(python_type.Request)
            _test_type(python_type.Response)
        else:
            _test_type(python_type)


def _test_type(t: typing.Type[pyuavcan.dsdl.CompositeObject]) -> None:
    o = _make_random_instance(t)
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
    return t()  # TODO


def _test_roundtrip_serialization(o: pyuavcan.dsdl.CompositeObject) -> None:
    sr = pyuavcan.dsdl.serialize(o)
    print('Roundtrip serialization test; object/serialized:')
    print('\t', o, sep='')
    print('\t', bytes(sr).hex() or '<empty>', sep='')
    d = pyuavcan.dsdl.try_deserialize(type(o), sr)
    assert d is not None
    assert type(o) is type(d)
    assert pyuavcan.dsdl.get_type(o) == pyuavcan.dsdl.get_type(d)
    assert _are_close(o, d)
    assert str(o) == str(d)
    assert repr(o) == repr(d)


def _are_close(a: typing.Any, b: typing.Any) -> bool:
    if isinstance(a, pyuavcan.dsdl.CompositeObject) and isinstance(b, pyuavcan.dsdl.CompositeObject):
        if type(a) != type(b):
            return False
        assert pyuavcan.dsdl.get_type(a) == pyuavcan.dsdl.get_type(b)
        for f in pyuavcan.dsdl.get_type(a).fields:
            if not isinstance(f, pydsdl.PaddingField):
                if not _are_close(_get_attribute(a, f.name), _get_attribute(b, f.name)):
                    return False
        return True                 # Empty objects of same type compare equal

    elif isinstance(a, numpy.ndarray) and isinstance(b, numpy.ndarray):
        return all(starmap(_are_close, zip(a, b))) if len(a) == len(b) and a.dtype == b.dtype else False

    elif a is None and b is None:   # These occur, for example, in unions
        return True

    else:
        return numpy.allclose(a, b)


def _get_attribute(o: pyuavcan.dsdl.CompositeObject, name: str) -> typing.Any:
    try:
        return getattr(o, name)
    except AttributeError:
        return getattr(o, name + '_')   # Attributes whose names match reserved words are suffixed with an underscore.
