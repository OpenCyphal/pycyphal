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
import random
import pathlib
import logging
from itertools import starmap
from functools import partial

import pyuavcan.dsdl


_PROJECT_ROOT_DIR = pathlib.Path(__file__).parent.parent
_DESTINATION_DIRECTORY = _PROJECT_ROOT_DIR / pathlib.Path('.test_dsdl_generated')
_PUBLIC_REGULATED_DATA_TYPES = _PROJECT_ROOT_DIR / 'public_regulated_data_types.cache'


_logger = logging.getLogger(__name__)


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
    for dsdl_type in info.types:
        if isinstance(dsdl_type, pydsdl.ServiceType):
            _test_type(dsdl_type.request_type)
            _test_type(dsdl_type.response_type)
        else:
            _test_type(dsdl_type)


def _test_type(data_type: pydsdl.CompositeType) -> None:
    _logger.info('Roundtrip serialization test of %s', data_type)
    _test_roundtrip_serialization(pyuavcan.dsdl.get_generated_implementation_of(data_type)())
    for _ in range(10):
        o = _make_random_object(data_type)
        _test_roundtrip_serialization(o)


def _make_random_object(data_type: pydsdl.SerializableType) -> typing.Any:
    if isinstance(data_type, pydsdl.BooleanType):
        return random.choice([False, True])

    elif isinstance(data_type, pydsdl.IntegerType):  # noinspection PyTypeChecker
        return random.randint(int(data_type.inclusive_value_range.min),
                              int(data_type.inclusive_value_range.max))

    elif isinstance(data_type, pydsdl.FloatType):
        if data_type.bit_length < 64:
            return random.uniform(float(data_type.inclusive_value_range.min),
                                  float(data_type.inclusive_value_range.max))
        else:
            # uniform() with 64-bit floats degenerates into inf due to a numerical instability in the standard library.
            return random.random() * 1e9

    elif isinstance(data_type, pydsdl.FixedLengthArrayType):
        return [_make_random_object(data_type.element_type) for _ in range(data_type.capacity)]

    elif isinstance(data_type, pydsdl.VariableLengthArrayType):
        length = random.randint(0, data_type.capacity)
        return [_make_random_object(data_type.element_type) for _ in range(length)]

    elif isinstance(data_type, pydsdl.StructureType):
        o = pyuavcan.dsdl.get_generated_implementation_of(data_type)()
        for f in data_type.fields_except_padding:
            v = _make_random_object(f.data_type)
            pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    elif isinstance(data_type, pydsdl.UnionType):
        f = random.choice(data_type.fields)
        v = _make_random_object(f.data_type)
        o = pyuavcan.dsdl.get_generated_implementation_of(data_type)()
        pyuavcan.dsdl.set_attribute(o, f.name, v)
        return o

    else:
        raise TypeError(f'Unsupported type: {type(data_type)}')


def _test_roundtrip_serialization(o: pyuavcan.dsdl.CompositeObject) -> None:
    sr = pyuavcan.dsdl.serialize(o)
    d = pyuavcan.dsdl.try_deserialize(type(o), sr)
    assert d is not None
    assert type(o) is type(d)
    assert pyuavcan.dsdl.get_type(o) == pyuavcan.dsdl.get_type(d)
    assert _are_close(pyuavcan.dsdl.get_type(o), o, d), f'{o} != {d}; sr: {bytes(sr).hex()}'
    # Similar floats may produce drastically different string representations, so if there is at least one float inside,
    # we skip the string representation equality check.
    if pydsdl.FloatType.__name__ not in repr(pyuavcan.dsdl.get_type(d)):
        assert str(o) == str(d)
        assert repr(o) == repr(d)


def _are_close(data_type: pydsdl.SerializableType, a: typing.Any, b: typing.Any) -> bool:
    if a is None or b is None:  # These occur, for example, in unions
        return (a is None) == (b is None)

    elif isinstance(data_type, pydsdl.CompositeType):
        if type(a) != type(b):
            return False
        for f in pyuavcan.dsdl.get_type(a).fields_except_padding:
            if not _are_close(f.data_type,
                              pyuavcan.dsdl.get_attribute(a, f.name),
                              pyuavcan.dsdl.get_attribute(b, f.name)):
                return False
        return True                 # Empty objects of same type compare equal

    elif isinstance(data_type, pydsdl.ArrayType):
        return all(starmap(partial(_are_close, data_type.element_type), zip(a, b))) \
            if len(a) == len(b) and a.dtype == b.dtype else False

    elif isinstance(data_type, pydsdl.FloatType):
        t = {
            16: numpy.float16,
            32: numpy.float32,
            64: numpy.float64,
        }[data_type.bit_length]
        return numpy.allclose(t(a), t(b))

    else:
        return numpy.allclose(a, b)
