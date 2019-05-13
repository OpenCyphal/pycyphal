#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import shutil
import pydsdl
import pytest
import pathlib
import logging

import pyuavcan.dsdl
from . import _random_serialization, _manual
from ._util import make_random_object


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

    _manual.test()

    _test_package(uavcan_info)
    _test_package(test_info)

    sys.path = original_sys_path


def _test_package(info: pyuavcan.dsdl.GeneratedPackageInfo) -> None:
    _random_serialization.test_package(info, _NUM_RANDOM_SAMPLES)

    for model in info.types:
        if isinstance(model, pydsdl.ServiceType):
            _test_type(model.request_type)
            _test_type(model.response_type)
        else:
            _test_type(model)


def _test_type(model: pydsdl.CompositeType) -> None:
    _test_constants(model)
    for _ in range(10):
        _test_textual_representations(model, make_random_object(model))


def _test_constants(model: pydsdl.CompositeType) -> None:
    cls = pyuavcan.dsdl.get_generated_class(model)
    for c in model.constants:
        if isinstance(c.data_type, pydsdl.PrimitiveType):
            reference = c.value
            generated = pyuavcan.dsdl.get_attribute(cls, c.name)
            assert isinstance(reference, pydsdl.Primitive)
            assert reference.native_value == pytest.approx(generated), \
                'The generated constant does not compare equal against the DSDL source'


def _test_textual_representations(model: pydsdl.CompositeType, obj: pyuavcan.dsdl.CompositeObject) -> None:
    for fn in [str, repr]:
        assert callable(fn)
        s = fn(obj)
        for f in model.fields_except_padding:
            field_present = (f'{f.name}=' in s) or (f'{f.name}_=' in s)
            if isinstance(model, pydsdl.UnionType):
                # In unions only the active field is printed. The active field may contain nested fields which
                # may be named similarly to other fields in the current union, so we can't easily ensure lack of
                # non-active fields in the output.
                field_active = pyuavcan.dsdl.get_attribute(obj, f.name) is not None
                if field_active:
                    assert field_present, f'{f.name}: {s}'
            else:
                # In structures all fields are printed always.
                assert field_present, f'{f.name}: {s}'
