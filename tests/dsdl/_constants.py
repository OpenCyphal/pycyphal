#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pydsdl
import pytest
import pyuavcan.dsdl
from . import _util


def _unittest_constants(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    for info in generated_packages:
        for model in _util.expand_service_types(info.models, keep_services=True):
            cls = pyuavcan.dsdl.get_class(model)
            for c in model.constants:
                if isinstance(c.data_type, pydsdl.PrimitiveType):
                    reference = c.value
                    generated = pyuavcan.dsdl.get_attribute(cls, c.name)
                    assert isinstance(reference, pydsdl.Primitive)
                    assert reference.native_value == pytest.approx(generated), \
                        'The generated constant does not compare equal against the DSDL source'
            if issubclass(cls, pyuavcan.dsdl.FixedPortObject):
                assert issubclass(cls, pyuavcan.dsdl.CompositeObject) and issubclass(cls, pyuavcan.dsdl.FixedPortObject)
                assert pyuavcan.dsdl.get_fixed_port_id(cls) == pyuavcan.dsdl.get_model(cls).fixed_port_id
