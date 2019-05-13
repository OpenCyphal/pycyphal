#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pydsdl
import pytest
import pyuavcan.dsdl
from ._util import expand_service_types


def _unittest_constants(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    for info in generated_packages:
        for model in expand_service_types(info.types, keep_services=True):
            cls = pyuavcan.dsdl.get_generated_class(model)
            for c in model.constants:
                if isinstance(c.data_type, pydsdl.PrimitiveType):
                    reference = c.value
                    generated = pyuavcan.dsdl.get_attribute(cls, c.name)
                    assert isinstance(reference, pydsdl.Primitive)
                    assert reference.native_value == pytest.approx(generated), \
                        'The generated constant does not compare equal against the DSDL source'
