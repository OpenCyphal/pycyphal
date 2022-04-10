# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import pydsdl
import pytest
import pycyphal.dsdl
from . import _util


def _unittest_slow_constants(compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    for info in compiled:
        for model in _util.expand_service_types(info.models, keep_services=True):
            dtype = pycyphal.dsdl.get_class(model)
            for c in model.constants:
                if isinstance(c.data_type, pydsdl.PrimitiveType):  # pragma: no branch
                    reference = c.value
                    generated = pycyphal.dsdl.get_attribute(dtype, c.name)
                    assert isinstance(reference, pydsdl.Primitive)
                    assert reference.native_value == pytest.approx(
                        generated
                    ), "The generated constant does not compare equal against the DSDL source"
            if issubclass(dtype, pycyphal.dsdl.FixedPortObject):
                assert issubclass(dtype, pycyphal.dsdl.CompositeObject) and issubclass(
                    dtype, pycyphal.dsdl.FixedPortObject
                )
                assert pycyphal.dsdl.get_fixed_port_id(dtype) == pycyphal.dsdl.get_model(dtype).fixed_port_id
