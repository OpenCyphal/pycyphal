#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pydsdl
import pyuavcan.dsdl
from . import _util


def _unittest_slow_textual(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    def validate(obj: pyuavcan.dsdl.CompositeObject, s: str) -> None:
        for f in model.fields_except_padding:
            field_present = (f"{f.name}=" in s) or (f"{f.name}_=" in s)
            if isinstance(model.inner_type, pydsdl.UnionType):
                # In unions only the active field is printed.
                # The active field may contain nested fields which  may be named similarly to other fields
                # in the current union, so we can't easily ensure lack of non-active fields in the output.
                field_active = pyuavcan.dsdl.get_attribute(obj, f.name) is not None
                if field_active:
                    assert field_present, f"{f.name}: {s}"
            else:
                # In structures all fields are printed always.
                assert field_present, f"{f.name}: {s}"

    for info in generated_packages:
        for model in _util.expand_service_types(info.models):
            for fn in [str, repr]:
                assert callable(fn)
                for _ in range(10):
                    ob = _util.make_random_object(model)
                    validate(ob, fn(ob))
