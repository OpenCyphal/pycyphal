# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import pydsdl
import pycyphal.dsdl
from . import _util


def _unittest_slow_textual(compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    def validate(obj: pycyphal.dsdl.CompositeObject, s: str) -> None:
        for f in model.fields_except_padding:  # pylint: disable=undefined-loop-variable
            field_present = (f"{f.name}=" in s) or (f"{f.name}_=" in s)
            if isinstance(model.inner_type, pydsdl.UnionType):  # pylint: disable=undefined-loop-variable
                # In unions only the active field is printed.
                # The active field may contain nested fields which  may be named similarly to other fields
                # in the current union, so we can't easily ensure lack of non-active fields in the output.
                field_active = pycyphal.dsdl.get_attribute(obj, f.name) is not None
                if field_active:
                    assert field_present, f"{f.name}: {s}"
            else:
                # In structures all fields are printed always.
                assert field_present, f"{f.name}: {s}"

    for info in compiled:
        for model in _util.expand_service_types(info.models):
            for fn in [str, repr]:
                assert callable(fn)
                for _ in range(10):
                    ob = _util.make_random_object(model)
                    validate(ob, fn(ob))
