#!/usr/bin/env python3
# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import types
import pycyphal
import pycyphal.application

print(".. autosummary::")
print("   :nosignatures:")
print()

# noinspection PyTypeChecker
pycyphal.util.import_submodules(pycyphal.application)
for name in dir(pycyphal.application):
    entity = getattr(pycyphal.application, name)
    if isinstance(entity, types.ModuleType) and not name.startswith("_"):
        print(f"   {entity.__name__}")

print()
