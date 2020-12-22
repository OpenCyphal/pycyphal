#!/usr/bin/env python3
# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import types
import pyuavcan
import pyuavcan.application

print(".. autosummary::")
print("   :nosignatures:")
print()

# noinspection PyTypeChecker
pyuavcan.util.import_submodules(pyuavcan.application)
for name in dir(pyuavcan.application):
    entity = getattr(pyuavcan.application, name)
    if isinstance(entity, types.ModuleType) and not name.startswith("_"):
        print(f"   {entity.__name__}")

print()
