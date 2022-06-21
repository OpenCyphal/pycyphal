#!/usr/bin/env python3
# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import re
import pycyphal

print(".. autosummary::")
print("   :nosignatures:")
print()

# noinspection PyTypeChecker
pycyphal.util.import_submodules(pycyphal.transport)
for cls in pycyphal.util.iter_descendants(pycyphal.transport.Transport):
    export_module_name = re.sub(r"\._[_a-zA-Z0-9]*", "", cls.__module__)
    print(f"   {export_module_name}.{cls.__name__}")

print()
