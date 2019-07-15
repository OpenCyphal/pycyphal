#!/usr/bin/env python3
#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import re
import pyuavcan

print('.. autosummary::')
print('   :nosignatures:')
print()

# noinspection PyTypeChecker
pyuavcan.util.import_submodules(pyuavcan.transport)
for cls in pyuavcan.util.iter_descendants(pyuavcan.transport.Transport):
    export_module_name = re.sub(r'\._[_a-zA-Z0-9]*', '', cls.__module__)
    print(f'   {export_module_name}.{cls.__name__}')

print()
