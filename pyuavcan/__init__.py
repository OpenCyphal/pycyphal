#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys

if sys.version_info[:2] < (3, 7):   # pragma: no cover
    raise RuntimeError('A newer version of Python is required')

__version__ = 0, 1, 1
__license__ = 'MIT'

import pyuavcan.util            # noqa
import pyuavcan.dsdl            # noqa
import pyuavcan.transport       # noqa
import pyuavcan.presentation    # noqa

# pyuavcan.application is not imported by default because it depends on the standard DSDL namespace "uavcan";
# it is necessary to ensure that the corresponding Python package is generated before importing pyuavcan.application.
