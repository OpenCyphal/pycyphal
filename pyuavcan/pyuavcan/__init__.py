#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os as _os
import sys as _sys

if _sys.version_info[:2] < (3, 7):   # pragma: no cover
    raise RuntimeError('A newer version of Python is required')

with open(_os.path.join(_os.path.dirname(__file__), 'VERSION')) as _version:
    __version__ = _version.read().strip()
__version_info__ = tuple(map(int, __version__.split('.')))
__license__ = 'MIT'


# Version of the UAVCAN protocol implemented by this library.
UAVCAN_SPECIFICATION_VERSION = 1, 0


# The sub-packages are included in the order of their interdependency
import pyuavcan.util as util                    # noqa
import pyuavcan.dsdl as dsdl                    # noqa
import pyuavcan.transport as transport          # noqa
import pyuavcan.presentation as presentation    # noqa

# pyuavcan.application is not imported by default because it depends on the standard DSDL namespace "uavcan";
# it is necessary to ensure that the corresponding Python package is generated before importing pyuavcan.application.
