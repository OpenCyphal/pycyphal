#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os as _os
from ._main import main as main

with open(_os.path.join(_os.path.dirname(__file__), 'VERSION')) as _version:
    __version__ = _version.read().strip()
__version_info__ = tuple(map(int, __version__.split('.')))
__license__ = 'MIT'
