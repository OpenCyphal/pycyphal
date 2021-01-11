# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os as _os
from . import dsdl as dsdl

assert ("PYTHONASYNCIODEBUG" in _os.environ) or (
    _os.environ.get("IGNORE_PYTHONASYNCIODEBUG", False)
), "PYTHONASYNCIODEBUG should be set while running the tests"
