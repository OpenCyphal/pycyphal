# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import os as _os
from . import dsdl as dsdl

assert _os.environ.get("PYTHONASYNCIODEBUG", False), "PYTHONASYNCIODEBUG should be set while running the tests"
