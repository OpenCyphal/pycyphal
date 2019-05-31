#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os as _os


SKIP_SLOW = bool(_os.environ.get('PYUAVCAN_TEST_SKIP_SLOW', False))

assert _os.environ.get('PYTHONASYNCIODEBUG', False), 'PYTHONASYNCIODEBUG should be set while running the tests'
