#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

r"""
The following submodules are auto-imported when the root module ``pyuavcan`` is imported:

- :mod:`pyuavcan.dsdl`

- :mod:`pyuavcan.transport`, but not concrete transport implementation submodules.
  For example, if you need the CAN transport, import :mod:`pyuavcan.transport.can` manually.

- :mod:`pyuavcan.presentation`

- :mod:`pyuavcan.util`

The submodule :mod:`pyuavcan.application` is not auto-imported because in order to have it imported
the DSDL-generated package ``uavcan`` containing the standard data types must be generated first.
"""

import os as _os
import sys as _sys

if _sys.version_info[:2] < (3, 7):   # pragma: no cover
    raise RuntimeError('A newer version of Python is required')

with open(_os.path.join(_os.path.dirname(__file__), 'VERSION')) as _version:
    __version__ = _version.read().strip()
__version_info__ = tuple(map(int, __version__.split('.')))
__license__ = 'MIT'


#: Version of the UAVCAN protocol implemented by this library, major and minor.
#: Use this value to populate the corresponding field in ``uavcan.node.GetInfo.Response``.
UAVCAN_SPECIFICATION_VERSION = 1, 0


# The sub-packages are imported in the order of their interdependency.
import pyuavcan.util as util                    # noqa
import pyuavcan.dsdl as dsdl                    # noqa
import pyuavcan.transport as transport          # noqa
import pyuavcan.presentation as presentation    # noqa
