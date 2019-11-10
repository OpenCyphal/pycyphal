#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

r"""
Submodule import policy
+++++++++++++++++++++++

The following submodules are auto-imported when the root module ``pyuavcan`` is imported:

- :mod:`pyuavcan.dsdl`

- :mod:`pyuavcan.transport`, but not concrete transport implementation submodules.
  For example, if you need the CAN transport, import :mod:`pyuavcan.transport.can` manually.

- :mod:`pyuavcan.presentation`

- :mod:`pyuavcan.util`

The submodule :mod:`pyuavcan.application` is not auto-imported because in order to have it imported
the DSDL-generated package ``uavcan`` containing the standard data types must be generated first.

There are no internal (hidden) API between the submodules; they rely only on each other's public API.


CLI tool invocation
+++++++++++++++++++

The module can be run as ``python -m pyuavcan`` to invoke the CLI tool.


Log level override
++++++++++++++++++

The environment variable ``PYUAVCAN_LOGLEVEL`` can be set to one of the following values to override
the library log level:

- ``CRITICAL``
- ``FATAL``
- ``ERROR``
- ``WARNING``
- ``INFO``
- ``DEBUG``

If not set, the log level is determined following the regular policies of the Python's standard ``logging`` library.
"""

import os as _os
import sys as _sys
import pathlib as _pathlib


with open(_os.path.join(_os.path.dirname(__file__), 'VERSION')) as _version:
    __version__ = _version.read().strip()
__version_info__ = tuple(map(int, __version__.split('.')))
__license__ = 'MIT'
__author__ = 'UAVCAN Development Team'


UAVCAN_SPECIFICATION_VERSION = 1, 0
"""
Version of the UAVCAN protocol implemented by this library, major and minor.
Use this value to populate the corresponding field in ``uavcan.node.GetInfo.Response``.
"""


if _sys.version_info[:2] < (3, 7):   # pragma: no cover
    raise RuntimeError('A newer version of Python is required')


# Configure logging if requested via environment variable.
# Accepted values: CRITICAL, ERROR, WARNING, INFO, DEBUG
_log_level_from_env = _os.environ.get('PYUAVCAN_LOGLEVEL')
if _log_level_from_env is not None:
    import logging as _logging
    _logging.basicConfig(format='%(asctime)s %(process)5d %(levelname)-8s %(name)s: %(message)s',
                         level=_log_level_from_env)
    _logging.getLogger(__name__).setLevel(_log_level_from_env)
    _logging.getLogger(__name__).info('Log config from env var; level: %r', _log_level_from_env)


VERSION_AGNOSTIC_DATA_DIR: _pathlib.Path
"""
The root directory of version-specific data directories.
Its location is platform-dependent.
It is shared for all versions of the library.
"""

if hasattr(_sys, 'getwindowsversion'):  # pragma: no cover
    _appdata_env = _os.getenv('LOCALAPPDATA') or _os.getenv('APPDATA')
    assert _appdata_env, 'Cannot determine the location of the app data directory'
    VERSION_AGNOSTIC_DATA_DIR = _pathlib.Path(_appdata_env, 'UAVCAN', 'PyUAVCAN')
else:
    VERSION_AGNOSTIC_DATA_DIR = _pathlib.Path('~/.uavcan/pyuavcan').expanduser()

VERSION_SPECIFIC_DATA_DIR: _pathlib.Path = VERSION_AGNOSTIC_DATA_DIR / ('v' + '.'.join(map(str, __version_info__[:2])))
"""
The directory specific to this version of the library where resources and files are stored.
This is always a subdirectory of :data:`VERSION_AGNOSTIC_DATA_DIR`.
The version is specified down to the minor version, ignoring the patch version (e.g, 1.1),
so that versions of the library that differ only by the patch version number will use the same directory.

This directory contains the default destination path for highly volatile or low-value files such as
generated DSDL packages and some CLI-specific entities.
Having such files segregated by the library version number ensures that when the library is updated,
it will not encounter compatibility issues with older formats.
"""

# The sub-packages are imported in the order of their interdependency.
import pyuavcan.util as util                    # noqa
import pyuavcan.dsdl as dsdl                    # noqa
import pyuavcan.transport as transport          # noqa
import pyuavcan.presentation as presentation    # noqa
