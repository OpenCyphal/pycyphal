# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

r"""
Submodule import policy
+++++++++++++++++++++++

The following submodules are auto-imported when the root module ``pyuavcan`` is imported:

- :mod:`pyuavcan.dsdl`
- :mod:`pyuavcan.transport`, but not concrete transport implementation submodules.
- :mod:`pyuavcan.presentation`
- :mod:`pyuavcan.util`

Submodule :mod:`pyuavcan.application` is not auto-imported because in order to have it imported
the DSDL-generated package ``uavcan`` containing the standard data types must be compiled first.


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
"""

import os as _os
import sys as _sys


with open(_os.path.join(_os.path.dirname(__file__), "VERSION")) as _version:
    __version__ = _version.read().strip()
__version_info__ = tuple(map(int, __version__.split(".")[:3]))
__author__ = "UAVCAN Consortium"
__copyright__ = "Copyright (c) 2019 UAVCAN Consortium"
__email__ = "consortium@uavcan.org"
__license__ = "MIT"


UAVCAN_SPECIFICATION_VERSION = 1, 0
"""
Version of the UAVCAN protocol implemented by this library, major and minor.
The corresponding field in ``uavcan.node.GetInfo.Response`` is initialized from this value,
see :func:`pyuavcan.application.make_node`.
"""


if _sys.version_info[:2] < (3, 7):  # pragma: no cover
    raise RuntimeError("A newer version of Python is required")


_log_level_from_env = _os.environ.get("PYUAVCAN_LOGLEVEL")
if _log_level_from_env is not None:
    import logging as _logging

    _logging.basicConfig(
        format="%(asctime)s %(process)5d %(levelname)-8s %(name)s: %(message)s", level=_log_level_from_env
    )
    _logging.getLogger(__name__).setLevel(_log_level_from_env)
    _logging.getLogger(__name__).info("Log config from env var; level: %r", _log_level_from_env)


# The sub-packages are imported in the order of their interdependency.
import pyuavcan.util as util  # pylint: disable=wrong-import-position  # noqa
import pyuavcan.dsdl as dsdl  # pylint: disable=wrong-import-position  # noqa
import pyuavcan.transport as transport  # pylint: disable=wrong-import-position  # noqa
import pyuavcan.presentation as presentation  # pylint: disable=wrong-import-position  # noqa
