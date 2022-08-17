# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

r"""
Submodule import policy
+++++++++++++++++++++++

The following submodules are auto-imported when the root module ``pycyphal`` is imported:

- :mod:`pycyphal.dsdl`
- :mod:`pycyphal.transport`, but not concrete transport implementation submodules.
- :mod:`pycyphal.presentation`
- :mod:`pycyphal.util`

Submodule :mod:`pycyphal.application` is not auto-imported because in order to have it imported
the DSDL-generated package ``uavcan`` containing the standard data types must be compiled first.


Log level override
++++++++++++++++++

The environment variable ``PYCYPHAL_LOGLEVEL`` can be set to one of the following values to override
the library log level:

- ``CRITICAL``
- ``FATAL``
- ``ERROR``
- ``WARNING``
- ``INFO``
- ``DEBUG``
"""

import os as _os


from ._version import __version__ as __version__

__version_info__ = tuple(map(int, __version__.split(".")[:3]))
__author__ = "OpenCyphal"
__copyright__ = "Copyright (c) 2019 OpenCyphal"
__email__ = "consortium@opencyphal.org"
__license__ = "MIT"


CYPHAL_SPECIFICATION_VERSION = 1, 0
"""
Version of the Cyphal protocol implemented by this library, major and minor.
The corresponding field in ``uavcan.node.GetInfo.Response`` is initialized from this value,
see :func:`pycyphal.application.make_node`.
"""


_log_level_from_env = _os.environ.get("PYCYPHAL_LOGLEVEL")
if _log_level_from_env is not None:
    import logging as _logging

    _logging.basicConfig(
        format="%(asctime)s %(process)5d %(levelname)-8s %(name)s: %(message)s", level=_log_level_from_env
    )
    _logging.getLogger(__name__).setLevel(_log_level_from_env)
    _logging.getLogger(__name__).info("Log config from env var; level: %r", _log_level_from_env)


# The sub-packages are imported in the order of their interdependency.
import pycyphal.util as util  # pylint: disable=R0402,C0413  # noqa
import pycyphal.dsdl as dsdl  # pylint: disable=R0402,C0413  # noqa
import pycyphal.transport as transport  # pylint: disable=R0402,C0413  # noqa
import pycyphal.presentation as presentation  # pylint: disable=R0402,C0413  # noqa
