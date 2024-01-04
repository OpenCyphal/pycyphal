# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

"""
This module is used for automatic generation of Python classes from DSDL type definitions and
also for various manipulations on them.
Auto-generated classes have a high-level application-facing API and built-in auto-generated
serialization and deserialization routines.

The serialization code heavily relies on NumPy and the data alignment analysis implemented in PyDSDL.
Some of the technical details are covered in the following posts:

- https://forum.opencyphal.org/t/pycyphal-design-thread/504
- https://github.com/OpenCyphal/pydsdl/pull/24

The main entity of this module is the function :func:`compile`.
"""

from ._compiler import compile as compile  # pylint: disable=redefined-builtin
from ._compiler import compile_all as compile_all
from ._compiler import GeneratedPackageInfo as GeneratedPackageInfo

from ._import_hook import install_import_hook as install_import_hook


def generate_package(*args, **kwargs):  # type: ignore  # pragma: no cover
    """Deprecated alias of :func:`compile`."""
    import warnings

    warnings.warn(
        "pycyphal.dsdl.generate_package() is deprecated; use pycyphal.dsdl.compile() instead.",
        DeprecationWarning,
    )
    return compile(*args, **kwargs)
