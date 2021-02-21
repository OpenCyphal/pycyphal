# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
This module is used for automatic generation of Python classes from DSDL type definitions and
also for various manipulations on them.
Auto-generated classes have a high-level application-facing API and built-in auto-generated
serialization and deserialization routines.

The serialization code heavily relies on NumPy and the data alignment analysis implemented in PyDSDL.
Some of the technical details are covered in the following posts:

- https://forum.uavcan.org/t/pyuavcan-design-thread/504
- https://github.com/UAVCAN/pydsdl/pull/24

The main entity of this module is the function :func:`compile`.

Below is the inheritance diagram for the classes defined in this module.

.. inheritance-diagram:: pyuavcan.dsdl._composite_object
   :parts: 1
"""

from ._compiler import compile as compile  # pylint: disable=redefined-builtin
from ._compiler import compile_all as compile_all
from ._compiler import GeneratedPackageInfo as GeneratedPackageInfo

from ._composite_object import serialize as serialize
from ._composite_object import deserialize as deserialize

from ._composite_object import CompositeObject as CompositeObject
from ._composite_object import ServiceObject as ServiceObject

from ._composite_object import FixedPortObject as FixedPortObject
from ._composite_object import FixedPortCompositeObject as FixedPortCompositeObject
from ._composite_object import FixedPortServiceObject as FixedPortServiceObject

from ._composite_object import get_fixed_port_id as get_fixed_port_id
from ._composite_object import get_model as get_model
from ._composite_object import get_class as get_class
from ._composite_object import get_extent_bytes as get_extent_bytes

from ._composite_object import get_attribute as get_attribute
from ._composite_object import set_attribute as set_attribute

from ._builtin_form import to_builtin as to_builtin
from ._builtin_form import update_from_builtin as update_from_builtin


def generate_package(*args, **kwargs):  # type: ignore  # pragma: no cover
    """Deprecated alias of :func:`compile`."""
    import warnings

    warnings.warn(
        "pyuavcan.dsdl.generate_package() is deprecated; use pyuavcan.dsdl.compile() instead.",
        DeprecationWarning,
    )
    return compile(*args, **kwargs)
