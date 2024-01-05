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

from typing import TypeVar, Type, Sequence, cast, Any, Iterable, Optional, Dict
from ._compiler import compile as compile  # pylint: disable=redefined-builtin
from ._compiler import compile_all as compile_all
from ._compiler import GeneratedPackageInfo as GeneratedPackageInfo

from ._import_hook import install_import_hook as install_import_hook
import pydsdl

_T = TypeVar("T")


def serialize(obj: Any) -> Iterable[memoryview]:
    """
    A wrapper over ``nunavut_support.serialize``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.serialize(obj)


def deserialize(dtype: Type[_T], fragmented_serialized_representation: Sequence[memoryview]) -> Optional[_T]:
    """
    A wrapper over ``nunavut_support.deserialize``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.deserialize(dtype, fragmented_serialized_representation)


def get_model(class_or_instance: Any) -> pydsdl.CompositeType:
    """
    A wrapper over ``nunavut_support.get_model``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.get_model(class_or_instance)


def get_class(model: pydsdl.CompositeType) -> type:
    """
    A wrapper over ``nunavut_support.get_class``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.get_class(model)


def get_extent_bytes(class_or_instance: Any) -> int:
    """
    A wrapper over ``nunavut_support.get_extent_bytes``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.get_extent_bytes(class_or_instance)


def get_fixed_port_id(class_or_instance: Any) -> Optional[int]:
    """
    A wrapper over ``nunavut_support.get_fixed_port_id``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.get_fixed_port_id(class_or_instance)


def get_attribute(obj: Any, name: str) -> Any:
    """
    A wrapper over ``nunavut_support.get_attribute``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.get_attribute(obj, name)


def set_attribute(obj: Any, name: str, value: Any) -> None:
    """
    A wrapper over ``nunavut_support.set_attribute``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.set_attribute(obj, name, value)


def is_serializable(dtype: Any) -> bool:
    """
    A wrapper over ``nunavut_support.is_serializable``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.is_serializable(dtype)


def is_message_type(dtype: Any) -> bool:
    """
    A wrapper over ``nunavut_support.is_message_type``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.is_message_type(dtype)


def is_service_type(dtype: Any) -> bool:
    """
    A wrapper over ``nunavut_support.is_service_type``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.is_service_type(dtype)


def to_builtin(obj: object) -> Dict[str, Any]:
    """
    A wrapper over ``nunavut_support.to_builtin``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.to_builtin(obj)


def update_from_builtin(destination: _T, source: Any) -> _T:
    """
    A wrapper over ``nunavut_support.update_from_builtin``.
    The ``nunavut_support`` module will be generated automatically if it is not importable.
    """
    import nunavut_support

    return nunavut_support.update_from_builtin(destination, source)


def generate_package(*args, **kwargs):  # type: ignore  # pragma: no cover
    """Deprecated alias of :func:`compile`."""
    import warnings

    warnings.warn(
        "pycyphal.dsdl.generate_package() is deprecated; use pycyphal.dsdl.compile() instead.",
        DeprecationWarning,
    )
    return compile(*args, **kwargs)
