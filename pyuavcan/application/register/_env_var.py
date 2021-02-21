# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import logging
from typing import Optional, Dict, Tuple, List, Iterable
from . import Value, Empty, String, Unstructured, Bit
from . import Integer8, Integer16, Integer32, Integer64
from . import Natural8, Natural16, Natural32, Natural64
from . import Real16, Real32, Real64


_logger = logging.getLogger(__name__)


def parse_environment_variables(env: Optional[Dict[str, str]] = None) -> Dict[str, Value]:
    """
    Given a list of environment variables, generates pairs of (name, :class:`Value`).
    A register name is mapped to the environment variable name as follows:

    >>> name = 'm.motor.flux_linkage'
    >>> ty = 'real32'
    >>> (name + "." + ty).upper().replace(".", "_" * 2)  # Name mapping rule.
    'M__MOTOR__FLUX_LINKAGE__REAL32'

    Where ``ty`` is the name of the value option from ``uavcan.register.Value`` (see :class:`Value`),
    like ``bit``, ``integer8``, etc.
    Environment variables that contain invalid values or named incorrectly are simply ignored.

    - ``string`` values are accepted as-is.
    - ``unstructured`` values are hex-decoded (e.g., ``68656c6c6f`` --> "hello").
    - ``bit``, ``integer*``, ``natural*``, ``real*`` are assumed to be in decimal notation.

    Array items are space-separated.

    >>> parsed = parse_environment_variables({
    ...     "M__MOTOR__FLUX_LINKAGE__REAL32":    "1.23 4.56",                         # Space-separated.
    ...     "M__MOTOR__VENDOR_ID__STRING":       "Name: Sirius Cyber Corp.",          # Regular string as-is.
    ...     "M__MOTOR__UNIQUE_ID__UNSTRUCTURED": "587ebed4a860984ab78b2095ee07484c",  # Hex-encoded binary blob.
    ...     "LD_PRELOAD":                        "/opt/unrelated.so",       # Unrelated variables are simply ignored.
    ... })
    >>> parsed  # doctest: +NORMALIZE_WHITESPACE
    {'m.motor.flux_linkage': uavcan.register.Value...real32=...[1.23,4.56]...
     'm.motor.vendor_id':    uavcan.register.Value...string=...'Name: Sirius Cyber Corp.'...
     'm.motor.unique_id':    uavcan.register.Value...unstructured=...}

    :param env: If not provided, defaults to :data:`os.environ`.
    """
    if env is None:
        env = os.environ.copy()
    return dict(_parse(env))


def _parse(env: Dict[str, str]) -> Iterable[Tuple[str, Value]]:
    for env_name, env_value in env.items():
        name_type = _parse_name_and_type(env_name)
        if not name_type:
            continue

        reg_name, reg_type_name = name_type
        try:
            value = _parse_value(reg_type_name, env_value)
        except ValueError:
            value = None
        if value is None:
            _logger.info("Could not parse environment variable %r with value %r", env_name, env_value)
            continue

        yield reg_name, value


def _parse_value(ty: str, text: str) -> Optional[Value]:
    # pylint: disable=multiple-statements

    if ty == "empty":
        return Value(empty=Empty())
    if ty == "string":
        return Value(string=String(text))
    if ty == "unstructured":
        return Value(unstructured=Unstructured(bytes.fromhex(text)))
    if ty == "bit":
        return Value(bit=Bit(list(map(bool, text.split()))))

    def as_int() -> List[int]:
        return list(map(int, text.split()))

    # fmt: off
    if ty == "integer8":  return Value(integer8=Integer8(as_int()))
    if ty == "integer16": return Value(integer16=Integer16(as_int()))
    if ty == "integer32": return Value(integer32=Integer32(as_int()))
    if ty == "integer64": return Value(integer64=Integer64(as_int()))
    if ty == "natural8":  return Value(natural8=Natural8(as_int()))
    if ty == "natural16": return Value(natural16=Natural16(as_int()))
    if ty == "natural32": return Value(natural32=Natural32(as_int()))
    if ty == "natural64": return Value(natural64=Natural64(as_int()))
    # fmt: on

    def as_float() -> List[float]:
        return list(map(float, text.split()))

    # fmt: off
    if ty == "real16": return Value(real16=Real16(as_float()))
    if ty == "real32": return Value(real32=Real32(as_float()))
    if ty == "real64": return Value(real64=Real64(as_float()))
    # fmt: on

    return None


def _parse_name_and_type(name: str) -> Optional[Tuple[str, str]]:
    components = name.lower().split("__")
    if len(components) < 2:
        return None
    *components, ty = components
    return ".".join(components), ty
