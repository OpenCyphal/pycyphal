# Copyright (C) 2021  OpenCyphal  <opencyphal.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# pylint: disable=wrong-import-position

"""
Implementation of the Cyphal register interface as defined in the Cyphal Specification
(section 5.3 *Application-layer functions*).
"""

import uavcan.primitive
import uavcan.primitive.array

# import X as Y is not an accepted form; see https://github.com/python/mypy/issues/11706
Empty = uavcan.primitive.Empty_1
String = uavcan.primitive.String_1
Unstructured = uavcan.primitive.Unstructured_1
Bit = uavcan.primitive.array.Bit_1
Integer64 = uavcan.primitive.array.Integer64_1
Integer32 = uavcan.primitive.array.Integer32_1
Integer16 = uavcan.primitive.array.Integer16_1
Integer8 = uavcan.primitive.array.Integer8_1
Natural64 = uavcan.primitive.array.Natural64_1
Natural32 = uavcan.primitive.array.Natural32_1
Natural16 = uavcan.primitive.array.Natural16_1
Natural8 = uavcan.primitive.array.Natural8_1
Real64 = uavcan.primitive.array.Real64_1
Real32 = uavcan.primitive.array.Real32_1
Real16 = uavcan.primitive.array.Real16_1

from ._value import Value as Value
from ._value import ValueProxy as ValueProxy
from ._value import RelaxedValue as RelaxedValue
from ._value import ValueConversionError as ValueConversionError

from . import backend as backend

from ._registry import Registry as Registry
from ._registry import ValueProxyWithFlags as ValueProxyWithFlags
from ._registry import MissingRegisterError as MissingRegisterError


def get_environment_variable_name(register_name: str) -> str:
    """
    Convert the name of the register to the name of the environment variable that assigns it.

    >>> get_environment_variable_name("m.motor.inductance_dq")
    'M__MOTOR__INDUCTANCE_DQ'
    """
    return register_name.upper().replace(".", "__")
