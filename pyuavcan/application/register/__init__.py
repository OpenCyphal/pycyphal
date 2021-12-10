# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>
#
# Workaround for the odd behavior of MyPy https://github.com/python/mypy/issues/11706
# mypy: implicit_reexport=True

"""
Implementation of the UAVCAN register interface as defined in the UAVCAN Specification
(section 5.3 *Application-layer functions*).
"""

from uavcan.primitive import Empty_1 as Empty
from uavcan.primitive import String_1 as String
from uavcan.primitive import Unstructured_1 as Unstructured
from uavcan.primitive.array import Bit_1 as Bit
from uavcan.primitive.array import Integer64_1 as Integer64
from uavcan.primitive.array import Integer32_1 as Integer32
from uavcan.primitive.array import Integer16_1 as Integer16
from uavcan.primitive.array import Integer8_1 as Integer8
from uavcan.primitive.array import Natural64_1 as Natural64
from uavcan.primitive.array import Natural32_1 as Natural32
from uavcan.primitive.array import Natural16_1 as Natural16
from uavcan.primitive.array import Natural8_1 as Natural8
from uavcan.primitive.array import Real64_1 as Real64
from uavcan.primitive.array import Real32_1 as Real32
from uavcan.primitive.array import Real16_1 as Real16

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
