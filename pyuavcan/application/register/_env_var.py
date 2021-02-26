# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import logging
from typing import Union, List
from . import ValueProxy, ValueConversionError


_logger = logging.getLogger(__name__)


def update_from_environment(value: ValueProxy, register_name: str) -> bool:
    """
    Update the supplied value in-place from the environment variable.

    :param value:
        The register value to update in-place.

    :param register_name:
        E.g., ``uavcan.node.id``.
        This name is mapped to the environment variable name, e.g., ``UAVCAN__NODE__ID``.

    :return:
        - True if the environment variable exists and contains a valid value that has been applied.
        - False if no matching environment variable exists (the value is not updated).

    :raises: :class:`ValueConversionError` if the value cannot be converted.
    """
    env_name = register_name.upper().replace(".", "__")
    try:
        env_val = os.environb[env_name.encode()]
    except LookupError:
        return False

    if value.value.empty or value.value.string or value.value.unstructured:
        value.assign(env_val)
    else:
        numbers: List[Union[int, float]] = []
        for nt in env_val.split():
            try:
                numbers.append(int(nt))
            except ValueError:
                try:
                    numbers.append(float(nt))
                except ValueError:
                    raise ValueConversionError(
                        f"Cannot update register {register_name!r} from environment value {env_val!r}"
                    ) from None
        value.assign(numbers)

    return True
