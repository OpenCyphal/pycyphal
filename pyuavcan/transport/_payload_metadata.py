# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class PayloadMetadata:
    """
    This information is obtained from the data type definition.

    Eventually, this type might include the runtime type identification information,
    if it is ever implemented in UAVCAN. The alpha revision used to contain the "data type hash" field here,
    but this concept was found deficient and removed from the proposal.
    You can find related discussion in https://forum.uavcan.org/t/alternative-transport-protocols-in-uavcan/324.
    """

    extent_bytes: int
    """
    The minimum amount of memory required to hold any serialized representation of any compatible version
    of the data type; or, on other words, it is the the maximum possible size of received objects.
    The size is specified in bytes because extent is guaranteed (by definition) to be an integer number of bytes long.

    This parameter is determined by the data type author at the data type definition time.
    It is typically larger than the maximum object size in order to allow the data type author to
    introduce more fields in the future versions of the type;
    for example, ``MyMessage.1.0`` may have the maximum size of 100 bytes and the extent 200 bytes;
    a revised version ``MyMessage.1.1`` may have the maximum size anywhere between 0 and 200 bytes.
    It is always safe to pick a larger value if not sure.
    You will find a more rigorous description in the UAVCAN Specification.

    Transport implementations may use this information to statically size receive buffers or
    to perform early detection of malformed transfers if the size of their payload exceeds this limit.
    """

    def __post_init__(self) -> None:
        if self.extent_bytes < 0:
            raise ValueError(f"Invalid extent [byte]: {self.extent_bytes}")
