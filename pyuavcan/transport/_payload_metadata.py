#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class PayloadMetadata:
    compact_data_type_id: int   # Obtainable from PyDSDL; https://forum.uavcan.org/t/alternative-transport-protocols/324
    max_size_bytes:       int   # Max size of the serialized representation
