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

    def __str__(self) -> str:
        return f'{type(self).__name__}(' \
            f'compact_data_type_id=0x{self.compact_data_type_id:08x}, ' \
            f'max_size_bytes={self.max_size_bytes})'

    def __repr__(self) -> str:
        return self.__str__()
