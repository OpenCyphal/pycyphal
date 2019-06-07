#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class PayloadMetadata:
    COMPACT_DATA_TYPE_ID_MASK = 2 ** 64 - 1

    compact_data_type_id: int   # Obtainable from PyDSDL; https://forum.uavcan.org/t/alternative-transport-protocols/324
    max_size_bytes:       int   # Max size of the serialized representation

    def __post_init__(self) -> None:
        if not (0 <= self.compact_data_type_id <= self.COMPACT_DATA_TYPE_ID_MASK):
            raise ValueError(f'Invalid compact data type ID: {hex(self.compact_data_type_id)}')

        if self.max_size_bytes < 0:
            raise ValueError(f'Invalid max size [byte]: {self.max_size_bytes}')

    def __str__(self) -> str:
        return f'{type(self).__name__}(' \
            f'compact_data_type_id=0x{self.compact_data_type_id:08x}, ' \
            f'max_size_bytes={self.max_size_bytes})'

    def __repr__(self) -> str:
        return self.__str__()
