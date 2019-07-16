#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import dataclasses


@dataclasses.dataclass(frozen=True)
class PayloadMetadata:
    DATA_TYPE_HASH_MASK = 2 ** 64 - 1

    data_type_hash: int   # Obtainable from PyDSDL; https://forum.uavcan.org/t/alternative-transport-protocols/324
    max_size_bytes: int   # Max size of the serialized representation

    def __post_init__(self) -> None:
        if not (0 <= self.data_type_hash <= self.DATA_TYPE_HASH_MASK):
            raise ValueError(f'Invalid data type hash: {hex(self.data_type_hash)}')

        if self.max_size_bytes < 0:
            raise ValueError(f'Invalid max size [byte]: {self.max_size_bytes}')

    def __repr__(self) -> str:
        return f'{type(self).__name__}(' \
            f'data_type_hash=0x{self.data_type_hash:08x}, ' \
            f'max_size_bytes={self.max_size_bytes})'
