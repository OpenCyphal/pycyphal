#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import dataclasses
import pyuavcan.transport


TRANSFER_ID_MODULO = 32


@dataclasses.dataclass
class Frame:
    class Format(enum.IntEnum):
        STANDARD = 11
        EXTENDED = 29

    identifier: int
    data:       bytearray
    format:     Format

    @property
    def data_length_code(self) -> int:
        try:
            return _LENGTH_TO_DLC[len(self.data)]
        except LookupError:
            raise ValueError(f'{len(self.data)} bytes is not a valid data length; '
                             f'valid length values are: {list(_LENGTH_TO_DLC.keys())}') from None

    @staticmethod
    def parse_data_length_code(dlc: int) -> int:
        try:
            return _DLC_TO_LENGTH[dlc]
        except LookupError:
            raise ValueError(f'{dlc} is not a valid DLC') from None

    @staticmethod
    def new(identifier:        int,
            padded_payload:    memoryview,
            transfer_id:       int,
            start_of_transfer: bool,
            end_of_transfer:   bool,
            toggle_bit:        bool) -> Frame:
        if not (0 <= identifier < 2 ** 29):
            raise ValueError(f'Invalid CAN ID: {identifier}')

        if transfer_id < 0:
            raise ValueError(f'Invalid transfer ID: {transfer_id}')

        tail_byte = transfer_id % TRANSFER_ID_MODULO
        if start_of_transfer:
            tail_byte |= 1 << 7
        if end_of_transfer:
            tail_byte |= 1 << 6
        if toggle_bit:
            tail_byte |= 1 << 5

        data = bytearray(padded_payload)
        data.append(tail_byte)
        if len(data) not in _LENGTH_TO_DLC:
            raise ValueError(f'Unsupported payload length: {len(padded_payload)}')

        return Frame(identifier=identifier, data=data, format=Frame.Format.EXTENDED)


@dataclasses.dataclass
class ReceivedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    @property
    def padded_payload(self) -> memoryview:
        return memoryview(self.data)[:-1]

    @property
    def start_of_transfer(self) -> bool:
        return self._tail & (1 << 7) != 0

    @property
    def end_of_transfer(self) -> bool:
        return self._tail & (1 << 6) != 0

    @property
    def toggle_bit(self) -> bool:
        return self._tail & (1 << 5) != 0

    @property
    def transfer_id(self) -> int:
        return self._tail & (TRANSFER_ID_MODULO - 1)

    @property
    def _tail(self) -> int:
        return self.data[-1]


_DLC_TO_LENGTH = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]
_LENGTH_TO_DLC: typing.Dict[int, int] = dict(zip(*list(zip(*enumerate(_DLC_TO_LENGTH)))[::-1]))  # type: ignore
assert len(_LENGTH_TO_DLC) == 16 == len(_DLC_TO_LENGTH)
for item in _DLC_TO_LENGTH:
    assert _DLC_TO_LENGTH[_LENGTH_TO_DLC[item]] == item, 'Invalid DLC tables'
