#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport
from . import media as _media


TRANSFER_ID_MODULO = 32

TRANSFER_CRC_LENGTH_BYTES = 2


@dataclasses.dataclass(frozen=True)
class UAVCANFrame:
    identifier:        int
    padded_payload:    memoryview
    transfer_id:       int
    start_of_transfer: bool
    end_of_transfer:   bool
    toggle_bit:        bool
    loopback:          bool

    def __post_init__(self) -> None:
        if not (0 <= self.identifier <= 2 ** 29):
            raise ValueError(f'Invalid identifier: {self.identifier}')

        if self.transfer_id < 0:
            raise ValueError('Transfer ID cannot be negative')

        if self.start_of_transfer and not self.toggle_bit:
            raise ValueError(f'The toggle bit must be set in the first frame of the transfer')

    def compile(self) -> _media.DataFrame:
        tail = self.transfer_id % TRANSFER_ID_MODULO
        if self.start_of_transfer:
            tail |= 1 << 7
        if self.end_of_transfer:
            tail |= 1 << 6
        if self.toggle_bit:
            tail |= 1 << 5

        data = bytearray(self.padded_payload)
        data.append(tail)

        return _media.DataFrame(identifier=self.identifier,
                                data=data,
                                format=_media.FrameFormat.EXTENDED,
                                loopback=self.loopback)

    @staticmethod
    def get_required_padding(data_length: int) -> int:
        return _media.DataFrame.get_required_padding(data_length + 1)   # +1 for the tail byte


@dataclasses.dataclass(frozen=True)
class TimestampedUAVCANFrame(UAVCANFrame):
    timestamp: pyuavcan.transport.Timestamp

    @staticmethod
    def try_parse(source: _media.TimestampedDataFrame) -> typing.Optional[TimestampedUAVCANFrame]:
        if source.format != _media.FrameFormat.EXTENDED:
            return None

        if len(source.data) < 1:
            return None

        padded_payload, tail = memoryview(source.data)[:-1], source.data[-1]
        transfer_id = tail & (TRANSFER_ID_MODULO - 1)
        sot, eot, tog = tuple(tail & (1 << x) != 0 for x in (7, 6, 5))
        if sot and not tog:
            return None

        return TimestampedUAVCANFrame(timestamp=source.timestamp,
                                      identifier=source.identifier,
                                      padded_payload=padded_payload,
                                      transfer_id=transfer_id,
                                      start_of_transfer=sot,
                                      end_of_transfer=eot,
                                      toggle_bit=tog,
                                      loopback=source.loopback)


def compute_transfer_id_forward_distance(a: int, b: int) -> int:
    """
    The algorithm is defined in the CAN bus transport layer specification of the UAVCAN Specification.
    """
    assert a >= 0 and b >= 0
    a %= TRANSFER_ID_MODULO
    b %= TRANSFER_ID_MODULO
    d = b - a
    if d < 0:
        d += TRANSFER_ID_MODULO

    assert 0 <= d < TRANSFER_ID_MODULO
    assert (a + d) & (TRANSFER_ID_MODULO - 1) == b
    return d


def _unittest_can_transfer_id_forward_distance() -> None:
    cfd = compute_transfer_id_forward_distance
    assert 0 == cfd(0, 0)
    assert 1 == cfd(0, 1)
    assert 7 == cfd(0, 7)
    assert 0 == cfd(7, 7)
    assert 1 == cfd(31, 0)
    assert 5 == cfd(0, 5)
    assert 31 == cfd(31, 30)
    assert 30 == cfd(7, 5)
