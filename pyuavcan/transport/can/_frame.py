# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.util
from .media import DataFrame, FrameFormat


TRANSFER_ID_MODULO = 32

TRANSFER_CRC_LENGTH_BYTES = 2


@dataclasses.dataclass(frozen=True)
class UAVCANFrame:
    identifier: int
    transfer_id: int
    start_of_transfer: bool
    end_of_transfer: bool
    toggle_bit: bool
    padded_payload: memoryview

    def __post_init__(self) -> None:
        if self.transfer_id < 0:
            raise ValueError("Transfer ID cannot be negative")

        if self.start_of_transfer and not self.toggle_bit:
            raise ValueError(f"The toggle bit must be set in the first frame of the transfer")

    def compile(self) -> DataFrame:
        tail = self.transfer_id % TRANSFER_ID_MODULO
        if self.start_of_transfer:
            tail |= 1 << 7
        if self.end_of_transfer:
            tail |= 1 << 6
        if self.toggle_bit:
            tail |= 1 << 5

        data = bytearray(self.padded_payload)
        data.append(tail)
        return DataFrame(FrameFormat.EXTENDED, self.identifier, data)

    @staticmethod
    def parse(source: DataFrame) -> typing.Optional[UAVCANFrame]:
        if source.format != FrameFormat.EXTENDED:
            return None
        if len(source.data) < 1:
            return None

        padded_payload, tail = memoryview(source.data)[:-1], source.data[-1]
        transfer_id = tail & (TRANSFER_ID_MODULO - 1)
        sot, eot, tog = tuple(tail & (1 << x) != 0 for x in (7, 6, 5))
        if sot and not tog:
            return None

        return UAVCANFrame(
            identifier=source.identifier,
            transfer_id=transfer_id,
            start_of_transfer=sot,
            end_of_transfer=eot,
            toggle_bit=tog,
            padded_payload=padded_payload,
        )

    @staticmethod
    def get_required_padding(data_length: int) -> int:
        return DataFrame.get_required_padding(data_length + 1)  # +1 for the tail byte

    def __repr__(self) -> str:
        kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        kwargs["identifier"] = f"0x{self.identifier:08x}"
        kwargs["padded_payload"] = bytes(self.padded_payload).hex()
        return pyuavcan.util.repr_attributes(self, **kwargs)


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


def _unittest_can_uavcan_frame() -> None:
    from pytest import raises

    UAVCANFrame(123, 123, True, False, True, memoryview(b""))
    UAVCANFrame(123, 123, False, False, True, memoryview(b""))
    UAVCANFrame(123, 123, False, False, False, memoryview(b""))

    with raises(ValueError):
        UAVCANFrame(123, -1, True, False, True, memoryview(b""))

    with raises(ValueError):
        UAVCANFrame(123, 123, True, False, False, memoryview(b""))

    ref = UAVCANFrame(
        identifier=0,
        transfer_id=0,
        start_of_transfer=False,
        end_of_transfer=False,
        toggle_bit=False,
        padded_payload=memoryview(b""),
    )
    assert ref == UAVCANFrame.parse(DataFrame(FrameFormat.EXTENDED, 0, bytearray(b"\x00")))

    ref = UAVCANFrame(
        identifier=123456,
        transfer_id=12,
        start_of_transfer=True,
        end_of_transfer=False,
        toggle_bit=True,
        padded_payload=memoryview(b"Hello"),
    )
    assert ref == UAVCANFrame.parse(DataFrame(FrameFormat.EXTENDED, 123456, bytearray(b"Hello\xAC")))

    ref = UAVCANFrame(
        identifier=1234567,
        transfer_id=12,
        start_of_transfer=False,
        end_of_transfer=True,
        toggle_bit=True,
        padded_payload=memoryview(b"Hello"),
    )
    assert ref == UAVCANFrame.parse(DataFrame(FrameFormat.EXTENDED, 1234567, bytearray(b"Hello\x6C")))

    assert UAVCANFrame.parse(DataFrame(FrameFormat.EXTENDED, 1234567, bytearray(b"Hello\xCC"))) is None  # Bad toggle

    assert UAVCANFrame.parse(DataFrame(FrameFormat.EXTENDED, 1234567, bytearray(b""))) is None  # No tail byte

    assert UAVCANFrame.parse(DataFrame(FrameFormat.BASE, 123, bytearray(b"Hello\x6C"))) is None  # Bad frame format
