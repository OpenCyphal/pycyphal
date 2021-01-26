# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import enum
import typing
import dataclasses
import pyuavcan


class FrameFormat(enum.IntEnum):
    BASE = 11
    EXTENDED = 29


@dataclasses.dataclass(frozen=True)
class DataFrame:
    format: FrameFormat
    identifier: int
    data: bytearray

    def __post_init__(self) -> None:
        assert isinstance(self.format, FrameFormat)
        if not (0 <= self.identifier < 2 ** int(self.format)):
            raise ValueError(f"Invalid CAN ID for format {self.format}: {self.identifier}")

        if len(self.data) not in _LENGTH_TO_DLC:
            raise ValueError(f"Unsupported data length: {len(self.data)}")

    @property
    def dlc(self) -> int:
        """Not to be confused with ``len(data)``."""
        return _LENGTH_TO_DLC[len(self.data)]  # The length is checked at the time of construction

    @staticmethod
    def convert_dlc_to_length(dlc: int) -> int:
        try:
            return _DLC_TO_LENGTH[dlc]
        except LookupError:
            raise ValueError(f"{dlc} is not a valid DLC") from None

    @staticmethod
    def get_required_padding(data_length: int) -> int:
        """
        Computes padding to nearest valid CAN FD frame size.

        >>> DataFrame.get_required_padding(6)
        0
        >>> DataFrame.get_required_padding(61)
        3
        """
        supremum = next(x for x in _DLC_TO_LENGTH if x >= data_length)  # pragma: no branch
        assert supremum >= data_length
        return supremum - data_length

    def __repr__(self) -> str:
        ide = {
            FrameFormat.EXTENDED: "0x%08x",
            FrameFormat.BASE: "0x%03x",
        }[self.format] % self.identifier
        return pyuavcan.util.repr_attributes(self, id=ide, data=self.data.hex())


@dataclasses.dataclass(frozen=True)
class Envelope:
    """
    The envelope models a singular input/output frame transaction.
    It is a media layer frame extended with IO-related metadata.
    """

    frame: DataFrame
    loopback: bool
    """Loopback request for outgoing frames; loopback indicator for received frames."""


_DLC_TO_LENGTH = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]
_LENGTH_TO_DLC: typing.Dict[int, int] = dict(zip(*list(zip(*enumerate(_DLC_TO_LENGTH)))[::-1]))  # type: ignore
assert len(_LENGTH_TO_DLC) == 16 == len(_DLC_TO_LENGTH)
for item in _DLC_TO_LENGTH:
    assert _DLC_TO_LENGTH[_LENGTH_TO_DLC[item]] == item, "Invalid DLC tables"


def _unittest_can_media_frame() -> None:
    from pytest import raises

    for fmt in FrameFormat:
        with raises(ValueError):
            DataFrame(fmt, -1, bytearray())

        with raises(ValueError):
            DataFrame(fmt, 2 ** int(fmt), bytearray())

    with raises(ValueError):
        DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"a" * 9))

    with raises(ValueError):
        DataFrame.convert_dlc_to_length(16)

    for sz in range(100):
        try:
            f = DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"a" * sz))
        except ValueError:
            pass
        else:
            assert f.convert_dlc_to_length(f.dlc) == sz
