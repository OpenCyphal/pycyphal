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


class FrameFormat(enum.IntEnum):
    BASE = 11
    EXTENDED = 29


@dataclasses.dataclass(frozen=True)
class DataFrame:
    identifier: int
    data:       bytearray
    format:     FrameFormat
    loopback:   bool
    """Loopback request for outgoing frames; loopback indicator for received frames."""

    def __post_init__(self) -> None:
        assert isinstance(self.format, FrameFormat)
        if not (0 <= self.identifier < 2 ** int(self.format)):
            raise ValueError(f'Invalid CAN ID for format {self.format}: {self.identifier}')

        if len(self.data) not in _LENGTH_TO_DLC:
            raise ValueError(f'Unsupported data length: {len(self.data)}')

    @property
    def dlc(self) -> int:
        """Not to be confused with ``len(data)``."""
        return _LENGTH_TO_DLC[len(self.data)]       # The length is checked at the time of construction

    @staticmethod
    def convert_dlc_to_length(dlc: int) -> int:
        try:
            return _DLC_TO_LENGTH[dlc]
        except LookupError:
            raise ValueError(f'{dlc} is not a valid DLC') from None

    @staticmethod
    def get_required_padding(data_length: int) -> int:
        """
        Computes padding for nearest valid CAN FD frame size.

        >>> DataFrame.get_required_padding(6)
        0
        >>> DataFrame.get_required_padding(61)
        3
        """
        supremum = next(x for x in _DLC_TO_LENGTH if x >= data_length)  # pragma: no branch
        assert supremum >= data_length
        return supremum - data_length

    def is_same_manifestation(self, other: DataFrame) -> bool:
        """
        Compares two frames ignoring the information that is not representable on the physical layer.
        That means that only the CAN ID, data, and the format are compared.
        This can be used to ensure equality ignoring timestamps, loopback, and other properties that are
        not representable outside of this model.
        """
        return self.identifier == other.identifier \
            and self.data == other.data \
            and self.format == other.format

    def __str__(self) -> str:
        ide = {
            FrameFormat.EXTENDED: '0x%08x',
            FrameFormat.BASE: '0x%03x',
        }[self.format] % self.identifier
        data_hex = ' '.join(map('{:02x}'.format, self.data))
        data_ascii = ''.join((chr(x) if 32 <= x <= 126 else '.') for x in self.data)
        out = f"{ide}  {data_hex}  '{data_ascii}'{'  loopback' if self.loopback else ''}"
        return out


@dataclasses.dataclass(frozen=True)
class TimestampedDataFrame(DataFrame):
    timestamp: pyuavcan.transport.Timestamp

    def __str__(self) -> str:
        return f'{self.timestamp}: {super(TimestampedDataFrame, self).__str__()}'


_DLC_TO_LENGTH = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]
_LENGTH_TO_DLC: typing.Dict[int, int] = dict(zip(*list(zip(*enumerate(_DLC_TO_LENGTH)))[::-1]))  # type: ignore
assert len(_LENGTH_TO_DLC) == 16 == len(_DLC_TO_LENGTH)
for item in _DLC_TO_LENGTH:
    assert _DLC_TO_LENGTH[_LENGTH_TO_DLC[item]] == item, 'Invalid DLC tables'


def _unittest_can_media_frame() -> None:
    import re
    from pytest import raises

    assert str(DataFrame(0, bytearray(), FrameFormat.BASE, False)) == "0x000    ''"

    assert str(DataFrame(0x12345678, bytearray(b'Hello\x01\x02\x7F'), FrameFormat.EXTENDED, True)) == \
        "0x12345678  48 65 6c 6c 6f 01 02 7f  'Hello...'  loopback"

    assert re.match(
        r"2019-05-2\dT\d\d:\d\d:\d\d.502003/635720.258263416: "
        r"0x12345678 {2}48 65 6c 6c 6f 01 02 7f {2}'Hello...' {2}loopback",
        str(TimestampedDataFrame(0x12345678,
                                 bytearray(b'Hello\x01\x02\x7F'),
                                 FrameFormat.EXTENDED,
                                 True,
                                 pyuavcan.transport.Timestamp(system_ns=1558481132_502003000,
                                                              monotonic_ns=635720258263416))))

    assert DataFrame(123, bytearray(b'abc'), FrameFormat.EXTENDED, loopback=True).is_same_manifestation(
        TimestampedDataFrame(123, bytearray(b'abc'), FrameFormat.EXTENDED, loopback=False,
                             timestamp=pyuavcan.transport.Timestamp.now())
    )

    for fmt in FrameFormat:
        with raises(ValueError):
            DataFrame(-1, bytearray(), fmt, False)

        with raises(ValueError):
            DataFrame(2 ** int(fmt), bytearray(), fmt, True)

    with raises(ValueError):
        DataFrame(123, bytearray(b'a' * 9), FrameFormat.EXTENDED, True)

    with raises(ValueError):
        DataFrame.convert_dlc_to_length(16)

    for sz in range(100):
        try:
            f = DataFrame(123, bytearray(b'a' * sz), FrameFormat.EXTENDED, True)
        except ValueError:
            pass
        else:
            assert f.convert_dlc_to_length(f.dlc) == sz
