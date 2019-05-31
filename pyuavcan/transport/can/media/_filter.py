#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import itertools
import dataclasses
from . import _frame


@dataclasses.dataclass(frozen=True)
class FilterConfiguration:
    identifier: int
    mask:       int
    format:     typing.Optional[_frame.FrameFormat]  # None means no preference

    def __post_init__(self) -> None:
        max_bit_length = 2 ** self.identifier_bit_length - 1
        if not (0 <= self.identifier <= max_bit_length):
            raise ValueError(f'Invalid identifier: {self.identifier}')
        if not (0 <= self.mask <= max_bit_length):
            raise ValueError(f'Invalid mask: {self.mask}')

    @property
    def identifier_bit_length(self) -> int:
        # noinspection PyTypeChecker
        return int(self.format if self.format is not None else max(_frame.FrameFormat))

    @staticmethod
    def new_promiscuous() -> FilterConfiguration:
        """Returns a configuration that accepts all frames."""
        return FilterConfiguration(identifier=0, mask=0, format=None)

    @property
    def rank(self) -> int:
        """
        This is a part of the CAN acceptance filter configuration optimization algorithm.
        Observe that we return negative rank for configurations which do not distinguish between extended and
        base frames in order to discourage merger of configurations of different frame types, since they are
        hard to support in certain CAN controllers. The effect of this is that we guarantee that an ambivalent
        filter configuration will never appear if the controller has at least one acceptance filter.
        """
        mask_mask = 2 ** self.identifier_bit_length - 1
        rank = bin(self.mask & mask_mask).count('1')
        if self.format is None:
            rank -= int(self.identifier_bit_length)       # Discourage merger of ambivalent filters.
        return rank

    def merge(self, other: FilterConfiguration) -> FilterConfiguration:
        """
        This is a part of the CAN acceptance filter configuration optimization algorithm.
        """
        mask = self.mask & other.mask & ~(self.identifier ^ other.identifier)
        identifier = self.identifier & mask
        fmt = self.format if self.format == other.format else None
        return FilterConfiguration(identifier=identifier, mask=mask, format=fmt)

    def __str__(self) -> str:
        out = ''.join(
            (str((self.identifier >> bit) & 1) if self.mask & (1 << bit) != 0 else 'x')
            for bit in reversed(range(int(self.format or _frame.FrameFormat.EXTENDED)))
        )
        return (self.format.name[:3].lower() if self.format else "any") + ':' + out


def optimize_filter_configurations(configurations: typing.Iterable[FilterConfiguration],
                                   target_number_of_configurations: int) -> typing.List[FilterConfiguration]:
    """
    Implements the CAN acceptance filter configuration optimization algorithm described in the Specification
    (originally proposed by P. Kirienko and I. Sheremet).
    """
    if target_number_of_configurations < 1:
        raise ValueError(f'The number of configurations must be positive; found {target_number_of_configurations}')

    configurations = list(configurations)
    while len(configurations) > target_number_of_configurations:
        options = itertools.starmap(lambda ia, ib: (ia[0], ib[0], ia[1].merge(ib[1])),
                                    itertools.permutations(enumerate(configurations), 2))
        index_replace, index_remove, merged = max(options, key=lambda x: x[2].rank)
        configurations[index_replace] = merged
        del configurations[index_remove]  # Invalidates indexes

    assert all(map(lambda x: isinstance(x, FilterConfiguration), configurations))
    return configurations


def _unittest_can_media_filter_faults() -> None:
    from ._frame import FrameFormat
    from pytest import raises

    with raises(ValueError):
        FilterConfiguration(0, -1, None)

    with raises(ValueError):
        FilterConfiguration(-1, 0, None)

    for fmt in FrameFormat:
        with raises(ValueError):
            FilterConfiguration(2 ** int(fmt), 0, fmt)

        with raises(ValueError):
            FilterConfiguration(0, 2 ** int(fmt), fmt)

    with raises(ValueError):
        optimize_filter_configurations([], 0)


# noinspection SpellCheckingInspection
def _unittest_can_media_filter_str() -> None:
    from ._frame import FrameFormat

    assert str(FilterConfiguration(0b10101010,
                                   0b11101000,
                                   FrameFormat.EXTENDED)) == 'ext:xxxxxxxxxxxxxxxxxxxxx101x1xxx'

    assert str(FilterConfiguration(0b10101010101010101010101010101,
                                   0b10111111111111111111111111111,
                                   FrameFormat.EXTENDED)) == 'ext:1x101010101010101010101010101'

    assert str(FilterConfiguration(0b10101010101, 0b11111111111, FrameFormat.BASE)) == 'bas:10101010101'

    assert str(FilterConfiguration(123, 456, None)) == 'any:xxxxxxxxxxxxxxxxxxxx001xx1xxx'

    assert str(FilterConfiguration.new_promiscuous()) == 'any:xxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    assert repr(FilterConfiguration(123, 456, None)) == 'FilterConfiguration(identifier=123, mask=456, format=None)'


def _unittest_can_media_filter_merge() -> None:
    from ._frame import FrameFormat

    assert FilterConfiguration(123456, 0, None).rank == -29         # Worst rank
    assert FilterConfiguration(123456, 0b110, None).rank == -27     # Two better

    assert FilterConfiguration(1234, 0b110, FrameFormat.BASE).rank == 2

    assert FilterConfiguration(0b111, 0b111, FrameFormat.EXTENDED).merge(
        FilterConfiguration(0b111, 0b111, FrameFormat.BASE)).rank == -29 + 3
