#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import decimal
import datetime


_AnyScalar = typing.Union[float, int, decimal.Decimal]

_DECIMAL_NANO = decimal.Decimal('1e-9')


class Timestamp:
    """
    Timestamp objects are immutable in order to allow them to be hashable.
    """

    def __init__(self, wall_ns: int, monotonic_ns: int) -> None:
        """
        :param wall_ns:         Belongs to the domain of time.time_ns().
        :param monotonic_ns:    Belongs to the domain of time.monotonic_ns().
        """
        self._wall_ns = int(wall_ns)
        self._monotonic_ns = int(monotonic_ns)

        if self._wall_ns < 0 or self._monotonic_ns < 0:
            raise ValueError(f'Neither of the timestamp samples can be negative; found this: {self!r}')

    @staticmethod
    def from_second(wall: _AnyScalar, monotonic: _AnyScalar) -> Timestamp:
        return Timestamp(wall_ns=Timestamp._second_to_ns(wall),
                         monotonic_ns=Timestamp._second_to_ns(monotonic))

    @staticmethod
    def now() -> Timestamp:
        """Warning: clocks are sampled non-atomically! Monotonic sampled first."""
        return Timestamp(monotonic_ns=time.monotonic_ns(), wall_ns=time.time_ns())

    @property
    def wall(self) -> decimal.Decimal:
        return self._ns_to_second(self._wall_ns)

    @property
    def monotonic(self) -> decimal.Decimal:
        return self._ns_to_second(self._monotonic_ns)

    @property
    def wall_ns(self) -> int:
        return self._wall_ns

    @property
    def monotonic_ns(self) -> int:
        return self._monotonic_ns

    @staticmethod
    def _second_to_ns(x: _AnyScalar) -> int:
        return int(decimal.Decimal(x) / _DECIMAL_NANO)

    @staticmethod
    def _ns_to_second(x: int) -> decimal.Decimal:
        return decimal.Decimal(x) * _DECIMAL_NANO

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, Timestamp):
            return self._wall_ns == other._wall_ns and self._monotonic_ns == other._monotonic_ns
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self._wall_ns + self._monotonic_ns)

    def __str__(self) -> str:
        dt = datetime.datetime.fromtimestamp(float(self.wall))  # Precision loss is acceptable - wall time is imprecise
        iso = dt.isoformat(timespec='microseconds')
        return f'{iso}/{self.monotonic:.9f}'

    def __repr__(self) -> str:
        return f'{type(self).__name__}(wall_ns={self._wall_ns}, monotonic_ns={self._monotonic_ns})'
