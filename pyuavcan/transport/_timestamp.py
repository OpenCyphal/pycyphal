# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import time
import typing
import decimal
import datetime


_AnyScalar = typing.Union[float, int, decimal.Decimal]

_DECIMAL_NANO = decimal.Decimal("1e-9")


class Timestamp:
    """
    Timestamps are hashable and immutable.
    Timestamps can be compared for exact equality; relational comparison operators are not defined.

    A timestamp instance always contains a pair of time samples:
    the *system time*, also known as "wall time" or local civil time,
    and the monotonic time, which is used only for time interval measurement.
    """

    def __init__(self, system_ns: int, monotonic_ns: int) -> None:
        """
        Manual construction is rarely needed, except when implementing network drivers.
        See the static factory methods.

        :param system_ns:       Belongs to the domain of :func:`time.time_ns`. Units are nanoseconds.
        :param monotonic_ns:    Belongs to the domain of :func:`time.monotonic_ns`. Units are nanoseconds.
        """
        self._system_ns = int(system_ns)
        self._monotonic_ns = int(monotonic_ns)

        if self._system_ns < 0 or self._monotonic_ns < 0:
            raise ValueError(f"Neither of the timestamp samples can be negative; found this: {self!r}")

    @staticmethod
    def from_seconds(system: _AnyScalar, monotonic: _AnyScalar) -> Timestamp:
        """
        Both inputs are in seconds (not nanoseconds) of any numerical type.
        """
        return Timestamp(system_ns=Timestamp._second_to_ns(system), monotonic_ns=Timestamp._second_to_ns(monotonic))

    @staticmethod
    def now() -> Timestamp:
        """
        Constructs a new timestamp instance populated with current time.

        .. important:: Clocks are sampled non-atomically! Monotonic sampled first.
        """
        return Timestamp(monotonic_ns=time.monotonic_ns(), system_ns=time.time_ns())

    @staticmethod
    def combine_oldest(*arguments: Timestamp) -> Timestamp:
        """
        Picks lowest time values from the provided set of timestamps and constructs a new instance from those.

        This can be useful for transfer reception logic where the oldest frame timestamp is used as the
        transfer timestamp for multi-frame transfers to reduce possible timestamping error variation
        introduced in the media layer.

        >>> Timestamp.combine_oldest(
        ...     Timestamp(12345, 45600),
        ...     Timestamp(12300, 45699),
        ...     Timestamp(12399, 45678),
        ... )
        Timestamp(system_ns=12300, monotonic_ns=45600)
        """
        return Timestamp(
            system_ns=min(x.system_ns for x in arguments), monotonic_ns=min(x.monotonic_ns for x in arguments)
        )

    @property
    def system(self) -> decimal.Decimal:
        """System time in seconds."""
        return self._ns_to_second(self._system_ns)

    @property
    def monotonic(self) -> decimal.Decimal:
        """Monotonic time in seconds."""
        return self._ns_to_second(self._monotonic_ns)

    @property
    def system_ns(self) -> int:
        return self._system_ns

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
        """
        Performs an exact comparison of the timestamp components with nanosecond resolution.
        """
        if isinstance(other, Timestamp):
            return self._system_ns == other._system_ns and self._monotonic_ns == other._monotonic_ns
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._system_ns + self._monotonic_ns)

    def __str__(self) -> str:
        dt = datetime.datetime.fromtimestamp(float(self.system))  # Precision loss is OK - system time is imprecise
        iso = dt.isoformat(timespec="microseconds")
        return f"{iso}/{self.monotonic:.6f}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(system_ns={self._system_ns}, monotonic_ns={self._monotonic_ns})"


def _unittest_timestamp() -> None:
    from pytest import raises
    from decimal import Decimal

    Timestamp(0, 0)

    with raises(ValueError):
        Timestamp(-1, 0)

    with raises(ValueError):
        Timestamp(0, -1)

    ts = Timestamp.from_seconds(Decimal("5.123456789"), Decimal("123.456789"))
    assert ts.system_ns == 5123456789
    assert ts.monotonic_ns == 123456789000
    assert ts.system == Decimal("5.123456789")
    assert ts.monotonic == Decimal("123.456789")
    assert hash(ts) == hash(Timestamp(5123456789, 123456789000))
    assert hash(ts) != hash(Timestamp(123, 456))
    assert ts == Timestamp(5123456789, 123456789000)
    assert ts != Timestamp(123, 123456789000)
    assert ts != Timestamp(5123456789, 456)
    assert ts != "Hello"
    assert Timestamp.combine_oldest(Timestamp(123, 123456789000), Timestamp(5123456789, 456), ts) == Timestamp(123, 456)
    print(ts)
