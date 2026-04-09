from __future__ import annotations

from typing import cast

import pytest

from pycyphal2 import Instant
from pycyphal2.can import Filter, Frame, TimestampedFrame
from pycyphal2.can._interface import _CAN_EXT_ID_MASK


def test_frame_validation_and_normalization() -> None:
    assert Frame(id=123, data=cast(bytes, bytearray(b"ab"))).data == b"ab"
    assert TimestampedFrame(id=456, data=cast(bytes, memoryview(b"cd")), timestamp=Instant(ns=1)).data == b"cd"

    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        Frame(id=-1, data=b"")

    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        Frame(id="bad", data=b"")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        Frame(id=_CAN_EXT_ID_MASK + 1, data=b"")

    with pytest.raises(ValueError, match="Invalid CAN data length"):
        Frame(id=1, data=bytes(65))


def test_filter_validation_and_helpers() -> None:
    assert Filter.promiscuous() == Filter(id=0, mask=0)
    assert Filter(id=0b1010, mask=0b1111).rank == 4
    assert Filter(id=0b1010, mask=0b1111).merge(Filter(id=0b1000, mask=0b1111)) == Filter(id=0b1000, mask=0b1101)

    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        Filter(id=-1, mask=0)

    with pytest.raises(ValueError, match="Invalid CAN mask"):
        Filter(id=0, mask=_CAN_EXT_ID_MASK + 1)

    with pytest.raises(ValueError, match="target number of filters must be positive"):
        Filter.coalesce([], 0)


def test_filter_coalesce_reference_semantics() -> None:
    identical = [Filter(id=0x123, mask=0x1FFFFFFF), Filter(id=0x123, mask=0x1FFFFFFF)]
    assert Filter.coalesce(identical, 1) == [Filter(id=0x123, mask=0x1FFFFFFF)]

    filters = [
        Filter(id=0b0000, mask=0b1111),
        Filter(id=0b0001, mask=0b1111),
        Filter(id=0b0011, mask=0b1111),
    ]
    fused = Filter.coalesce(filters, 2)
    assert len(fused) == 2
    assert all(isinstance(item, Filter) for item in fused)

    wildcard = [Filter.promiscuous(), Filter(id=0x456, mask=0x1FFFFFFF)]
    assert Filter.coalesce(wildcard, 1) == [Filter.promiscuous()]
