from __future__ import annotations

import pytest

from pycyphal2.can import Frame
from pycyphal2.can._slcan import SLCANParser, encode_frame


def test_encode_classic_extended_frames() -> None:
    assert encode_frame(0x123, b"") == b"T000001230\r"
    assert encode_frame(0x1BADC0DE, b"\x01\xAB") == b"T1BADC0DE201AB\r"
    assert encode_frame(0x1FFFFFFF, bytes(range(8))) == b"T1FFFFFFF80001020304050607\r"


def test_encode_validation() -> None:
    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        encode_frame(-1, b"")

    with pytest.raises(ValueError, match="Invalid CAN data length"):
        encode_frame(0x123, bytes(range(9)))


def test_parse_classic_extended_frames() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000001232ABCD\r") == [Frame(id=0x123, data=b"\xAB\xCD")]
    assert parser.feed(b"T000001") == []
    assert parser.feed(b"230\r") == [Frame(id=0x123, data=b"")]
    assert parser.feed(b"x1BADC0DE201AB\r") == [Frame(id=0x1BADC0DE, data=b"\x01\xAB")]


def test_parse_classic_extended_frames_with_timestamp_suffix() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000001232ABCD1234\r") == [Frame(id=0x123, data=b"\xAB\xCD")]
    assert parser.feed(b"T10AE6EFF8000000FF000000A07071\r") == [
        Frame(id=0x10AE6EFF, data=b"\x00\x00\x00\xFF\x00\x00\x00\xA0"),
    ]
    assert parser.feed(b"T000001232ABCDzzzz\r") == []


def test_parse_multiple_frames_and_newlines() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000000010\rT1FFFFFFF1AA\r\n") == [
        Frame(id=1, data=b""),
        Frame(id=0x1FFFFFFF, data=b"\xAA"),
    ]


def test_parse_drops_unsupported_frame_types() -> None:
    parser = SLCANParser()

    assert parser.feed(b"t1231AA\rr1231\rR000001231\rT00000123155\r") == [Frame(id=0x123, data=b"\x55")]


def test_parse_drops_malformed_input_without_raising() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T00000123XAA\r") == []
    assert parser.feed(b"T000001232AA\r") == []
    assert parser.feed(b"T000001232AABBCC\r") == []
    assert parser.feed(b"T000001232AAGG\r") == []
    assert parser.feed(b"TFFFFFFFF0\r") == []
    assert parser.feed(b"V0102\rN1234\rT00000123155\r") == [Frame(id=0x123, data=b"\x55")]


def test_parser_bounds_overlong_input() -> None:
    parser = SLCANParser(max_line_length=10)

    assert parser.feed(b"T00000123155") == []
    assert parser.feed(b"\rT000001230\r") == [Frame(id=0x123, data=b"")]


def test_parser_bel_drops_buffered_error_response() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T00000123155\aT00000123166\r") == [Frame(id=0x123, data=b"\x66")]


def test_parser_rejects_invalid_buffer_limit() -> None:
    with pytest.raises(ValueError, match="Invalid maximum SLCAN line length"):
        SLCANParser(max_line_length=9)
