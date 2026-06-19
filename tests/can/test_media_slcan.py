from __future__ import annotations

import pytest

from pycyphal2.can import Frame
from pycyphal2.can._media_slcan import (
    SLCANParser,
    classify_init_response,
    encode_deinit,
    encode_frame,
    encode_init_sequence,
)


def test_encode_classic_extended_frames() -> None:
    assert encode_frame(0x123, b"") == b"T000001230\r"
    assert encode_frame(0x1BADC0DE, b"\x01\xab") == b"T1BADC0DE201AB\r"
    assert encode_frame(0x1FFFFFFF, bytes(range(8))) == b"T1FFFFFFF80001020304050607\r"


def test_encode_validation() -> None:
    with pytest.raises(ValueError, match="Invalid CAN identifier"):
        encode_frame(-1, b"")

    with pytest.raises(ValueError, match="Invalid CAN data length"):
        encode_frame(0x123, bytes(range(9)))


def test_encode_deinit() -> None:
    assert encode_deinit() == b"C\r"


def test_encode_init_sequence() -> None:
    assert encode_init_sequence(None) == [b"O\r"]  # No bitrate is guessed.
    assert encode_init_sequence(1_000_000) == [b"S8\r", b"O\r"]
    assert encode_init_sequence(250_000) == [b"S5\r", b"O\r"]
    assert encode_init_sequence(10_000) == [b"S0\r", b"O\r"]
    # Non-standard bitrate is passed through as-is rather than rejected.
    assert encode_init_sequence(123_456) == [b"S123456\r", b"O\r"]


def test_classify_init_response() -> None:
    assert classify_init_response(b"\r") is True
    assert classify_init_response(b"\x07") is False
    assert classify_init_response(b"xyz\rmore") is True  # First ACK wins.
    assert classify_init_response(b"xyz\x07") is False
    assert classify_init_response(b"") is None
    assert classify_init_response(b"junk") is None


def test_parse_classic_extended_frames() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000001232ABCD\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"T000001232abCd\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"t1231AA\r") == [Frame(id=0x123, data=b"\xaa")]
    assert parser.feed(b"t7FF1AA\r") == [Frame(id=0x7FF, data=b"\xaa")]
    assert parser.feed(b"t7FF0\r") == [Frame(id=0x7FF, data=b"")]
    assert parser.feed(b"T000001") == []
    assert parser.feed(b"230\r") == [Frame(id=0x123, data=b"")]
    assert parser.feed(b"x1BADC0DE201AB\r") == [Frame(id=0x1BADC0DE, data=b"\x01\xab")]


def test_parse_ignores_optional_frame_suffix() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000001232ABCD1234\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"T000001232ABCDL\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"T000001232ABCD1234L\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"T000001232ABCDzzzz\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"t1231AAL\r") == [Frame(id=0x123, data=b"\xaa")]
    assert parser.feed(b"T000001232ABCD1234\x03\r") == [Frame(id=0x123, data=b"\xab\xcd")]
    assert parser.feed(b"T10AE6EFF8000000FF000000A07071\r") == [
        Frame(id=0x10AE6EFF, data=b"\x00\x00\x00\xff\x00\x00\x00\xa0"),
    ]
    assert parser.feed(b"T10AE6EFF8000000FF000000A07071Lvendor\r") == [
        Frame(id=0x10AE6EFF, data=b"\x00\x00\x00\xff\x00\x00\x00\xa0"),
    ]


def test_parse_multiple_frames_and_newlines() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T000000010\rT1FFFFFFF1AA\r\n") == [
        Frame(id=1, data=b""),
        Frame(id=0x1FFFFFFF, data=b"\xaa"),
    ]


def test_parse_fd_and_drops_unsupported_frame_types() -> None:
    parser = SLCANParser()

    assert parser.feed(b"D000001239000102030405060708090A0B\r") == [Frame(id=0x123, data=bytes(range(12)))]
    assert parser.feed(b"r1231\rR000001231\rR1234f00d8\rr008\rT00000123155\r") == [Frame(id=0x123, data=b"\x55")]


def test_parse_ignores_adapter_commands_and_status_blocks() -> None:
    parser = SLCANParser()

    assert parser.feed(
        b"\r\aS8\rO\rL\rl\rC\rMFFFFFFFF\rm123\rU1\rZ1\rF20\rV0102\r"
        b"N00112233445566778899AABBCCDDEEFF\rT00000123155\r"
    ) == [Frame(id=0x123, data=b"\x55")]


def test_parse_drops_malformed_input_without_raising() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T00000123XAA\r") == []
    assert parser.feed(b"T000001232AA\r") == []
    assert parser.feed(b"T000001232AABBCC\r") == [Frame(id=0x123, data=b"\xaa\xbb")]
    assert parser.feed(b"T000001232AAGG\r") == []
    assert parser.feed(b"TFFFFFFFF0\r") == []
    assert parser.feed(b"tABC0\r") == []
    assert parser.feed(b"V0102\rN1234\rT00000123155\r") == [Frame(id=0x123, data=b"\x55")]


def test_parser_bounds_overlong_input() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T" + b"0" * 300) == []  # Exceeds the fixed line-length bound, dropped.
    assert parser.feed(b"\rT000001230\r") == [Frame(id=0x123, data=b"")]


def test_parser_bel_drops_buffered_error_response() -> None:
    parser = SLCANParser()

    assert parser.feed(b"T00000123155\aT00000123166\r") == [Frame(id=0x123, data=b"\x66")]
