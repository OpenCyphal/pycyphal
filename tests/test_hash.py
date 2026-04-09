"""Tests for transport-agnostic hash and CRC helpers."""

from __future__ import annotations

from pycyphal2._hash import (
    CRC32C_INITIAL,
    CRC32C_OUTPUT_XOR,
    CRC32C_RESIDUE,
    CRC16CCITT_FALSE_INITIAL,
    CRC16CCITT_FALSE_RESIDUE,
    crc32c_add,
    crc32c_full,
    crc16ccitt_false_add,
    crc16ccitt_false_full,
    rapidhash,
)


class TestCRC32C:
    def test_known_vector(self) -> None:
        assert crc32c_full(b"123456789") == 0xE3069283

    def test_empty(self) -> None:
        assert crc32c_full(b"") == 0x00000000

    def test_single_byte(self) -> None:
        assert crc32c_full(b"\x00") != 0
        assert isinstance(crc32c_full(b"\xff"), int)

    def test_residue_property(self) -> None:
        for data in (b"hello", b"", b"123456789", bytes(range(256))):
            crc = crc32c_full(data)
            assert crc32c_full(data + crc.to_bytes(4, "little")) == CRC32C_RESIDUE

    def test_incremental_matches_full(self) -> None:
        data = bytes(range(100))
        crc = crc32c_add(CRC32C_INITIAL, data[:37])
        crc = crc32c_add(crc, data[37:])
        assert (crc ^ CRC32C_OUTPUT_XOR) == crc32c_full(data)

    def test_memoryview(self) -> None:
        data = b"test data"
        assert crc32c_full(memoryview(data)) == crc32c_full(data)


class TestCRC16CCITTFALSE:
    def test_reference_vectors(self) -> None:
        assert crc16ccitt_false_full(b"") == 0xFFFF
        assert crc16ccitt_false_full(b"\x00") == 0xE1F0
        assert crc16ccitt_false_full(b"\xff") == 0xFF00
        assert crc16ccitt_false_full(b"A") == 0xB915
        assert crc16ccitt_false_full(b"123456789") == 0x29B1
        assert crc16ccitt_false_full(bytes(8)) == 0x313E
        assert crc16ccitt_false_full(b"\xff" * 8) == 0x97DF

    def test_incremental_matches_full(self) -> None:
        data = b"123456789"
        crc = CRC16CCITT_FALSE_INITIAL
        for b in data:
            crc = crc16ccitt_false_add(crc, bytes([b]))
        assert crc == crc16ccitt_false_full(data) == 0x29B1

    def test_two_chunk_matches_full(self) -> None:
        data = b"123456789"
        crc = crc16ccitt_false_add(CRC16CCITT_FALSE_INITIAL, data[:5])
        crc = crc16ccitt_false_add(crc, data[5:])
        assert crc == crc16ccitt_false_full(data) == 0x29B1

    def test_empty_input_is_identity(self) -> None:
        assert crc16ccitt_false_add(CRC16CCITT_FALSE_INITIAL, b"") == CRC16CCITT_FALSE_INITIAL
        assert crc16ccitt_false_add(0x1234, b"") == 0x1234

    def test_residue_property(self) -> None:
        for data in (b"Hello", b"123456789"):
            crc = crc16ccitt_false_full(data)
            augmented = data + crc.to_bytes(2, "big")
            assert crc16ccitt_false_full(augmented) == CRC16CCITT_FALSE_RESIDUE

    def test_memoryview(self) -> None:
        data = b"test data"
        assert crc16ccitt_false_full(memoryview(data)) == crc16ccitt_false_full(data)


class TestRapidHash:
    def test_golden_vectors(self) -> None:
        vectors = (
            (b"", 0x0338DC4BE2CECDAE),
            (b"x", 0x8C7DB958EB96E161),
            (b"abc", 0xCB475BEAFA9C0DA2),
            (b"hello", 0x2E2D7651B45F7946),
            (b"123456789", 0x7E7D033B96B916A1),
            (b"abcdefgh", 0xAB159E602A29F41F),
            (b"abcdefghijklmnop", 0xC78AE6A1774ADB1E),
            (b"abcdefghijklmnopq", 0x00C427C11A4463B8),
            (b"L" * 113, 0x0C2659AF62C90310),
            (b"P" * 1000, 0xE35E3294ED93C8DE),
            (b"the quick brown fox jumps over the lazy dog", 0x55889A01CA56B226),
        )
        for data, expected in vectors:
            assert rapidhash(data) == expected

    def test_string_matches_bytes(self) -> None:
        assert rapidhash("topic/name") == rapidhash(b"topic/name") == 0xF6145099F88B80BF

    def test_distinct_inputs_hash_differently(self) -> None:
        assert rapidhash(b"topic") != rapidhash(b"topic/")
