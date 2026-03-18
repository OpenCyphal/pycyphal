"""Tests for pycyphal._hash -- CRC-32C and rapidhash."""

from __future__ import annotations

import os

from pycyphal._hash import (
    CRC_INITIAL,
    CRC_OUTPUT_XOR,
    CRC_RESIDUE,
    _crc_add,
    crc32c,
    rapidhash,
)

# =====================================================================================================================
# CRC-32C Tests
# =====================================================================================================================


class TestCRC32C:
    def test_known_vector(self):
        """Standard CRC-32C test vector."""
        assert crc32c(b"123456789") == 0xE3069283

    def test_empty(self):
        assert crc32c(b"") == 0x00000000

    def test_single_byte(self):
        assert crc32c(b"\x00") != 0
        assert isinstance(crc32c(b"\xff"), int)

    def test_residue_property(self):
        """CRC of (data + CRC in LE) equals the residue constant."""
        for data in [b"hello", b"", b"123456789", os.urandom(256)]:
            c = crc32c(data)
            combined = data + c.to_bytes(4, "little")
            assert crc32c(combined) == CRC_RESIDUE, f"Residue check failed for data of len {len(data)}"

    def test_incremental(self):
        """_crc_add composes correctly: crc(a+b) == crc_add(crc_add(init, a), b) ^ xor."""
        data = os.urandom(100)
        full_crc = crc32c(data)
        split = 37
        state = _crc_add(CRC_INITIAL, data[:split])
        state = _crc_add(state, data[split:])
        assert (state ^ CRC_OUTPUT_XOR) == full_crc

    def test_memoryview(self):
        data = b"test data"
        assert crc32c(memoryview(data)) == crc32c(data)


# =====================================================================================================================
# rapidhash Tests
# =====================================================================================================================


class TestRapidhash:
    def test_empty(self):
        assert rapidhash(b"") == 232177599295442350

    def test_known_vectors(self):
        """Verify against known C reference outputs."""
        assert rapidhash(b"a") == 6457959414642172395
        assert rapidhash(b"ab") == 8872296267850602869
        assert rapidhash(b"abc") == 14647777377830833570
        assert rapidhash(b"hello") == 3327445792987248966
        assert rapidhash(b"pycyphal") == 3131592564933152817
        assert rapidhash(b"test") == 16388600957843709845

    def test_deterministic(self):
        data = b"deterministic"
        assert rapidhash(data) == rapidhash(data)

    def test_different_inputs_differ(self):
        assert rapidhash(b"foo") != rapidhash(b"bar")
