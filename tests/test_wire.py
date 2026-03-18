"""Comprehensive tests for the pycyphal._wire module."""

from __future__ import annotations

import math
import struct

import pytest
from pycyphal._hash import rapidhash

from pycyphal._wire import (
    HEADER_SIZE,
    LAGE_MAX,
    LAGE_MIN,
    SEQNO48_MASK,
    SUBJECT_ID_PINNED_MAX,
    HeaderType,
    broadcast_subject_id,
    gossip_shard_count,
    gossip_shard_subject_id,
    is_pinned,
    left_wins,
    log_age,
    pack_ack_header,
    pack_gossip_header,
    pack_msg_header,
    pack_rsp_ack_header,
    pack_rsp_header,
    pack_scout_header,
    subject_id_max,
    topic_hash,
    topic_subject_id,
    unpack_header,
)

# =====================================================================================================================
# Constants
# =====================================================================================================================


class TestConstants:
    def test_subject_id_pinned_max(self) -> None:
        assert SUBJECT_ID_PINNED_MAX == 8191
        assert SUBJECT_ID_PINNED_MAX == 0x1FFF

    def test_header_size(self) -> None:
        assert HEADER_SIZE == 24

    def test_seqno48_mask(self) -> None:
        assert SEQNO48_MASK == (1 << 48) - 1
        assert SEQNO48_MASK == 0xFFFF_FFFF_FFFF

    def test_lage_bounds(self) -> None:
        assert LAGE_MIN == -1
        assert LAGE_MAX == 35


# =====================================================================================================================
# HeaderType enum
# =====================================================================================================================


class TestHeaderType:
    def test_enum_values(self) -> None:
        assert HeaderType.MSG_BE == 0
        assert HeaderType.MSG_REL == 1
        assert HeaderType.MSG_ACK == 2
        assert HeaderType.MSG_NACK == 3
        assert HeaderType.RSP_BE == 4
        assert HeaderType.RSP_REL == 5
        assert HeaderType.RSP_ACK == 6
        assert HeaderType.RSP_NACK == 7
        assert HeaderType.GOSSIP == 8
        assert HeaderType.SCOUT == 9

    def test_all_ten_types(self) -> None:
        assert len(HeaderType) == 10

    def test_is_int_subclass(self) -> None:
        for ht in HeaderType:
            assert isinstance(ht, int)

    def test_contiguous_range(self) -> None:
        values = sorted(HeaderType)
        assert values == list(range(10))


# =====================================================================================================================
# topic_hash and hash-override parsing
# =====================================================================================================================


class TestTopicHash:
    """Cross-verify topic_hash against raw rapidhash for known strings."""

    def test_rapidhash_empty(self) -> None:
        assert topic_hash("") == rapidhash(b"")
        assert topic_hash("") == 232177599295442350

    def test_rapidhash_hello(self) -> None:
        assert topic_hash("hello") == rapidhash(b"hello")
        assert topic_hash("hello") == 3327445792987248966

    def test_rapidhash_single_char(self) -> None:
        assert topic_hash("a") == rapidhash(b"a")
        assert topic_hash("a") == 6457959414642172395

    def test_rapidhash_various_strings(self) -> None:
        cases = {
            "test": 16388600957843709845,
            "foo": 2360160339125950113,
            "bar": 12791351756655921208,
            "pycyphal": 3131592564933152817,
        }
        for name, expected in cases.items():
            assert topic_hash(name) == expected, f"Mismatch for {name!r}"

    def test_rapidhash_uavcan_topics(self) -> None:
        assert topic_hash("uavcan.node.Heartbeat.1.0") == 3897213858259282939
        assert topic_hash("uavcan.node.GetInfo.1.0") == 116166870293823273


class TestHashOverride:
    """Test the '#hexdigits' override syntax."""

    def test_simple_override(self) -> None:
        assert topic_hash("foo#1a2b") == 0x1A2B

    def test_override_zero(self) -> None:
        assert topic_hash("foo#0") == 0

    def test_override_single_digit(self) -> None:
        assert topic_hash("name#f") == 0xF

    def test_override_max_16_hex_digits(self) -> None:
        assert topic_hash("x#ffffffffffffffff") == 0xFFFFFFFFFFFFFFFF

    def test_override_16_digit_non_max(self) -> None:
        assert topic_hash("x#0000000000000001") == 1

    def test_override_too_many_digits_falls_through(self) -> None:
        """More than 16 hex digits -> not an override, uses rapidhash."""
        name = "x#00000000000000001"  # 17 hex digits
        assert topic_hash(name) == rapidhash(name.encode())

    def test_no_hash_sign(self) -> None:
        assert topic_hash("foobar") == rapidhash(b"foobar")

    def test_trailing_hash_no_digits(self) -> None:
        """'foo#' with nothing after '#' -> not an override."""
        name = "foo#"
        assert topic_hash(name) == rapidhash(name.encode())

    def test_uppercase_hex_not_recognized(self) -> None:
        """Only lowercase hex is accepted."""
        name = "foo#1A2B"
        assert topic_hash(name) == rapidhash(name.encode())

    def test_non_hex_chars_not_recognized(self) -> None:
        name = "foo#xyz"
        assert topic_hash(name) == rapidhash(name.encode())

    def test_override_pinned_value(self) -> None:
        assert topic_hash("pinned#0") == 0
        assert topic_hash("pinned#1fff") == 8191

    def test_multiple_hash_signs_uses_last(self) -> None:
        """rfind('#') means the last '#' is used for splitting."""
        assert topic_hash("a#b#ff") == 0xFF

    def test_hash_sign_at_start(self) -> None:
        assert topic_hash("#abc") == 0xABC


# =====================================================================================================================
# is_pinned
# =====================================================================================================================


class TestIsPinned:
    def test_zero_is_pinned(self) -> None:
        assert is_pinned(0) is True

    def test_max_pinned(self) -> None:
        assert is_pinned(SUBJECT_ID_PINNED_MAX) is True
        assert is_pinned(8191) is True

    def test_just_above_pinned(self) -> None:
        assert is_pinned(SUBJECT_ID_PINNED_MAX + 1) is False
        assert is_pinned(8192) is False

    def test_large_value(self) -> None:
        assert is_pinned(2**64 - 1) is False

    def test_mid_pinned(self) -> None:
        assert is_pinned(4096) is True

    def test_one(self) -> None:
        assert is_pinned(1) is True


# =====================================================================================================================
# topic_subject_id
# =====================================================================================================================


DEFAULT_MODULUS = 122743


class TestTopicSubjectId:
    def test_pinned_zero(self) -> None:
        assert topic_subject_id(0, 0, DEFAULT_MODULUS) == 0

    def test_pinned_max(self) -> None:
        assert topic_subject_id(8191, 0, DEFAULT_MODULUS) == 8191

    def test_pinned_ignores_evictions(self) -> None:
        """Pinned hashes return h regardless of evictions or modulus."""
        assert topic_subject_id(100, 5, DEFAULT_MODULUS) == 100
        assert topic_subject_id(100, 999, DEFAULT_MODULUS) == 100
        assert topic_subject_id(0, 42, 7) == 0

    def test_unpinned_zero_evictions(self) -> None:
        h = topic_hash("hello")  # 3327445792987248966
        expected = SUBJECT_ID_PINNED_MAX + 1 + (h % DEFAULT_MODULUS)
        assert topic_subject_id(h, 0, DEFAULT_MODULUS) == expected

    def test_unpinned_with_evictions(self) -> None:
        h = topic_hash("hello")
        # evictions=3 -> evictions^2=9
        raw = (h + 9) % (1 << 64)
        expected = SUBJECT_ID_PINNED_MAX + 1 + (raw % DEFAULT_MODULUS)
        assert topic_subject_id(h, 3, DEFAULT_MODULUS) == expected

    def test_first_unpinned_hash_value(self) -> None:
        """h = 8192 is the smallest non-pinned hash."""
        result = topic_subject_id(8192, 0, DEFAULT_MODULUS)
        assert result == SUBJECT_ID_PINNED_MAX + 1 + (8192 % DEFAULT_MODULUS)
        assert result == 16384

    def test_result_always_above_pinned_max_for_unpinned(self) -> None:
        for h in [8192, 10000, 2**32, 2**63, 2**64 - 1]:
            result = topic_subject_id(h, 0, DEFAULT_MODULUS)
            assert result > SUBJECT_ID_PINNED_MAX

    def test_result_within_modulus_range(self) -> None:
        """Unpinned subject-ID must be in [PINNED_MAX+1, PINNED_MAX+modulus]."""
        for h in [9999, 123456, 2**50]:
            result = topic_subject_id(h, 0, DEFAULT_MODULUS)
            assert SUBJECT_ID_PINNED_MAX + 1 <= result <= SUBJECT_ID_PINNED_MAX + DEFAULT_MODULUS

    def test_64bit_wrapping(self) -> None:
        """Large evictions should wrap at 64 bits."""
        h = topic_hash("hello")
        big_evictions = 2**32 - 1
        raw = (h + big_evictions * big_evictions) % (1 << 64)
        expected = SUBJECT_ID_PINNED_MAX + 1 + (raw % DEFAULT_MODULUS)
        assert topic_subject_id(h, big_evictions, DEFAULT_MODULUS) == expected

    def test_evictions_squared_effect(self) -> None:
        """Verify evictions enters as evictions^2, not evictions."""
        h = 100000
        e = 10
        raw = (h + e * e) % (1 << 64)
        expected = SUBJECT_ID_PINNED_MAX + 1 + (raw % DEFAULT_MODULUS)
        assert topic_subject_id(h, e, DEFAULT_MODULUS) == expected
        # Confirm it differs from using e directly
        raw_linear = (h + e) % (1 << 64)
        linear = SUBJECT_ID_PINNED_MAX + 1 + (raw_linear % DEFAULT_MODULUS)
        assert topic_subject_id(h, e, DEFAULT_MODULUS) != linear


# =====================================================================================================================
# Subject-ID ranges
# =====================================================================================================================


class TestSubjectIdRanges:
    def test_subject_id_max(self) -> None:
        assert subject_id_max(DEFAULT_MODULUS) == SUBJECT_ID_PINNED_MAX + DEFAULT_MODULUS
        assert subject_id_max(DEFAULT_MODULUS) == 130934

    def test_broadcast_subject_id(self) -> None:
        """broadcast = 2^ceil(log2(max+1)) - 1, which is the next power-of-two minus one."""
        bcast = broadcast_subject_id(DEFAULT_MODULUS)
        assert bcast == 131071
        assert bcast == (1 << 17) - 1
        # Verify it is >= subject_id_max
        assert bcast >= subject_id_max(DEFAULT_MODULUS)

    def test_gossip_shard_count(self) -> None:
        count = gossip_shard_count(DEFAULT_MODULUS)
        assert count == 136
        assert count == broadcast_subject_id(DEFAULT_MODULUS) - (subject_id_max(DEFAULT_MODULUS) + 1)

    def test_gossip_shard_subject_id(self) -> None:
        h = topic_hash("hello")
        sid = gossip_shard_subject_id(h, DEFAULT_MODULUS)
        count = gossip_shard_count(DEFAULT_MODULUS)
        assert sid == subject_id_max(DEFAULT_MODULUS) + 1 + (h % count)

    def test_gossip_shard_within_range(self) -> None:
        """Gossip shard subject-IDs must be in [sid_max+1, broadcast_sid-1]."""
        sid_max_val = subject_id_max(DEFAULT_MODULUS)
        bcast = broadcast_subject_id(DEFAULT_MODULUS)
        for name in ["hello", "test", "foo", "bar", ""]:
            h = topic_hash(name)
            sid = gossip_shard_subject_id(h, DEFAULT_MODULUS)
            assert sid_max_val + 1 <= sid < bcast

    def test_gossip_shard_second_example(self) -> None:
        h = topic_hash("test")
        sid = gossip_shard_subject_id(h, DEFAULT_MODULUS)
        count = gossip_shard_count(DEFAULT_MODULUS)
        assert sid == subject_id_max(DEFAULT_MODULUS) + 1 + (h % count)

    def test_small_modulus(self) -> None:
        """Verify formulas with a small modulus to check edge behavior."""
        mod = 1
        assert subject_id_max(mod) == SUBJECT_ID_PINNED_MAX + 1  # 8192
        bcast = broadcast_subject_id(mod)
        assert bcast == (1 << 14) - 1  # 16383, since 8192.bit_length() = 14
        count = gossip_shard_count(mod)
        assert count == bcast - (subject_id_max(mod) + 1)


# =====================================================================================================================
# CRDT helpers: log_age
# =====================================================================================================================


class TestLogAge:
    def test_zero_diff(self) -> None:
        assert log_age(10.0, 10.0) == -1

    def test_negative_diff(self) -> None:
        assert log_age(10.0, 9.0) == -1

    def test_diff_less_than_one(self) -> None:
        assert log_age(0.0, 0.5) == -1

    def test_diff_exactly_one(self) -> None:
        assert log_age(0.0, 1.0) == 0

    def test_diff_between_one_and_two(self) -> None:
        assert log_age(0.0, 1.5) == 0

    def test_diff_exactly_two(self) -> None:
        assert log_age(0.0, 2.0) == 1

    def test_diff_between_two_and_four(self) -> None:
        assert log_age(0.0, 3.0) == 1

    def test_diff_exactly_four(self) -> None:
        assert log_age(0.0, 4.0) == 2

    def test_diff_just_below_eight(self) -> None:
        assert log_age(0.0, 7.99) == 2

    def test_diff_exactly_eight(self) -> None:
        assert log_age(0.0, 8.0) == 3

    def test_diff_1024(self) -> None:
        assert log_age(0.0, 1024.0) == 10

    def test_diff_power_of_two_exact(self) -> None:
        for exp in range(0, 36):
            assert log_age(0.0, float(2**exp)) == min(exp, LAGE_MAX)

    def test_clamp_at_max(self) -> None:
        """Differences >= 2^36 should clamp to LAGE_MAX=35."""
        assert log_age(0.0, float(2**35)) == 35
        assert log_age(0.0, float(2**36)) == 35
        assert log_age(0.0, 1e11) == 35

    def test_very_small_positive_diff(self) -> None:
        """diff in (0, 1) should give floor(log2) < 0, clamped to -1."""
        assert log_age(0.0, 0.001) == -1
        assert log_age(0.0, 1e-10) == -1

    def test_non_zero_origin(self) -> None:
        assert log_age(100.0, 108.0) == 3  # diff = 8
        assert log_age(100.0, 100.5) == -1  # diff = 0.5

    def test_lage_bounds_constants(self) -> None:
        """Verify returned values respect LAGE_MIN and LAGE_MAX."""
        assert log_age(5.0, 5.0) == LAGE_MIN
        assert log_age(0.0, float(2**40)) == LAGE_MAX


# =====================================================================================================================
# CRDT helpers: left_wins
# =====================================================================================================================


class TestLeftWins:
    def test_older_wins(self) -> None:
        """Higher lage (older) wins."""
        assert left_wins(10, 0, 5, 0) is True
        assert left_wins(5, 0, 10, 0) is False

    def test_same_age_higher_hash_wins(self) -> None:
        assert left_wins(5, 100, 5, 50) is True
        assert left_wins(5, 50, 5, 100) is False

    def test_same_age_same_hash(self) -> None:
        """Exactly equal -> left does NOT win (not strictly greater)."""
        assert left_wins(5, 100, 5, 100) is False

    def test_negative_lage(self) -> None:
        assert left_wins(-1, 999, -1, 0) is True
        assert left_wins(-1, 0, -1, 999) is False

    def test_zero_values(self) -> None:
        assert left_wins(0, 0, 0, 0) is False
        assert left_wins(0, 1, 0, 0) is True
        assert left_wins(1, 0, 0, 0) is True

    def test_max_lage(self) -> None:
        assert left_wins(35, 0, 34, 2**64) is True

    def test_lage_dominates_hash(self) -> None:
        """Even with hash=0, higher lage wins over any hash."""
        assert left_wins(10, 0, 9, 2**64 - 1) is True


# =====================================================================================================================
# Header pack/unpack: MSG_BE, MSG_REL
# =====================================================================================================================


class TestMsgHeaders:
    @pytest.mark.parametrize("msg_type", [HeaderType.MSG_BE, HeaderType.MSG_REL])
    def test_roundtrip_basic(self, msg_type: HeaderType) -> None:
        hdr = pack_msg_header(msg_type, lage=5, evictions=42, topic_hash_val=0xDEADBEEF, tag=0x1234)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == msg_type
        assert d["lage"] == 5
        assert d["evictions"] == 42
        assert d["hash"] == 0xDEADBEEF
        assert d["tag"] == 0x1234

    @pytest.mark.parametrize("msg_type", [HeaderType.MSG_BE, HeaderType.MSG_REL])
    def test_zero_values(self, msg_type: HeaderType) -> None:
        hdr = pack_msg_header(msg_type, lage=0, evictions=0, topic_hash_val=0, tag=0)
        d = unpack_header(hdr)
        assert d["lage"] == 0
        assert d["evictions"] == 0
        assert d["hash"] == 0
        assert d["tag"] == 0

    @pytest.mark.parametrize("msg_type", [HeaderType.MSG_BE, HeaderType.MSG_REL])
    def test_max_values(self, msg_type: HeaderType) -> None:
        hdr = pack_msg_header(
            msg_type,
            lage=127,
            evictions=0xFFFFFFFF,
            topic_hash_val=0xFFFFFFFFFFFFFFFF,
            tag=0xFFFFFFFFFFFFFFFF,
        )
        d = unpack_header(hdr)
        assert d["lage"] == 127
        assert d["evictions"] == 0xFFFFFFFF
        assert d["hash"] == 0xFFFFFFFFFFFFFFFF
        assert d["tag"] == 0xFFFFFFFFFFFFFFFF

    def test_negative_lage(self) -> None:
        """lage is stored as a signed byte (offset 3)."""
        hdr = pack_msg_header(HeaderType.MSG_BE, lage=-1, evictions=0, topic_hash_val=0, tag=0)
        d = unpack_header(hdr)
        assert d["lage"] == -1

    def test_incompatibility_default_zero(self) -> None:
        hdr = pack_msg_header(HeaderType.MSG_BE, lage=0, evictions=0, topic_hash_val=0, tag=0)
        d = unpack_header(hdr)
        assert d["incompatibility"] == 0

    def test_type_byte_position(self) -> None:
        hdr = pack_msg_header(HeaderType.MSG_REL, lage=0, evictions=0, topic_hash_val=0, tag=0)
        assert hdr[0] == HeaderType.MSG_REL


# =====================================================================================================================
# Header pack/unpack: MSG_ACK, MSG_NACK
# =====================================================================================================================


class TestAckHeaders:
    @pytest.mark.parametrize("ack_type", [HeaderType.MSG_ACK, HeaderType.MSG_NACK])
    def test_roundtrip(self, ack_type: HeaderType) -> None:
        hdr = pack_ack_header(ack_type, topic_hash_val=0xCAFE, tag=0xBEEF)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == ack_type
        assert d["hash"] == 0xCAFE
        assert d["tag"] == 0xBEEF

    @pytest.mark.parametrize("ack_type", [HeaderType.MSG_ACK, HeaderType.MSG_NACK])
    def test_zero_values(self, ack_type: HeaderType) -> None:
        hdr = pack_ack_header(ack_type, topic_hash_val=0, tag=0)
        d = unpack_header(hdr)
        assert d["hash"] == 0
        assert d["tag"] == 0

    @pytest.mark.parametrize("ack_type", [HeaderType.MSG_ACK, HeaderType.MSG_NACK])
    def test_max_values(self, ack_type: HeaderType) -> None:
        hdr = pack_ack_header(ack_type, topic_hash_val=0xFFFFFFFFFFFFFFFF, tag=0xFFFFFFFFFFFFFFFF)
        d = unpack_header(hdr)
        assert d["hash"] == 0xFFFFFFFFFFFFFFFF
        assert d["tag"] == 0xFFFFFFFFFFFFFFFF

    def test_incompatibility_field(self) -> None:
        hdr = pack_ack_header(HeaderType.MSG_ACK, topic_hash_val=0, tag=0)
        d = unpack_header(hdr)
        assert d["incompatibility"] == 0


# =====================================================================================================================
# Header pack/unpack: RSP_BE, RSP_REL
# =====================================================================================================================


class TestRspHeaders:
    @pytest.mark.parametrize("rsp_type", [HeaderType.RSP_BE, HeaderType.RSP_REL])
    def test_roundtrip(self, rsp_type: HeaderType) -> None:
        hdr = pack_rsp_header(rsp_type, tag=0xAB, seqno=0x123456789ABC, topic_hash_val=0xDEAD, message_tag=0xBEEF)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == rsp_type
        assert d["tag"] == 0xAB
        assert d["seqno"] == 0x123456789ABC
        assert d["hash"] == 0xDEAD
        assert d["message_tag"] == 0xBEEF

    @pytest.mark.parametrize("rsp_type", [HeaderType.RSP_BE, HeaderType.RSP_REL])
    def test_zero_values(self, rsp_type: HeaderType) -> None:
        hdr = pack_rsp_header(rsp_type, tag=0, seqno=0, topic_hash_val=0, message_tag=0)
        d = unpack_header(hdr)
        assert d["tag"] == 0
        assert d["seqno"] == 0
        assert d["hash"] == 0
        assert d["message_tag"] == 0

    @pytest.mark.parametrize("rsp_type", [HeaderType.RSP_BE, HeaderType.RSP_REL])
    def test_max_seqno_48bit(self, rsp_type: HeaderType) -> None:
        max_seqno = SEQNO48_MASK
        hdr = pack_rsp_header(
            rsp_type, tag=0xFF, seqno=max_seqno, topic_hash_val=0xFFFFFFFFFFFFFFFF, message_tag=0xFFFFFFFFFFFFFFFF
        )
        d = unpack_header(hdr)
        assert d["tag"] == 0xFF
        assert d["seqno"] == max_seqno
        assert d["hash"] == 0xFFFFFFFFFFFFFFFF
        assert d["message_tag"] == 0xFFFFFFFFFFFFFFFF

    def test_tag_is_single_byte(self) -> None:
        hdr = pack_rsp_header(HeaderType.RSP_BE, tag=255, seqno=0, topic_hash_val=0, message_tag=0)
        d = unpack_header(hdr)
        assert d["tag"] == 255

    def test_seqno_little_endian_encoding(self) -> None:
        """Verify the 6-byte little-endian seqno encoding at offset 2."""
        seqno = 0x0102030405_06
        hdr = pack_rsp_header(HeaderType.RSP_BE, tag=0, seqno=seqno, topic_hash_val=0, message_tag=0)
        # Bytes at offset 2..7 should be seqno in LE
        assert hdr[2] == 0x06
        assert hdr[3] == 0x05
        assert hdr[4] == 0x04
        assert hdr[5] == 0x03
        assert hdr[6] == 0x02
        assert hdr[7] == 0x01


# =====================================================================================================================
# Header pack/unpack: RSP_ACK, RSP_NACK
# =====================================================================================================================


class TestRspAckHeaders:
    @pytest.mark.parametrize("ack_type", [HeaderType.RSP_ACK, HeaderType.RSP_NACK])
    def test_roundtrip(self, ack_type: HeaderType) -> None:
        hdr = pack_rsp_ack_header(ack_type, tag=7, seqno=999, topic_hash_val=0xABCD, message_tag=0x5678)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == ack_type
        assert d["tag"] == 7
        assert d["seqno"] == 999
        assert d["hash"] == 0xABCD
        assert d["message_tag"] == 0x5678

    @pytest.mark.parametrize("ack_type", [HeaderType.RSP_ACK, HeaderType.RSP_NACK])
    def test_zero_values(self, ack_type: HeaderType) -> None:
        hdr = pack_rsp_ack_header(ack_type, tag=0, seqno=0, topic_hash_val=0, message_tag=0)
        d = unpack_header(hdr)
        assert d["tag"] == 0
        assert d["seqno"] == 0
        assert d["hash"] == 0
        assert d["message_tag"] == 0

    def test_same_layout_as_rsp(self) -> None:
        """pack_rsp_ack_header delegates to pack_rsp_header; binary output must match."""
        a = pack_rsp_ack_header(HeaderType.RSP_ACK, tag=3, seqno=42, topic_hash_val=99, message_tag=77)
        b = pack_rsp_header(HeaderType.RSP_ACK, tag=3, seqno=42, topic_hash_val=99, message_tag=77)
        assert a == b


# =====================================================================================================================
# Header pack/unpack: GOSSIP
# =====================================================================================================================


class TestGossipHeader:
    def test_roundtrip(self) -> None:
        hdr = pack_gossip_header(lage=10, topic_hash_val=0xFEEDFACE, evictions=7, name_len=42)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.GOSSIP
        assert d["lage"] == 10
        assert d["hash"] == 0xFEEDFACE
        assert d["evictions"] == 7
        assert d["name_len"] == 42

    def test_zero_values(self) -> None:
        hdr = pack_gossip_header(lage=0, topic_hash_val=0, evictions=0, name_len=0)
        d = unpack_header(hdr)
        assert d["lage"] == 0
        assert d["hash"] == 0
        assert d["evictions"] == 0
        assert d["name_len"] == 0

    def test_max_values(self) -> None:
        hdr = pack_gossip_header(lage=127, topic_hash_val=0xFFFFFFFFFFFFFFFF, evictions=0xFFFFFFFF, name_len=255)
        d = unpack_header(hdr)
        assert d["lage"] == 127
        assert d["hash"] == 0xFFFFFFFFFFFFFFFF
        assert d["evictions"] == 0xFFFFFFFF
        assert d["name_len"] == 255

    def test_negative_lage(self) -> None:
        hdr = pack_gossip_header(lage=-1, topic_hash_val=0, evictions=0, name_len=0)
        d = unpack_header(hdr)
        assert d["lage"] == -1

    def test_type_byte(self) -> None:
        hdr = pack_gossip_header(lage=0, topic_hash_val=0, evictions=0, name_len=0)
        assert hdr[0] == HeaderType.GOSSIP

    def test_name_len_at_last_byte(self) -> None:
        hdr = pack_gossip_header(lage=0, topic_hash_val=0, evictions=0, name_len=200)
        assert hdr[HEADER_SIZE - 1] == 200

    def test_incompatibility_default_zero(self) -> None:
        hdr = pack_gossip_header(lage=0, topic_hash_val=0, evictions=0, name_len=0)
        d = unpack_header(hdr)
        assert d["incompatibility"] == 0


# =====================================================================================================================
# Header pack/unpack: SCOUT
# =====================================================================================================================


class TestScoutHeader:
    def test_roundtrip(self) -> None:
        hdr = pack_scout_header(pattern_len=100)
        assert len(hdr) == HEADER_SIZE
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.SCOUT
        assert d["pattern_len"] == 100

    def test_zero_pattern_len(self) -> None:
        hdr = pack_scout_header(pattern_len=0)
        d = unpack_header(hdr)
        assert d["pattern_len"] == 0

    def test_max_pattern_len(self) -> None:
        hdr = pack_scout_header(pattern_len=255)
        d = unpack_header(hdr)
        assert d["pattern_len"] == 255

    def test_type_byte(self) -> None:
        hdr = pack_scout_header(pattern_len=0)
        assert hdr[0] == HeaderType.SCOUT

    def test_pattern_len_at_last_byte(self) -> None:
        hdr = pack_scout_header(pattern_len=77)
        assert hdr[HEADER_SIZE - 1] == 77

    def test_mostly_zeros(self) -> None:
        """SCOUT header should be mostly zeros except type and pattern_len."""
        hdr = pack_scout_header(pattern_len=0)
        assert hdr == bytes([HeaderType.SCOUT]) + bytes(HEADER_SIZE - 1)

    def test_incompatibility_fields_zero(self) -> None:
        hdr = pack_scout_header(pattern_len=0)
        d = unpack_header(hdr)
        assert d["incompatibility"] == 0
        assert d["incompatibility1"] == 0


# =====================================================================================================================
# unpack_header error handling and edge cases
# =====================================================================================================================


class TestUnpackHeader:
    def test_too_short_raises(self) -> None:
        with pytest.raises(ValueError, match="Header too short"):
            unpack_header(b"\x00" * 23)

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="Header too short"):
            unpack_header(b"")

    def test_exactly_24_bytes(self) -> None:
        """24 bytes should not raise."""
        hdr = pack_msg_header(HeaderType.MSG_BE, 0, 0, 0, 0)
        assert len(hdr) == 24
        unpack_header(hdr)  # should not raise

    def test_longer_than_24_ok(self) -> None:
        """Extra trailing bytes are ignored."""
        hdr = pack_msg_header(HeaderType.MSG_BE, 0, 0, 0, 0) + b"\xff" * 10
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.MSG_BE

    def test_invalid_type_raises(self) -> None:
        buf = bytearray(HEADER_SIZE)
        buf[0] = 99  # not a valid HeaderType
        with pytest.raises(ValueError):
            unpack_header(bytes(buf))

    def test_memoryview_input(self) -> None:
        hdr = pack_msg_header(HeaderType.MSG_BE, lage=3, evictions=1, topic_hash_val=0xFF, tag=0xAA)
        d = unpack_header(memoryview(hdr))
        assert d["type"] == HeaderType.MSG_BE
        assert d["lage"] == 3
        assert d["hash"] == 0xFF

    def test_bytearray_input(self) -> None:
        hdr = pack_gossip_header(lage=5, topic_hash_val=123, evictions=0, name_len=10)
        d = unpack_header(bytearray(hdr))
        assert d["type"] == HeaderType.GOSSIP
        assert d["lage"] == 5

    def test_all_ten_types_roundtrip(self) -> None:
        """Every header type can be packed and unpacked without error."""
        headers = [
            pack_msg_header(HeaderType.MSG_BE, 0, 0, 0, 0),
            pack_msg_header(HeaderType.MSG_REL, 1, 2, 3, 4),
            pack_ack_header(HeaderType.MSG_ACK, 100, 200),
            pack_ack_header(HeaderType.MSG_NACK, 300, 400),
            pack_rsp_header(HeaderType.RSP_BE, 5, 6, 7, 8),
            pack_rsp_header(HeaderType.RSP_REL, 9, 10, 11, 12),
            pack_rsp_ack_header(HeaderType.RSP_ACK, 13, 14, 15, 16),
            pack_rsp_ack_header(HeaderType.RSP_NACK, 17, 18, 19, 20),
            pack_gossip_header(21, 22, 23, 24),
            pack_scout_header(25),
        ]
        types_seen = set()
        for hdr in headers:
            assert len(hdr) == HEADER_SIZE
            d = unpack_header(hdr)
            types_seen.add(d["type"])
        assert types_seen == set(HeaderType)


# =====================================================================================================================
# Cross-cutting: header binary layout spot-checks
# =====================================================================================================================


class TestHeaderBinaryLayout:
    def test_msg_header_layout(self) -> None:
        """Verify MSG_BE byte layout: type(1) void(1) incompat(1) lage(1) evict(4) hash(8) tag(8)."""
        hdr = pack_msg_header(
            HeaderType.MSG_BE, lage=7, evictions=0x0A0B0C0D, topic_hash_val=0x0102030405060708, tag=0x1112131415161718
        )
        assert hdr[0] == 0  # type = MSG_BE
        assert hdr[1] == 0  # void
        assert hdr[2] == 0  # incompatibility (not set by pack)
        assert hdr[3] == 7  # lage
        assert struct.unpack_from("<I", hdr, 4)[0] == 0x0A0B0C0D
        assert struct.unpack_from("<Q", hdr, 8)[0] == 0x0102030405060708
        assert struct.unpack_from("<Q", hdr, 16)[0] == 0x1112131415161718

    def test_ack_header_layout(self) -> None:
        """Verify ACK byte layout: type(1) void(3) incompat(4) hash(8) tag(8)."""
        hdr = pack_ack_header(HeaderType.MSG_ACK, topic_hash_val=0xAAAABBBBCCCCDDDD, tag=0x1111222233334444)
        assert hdr[0] == HeaderType.MSG_ACK
        assert struct.unpack_from("<Q", hdr, 8)[0] == 0xAAAABBBBCCCCDDDD
        assert struct.unpack_from("<Q", hdr, 16)[0] == 0x1111222233334444

    def test_gossip_header_layout(self) -> None:
        """Verify GOSSIP byte layout: type(1) void(2) lage(1) incompat(4) hash(8) evict(4) void(3) name_len(1)."""
        hdr = pack_gossip_header(lage=3, topic_hash_val=0xFEDCBA9876543210, evictions=0xAABBCCDD, name_len=0xEE)
        assert hdr[0] == HeaderType.GOSSIP
        assert hdr[3] == 3  # lage
        assert struct.unpack_from("<Q", hdr, 8)[0] == 0xFEDCBA9876543210
        assert struct.unpack_from("<I", hdr, 16)[0] == 0xAABBCCDD
        assert hdr[23] == 0xEE

    def test_scout_header_mostly_zeroed(self) -> None:
        hdr = pack_scout_header(pattern_len=42)
        assert hdr[0] == HeaderType.SCOUT
        assert hdr[23] == 42
        # Everything in between should be zero
        for i in range(1, 23):
            assert hdr[i] == 0, f"Byte {i} is {hdr[i]}, expected 0"

    def test_all_headers_little_endian(self) -> None:
        """Pack with a distinguishable value and check LE byte order in hash field (offset 8)."""
        val = 0x0807060504030201
        hdr = pack_msg_header(HeaderType.MSG_BE, 0, 0, val, 0)
        assert hdr[8] == 0x01
        assert hdr[9] == 0x02
        assert hdr[15] == 0x08


# =====================================================================================================================
# Boundary value tests
# =====================================================================================================================


class TestBoundaryValues:
    def test_topic_hash_max_uint64(self) -> None:
        """Override to max uint64."""
        h = topic_hash("x#ffffffffffffffff")
        assert h == 2**64 - 1
        assert not is_pinned(h)

    def test_topic_subject_id_max_uint64_hash(self) -> None:
        h = 2**64 - 1
        result = topic_subject_id(h, 0, DEFAULT_MODULUS)
        assert SUBJECT_ID_PINNED_MAX < result <= SUBJECT_ID_PINNED_MAX + DEFAULT_MODULUS

    def test_topic_subject_id_evictions_zero(self) -> None:
        h = 50000
        assert topic_subject_id(h, 0, DEFAULT_MODULUS) == SUBJECT_ID_PINNED_MAX + 1 + (h % DEFAULT_MODULUS)

    def test_log_age_exact_boundary_at_zero_diff(self) -> None:
        assert log_age(0.0, 0.0) == LAGE_MIN

    def test_header_size_always_24(self) -> None:
        """All pack functions return exactly 24 bytes."""
        funcs_and_args = [
            (pack_msg_header, (HeaderType.MSG_BE, 0, 0, 0, 0)),
            (pack_msg_header, (HeaderType.MSG_REL, 0, 0, 0, 0)),
            (pack_gossip_header, (0, 0, 0, 0)),
            (pack_scout_header, (0,)),
            (pack_ack_header, (HeaderType.MSG_ACK, 0, 0)),
            (pack_ack_header, (HeaderType.MSG_NACK, 0, 0)),
            (pack_rsp_header, (HeaderType.RSP_BE, 0, 0, 0, 0)),
            (pack_rsp_header, (HeaderType.RSP_REL, 0, 0, 0, 0)),
            (pack_rsp_ack_header, (HeaderType.RSP_ACK, 0, 0, 0, 0)),
            (pack_rsp_ack_header, (HeaderType.RSP_NACK, 0, 0, 0, 0)),
        ]
        for func, args in funcs_and_args:
            result = func(*args)
            assert len(result) == 24, f"{func.__name__} returned {len(result)} bytes"
            assert isinstance(result, bytes)

    def test_seqno_mask_in_rsp(self) -> None:
        """Seqno field is 48 bits; pack/unpack preserves the full range."""
        max_48 = SEQNO48_MASK
        hdr = pack_rsp_header(HeaderType.RSP_BE, tag=0, seqno=max_48, topic_hash_val=0, message_tag=0)
        d = unpack_header(hdr)
        assert d["seqno"] == max_48

    def test_msg_lage_signed_range(self) -> None:
        """lage byte is signed: test -128 to 127."""
        for lage in [-128, -1, 0, 1, 127]:
            hdr = pack_msg_header(HeaderType.MSG_BE, lage=lage, evictions=0, topic_hash_val=0, tag=0)
            d = unpack_header(hdr)
            assert d["lage"] == lage


# =====================================================================================================================
# rapidhash reference tests (golden values from C rapidhash.h V3)
# =====================================================================================================================


class TestRapidhash:
    """Verify pure-Python rapidhash matches the reference C implementation (rapidhash.h V3)."""

    def test_empty(self) -> None:
        assert rapidhash(b"") == 232177599295442350

    def test_single_bytes(self) -> None:
        assert rapidhash(b"a") == 6457959414642172395
        assert rapidhash(b"ab") == 8872296267850602869
        assert rapidhash(b"abc") == 14647777377830833570

    def test_4_to_7_bytes(self) -> None:
        assert rapidhash(b"abcd") == 17939050396679037234
        assert rapidhash(b"abcde") == 14266022930908899504
        assert rapidhash(b"abcdef") == 18257973576243341225
        assert rapidhash(b"abcdefg") == 2837523131410848269

    def test_8_to_16_bytes(self) -> None:
        assert rapidhash(b"abcdefgh") == 12327933690858042399
        assert rapidhash(b"abcdefghijklmno") == 6106948830996220096
        assert rapidhash(b"abcdefghijklmnop") == 14378558341499247390

    def test_medium_path_17_bytes(self) -> None:
        assert rapidhash(b"abcdefghijklmnopq") == 55212805758149560

    def test_medium_path_33_bytes(self) -> None:
        assert rapidhash(b"abcdefghijklmnopqrstuvwxyz0123456") == 9206725336149749049

    def test_medium_path_48_bytes(self) -> None:
        assert rapidhash(b"abcdefghijklmnopqrstuvwxyz01234567890ABCDEFGHIJK") == 14164218575214757208

    def test_medium_path_65_bytes(self) -> None:
        data = bytes(ord("a") + (i % 26) for i in range(65))
        assert rapidhash(data) == 16599151913521419783

    def test_medium_path_97_bytes(self) -> None:
        data = bytes(ord("a") + (i % 26) for i in range(97))
        assert rapidhash(data) == 7787509548221086089

    def test_medium_path_112_bytes(self) -> None:
        data = bytes(ord("a") + (i % 26) for i in range(112))
        assert rapidhash(data) == 5820722958982198621

    def test_bulk_path_113_bytes(self) -> None:
        data = bytes(ord("a") + (i % 26) for i in range(113))
        assert rapidhash(data) == 3052845270717700940

    def test_bulk_path_224_bytes(self) -> None:
        data = bytes(ord("a") + (i % 26) for i in range(224))
        assert rapidhash(data) == 3919655397064389956

    def test_bulk_path_256_bytes_range(self) -> None:
        assert rapidhash(bytes(range(256))) == 11741873740586552221

    def test_all_zeros(self) -> None:
        assert rapidhash(b"\x00" * 1) == 5702620981742189058
        assert rapidhash(b"\x00" * 8) == 11456056956516475379
        assert rapidhash(b"\x00" * 16) == 11820096523416114993
        assert rapidhash(b"\x00" * 64) == 5683474723485895682
        assert rapidhash(b"\x00" * 128) == 5941051076149292645
        assert rapidhash(b"\x00" * 256) == 16340242527745749067

    def test_all_ff(self) -> None:
        assert rapidhash(b"\xff" * 1) == 13606465366094334334
        assert rapidhash(b"\xff" * 8) == 9950808644265218413
        assert rapidhash(b"\xff" * 16) == 1685515674368760865
        assert rapidhash(b"\xff" * 64) == 13646541276577895361
        assert rapidhash(b"\xff" * 128) == 13618355842615861923
        assert rapidhash(b"\xff" * 256) == 10134260856215968254

    def test_domain_strings(self) -> None:
        assert rapidhash(b"hello") == 3327445792987248966
        assert rapidhash(b"pycyphal") == 3131592564933152817
        assert rapidhash(b"uavcan.node.Heartbeat.1.0") == 3897213858259282939
        assert rapidhash(b"uavcan.node.GetInfo.1.0") == 116166870293823273
        assert rapidhash(b"test") == 16388600957843709845
        assert rapidhash(b"foo") == 2360160339125950113
        assert rapidhash(b"bar") == 12791351756655921208


# =====================================================================================================================
# Header serialization golden tests
# =====================================================================================================================


class TestHeaderGolden:
    """Verify pack functions produce exact expected byte sequences."""

    def test_msg_be_golden(self) -> None:
        hdr = pack_msg_header(
            HeaderType.MSG_BE, lage=5, evictions=42, topic_hash_val=0xDEADBEEFCAFEBABE, tag=0x0123456789ABCDEF
        )
        expected = bytearray(24)
        expected[0] = 0  # MSG_BE
        expected[3] = 5  # lage
        struct.pack_into("<I", expected, 4, 42)
        struct.pack_into("<Q", expected, 8, 0xDEADBEEFCAFEBABE)
        struct.pack_into("<Q", expected, 16, 0x0123456789ABCDEF)
        assert hdr == bytes(expected)

    def test_msg_rel_golden(self) -> None:
        hdr = pack_msg_header(HeaderType.MSG_REL, lage=-1, evictions=0, topic_hash_val=0, tag=0xFFFFFFFFFFFFFFFF)
        expected = bytearray(24)
        expected[0] = 1  # MSG_REL
        expected[3] = 0xFF  # -1 as unsigned byte
        struct.pack_into("<Q", expected, 16, 0xFFFFFFFFFFFFFFFF)
        assert hdr == bytes(expected)

    def test_msg_be_max_values(self) -> None:
        hdr = pack_msg_header(
            HeaderType.MSG_BE,
            lage=127,
            evictions=0xFFFFFFFF,
            topic_hash_val=0xFFFFFFFFFFFFFFFF,
            tag=0xFFFFFFFFFFFFFFFF,
        )
        expected = bytearray(24)
        expected[0] = 0
        expected[3] = 127
        struct.pack_into("<I", expected, 4, 0xFFFFFFFF)
        struct.pack_into("<Q", expected, 8, 0xFFFFFFFFFFFFFFFF)
        struct.pack_into("<Q", expected, 16, 0xFFFFFFFFFFFFFFFF)
        assert hdr == bytes(expected)

    def test_msg_ack_golden(self) -> None:
        hdr = pack_ack_header(HeaderType.MSG_ACK, topic_hash_val=0xAAAABBBBCCCCDDDD, tag=0x1111222233334444)
        expected = bytearray(24)
        expected[0] = 2  # MSG_ACK
        struct.pack_into("<Q", expected, 8, 0xAAAABBBBCCCCDDDD)
        struct.pack_into("<Q", expected, 16, 0x1111222233334444)
        assert hdr == bytes(expected)

    def test_msg_nack_golden(self) -> None:
        hdr = pack_ack_header(HeaderType.MSG_NACK, topic_hash_val=0, tag=0)
        expected = bytearray(24)
        expected[0] = 3  # MSG_NACK
        assert hdr == bytes(expected)

    def test_rsp_be_golden(self) -> None:
        hdr = pack_rsp_header(
            HeaderType.RSP_BE, tag=0xAB, seqno=0x0102030405_06, topic_hash_val=0xDEAD, message_tag=0xBEEF
        )
        expected = bytearray(24)
        expected[0] = 4  # RSP_BE
        expected[1] = 0xAB  # tag
        # seqno 48-bit LE at offset 2
        expected[2] = 0x06
        expected[3] = 0x05
        expected[4] = 0x04
        expected[5] = 0x03
        expected[6] = 0x02
        expected[7] = 0x01
        struct.pack_into("<Q", expected, 8, 0xDEAD)
        struct.pack_into("<Q", expected, 16, 0xBEEF)
        assert hdr == bytes(expected)

    def test_rsp_rel_golden(self) -> None:
        hdr = pack_rsp_header(HeaderType.RSP_REL, tag=0, seqno=0, topic_hash_val=0, message_tag=0)
        expected = bytearray(24)
        expected[0] = 5  # RSP_REL
        assert hdr == bytes(expected)

    def test_gossip_golden(self) -> None:
        hdr = pack_gossip_header(lage=3, topic_hash_val=0xFEDCBA9876543210, evictions=0xAABBCCDD, name_len=0xEE)
        expected = bytearray(24)
        expected[0] = 8  # GOSSIP
        expected[3] = 3  # lage
        struct.pack_into("<Q", expected, 8, 0xFEDCBA9876543210)
        struct.pack_into("<I", expected, 16, 0xAABBCCDD)
        expected[23] = 0xEE
        assert hdr == bytes(expected)

    def test_scout_golden(self) -> None:
        hdr = pack_scout_header(pattern_len=42)
        expected = bytearray(24)
        expected[0] = 9  # SCOUT
        expected[23] = 42
        assert hdr == bytes(expected)

    def test_roundtrip_msg_be(self) -> None:
        hdr = pack_msg_header(
            HeaderType.MSG_BE, lage=5, evictions=42, topic_hash_val=0xDEADBEEFCAFEBABE, tag=0x0123456789ABCDEF
        )
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.MSG_BE
        assert d["lage"] == 5
        assert d["evictions"] == 42
        assert d["hash"] == 0xDEADBEEFCAFEBABE
        assert d["tag"] == 0x0123456789ABCDEF

    def test_roundtrip_gossip(self) -> None:
        hdr = pack_gossip_header(lage=-1, topic_hash_val=0x1234567890ABCDEF, evictions=999, name_len=200)
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.GOSSIP
        assert d["lage"] == -1
        assert d["hash"] == 0x1234567890ABCDEF
        assert d["evictions"] == 999
        assert d["name_len"] == 200

    def test_roundtrip_rsp(self) -> None:
        hdr = pack_rsp_header(
            HeaderType.RSP_BE, tag=0xFF, seqno=SEQNO48_MASK, topic_hash_val=0xCAFEBABE, message_tag=0xDEADBEEF
        )
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.RSP_BE
        assert d["tag"] == 0xFF
        assert d["seqno"] == SEQNO48_MASK
        assert d["hash"] == 0xCAFEBABE
        assert d["message_tag"] == 0xDEADBEEF

    def test_roundtrip_scout(self) -> None:
        hdr = pack_scout_header(pattern_len=255)
        d = unpack_header(hdr)
        assert d["type"] == HeaderType.SCOUT
        assert d["pattern_len"] == 255
        assert d["incompatibility"] == 0
        assert d["incompatibility1"] == 0
