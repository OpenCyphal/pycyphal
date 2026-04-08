import struct
from pycyphal2._header import *

# =====================================================================================================================
# MsgBeHeader (TYPE=0) and MsgRelHeader (TYPE=1)
# =====================================================================================================================


def test_msg_be_roundtrip() -> None:
    h = MsgBeHeader(topic_log_age=5, topic_evictions=100, topic_hash=0xDEADBEEFCAFEBABE, tag=0x1234)
    assert h.TYPE == 0
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 0
    out = MsgBeHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_msg_rel_roundtrip() -> None:
    h = MsgRelHeader(topic_log_age=0, topic_evictions=0, topic_hash=0, tag=0)
    assert h.TYPE == 1
    buf = h.serialize()
    assert buf[0] == 1
    out = MsgRelHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_msg_be_signed_lage_negative() -> None:
    h = MsgBeHeader(topic_log_age=-1, topic_evictions=0, topic_hash=0, tag=0)
    buf = h.serialize()
    out = MsgBeHeader.deserialize(buf)
    assert out is not None
    assert out.topic_log_age == -1


def test_msg_rel_signed_lage_negative() -> None:
    h = MsgRelHeader(topic_log_age=-1, topic_evictions=0, topic_hash=0, tag=0)
    buf = h.serialize()
    out = MsgRelHeader.deserialize(buf)
    assert out is not None
    assert out.topic_log_age == -1


def test_msg_be_signed_lage_positive() -> None:
    h = MsgBeHeader(topic_log_age=35, topic_evictions=0, topic_hash=0, tag=0)
    buf = h.serialize()
    out = MsgBeHeader.deserialize(buf)
    assert out is not None
    assert out.topic_log_age == 35


def test_msg_be_max_values() -> None:
    h = MsgBeHeader(
        topic_log_age=35,
        topic_evictions=0xFFFFFFFF,
        topic_hash=0xFFFFFFFFFFFFFFFF,
        tag=0xFFFFFFFFFFFFFFFF,
    )
    buf = h.serialize()
    out = MsgBeHeader.deserialize(buf)
    assert out is not None
    assert out.topic_evictions == 0xFFFFFFFF
    assert out.topic_hash == 0xFFFFFFFFFFFFFFFF
    assert out.tag == 0xFFFFFFFFFFFFFFFF


def test_msg_rel_max_values() -> None:
    h = MsgRelHeader(
        topic_log_age=35,
        topic_evictions=0xFFFFFFFF,
        topic_hash=0xFFFFFFFFFFFFFFFF,
        tag=0xFFFFFFFFFFFFFFFF,
    )
    buf = h.serialize()
    out = MsgRelHeader.deserialize(buf)
    assert out is not None
    assert out.topic_evictions == 0xFFFFFFFF
    assert out.topic_hash == 0xFFFFFFFFFFFFFFFF


def test_msg_lage_out_of_range_rejected() -> None:
    high = bytearray(MsgBeHeader(topic_log_age=35, topic_evictions=0, topic_hash=0, tag=0).serialize())
    high[3] = 36
    assert MsgBeHeader.deserialize(bytes(high)) is None

    low = bytearray(MsgRelHeader(topic_log_age=-1, topic_evictions=0, topic_hash=0, tag=0).serialize())
    low[3] = 0xFE  # -2 as int8
    assert MsgRelHeader.deserialize(bytes(low)) is None


def test_msg_incompatibility_rejection() -> None:
    h = MsgBeHeader(topic_log_age=0, topic_evictions=0, topic_hash=0, tag=0)
    buf = bytearray(h.serialize())
    buf[2] = 1  # non-zero incompatibility byte
    assert MsgBeHeader.deserialize(bytes(buf)) is None
    assert MsgRelHeader.deserialize(bytes(buf)) is None


def test_msg_short_buffer() -> None:
    assert MsgBeHeader.deserialize(b"\x00" * (HEADER_SIZE - 1)) is None
    assert MsgRelHeader.deserialize(b"") is None


# =====================================================================================================================
# MsgAckHeader (TYPE=2) and MsgNackHeader (TYPE=3)
# =====================================================================================================================


def test_msg_ack_roundtrip() -> None:
    h = MsgAckHeader(topic_hash=0xCAFEBABEDEADBEEF, tag=42)
    assert h.TYPE == 2
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 2
    out = MsgAckHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_msg_nack_roundtrip() -> None:
    h = MsgNackHeader(topic_hash=0x1111111111111111, tag=0xFFFFFFFFFFFFFFFF)
    assert h.TYPE == 3
    buf = h.serialize()
    assert buf[0] == 3
    out = MsgNackHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_msg_ack_incompatibility_rejection() -> None:
    """Bytes 4-7 must be zero; non-zero should cause rejection."""
    h = MsgAckHeader(topic_hash=0, tag=0)
    buf = bytearray(h.serialize())
    struct.pack_into("<I", buf, 4, 1)  # set incompatibility field
    assert MsgAckHeader.deserialize(bytes(buf)) is None
    assert MsgNackHeader.deserialize(bytes(buf)) is None


def test_msg_nack_incompatibility_rejection_large() -> None:
    h = MsgNackHeader(topic_hash=0, tag=0)
    buf = bytearray(h.serialize())
    struct.pack_into("<I", buf, 4, 0xFFFFFFFF)
    assert MsgNackHeader.deserialize(bytes(buf)) is None


def test_msg_ack_short_buffer() -> None:
    assert MsgAckHeader.deserialize(b"\x02" * 10) is None
    assert MsgNackHeader.deserialize(b"") is None


# =====================================================================================================================
# RspBeHeader (TYPE=4) and RspRelHeader (TYPE=5)
# =====================================================================================================================


def test_rsp_be_roundtrip() -> None:
    h = RspBeHeader(tag=0xAB, seqno=12345, topic_hash=0xDEADDEADDEADDEAD, message_tag=0x9999)
    assert h.TYPE == 4
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 4
    out = RspBeHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_rsp_rel_roundtrip() -> None:
    h = RspRelHeader(tag=0, seqno=0, topic_hash=0, message_tag=0)
    assert h.TYPE == 5
    buf = h.serialize()
    assert buf[0] == 5
    out = RspRelHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_rsp_seqno_48bit_truncation() -> None:
    max_48 = (1 << 48) - 1
    h = RspBeHeader(tag=0, seqno=max_48, topic_hash=0, message_tag=0)
    buf = h.serialize()
    out = RspBeHeader.deserialize(buf)
    assert out is not None
    assert out.seqno == max_48

    # A value exceeding 48 bits should be truncated to the lower 48 bits.
    over = (1 << 48) + 7
    h2 = RspRelHeader(tag=0, seqno=over, topic_hash=0, message_tag=0)
    buf2 = h2.serialize()
    out2 = RspRelHeader.deserialize(buf2)
    assert out2 is not None
    assert out2.seqno == 7


def test_rsp_tag_u8() -> None:
    h = RspBeHeader(tag=255, seqno=0, topic_hash=0, message_tag=0)
    buf = h.serialize()
    out = RspBeHeader.deserialize(buf)
    assert out is not None
    assert out.tag == 255

    # Tag exceeding u8 should be masked to lower 8 bits.
    h2 = RspRelHeader(tag=0x1FF, seqno=0, topic_hash=0, message_tag=0)
    buf2 = h2.serialize()
    out2 = RspRelHeader.deserialize(buf2)
    assert out2 is not None
    assert out2.tag == 0xFF


def test_rsp_short_buffer() -> None:
    assert RspBeHeader.deserialize(b"\x04" * 23) is None
    assert RspRelHeader.deserialize(b"") is None


# =====================================================================================================================
# RspAckHeader (TYPE=6) and RspNackHeader (TYPE=7)
# =====================================================================================================================


def test_rsp_ack_roundtrip() -> None:
    h = RspAckHeader(tag=42, seqno=999, topic_hash=0xABCDABCDABCDABCD, message_tag=0x5555)
    assert h.TYPE == 6
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 6
    out = RspAckHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_rsp_nack_roundtrip() -> None:
    h = RspNackHeader(tag=0xFF, seqno=0xFFFFFFFFFFFF, topic_hash=0xFFFFFFFFFFFFFFFF, message_tag=0xFFFFFFFFFFFFFFFF)
    assert h.TYPE == 7
    buf = h.serialize()
    assert buf[0] == 7
    out = RspNackHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_rsp_ack_short_buffer() -> None:
    assert RspAckHeader.deserialize(b"") is None
    assert RspNackHeader.deserialize(b"\x07" * 20) is None


# =====================================================================================================================
# GossipHeader (TYPE=8)
# =====================================================================================================================


def test_gossip_roundtrip() -> None:
    h = GossipHeader(topic_log_age=10, topic_hash=0xBEEFBEEFBEEFBEEF, topic_evictions=777, name_len=42)
    assert h.TYPE == 8
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 8
    out = GossipHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_gossip_signed_lage() -> None:
    h = GossipHeader(topic_log_age=-1, topic_hash=0, topic_evictions=0, name_len=0)
    buf = h.serialize()
    out = GossipHeader.deserialize(buf)
    assert out is not None
    assert out.topic_log_age == -1


def test_gossip_signed_lage_min() -> None:
    h = GossipHeader(topic_log_age=-1, topic_hash=0, topic_evictions=0, name_len=0)
    buf = h.serialize()
    out = GossipHeader.deserialize(buf)
    assert out is not None
    assert out.topic_log_age == -1


def test_gossip_lage_out_of_range_rejected() -> None:
    buf = bytearray(GossipHeader(topic_log_age=35, topic_hash=0, topic_evictions=0, name_len=0).serialize())
    buf[3] = 36
    assert GossipHeader.deserialize(bytes(buf)) is None


def test_gossip_short_buffer() -> None:
    assert GossipHeader.deserialize(b"\x08" * 10) is None


# =====================================================================================================================
# ScoutHeader (TYPE=9)
# =====================================================================================================================


def test_scout_roundtrip() -> None:
    h = ScoutHeader(pattern_len=100)
    assert h.TYPE == 9
    buf = h.serialize()
    assert len(buf) == HEADER_SIZE
    assert buf[0] == 9
    out = ScoutHeader.deserialize(buf)
    assert out is not None
    assert out == h


def test_scout_zero_pattern_len() -> None:
    h = ScoutHeader(pattern_len=0)
    buf = h.serialize()
    out = ScoutHeader.deserialize(buf)
    assert out is not None
    assert out.pattern_len == 0


def test_scout_max_pattern_len() -> None:
    h = ScoutHeader(pattern_len=255)
    buf = h.serialize()
    out = ScoutHeader.deserialize(buf)
    assert out is not None
    assert out.pattern_len == 255


def test_scout_reserved_bytes_8_15_nonzero() -> None:
    """Bytes 8-15 (u64) must be zero; non-zero should cause rejection."""
    h = ScoutHeader(pattern_len=0)
    buf = bytearray(h.serialize())
    buf[8] = 1
    assert ScoutHeader.deserialize(bytes(buf)) is None


def test_scout_reserved_bytes_4_7_nonzero() -> None:
    """Bytes 4-7 (u32) must be zero; non-zero should cause rejection."""
    h = ScoutHeader(pattern_len=0)
    buf = bytearray(h.serialize())
    buf[4] = 0xFF
    assert ScoutHeader.deserialize(bytes(buf)) is None


def test_scout_reserved_both_ranges_nonzero() -> None:
    h = ScoutHeader(pattern_len=5)
    buf = bytearray(h.serialize())
    struct.pack_into("<Q", buf, 8, 0xFFFFFFFFFFFFFFFF)
    struct.pack_into("<I", buf, 16, 0xFFFFFFFF)
    assert ScoutHeader.deserialize(bytes(buf)) is None


def test_scout_short_buffer() -> None:
    assert ScoutHeader.deserialize(b"\x09") is None
    assert ScoutHeader.deserialize(b"") is None


# =====================================================================================================================
# deserialize_header dispatcher
# =====================================================================================================================


def test_deserialize_header_dispatches_all_types() -> None:
    headers: list[
        MsgBeHeader
        | MsgRelHeader
        | MsgAckHeader
        | MsgNackHeader
        | RspBeHeader
        | RspRelHeader
        | RspAckHeader
        | RspNackHeader
        | GossipHeader
        | ScoutHeader
    ] = [
        MsgBeHeader(topic_log_age=1, topic_evictions=2, topic_hash=3, tag=4),
        MsgRelHeader(topic_log_age=-1, topic_evictions=0, topic_hash=0, tag=0),
        MsgAckHeader(topic_hash=123, tag=456),
        MsgNackHeader(topic_hash=789, tag=0),
        RspBeHeader(tag=10, seqno=20, topic_hash=30, message_tag=40),
        RspRelHeader(tag=0, seqno=1, topic_hash=2, message_tag=3),
        RspAckHeader(tag=1, seqno=2, topic_hash=3, message_tag=4),
        RspNackHeader(tag=5, seqno=6, topic_hash=7, message_tag=8),
        GossipHeader(topic_log_age=0, topic_hash=0, topic_evictions=0, name_len=0),
        ScoutHeader(pattern_len=50),
    ]
    for hdr in headers:
        buf = hdr.serialize()
        result = deserialize_header(buf)
        assert result is not None, f"Failed to deserialize {type(hdr).__name__}"
        assert result == hdr
        assert type(result) is type(hdr)


def test_deserialize_header_unknown_type() -> None:
    buf = bytearray(HEADER_SIZE)
    buf[0] = 10  # no header type 10
    assert deserialize_header(bytes(buf)) is None

    buf[0] = 255
    assert deserialize_header(bytes(buf)) is None


def test_deserialize_header_short_buffer() -> None:
    assert deserialize_header(b"") is None
    assert deserialize_header(b"\x00") is None
    assert deserialize_header(b"\x00" * (HEADER_SIZE - 1)) is None
