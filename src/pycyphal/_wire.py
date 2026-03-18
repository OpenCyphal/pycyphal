from __future__ import annotations

import math
import struct
from enum import IntEnum

from ._hash import rapidhash

SUBJECT_ID_PINNED_MAX = 0x1FFF  # 8191
HEADER_SIZE = 24
SEQNO48_MASK = (1 << 48) - 1


class HeaderType(IntEnum):
    MSG_BE = 0
    MSG_REL = 1
    MSG_ACK = 2
    MSG_NACK = 3
    RSP_BE = 4
    RSP_REL = 5
    RSP_ACK = 6
    RSP_NACK = 7
    GOSSIP = 8
    SCOUT = 9


# =====================================================================================================================
# Hashing
# =====================================================================================================================


def _parse_hash_override(name: str) -> int | None:
    """Parse 'name#hexdigits' syntax. Returns hash value or None."""
    idx = name.rfind("#")
    if idx < 0 or idx == len(name) - 1:
        return None
    hex_part = name[idx + 1 :]
    if len(hex_part) > 16:
        return None
    try:
        for c in hex_part:
            if c not in "0123456789abcdef":
                return None
        return int(hex_part, 16)
    except ValueError:
        return None


def topic_hash(name: str) -> int:
    """Compute topic hash: pinned override or rapidhash."""
    override = _parse_hash_override(name)
    if override is not None:
        return override
    return rapidhash(name.encode())


def is_pinned(h: int) -> bool:
    return h <= SUBJECT_ID_PINNED_MAX


# =====================================================================================================================
# Subject-ID computation
# =====================================================================================================================


def topic_subject_id(h: int, evictions: int, modulus: int) -> int:
    if is_pinned(h):
        return h
    # Use 64-bit wrapping arithmetic for (hash + evictions^2) like the C implementation
    raw = (h + evictions * evictions) % (1 << 64)
    return SUBJECT_ID_PINNED_MAX + 1 + (raw % modulus)


def subject_id_max(modulus: int) -> int:
    return SUBJECT_ID_PINNED_MAX + modulus


def broadcast_subject_id(modulus: int) -> int:
    return (1 << (subject_id_max(modulus).bit_length())) - 1


def gossip_shard_count(modulus: int) -> int:
    return broadcast_subject_id(modulus) - (subject_id_max(modulus) + 1)


def gossip_shard_subject_id(topic_hash_val: int, modulus: int) -> int:
    count = gossip_shard_count(modulus)
    shard_index = topic_hash_val % count
    return subject_id_max(modulus) + 1 + shard_index


# =====================================================================================================================
# CRDT helpers
# =====================================================================================================================

LAGE_MIN = -1
LAGE_MAX = 35


def log_age(origin_s: float, now_s: float) -> int:
    """floor(log2(max(0, now - origin))), clamped to [LAGE_MIN, LAGE_MAX]."""
    diff = now_s - origin_s
    if diff <= 0:
        return LAGE_MIN
    result = int(math.floor(math.log2(diff)))
    return max(LAGE_MIN, min(LAGE_MAX, result))


def left_wins(l_lage: int, l_hash: int, r_lage: int, r_hash: int) -> bool:
    """Older (higher log-age) wins; on tie, higher hash wins."""
    if l_lage != r_lage:
        return l_lage > r_lage
    return l_hash > r_hash


# =====================================================================================================================
# Header serialization
# =====================================================================================================================


def pack_msg_header(msg_type: HeaderType, lage: int, evictions: int, topic_hash_val: int, tag: int) -> bytes:
    """
    Pack MSG_BE or MSG_REL header (24 bytes).
    Layout: type(1) void(1) incompatibility(1) lage(1) evictions(4) hash(8) tag(8)
    """
    buf = bytearray(HEADER_SIZE)
    buf[0] = msg_type
    buf[3] = lage & 0xFF  # signed byte
    struct.pack_into("<I", buf, 4, evictions & 0xFFFFFFFF)
    struct.pack_into("<Q", buf, 8, topic_hash_val & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into("<Q", buf, 16, tag & 0xFFFFFFFFFFFFFFFF)
    return bytes(buf)


def pack_gossip_header(lage: int, topic_hash_val: int, evictions: int, name_len: int) -> bytes:
    """
    Pack GOSSIP header (24 bytes).
    Layout: type(1) void(2) lage(1) incompatibility(4) hash(8) evictions(4) void(3) name_len(1)
    """
    buf = bytearray(HEADER_SIZE)
    buf[0] = HeaderType.GOSSIP
    buf[3] = lage & 0xFF
    # incompatibility at offset 4 = 0
    struct.pack_into("<Q", buf, 8, topic_hash_val & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into("<I", buf, 16, evictions & 0xFFFFFFFF)
    buf[HEADER_SIZE - 1] = name_len & 0xFF
    return bytes(buf)


def pack_scout_header(pattern_len: int) -> bytes:
    """
    Pack SCOUT header (24 bytes).
    Layout: type(1) void(3) incompatibility(4) incompatibility1(8) void(7) pattern_len(1)
    """
    buf = bytearray(HEADER_SIZE)
    buf[0] = HeaderType.SCOUT
    buf[HEADER_SIZE - 1] = pattern_len & 0xFF
    return bytes(buf)


def pack_ack_header(ack_type: HeaderType, topic_hash_val: int, tag: int) -> bytes:
    """
    Pack MSG_ACK or MSG_NACK header (24 bytes).
    Layout: type(1) void(3) incompatibility(4) hash(8) tag(8)
    """
    buf = bytearray(HEADER_SIZE)
    buf[0] = ack_type
    struct.pack_into("<Q", buf, 8, topic_hash_val & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into("<Q", buf, 16, tag & 0xFFFFFFFFFFFFFFFF)
    return bytes(buf)


def pack_rsp_header(rsp_type: HeaderType, tag: int, seqno: int, topic_hash_val: int, message_tag: int) -> bytes:
    """
    Pack RSP_BE or RSP_REL header (24 bytes).
    Layout: type(1) tag(1) seqno(6) hash(8) message_tag(8)
    """
    buf = bytearray(HEADER_SIZE)
    buf[0] = rsp_type
    buf[1] = tag & 0xFF
    # seqno is 48-bit little-endian at offset 2
    for i in range(6):
        buf[2 + i] = (seqno >> (i * 8)) & 0xFF
    struct.pack_into("<Q", buf, 8, topic_hash_val & 0xFFFFFFFFFFFFFFFF)
    struct.pack_into("<Q", buf, 16, message_tag & 0xFFFFFFFFFFFFFFFF)
    return bytes(buf)


def pack_rsp_ack_header(ack_type: HeaderType, tag: int, seqno: int, topic_hash_val: int, message_tag: int) -> bytes:
    """Same layout as RSP header for RSP_ACK/RSP_NACK (no payload)."""
    return pack_rsp_header(ack_type, tag, seqno, topic_hash_val, message_tag)


def unpack_header(data: bytes | bytearray | memoryview) -> dict:
    """
    Unpack a 24-byte header, dispatching on the type byte.
    Returns a dict with at least 'type' key.
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Header too short: {len(data)} < {HEADER_SIZE}")

    msg_type = HeaderType(data[0])
    result: dict = {"type": msg_type}

    if msg_type in (HeaderType.MSG_BE, HeaderType.MSG_REL):
        result["incompatibility"] = data[2]
        result["lage"] = struct.unpack_from("b", data, 3)[0]
        result["evictions"] = struct.unpack_from("<I", data, 4)[0]
        result["hash"] = struct.unpack_from("<Q", data, 8)[0]
        result["tag"] = struct.unpack_from("<Q", data, 16)[0]

    elif msg_type in (HeaderType.MSG_ACK, HeaderType.MSG_NACK):
        result["incompatibility"] = struct.unpack_from("<I", data, 4)[0]
        result["hash"] = struct.unpack_from("<Q", data, 8)[0]
        result["tag"] = struct.unpack_from("<Q", data, 16)[0]

    elif msg_type in (HeaderType.RSP_BE, HeaderType.RSP_REL):
        result["tag"] = data[1]
        seqno = 0
        for i in range(6):
            seqno |= data[2 + i] << (i * 8)
        result["seqno"] = seqno
        result["hash"] = struct.unpack_from("<Q", data, 8)[0]
        result["message_tag"] = struct.unpack_from("<Q", data, 16)[0]

    elif msg_type in (HeaderType.RSP_ACK, HeaderType.RSP_NACK):
        result["tag"] = data[1]
        seqno = 0
        for i in range(6):
            seqno |= data[2 + i] << (i * 8)
        result["seqno"] = seqno
        result["hash"] = struct.unpack_from("<Q", data, 8)[0]
        result["message_tag"] = struct.unpack_from("<Q", data, 16)[0]

    elif msg_type == HeaderType.GOSSIP:
        result["lage"] = struct.unpack_from("b", data, 3)[0]
        result["incompatibility"] = struct.unpack_from("<I", data, 4)[0]
        result["hash"] = struct.unpack_from("<Q", data, 8)[0]
        result["evictions"] = struct.unpack_from("<I", data, 16)[0]
        result["name_len"] = data[HEADER_SIZE - 1]

    elif msg_type == HeaderType.SCOUT:
        result["incompatibility"] = struct.unpack_from("<I", data, 4)[0]
        result["incompatibility1"] = struct.unpack_from("<Q", data, 8)[0]
        result["pattern_len"] = data[HEADER_SIZE - 1]

    return result
