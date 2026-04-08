from __future__ import annotations

import struct
from dataclasses import dataclass

U64_MASK = 0xFFFFFFFFFFFFFFFF

HEADER_SIZE = 24
SEQNO48_MASK = (1 << 48) - 1
LAGE_MIN = -1
LAGE_MAX = 35


# =====================================================================================================================
# MSG headers
# =====================================================================================================================


@dataclass(frozen=True)
class MsgBeHeader:
    TYPE = 0

    topic_log_age: int
    topic_evictions: int
    topic_hash: int
    tag: int

    def serialize(self) -> bytes:
        return _serialize_msg(self.TYPE, self.topic_log_age, self.topic_evictions, self.topic_hash, self.tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> MsgBeHeader | None:
        r = _deserialize_msg(buf)
        return MsgBeHeader(*r) if r is not None else None


@dataclass(frozen=True)
class MsgRelHeader:
    TYPE = 1

    topic_log_age: int
    topic_evictions: int
    topic_hash: int
    tag: int

    def serialize(self) -> bytes:
        return _serialize_msg(self.TYPE, self.topic_log_age, self.topic_evictions, self.topic_hash, self.tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> MsgRelHeader | None:
        r = _deserialize_msg(buf)
        return MsgRelHeader(*r) if r is not None else None


def _serialize_msg(ty: int, lage: int, evictions: int, topic_hash: int, tag: int) -> bytes:
    buf = bytearray(HEADER_SIZE)
    buf[0] = ty
    buf[3] = lage & 0xFF
    struct.pack_into("<I", buf, 4, evictions & 0xFFFFFFFF)
    struct.pack_into("<Q", buf, 8, topic_hash & U64_MASK)
    struct.pack_into("<Q", buf, 16, tag & U64_MASK)
    return bytes(buf)


def _deserialize_msg(buf: bytes | memoryview) -> tuple[int, int, int, int] | None:
    if len(buf) < HEADER_SIZE:
        return None
    if buf[2] != 0:  # incompatibility
        return None
    lage = struct.unpack_from("<b", buf, 3)[0]
    if not (LAGE_MIN <= lage <= LAGE_MAX):
        return None
    evictions = struct.unpack_from("<I", buf, 4)[0]
    topic_hash = struct.unpack_from("<Q", buf, 8)[0]
    tag = struct.unpack_from("<Q", buf, 16)[0]
    return (lage, evictions, topic_hash, tag)


# =====================================================================================================================
# MSG ACK/NACK headers
# =====================================================================================================================


@dataclass(frozen=True)
class MsgAckHeader:
    TYPE = 2

    topic_hash: int
    tag: int

    def serialize(self) -> bytes:
        return _serialize_msg_ack(self.TYPE, self.topic_hash, self.tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> MsgAckHeader | None:
        r = _deserialize_msg_ack(buf)
        return MsgAckHeader(*r) if r is not None else None


@dataclass(frozen=True)
class MsgNackHeader:
    TYPE = 3

    topic_hash: int
    tag: int

    def serialize(self) -> bytes:
        return _serialize_msg_ack(self.TYPE, self.topic_hash, self.tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> MsgNackHeader | None:
        r = _deserialize_msg_ack(buf)
        return MsgNackHeader(*r) if r is not None else None


def _serialize_msg_ack(ty: int, topic_hash: int, tag: int) -> bytes:
    buf = bytearray(HEADER_SIZE)
    buf[0] = ty
    struct.pack_into("<Q", buf, 8, topic_hash & U64_MASK)
    struct.pack_into("<Q", buf, 16, tag & U64_MASK)
    return bytes(buf)


def _deserialize_msg_ack(buf: bytes | memoryview) -> tuple[int, int] | None:
    if len(buf) < HEADER_SIZE:
        return None
    if struct.unpack_from("<I", buf, 4)[0] != 0:  # incompatibility
        return None
    topic_hash = struct.unpack_from("<Q", buf, 8)[0]
    tag = struct.unpack_from("<Q", buf, 16)[0]
    return (topic_hash, tag)


# =====================================================================================================================
# RSP headers (responses)
# =====================================================================================================================


@dataclass(frozen=True)
class RspBeHeader:
    TYPE = 4

    tag: int  # u8
    seqno: int  # u48
    topic_hash: int
    message_tag: int

    def serialize(self) -> bytes:
        return _serialize_rsp(self.TYPE, self.tag, self.seqno, self.topic_hash, self.message_tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> RspBeHeader | None:
        r = _deserialize_rsp(buf)
        return RspBeHeader(*r) if r is not None else None


@dataclass(frozen=True)
class RspRelHeader:
    TYPE = 5

    tag: int
    seqno: int
    topic_hash: int
    message_tag: int

    def serialize(self) -> bytes:
        return _serialize_rsp(self.TYPE, self.tag, self.seqno, self.topic_hash, self.message_tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> RspRelHeader | None:
        r = _deserialize_rsp(buf)
        return RspRelHeader(*r) if r is not None else None


# =====================================================================================================================
# RSP ACK/NACK headers
# =====================================================================================================================


@dataclass(frozen=True)
class RspAckHeader:
    TYPE = 6

    tag: int
    seqno: int
    topic_hash: int
    message_tag: int

    def serialize(self) -> bytes:
        return _serialize_rsp(self.TYPE, self.tag, self.seqno, self.topic_hash, self.message_tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> RspAckHeader | None:
        r = _deserialize_rsp(buf)
        return RspAckHeader(*r) if r is not None else None


@dataclass(frozen=True)
class RspNackHeader:
    TYPE = 7

    tag: int
    seqno: int
    topic_hash: int
    message_tag: int

    def serialize(self) -> bytes:
        return _serialize_rsp(self.TYPE, self.tag, self.seqno, self.topic_hash, self.message_tag)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> RspNackHeader | None:
        r = _deserialize_rsp(buf)
        return RspNackHeader(*r) if r is not None else None


def _serialize_rsp(ty: int, tag: int, seqno: int, topic_hash: int, message_tag: int) -> bytes:
    buf = bytearray(HEADER_SIZE)
    buf[0] = ty
    buf[1] = tag & 0xFF
    seqno48 = seqno & SEQNO48_MASK
    for i in range(6):
        buf[2 + i] = (seqno48 >> (i * 8)) & 0xFF
    struct.pack_into("<Q", buf, 8, topic_hash & U64_MASK)
    struct.pack_into("<Q", buf, 16, message_tag & U64_MASK)
    return bytes(buf)


def _deserialize_rsp(buf: bytes | memoryview) -> tuple[int, int, int, int] | None:
    if len(buf) < HEADER_SIZE:
        return None
    tag = buf[1]
    seqno = 0
    for i in range(6):
        seqno |= buf[2 + i] << (i * 8)
    topic_hash = struct.unpack_from("<Q", buf, 8)[0]
    message_tag = struct.unpack_from("<Q", buf, 16)[0]
    return (tag, seqno, topic_hash, message_tag)


# =====================================================================================================================
# GOSSIP header
# =====================================================================================================================


@dataclass(frozen=True)
class GossipHeader:
    TYPE = 8

    topic_log_age: int
    topic_hash: int
    topic_evictions: int
    name_len: int

    def serialize(self) -> bytes:
        buf = bytearray(HEADER_SIZE)
        buf[0] = self.TYPE
        buf[3] = self.topic_log_age & 0xFF
        struct.pack_into("<Q", buf, 8, self.topic_hash & U64_MASK)
        struct.pack_into("<I", buf, 16, self.topic_evictions & 0xFFFFFFFF)
        buf[23] = self.name_len & 0xFF
        return bytes(buf)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> GossipHeader | None:
        if len(buf) < HEADER_SIZE:
            return None
        if struct.unpack_from("<I", buf, 4)[0] != 0:
            return None
        lage = struct.unpack_from("<b", buf, 3)[0]
        if not (LAGE_MIN <= lage <= LAGE_MAX):
            return None
        topic_hash = struct.unpack_from("<Q", buf, 8)[0]
        evictions = struct.unpack_from("<I", buf, 16)[0]
        name_len = buf[23]
        return GossipHeader(lage, topic_hash, evictions, name_len)


# =====================================================================================================================
# SCOUT header
# =====================================================================================================================


@dataclass(frozen=True)
class ScoutHeader:
    TYPE = 9

    pattern_len: int

    def serialize(self) -> bytes:
        buf = bytearray(HEADER_SIZE)
        buf[0] = self.TYPE
        buf[23] = self.pattern_len & 0xFF
        return bytes(buf)

    @staticmethod
    def deserialize(buf: bytes | memoryview) -> ScoutHeader | None:
        if len(buf) < HEADER_SIZE:
            return None
        if struct.unpack_from("<I", buf, 4)[0] != 0:
            return None
        if struct.unpack_from("<Q", buf, 8)[0] != 0:
            return None
        return ScoutHeader(buf[23])


# =====================================================================================================================
# Dispatcher
# =====================================================================================================================

HeaderType = (
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
)


def deserialize_header(buf: bytes | memoryview) -> HeaderType | None:
    """Deserialize a 24-byte session-layer header. Returns None on validation failure."""
    if len(buf) < 1:
        return None
    ty = buf[0]
    if ty == 0:
        return MsgBeHeader.deserialize(buf)
    if ty == 1:
        return MsgRelHeader.deserialize(buf)
    if ty == 2:
        return MsgAckHeader.deserialize(buf)
    if ty == 3:
        return MsgNackHeader.deserialize(buf)
    if ty == 4:
        return RspBeHeader.deserialize(buf)
    if ty == 5:
        return RspRelHeader.deserialize(buf)
    if ty == 6:
        return RspAckHeader.deserialize(buf)
    if ty == 7:
        return RspNackHeader.deserialize(buf)
    if ty == 8:
        return GossipHeader.deserialize(buf)
    if ty == 9:
        return ScoutHeader.deserialize(buf)
    return None
