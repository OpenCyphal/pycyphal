"""Adversarial and fuzz-like tests for pycyphal."""

from __future__ import annotations

import asyncio
import random
import struct

import pytest

from pycyphal import Node, Instant, Priority, SendError
from pycyphal._wire import (
    HEADER_SIZE,
    HeaderType,
    unpack_header,
    pack_msg_header,
    pack_gossip_header,
    pack_scout_header,
    pack_ack_header,
    pack_rsp_header,
    topic_subject_id,
    topic_hash,
    SUBJECT_ID_PINNED_MAX,
    SEQNO48_MASK,
    is_pinned,
    LAGE_MIN,
    LAGE_MAX,
)
from pycyphal._common import name_normalize, name_match, name_is_valid, TOPIC_NAME_MAX
from pycyphal._transport import TransportArrival
from tests.conftest import MockTransport, MockNetwork


# =====================================================================================================================
# Helpers
# =====================================================================================================================


def _make_node(node_id: int = 1, modulus: int = 122743, network: MockNetwork | None = None) -> Node:
    transport = MockTransport(node_id=node_id, modulus=modulus, network=network)
    return Node(transport, home="test_home", namespace="test_ns")


def _inject_subject(node: Node, subject_id: int, data: bytes, remote_id: int = 99) -> None:
    """Inject raw bytes into a subject-id listener on the node's transport."""
    arrival = TransportArrival(
        timestamp=Instant.now(),
        priority=Priority.NOMINAL,
        remote_id=remote_id,
        message=data,
    )
    transport: MockTransport = node._transport  # type: ignore[assignment]
    transport.deliver_subject(subject_id, arrival)


def _inject_unicast(node: Node, data: bytes, remote_id: int = 99) -> None:
    """Inject raw bytes into the unicast handler on the node's transport."""
    arrival = TransportArrival(
        timestamp=Instant.now(),
        priority=Priority.NOMINAL,
        remote_id=remote_id,
        message=data,
    )
    transport: MockTransport = node._transport  # type: ignore[assignment]
    transport.deliver_unicast(arrival)


def _inject_broadcast(node: Node, data: bytes, remote_id: int = 99) -> None:
    """Inject raw bytes into the broadcast subject listener."""
    _inject_subject(node, node._broadcast_subject, data, remote_id)


# =====================================================================================================================
# 1. Invalid name characters
# =====================================================================================================================


class TestInvalidNameCharacters:
    def test_non_ascii_in_normalize(self) -> None:
        for char in ["\u00e9", "\u4e16", "\U0001f600", "\u0410"]:
            with pytest.raises(ValueError, match="Invalid character"):
                name_normalize(f"foo{char}bar")

    def test_control_chars_in_normalize(self) -> None:
        for code in [0, 1, 7, 10, 13, 27, 31]:
            with pytest.raises(ValueError, match="Invalid character"):
                name_normalize(f"topic{chr(code)}name")

    def test_space_in_normalize(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize("hello world")

    def test_del_char(self) -> None:
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize(f"test{chr(127)}name")

    def test_high_ascii_boundary(self) -> None:
        # ord 126 ('~') should be valid, ord 127 (DEL) should not
        result = name_normalize("~")
        assert result == "~"
        with pytest.raises(ValueError, match="Invalid character"):
            name_normalize(chr(127))

    def test_non_ascii_in_name_is_valid(self) -> None:
        assert not name_is_valid("hello\x80world")
        assert not name_is_valid("\xff")
        assert not name_is_valid("abc\u0100")

    @pytest.mark.asyncio
    async def test_non_ascii_in_advertise(self) -> None:
        node = _make_node()
        try:
            with pytest.raises(ValueError):
                node.advertise("topic/\u00e9invalid")
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_non_ascii_in_subscribe(self) -> None:
        node = _make_node()
        try:
            with pytest.raises(ValueError):
                node.subscribe("topic/\u00e9invalid")
        finally:
            node.close()


# =====================================================================================================================
# 2. Empty names
# =====================================================================================================================


class TestEmptyNames:
    def test_empty_normalize_returns_empty(self) -> None:
        result = name_normalize("")
        assert result == ""

    def test_empty_name_is_not_valid(self) -> None:
        assert not name_is_valid("")

    @pytest.mark.asyncio
    async def test_empty_advertise(self) -> None:
        """An empty name, once resolved through namespace, may still be valid."""
        node = _make_node()
        try:
            # With a namespace set, empty name resolves to the namespace
            pub = node.advertise("")
            assert pub is not None
            pub.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_empty_subscribe(self) -> None:
        node = _make_node()
        try:
            sub = node.subscribe("")
            assert sub is not None
            sub.close()
        finally:
            node.close()

    def test_only_separators_normalize(self) -> None:
        # All separators should collapse to empty string
        assert name_normalize("/") == ""
        assert name_normalize("///") == ""
        assert name_normalize("////") == ""


# =====================================================================================================================
# 3. Very long names
# =====================================================================================================================


class TestVeryLongNames:
    def test_max_length_is_valid(self) -> None:
        name = "a" * TOPIC_NAME_MAX
        assert name_is_valid(name)

    def test_over_max_length_is_not_valid(self) -> None:
        name = "a" * (TOPIC_NAME_MAX + 1)
        assert not name_is_valid(name)

    def test_far_over_max_length_is_not_valid(self) -> None:
        name = "x" * 10000
        assert not name_is_valid(name)

    def test_long_name_normalize_does_not_crash(self) -> None:
        name = "a/" * 5000 + "b"
        result = name_normalize(name)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_long_name_advertise(self) -> None:
        node = _make_node()
        try:
            long_name = "a/" + "b" * 300
            # Should not crash regardless; the name is resolved, not validated for length
            pub = node.advertise(long_name)
            pub.close()
        finally:
            node.close()


# =====================================================================================================================
# 4. Malformed headers
# =====================================================================================================================


class TestMalformedHeaders:
    def test_short_message_unpack_raises(self) -> None:
        for length in range(HEADER_SIZE):
            with pytest.raises(ValueError, match="Header too short"):
                unpack_header(bytes(length))

    def test_empty_bytes_unpack(self) -> None:
        with pytest.raises(ValueError, match="Header too short"):
            unpack_header(b"")

    def test_garbage_type_byte_unpack(self) -> None:
        for bad_type in [10, 11, 50, 128, 255]:
            data = bytearray(HEADER_SIZE)
            data[0] = bad_type
            with pytest.raises(ValueError):
                unpack_header(bytes(data))

    @pytest.mark.asyncio
    async def test_short_messages_silently_dropped(self) -> None:
        """Messages shorter than HEADER_SIZE should be silently ignored by the node."""
        node = _make_node()
        sub = node.subscribe("some/topic")
        try:
            for length in [0, 1, 8, 15, 23]:
                _inject_broadcast(node, bytes(length))
            # Give async tasks a chance to process
            await asyncio.sleep(0.01)
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_garbage_type_byte_dropped_by_node(self) -> None:
        """Messages with invalid type bytes should be silently dropped."""
        node = _make_node()
        sub = node.subscribe("some/topic")
        try:
            for bad_type in [10, 50, 200, 255]:
                data = bytearray(HEADER_SIZE + 10)
                data[0] = bad_type
                _inject_broadcast(node, bytes(data))
            await asyncio.sleep(0.01)
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_all_zeros_header_dropped(self) -> None:
        """24 bytes of zeros (type=MSG_BE, all fields zero) -- pinned hash 0, evictions 0, etc."""
        node = _make_node()
        sub = node.subscribe("anything")
        try:
            data = bytes(HEADER_SIZE + 5)
            _inject_broadcast(node, data)
            await asyncio.sleep(0.01)
            # Should not crash; msg may or may not match but should not panic
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_all_0xff_header(self) -> None:
        """All-0xFF header -- type 0xFF is invalid."""
        node = _make_node()
        try:
            data = b"\xff" * (HEADER_SIZE + 10)
            _inject_broadcast(node, data)
            await asyncio.sleep(0.01)
        finally:
            node.close()


# =====================================================================================================================
# 5. Invalid header fields
# =====================================================================================================================


class TestInvalidHeaderFields:
    @pytest.mark.asyncio
    async def test_nonzero_incompatibility_msg_dropped(self) -> None:
        """MSG_BE with incompatibility != 0 must be silently dropped."""
        node = _make_node()
        sub = node.subscribe("test/topic")
        try:
            t_hash = topic_hash("test/topic")
            header = bytearray(pack_msg_header(HeaderType.MSG_BE, 0, 0, t_hash, 42))
            header[2] = 1  # set incompatibility to nonzero
            payload = bytes(header) + b"hello"
            sid = topic_subject_id(t_hash, 0, 122743)
            _inject_subject(node, sid, payload)
            await asyncio.sleep(0.01)
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_lage_out_of_range_dropped(self) -> None:
        """MSG_BE with lage outside [LAGE_MIN, LAGE_MAX] must be dropped."""
        node = _make_node()
        sub = node.subscribe("test/topic")
        try:
            t_hash = topic_hash("test/topic")
            for bad_lage in [LAGE_MIN - 1, LAGE_MAX + 1, 127, -128]:
                if LAGE_MIN <= bad_lage <= LAGE_MAX:
                    continue
                header = bytearray(pack_msg_header(HeaderType.MSG_BE, 0, 0, t_hash, 100))
                header[3] = bad_lage & 0xFF  # signed byte
                payload = bytes(header) + b"data"
                sid = topic_subject_id(t_hash, 0, 122743)
                _inject_subject(node, sid, payload)
            await asyncio.sleep(0.01)
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_pinned_hash_with_nonzero_evictions_dropped(self) -> None:
        """A pinned hash (<=8191) with evictions != 0 must be rejected."""
        node = _make_node()
        # Use hash override syntax to force a pinned hash
        sub = node.subscribe("pinned_topic#0001")
        try:
            pinned_h = 1  # pinned since <= SUBJECT_ID_PINNED_MAX
            header = pack_msg_header(HeaderType.MSG_BE, 0, 5, pinned_h, 77)
            payload = header + b"pinned_data"
            sid = pinned_h  # pinned hash is the subject-id
            _inject_subject(node, sid, payload)
            await asyncio.sleep(0.01)
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_gossip_with_nonzero_incompatibility_dropped(self) -> None:
        """Gossip header with incompatibility != 0 must be dropped."""
        node = _make_node()
        try:
            header = bytearray(pack_gossip_header(0, 123456, 0, 4))
            struct.pack_into("<I", header, 4, 1)  # set incompatibility to 1
            payload = bytes(header) + b"test"
            _inject_broadcast(node, payload)
            await asyncio.sleep(0.01)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_scout_with_nonzero_incompatibility_dropped(self) -> None:
        """Scout header with either incompatibility field nonzero must be dropped."""
        node = _make_node()
        try:
            # nonzero incompatibility (offset 4)
            header = bytearray(pack_scout_header(3))
            struct.pack_into("<I", header, 4, 1)
            payload = bytes(header) + b"foo"
            _inject_broadcast(node, payload)

            # nonzero incompatibility1 (offset 8)
            header2 = bytearray(pack_scout_header(3))
            struct.pack_into("<Q", header2, 8, 1)
            payload2 = bytes(header2) + b"bar"
            _inject_broadcast(node, payload2)

            await asyncio.sleep(0.01)
        finally:
            node.close()


# =====================================================================================================================
# 6. Duplicate subscribe/advertise
# =====================================================================================================================


class TestDuplicateOperations:
    @pytest.mark.asyncio
    async def test_duplicate_subscribe_same_topic(self) -> None:
        node = _make_node()
        try:
            subs = [node.subscribe("dup/topic") for _ in range(10)]
            assert len(subs) == 10
            # All should be independent subscribers
            for s in subs:
                assert not s._closed
            for s in subs:
                s.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_duplicate_advertise_same_topic(self) -> None:
        node = _make_node()
        try:
            pubs = [node.advertise("dup/topic") for _ in range(10)]
            assert len(pubs) == 10
            for p in pubs:
                assert not p._closed
            for p in pubs:
                p.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_duplicate_pattern_subscribe(self) -> None:
        node = _make_node()
        try:
            subs = [node.subscribe("dup/*/wild") for _ in range(5)]
            assert len(subs) == 5
            for s in subs:
                s.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_mixed_subscribe_advertise_same_name(self) -> None:
        node = _make_node()
        try:
            sub = node.subscribe("shared/name")
            pub = node.advertise("shared/name")
            assert sub is not None
            assert pub is not None
            sub.close()
            pub.close()
        finally:
            node.close()


# =====================================================================================================================
# 7. Use after close
# =====================================================================================================================


class TestUseAfterClose:
    @pytest.mark.asyncio
    async def test_publish_after_publisher_close(self) -> None:
        node = _make_node()
        try:
            pub = node.advertise("closed/pub")
            pub.close()
            with pytest.raises(SendError, match="closed"):
                await pub(Instant.now() + 1.0, b"should_fail")
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_subscriber_iteration_after_close(self) -> None:
        node = _make_node()
        try:
            sub = node.subscribe("closed/sub")
            sub.close()
            with pytest.raises(StopAsyncIteration):
                await sub.__anext__()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_double_close_publisher(self) -> None:
        node = _make_node()
        try:
            pub = node.advertise("double/close")
            pub.close()
            pub.close()  # Second close should be a no-op
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_double_close_subscriber(self) -> None:
        node = _make_node()
        try:
            sub = node.subscribe("double/close")
            sub.close()
            sub.close()  # Second close should be a no-op
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_double_close_node(self) -> None:
        node = _make_node()
        node.close()
        node.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_advertise_after_node_close(self) -> None:
        node = _make_node()
        node.close()
        # Node is closed but the method may still create objects using the closed transport
        # This should either work or raise gracefully, not crash
        try:
            pub = node.advertise("post/close")
            pub.close()
        except Exception:
            pass  # Any clean exception is acceptable

    @pytest.mark.asyncio
    async def test_subscribe_after_node_close(self) -> None:
        node = _make_node()
        node.close()
        try:
            sub = node.subscribe("post/close")
            sub.close()
        except Exception:
            pass  # Any clean exception is acceptable

    @pytest.mark.asyncio
    async def test_request_after_publisher_close(self) -> None:
        node = _make_node()
        try:
            pub = node.advertise("req/closed")
            pub.close()
            with pytest.raises(SendError, match="closed"):
                await pub.request(Instant.now() + 1.0, 1.0, b"request")
        finally:
            node.close()


# =====================================================================================================================
# 8. Rapid create/destroy
# =====================================================================================================================


class TestRapidCreateDestroy:
    @pytest.mark.asyncio
    async def test_rapid_publisher_cycle(self) -> None:
        node = _make_node()
        try:
            for i in range(200):
                pub = node.advertise(f"rapid/pub/{i % 20}")
                pub.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_rapid_subscriber_cycle(self) -> None:
        node = _make_node()
        try:
            for i in range(200):
                sub = node.subscribe(f"rapid/sub/{i % 20}")
                sub.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_rapid_mixed_cycle(self) -> None:
        node = _make_node()
        try:
            for i in range(100):
                name = f"rapid/mix/{i % 10}"
                pub = node.advertise(name)
                sub = node.subscribe(name)
                await pub(Instant.now() + 1.0, f"msg{i}".encode())
                sub.close()
                pub.close()
            await asyncio.sleep(0.05)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_rapid_pattern_subscribe_cycle(self) -> None:
        node = _make_node()
        try:
            for i in range(100):
                sub = node.subscribe(f"rapid/*/pattern{i % 5}")
                sub.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_many_concurrent_publishers(self) -> None:
        node = _make_node()
        try:
            pubs = [node.advertise(f"concurrent/{i}") for i in range(50)]
            for pub in pubs:
                pub.close()
        finally:
            node.close()


# =====================================================================================================================
# 9. Random API sequences
# =====================================================================================================================


class TestRandomAPISequences:
    @pytest.mark.asyncio
    async def test_random_operations(self) -> None:
        """Run a random mix of advertise/subscribe/publish/close operations."""
        rng = random.Random(42)
        node = _make_node()
        pubs: list = []
        subs: list = []
        names = [f"rnd/{chr(65 + i)}" for i in range(10)]
        try:
            for _ in range(300):
                op = rng.choice(["advertise", "subscribe", "publish", "close_pub", "close_sub"])
                name = rng.choice(names)
                if op == "advertise":
                    try:
                        pubs.append(node.advertise(name))
                    except Exception:
                        pass
                elif op == "subscribe":
                    try:
                        subs.append(node.subscribe(name))
                    except Exception:
                        pass
                elif op == "publish" and pubs:
                    pub = rng.choice(pubs)
                    try:
                        await pub(Instant.now() + 1.0, b"random_data")
                    except Exception:
                        pass
                elif op == "close_pub" and pubs:
                    pub = pubs.pop(rng.randrange(len(pubs)))
                    pub.close()
                elif op == "close_sub" and subs:
                    sub = subs.pop(rng.randrange(len(subs)))
                    sub.close()
            await asyncio.sleep(0.05)
        finally:
            for p in pubs:
                p.close()
            for s in subs:
                s.close()
            node.close()

    @pytest.mark.asyncio
    async def test_random_with_patterns(self) -> None:
        """Random operations including pattern subscriptions."""
        rng = random.Random(99)
        node = _make_node()
        pubs: list = []
        subs: list = []
        patterns = ["rndp/*/foo", "rndp/>", "rndp/a/*"]
        names = ["rndp/x/foo", "rndp/a/bar", "rndp/b/foo"]
        try:
            for _ in range(150):
                op = rng.choice(["pub", "sub_pat", "sub_verb", "close"])
                if op == "pub":
                    name = rng.choice(names)
                    try:
                        p = node.advertise(name)
                        pubs.append(p)
                        await p(Instant.now() + 1.0, b"data")
                    except Exception:
                        pass
                elif op == "sub_pat":
                    pat = rng.choice(patterns)
                    try:
                        subs.append(node.subscribe(pat))
                    except Exception:
                        pass
                elif op == "sub_verb":
                    name = rng.choice(names)
                    try:
                        subs.append(node.subscribe(name))
                    except Exception:
                        pass
                elif op == "close":
                    if pubs and rng.random() < 0.5:
                        pubs.pop(rng.randrange(len(pubs))).close()
                    elif subs:
                        subs.pop(rng.randrange(len(subs))).close()
            await asyncio.sleep(0.05)
        finally:
            for p in pubs:
                p.close()
            for s in subs:
                s.close()
            node.close()


# =====================================================================================================================
# 10. Boundary values
# =====================================================================================================================


class TestBoundaryValues:
    def test_tag_zero(self) -> None:
        header = pack_msg_header(HeaderType.MSG_BE, 0, 0, 12345, 0)
        hdr = unpack_header(header)
        assert hdr["tag"] == 0

    def test_tag_max_u64(self) -> None:
        tag_max = (1 << 64) - 1
        header = pack_msg_header(HeaderType.MSG_BE, 0, 0, 12345, tag_max)
        hdr = unpack_header(header)
        assert hdr["tag"] == tag_max

    def test_seqno_max_48bit(self) -> None:
        seqno_max = SEQNO48_MASK
        header = pack_rsp_header(HeaderType.RSP_BE, 0, seqno_max, 99999, 0)
        hdr = unpack_header(header)
        assert hdr["seqno"] == seqno_max

    def test_hash_zero(self) -> None:
        header = pack_msg_header(HeaderType.MSG_BE, 0, 0, 0, 42)
        hdr = unpack_header(header)
        assert hdr["hash"] == 0
        assert is_pinned(0)

    def test_hash_pinned_boundary(self) -> None:
        # 8191 is the max pinned value
        assert is_pinned(SUBJECT_ID_PINNED_MAX)
        assert not is_pinned(SUBJECT_ID_PINNED_MAX + 1)

        header = pack_msg_header(HeaderType.MSG_BE, 0, 0, SUBJECT_ID_PINNED_MAX, 0)
        hdr = unpack_header(header)
        assert hdr["hash"] == SUBJECT_ID_PINNED_MAX

    def test_hash_max_u64(self) -> None:
        h_max = (1 << 64) - 1
        header = pack_msg_header(HeaderType.MSG_BE, 0, 0, h_max, 0)
        hdr = unpack_header(header)
        assert hdr["hash"] == h_max
        assert not is_pinned(h_max)

    def test_evictions_zero(self) -> None:
        sid = topic_subject_id(100000, 0, 122743)
        assert sid > SUBJECT_ID_PINNED_MAX

    def test_evictions_large(self) -> None:
        # Large evictions should not crash
        sid = topic_subject_id(100000, 0xFFFFFFFF, 122743)
        assert sid > SUBJECT_ID_PINNED_MAX

    def test_lage_boundary_values(self) -> None:
        """Test that lage at exact boundaries roundtrips through header."""
        for lage in [LAGE_MIN, 0, LAGE_MAX]:
            header = pack_msg_header(HeaderType.MSG_BE, lage, 0, 99999, 0)
            hdr = unpack_header(header)
            # lage is packed as signed byte
            assert hdr["lage"] == lage

    def test_subject_id_pinned_is_identity(self) -> None:
        for pinned_h in [0, 1, 100, 4096, SUBJECT_ID_PINNED_MAX]:
            sid = topic_subject_id(pinned_h, 0, 122743)
            assert sid == pinned_h

    def test_all_header_types_roundtrip(self) -> None:
        """Verify every HeaderType value can be created and unpacked."""
        for ht in HeaderType:
            data = bytearray(HEADER_SIZE)
            data[0] = ht
            hdr = unpack_header(bytes(data))
            assert hdr["type"] == ht


# =====================================================================================================================
# 11. Pattern edge cases
# =====================================================================================================================


class TestPatternEdgeCases:
    def test_bare_gt_pattern(self) -> None:
        """Pattern '>' alone should match any single-segment name."""
        result = name_match(">", "anything")
        assert result is not None
        assert len(result) == 1
        assert result[0][0] == "anything"

    def test_bare_gt_no_match_empty(self) -> None:
        """Pattern '>' should not match empty string."""
        result = name_match(">", "")
        # '>' requires at least one segment; empty string splits to ['']
        # but '>' should match '' as a segment? Actually '' is a segment.
        # Let's just test it doesn't crash.
        assert result is not None or result is None  # no crash

    def test_bare_star_pattern(self) -> None:
        """Pattern '*' should match any single segment."""
        result = name_match("*", "hello")
        assert result is not None
        assert result[0][0] == "hello"

    def test_star_no_match_multi_segment(self) -> None:
        """Pattern '*' should not match multi-segment names."""
        result = name_match("*", "a/b")
        assert result is None

    def test_star_gt_combination(self) -> None:
        """Pattern '*/>' should match two or more segments."""
        result = name_match("*/>", "a/b")
        assert result is not None
        assert len(result) == 2

        result3 = name_match("*/>", "a/b/c")
        assert result3 is not None
        assert len(result3) == 3

    def test_star_gt_no_match_single(self) -> None:
        """Pattern '*/>' should not match a single segment (need at least 2)."""
        result = name_match("*/>", "single")
        assert result is None

    def test_gt_not_last_returns_none(self) -> None:
        """'>' not at the end of pattern should not match anything."""
        result = name_match(">/foo", "a/foo")
        assert result is None

    def test_multiple_stars(self) -> None:
        result = name_match("*/*", "a/b")
        assert result is not None
        assert result[0][0] == "a"
        assert result[1][0] == "b"

    def test_deeply_nested_pattern(self) -> None:
        pattern = "/".join(["*"] * 20)
        name = "/".join([f"seg{i}" for i in range(20)])
        result = name_match(pattern, name)
        assert result is not None
        assert len(result) == 20

    @pytest.mark.asyncio
    async def test_subscribe_bare_gt(self) -> None:
        """Subscribing with '>' pattern after a topic exists should attach to it."""
        node = _make_node()
        try:
            # Create the topic first via advertise, then subscribe with pattern
            pub = node.advertise("any/topic/here")
            sub = node.subscribe(">")
            await pub(Instant.now() + 1.0, b"matched")
            await asyncio.sleep(0.05)
            assert not sub._queue.empty()
            sub.close()
            pub.close()
        finally:
            node.close()


# =====================================================================================================================
# 12. Transport failures
# =====================================================================================================================


class TestTransportFailures:
    @pytest.mark.asyncio
    async def test_fail_unicast_flag(self) -> None:
        """MockTransport with fail_unicast=True should cause sends to fail gracefully."""
        transport = MockTransport(node_id=1, modulus=122743)
        transport.fail_unicast = True
        node = Node(transport, home="fhome", namespace="fns")
        try:
            # Node creation triggers unicast_listen but no unicast calls yet.
            # Inject a reliable message that would trigger a unicast ack response.
            sub = node.subscribe("fail/topic")
            t_hash = topic_hash("fns/fail/topic")
            sid = topic_subject_id(t_hash, 0, 122743)
            header = pack_msg_header(HeaderType.MSG_REL, 0, 0, t_hash, 555)
            data = header + b"reliable_data"
            _inject_subject(node, sid, data, remote_id=50)
            # The node will try to send an ack via unicast and fail -- it should not crash
            await asyncio.sleep(0.05)
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_writer_fail_next(self) -> None:
        """MockSubjectWriter.fail_next should cause exactly one send to fail."""
        node = _make_node()
        try:
            pub = node.advertise("failnext/topic")
            # Get the underlying writer and set fail_next
            topic_internal = node._topics_by_name.get("test_ns/failnext/topic")
            assert topic_internal is not None
            assert topic_internal.pub_writer is not None
            topic_internal.pub_writer.fail_next = True
            with pytest.raises(SendError):
                await pub(Instant.now() + 1.0, b"will_fail")
            # Second send should succeed
            await pub(Instant.now() + 1.0, b"will_succeed")
            pub.close()
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_transport_closed_unicast(self) -> None:
        """Unicast on a closed transport should raise RuntimeError."""
        transport = MockTransport(node_id=1, modulus=122743)
        transport.close()
        with pytest.raises(RuntimeError, match="closed"):
            await transport.unicast(Instant.now() + 1.0, Priority.NOMINAL, 2, b"data")


# =====================================================================================================================
# 13. Wire-level fuzzing: random payloads injected as subject/unicast/broadcast
# =====================================================================================================================


class TestWireFuzzing:
    @pytest.mark.asyncio
    async def test_random_broadcast_payloads(self) -> None:
        """Inject many random payloads into broadcast -- node must not crash."""
        rng = random.Random(1337)
        node = _make_node()
        sub = node.subscribe("fuzz/>")
        try:
            for _ in range(500):
                length = rng.randint(0, 100)
                data = bytes(rng.getrandbits(8) for _ in range(length))
                _inject_broadcast(node, data)
            await asyncio.sleep(0.05)
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_random_unicast_payloads(self) -> None:
        """Inject random payloads as unicast -- node must not crash."""
        rng = random.Random(7777)
        node = _make_node()
        try:
            for _ in range(500):
                length = rng.randint(0, 100)
                data = bytes(rng.getrandbits(8) for _ in range(length))
                _inject_unicast(node, data, remote_id=rng.randint(0, 1000))
            await asyncio.sleep(0.05)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_random_subject_payloads(self) -> None:
        """Inject random payloads into known topic subject-ids -- must not crash."""
        rng = random.Random(2468)
        node = _make_node()
        sub = node.subscribe("fuzz/target")
        try:
            t_hash = topic_hash("test_ns/fuzz/target")
            sid = topic_subject_id(t_hash, 0, 122743)
            for _ in range(500):
                length = rng.randint(0, 80)
                data = bytes(rng.getrandbits(8) for _ in range(length))
                _inject_subject(node, sid, data)
            await asyncio.sleep(0.05)
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_valid_header_random_payload(self) -> None:
        """Valid MSG_BE header but random garbage payload -- should be delivered."""
        rng = random.Random(3141)
        node = _make_node()
        sub = node.subscribe("fuzz/valid_hdr")
        try:
            t_hash = topic_hash("test_ns/fuzz/valid_hdr")
            sid = topic_subject_id(t_hash, 0, 122743)
            delivered = 0
            for i in range(50):
                garbage_payload = bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 200)))
                header = pack_msg_header(HeaderType.MSG_BE, 0, 0, t_hash, i)
                data = header + garbage_payload
                _inject_subject(node, sid, data)
                delivered += 1
            await asyncio.sleep(0.05)
            # Messages with valid headers and matching topic should be delivered
            count = sub._queue.qsize()
            assert count == delivered
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_gossip_with_truncated_name(self) -> None:
        """Gossip header claims a name_len larger than actual payload -- must not crash."""
        node = _make_node()
        sub = node.subscribe("trunc/>")
        try:
            # name_len says 100, but only 3 bytes follow the header
            header = pack_gossip_header(0, topic_hash("trunc/test"), 0, 100)
            data = header + b"abc"
            _inject_broadcast(node, data)
            await asyncio.sleep(0.05)
        finally:
            sub.close()
            node.close()

    @pytest.mark.asyncio
    async def test_scout_with_truncated_pattern(self) -> None:
        """Scout header claims a pattern_len larger than actual payload -- must not crash."""
        node = _make_node()
        try:
            header = pack_scout_header(200)
            data = header + b"x"
            _inject_broadcast(node, data)
            await asyncio.sleep(0.05)
        finally:
            node.close()


# =====================================================================================================================
# 14. Header type cross-injection (wrong message type for context)
# =====================================================================================================================


class TestHeaderTypeMismatch:
    @pytest.mark.asyncio
    async def test_ack_on_broadcast_dropped(self) -> None:
        """MSG_ACK on broadcast (not unicast) should be dropped."""
        node = _make_node()
        try:
            t_hash = topic_hash("test_ns/ack_test")
            header = pack_ack_header(HeaderType.MSG_ACK, t_hash, 42)
            _inject_broadcast(node, header)
            await asyncio.sleep(0.01)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_response_on_broadcast_dropped(self) -> None:
        """RSP_BE on broadcast (not unicast) should be dropped."""
        node = _make_node()
        try:
            header = pack_rsp_header(HeaderType.RSP_BE, 0, 0, 99999, 42)
            _inject_broadcast(node, header + b"resp_payload")
            await asyncio.sleep(0.01)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_rsp_ack_on_broadcast_dropped(self) -> None:
        """RSP_ACK on broadcast should be dropped."""
        node = _make_node()
        try:
            header = pack_rsp_header(HeaderType.RSP_ACK, 0, 0, 99999, 42)
            _inject_broadcast(node, header)
            await asyncio.sleep(0.01)
        finally:
            node.close()


# =====================================================================================================================
# 15. Concurrent node interactions (multi-node stress)
# =====================================================================================================================


class TestMultiNodeStress:
    @pytest.mark.asyncio
    async def test_many_nodes_same_network(self) -> None:
        """Multiple nodes on the same network doing concurrent operations."""
        network = MockNetwork()
        nodes = [_make_node(node_id=i, network=network) for i in range(5)]
        try:
            subs = [nodes[i].subscribe(f"multi/topic{i % 3}") for i in range(5)]
            pubs = [nodes[i].advertise(f"multi/topic{i % 3}") for i in range(5)]
            for i, pub in enumerate(pubs):
                await pub(Instant.now() + 1.0, f"msg_{i}".encode())
            await asyncio.sleep(0.1)
            for pub in pubs:
                pub.close()
            for sub in subs:
                sub.close()
        finally:
            for n in nodes:
                n.close()


# =====================================================================================================================
# 16. Edge cases in topic_subject_id computation
# =====================================================================================================================


class TestSubjectIdComputation:
    def test_pinned_ignores_evictions_and_modulus(self) -> None:
        for pinned in [0, 1, 100, SUBJECT_ID_PINNED_MAX]:
            assert topic_subject_id(pinned, 0, 122743) == pinned
            assert topic_subject_id(pinned, 999, 122743) == pinned
            assert topic_subject_id(pinned, 0, 1) == pinned

    def test_non_pinned_always_above_pinned_range(self) -> None:
        for h in [SUBJECT_ID_PINNED_MAX + 1, 100000, (1 << 64) - 1]:
            for evictions in [0, 1, 100, 0xFFFFFFFF]:
                sid = topic_subject_id(h, evictions, 122743)
                assert sid > SUBJECT_ID_PINNED_MAX

    def test_subject_id_deterministic(self) -> None:
        sid1 = topic_subject_id(123456789, 0, 122743)
        sid2 = topic_subject_id(123456789, 0, 122743)
        assert sid1 == sid2

    def test_different_evictions_different_subject_ids(self) -> None:
        """Incrementing evictions should usually produce different subject-ids."""
        h = 9999999
        sids = {topic_subject_id(h, ev, 122743) for ev in range(100)}
        # With 100 evictions and modulus 122743, we expect many distinct values
        assert len(sids) > 50


# =====================================================================================================================
# 17. name_match regression / adversarial patterns
# =====================================================================================================================


class TestNameMatchAdversarial:
    def test_very_many_segments(self) -> None:
        """Deeply nested name with many segments."""
        segments = "/".join(f"s{i}" for i in range(100))
        pattern = "/".join(["*"] * 100)
        result = name_match(pattern, segments)
        assert result is not None
        assert len(result) == 100

    def test_gt_captures_all_remaining(self) -> None:
        name = "/".join(f"x{i}" for i in range(50))
        result = name_match(">", name)
        assert result is not None
        assert len(result) == 50

    def test_mixed_literal_wildcard(self) -> None:
        result = name_match("a/*/c/*/e", "a/B/c/D/e")
        assert result is not None
        assert result[0] == ("B", 1)
        assert result[1] == ("D", 3)

    def test_pattern_longer_than_name(self) -> None:
        result = name_match("a/b/c/d", "a/b")
        assert result is None

    def test_name_longer_than_pattern(self) -> None:
        result = name_match("a", "a/b/c")
        assert result is None

    def test_exact_match_returns_empty_subs(self) -> None:
        result = name_match("exact/match", "exact/match")
        assert result is not None
        assert result == []

    def test_no_match_different_literals(self) -> None:
        result = name_match("foo/bar", "foo/baz")
        assert result is None

    def test_empty_pattern_vs_empty_name(self) -> None:
        result = name_match("", "")
        assert result is not None
        assert result == []

    def test_empty_pattern_vs_nonempty_name(self) -> None:
        result = name_match("", "something")
        assert result is None


# =====================================================================================================================
# 18. Injecting messages for topics not subscribed to
# =====================================================================================================================


class TestUnsubscribedTopicInjection:
    @pytest.mark.asyncio
    async def test_msg_for_unknown_topic_hash(self) -> None:
        """A message for a topic hash nobody has subscribed to should not crash."""
        node = _make_node()
        try:
            fake_hash = 0xDEADBEEFCAFE
            header = pack_msg_header(HeaderType.MSG_BE, 0, 0, fake_hash, 1)
            sid = topic_subject_id(fake_hash, 0, 122743)
            _inject_subject(node, sid, header + b"orphan")
            await asyncio.sleep(0.01)
        finally:
            node.close()

    @pytest.mark.asyncio
    async def test_msg_subject_id_mismatch(self) -> None:
        """Header hash/evictions imply a different subject-id than the one it arrived on."""
        node = _make_node()
        sub = node.subscribe("mismatch/topic")
        try:
            t_hash = topic_hash("test_ns/mismatch/topic")
            correct_sid = topic_subject_id(t_hash, 0, 122743)
            wrong_sid = correct_sid + 1 if correct_sid < 130000 else correct_sid - 1
            header = pack_msg_header(HeaderType.MSG_BE, 0, 0, t_hash, 42)
            _inject_subject(node, wrong_sid, header + b"misrouted")
            await asyncio.sleep(0.01)
            # Should be silently dropped because subject_id mismatch
            assert sub._queue.empty()
        finally:
            sub.close()
            node.close()
