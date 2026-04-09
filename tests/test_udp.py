"""Comprehensive tests for pycyphal2.udp -- Cyphal/UDP transport."""

from __future__ import annotations

import asyncio
import os
import struct
from ipaddress import IPv4Address
from unittest.mock import patch

import pytest

from pycyphal2 import (
    eui64,
    Instant,
    Priority,
    SendError,
    TransportArrival,
)
from pycyphal2._hash import (
    CRC32C_INITIAL,
    CRC32C_OUTPUT_XOR,
    crc32c_add,
    crc32c_full,
)
from pycyphal2.udp import (
    HEADER_SIZE,
    HEADER_VERSION,
    IPv4_MCAST_PREFIX,
    IPv4_SUBJECT_ID_MAX,
    TRANSFER_ID_MASK,
    UDP_PORT,
    Interface,
    UDPTransport,
    _FrameHeader,
    _RxReassembler,
    _SUBJECT_ID_MODULUS_MAX,
    _TransferSlot,
    _header_deserialize,
    _header_serialize,
    _make_subject_endpoint,
    _segment_transfer,
    _UDPTransportImpl,
)

# =====================================================================================================================
# Header Tests
# =====================================================================================================================


class TestHeader:
    def test_roundtrip(self):
        """Serialize then deserialize; all fields must match."""
        cases = [
            (0, 0, 0, 0, 0),
            (4, 0xDEADBEEF, 0x0200001234567890, 0, 5),
            (7, TRANSFER_ID_MASK, (1 << 64) - 1, 0xFFFFFFFF, 0xFFFFFFFF),
            (2, 42, 12345, 100, 500),
        ]
        for priority, tid, uid, offset, size in cases:
            prefix_crc = crc32c_full(b"test")
            hdr = _header_serialize(priority, tid, uid, offset, size, prefix_crc)
            assert len(hdr) == HEADER_SIZE
            parsed = _header_deserialize(hdr)
            assert parsed is not None, f"Failed to parse: pri={priority} tid={tid}"
            assert parsed.priority == priority
            assert parsed.transfer_id == (tid & TRANSFER_ID_MASK)
            assert parsed.sender_uid == uid
            assert parsed.frame_payload_offset == offset
            assert parsed.transfer_payload_size == size
            assert parsed.prefix_crc == prefix_crc

    def test_version_bits(self):
        """Byte 0 low 5 bits are HEADER_VERSION=2, high 3 bits are priority."""
        hdr = _header_serialize(5, 0, 0, 0, 0, 0)
        assert (hdr[0] & 0x1F) == HEADER_VERSION
        assert ((hdr[0] >> 5) & 0x07) == 5

    def test_bitflip_rejected(self):
        """A single bit flip in any byte invalidates the header CRC."""
        hdr = _header_serialize(4, 42, 12345, 0, 100, crc32c_full(b"x"))
        for byte_idx in range(HEADER_SIZE):
            for bit in range(8):
                corrupted = bytearray(hdr)
                corrupted[byte_idx] ^= 1 << bit
                assert (
                    _header_deserialize(bytes(corrupted)) is None
                ), f"Bit flip at byte {byte_idx} bit {bit} not caught"

    def test_wrong_version(self):
        hdr = bytearray(_header_serialize(4, 42, 12345, 0, 100, 0))
        # Set version to 3 (clear bit 1, keep bit 0 set, set bit 1 to make version=3)
        hdr[0] = (hdr[0] & 0xE0) | 3  # version=3, keep priority
        # Re-compute header CRC
        struct.pack_into("<I", hdr, 28, crc32c_full(bytes(hdr[:28])))
        assert _header_deserialize(bytes(hdr)) is None

    def test_incompatibility_rejected(self):
        hdr = bytearray(_header_serialize(4, 42, 12345, 0, 100, 0))
        hdr[1] = 0xE0  # Set all 3 incompatibility bits
        struct.pack_into("<I", hdr, 28, crc32c_full(bytes(hdr[:28])))
        assert _header_deserialize(bytes(hdr)) is None

    def test_too_short(self):
        assert _header_deserialize(b"") is None
        assert _header_deserialize(b"\x00" * 31) is None

    def test_transfer_id_48bit_wrap(self):
        """transfer_id > 48 bits gets truncated to 48 bits."""
        big_tid = (1 << 48) + 42
        hdr = _header_serialize(0, big_tid, 0, 0, 0, crc32c_full(b""))
        parsed = _header_deserialize(hdr)
        assert parsed is not None
        assert parsed.transfer_id == 42  # Only low 48 bits


# =====================================================================================================================
# TX Segmentation Tests
# =====================================================================================================================


class TestTXSegmentation:
    def test_single_frame(self):
        payload = b"hello"
        frames = _segment_transfer(4, 1, 100, payload, mtu=1400)
        assert len(frames) == 1
        assert len(frames[0]) == HEADER_SIZE + len(payload)
        hdr = _header_deserialize(frames[0][:HEADER_SIZE])
        assert hdr is not None
        assert hdr.priority == 4
        assert hdr.transfer_id == 1
        assert hdr.sender_uid == 100
        assert hdr.frame_payload_offset == 0
        assert hdr.transfer_payload_size == 5
        assert hdr.prefix_crc == crc32c_full(payload)
        assert frames[0][HEADER_SIZE:] == payload

    def test_multi_frame(self):
        """Payload of 350 bytes with MTU 100 -> 4 frames."""
        payload = os.urandom(350)
        frames = _segment_transfer(2, 99, 200, payload, mtu=100)
        assert len(frames) == 4  # ceil(350/100) = 4

        offset = 0
        running_crc = CRC32C_INITIAL
        for i, frame in enumerate(frames):
            hdr = _header_deserialize(frame[:HEADER_SIZE])
            assert hdr is not None
            assert hdr.priority == 2
            assert hdr.transfer_id == 99
            assert hdr.sender_uid == 200
            assert hdr.frame_payload_offset == offset
            assert hdr.transfer_payload_size == 350
            chunk = frame[HEADER_SIZE:]
            expected_chunk_size = min(100, 350 - offset)
            assert len(chunk) == expected_chunk_size
            assert chunk == payload[offset : offset + expected_chunk_size]
            running_crc = crc32c_add(running_crc, chunk)
            assert hdr.prefix_crc == (running_crc ^ CRC32C_OUTPUT_XOR)
            offset += expected_chunk_size

        assert offset == 350

    def test_empty_payload(self):
        frames = _segment_transfer(0, 0, 0, b"", mtu=1400)
        assert len(frames) == 1
        assert len(frames[0]) == HEADER_SIZE  # Header only, no payload
        hdr = _header_deserialize(frames[0][:HEADER_SIZE])
        assert hdr is not None
        assert hdr.frame_payload_offset == 0
        assert hdr.transfer_payload_size == 0
        assert hdr.prefix_crc == crc32c_full(b"")

    def test_exact_mtu_boundary(self):
        """Payload exactly equal to MTU -> single frame."""
        payload = os.urandom(100)
        frames = _segment_transfer(0, 0, 0, payload, mtu=100)
        assert len(frames) == 1

    def test_one_byte_over_mtu(self):
        """Payload one byte over MTU -> two frames."""
        payload = os.urandom(101)
        frames = _segment_transfer(0, 0, 0, payload, mtu=100)
        assert len(frames) == 2
        hdr0 = _header_deserialize(frames[0][:HEADER_SIZE])
        hdr1 = _header_deserialize(frames[1][:HEADER_SIZE])
        assert hdr0 is not None and hdr1 is not None
        assert hdr0.frame_payload_offset == 0
        assert hdr1.frame_payload_offset == 100
        assert len(frames[0]) == HEADER_SIZE + 100
        assert len(frames[1]) == HEADER_SIZE + 1

    def test_large_payload(self):
        """3.5x MTU -> 4 frames."""
        mtu = 200
        payload = os.urandom(mtu * 3 + mtu // 2)  # 700 bytes
        frames = _segment_transfer(0, 0, 0, payload, mtu=mtu)
        assert len(frames) == 4  # ceil(700/200) = 4
        # Reassemble and verify
        reassembled = b""
        for frame in frames:
            reassembled += frame[HEADER_SIZE:]
        assert reassembled == payload

    def test_memoryview_payload(self):
        payload = b"test payload"
        frames = _segment_transfer(0, 0, 0, memoryview(payload), mtu=1400)
        assert len(frames) == 1
        assert frames[0][HEADER_SIZE:] == payload


# =====================================================================================================================
# RX Reassembly Tests
# =====================================================================================================================


class TestRXReassembly:
    def _make_frames(
        self, payload: bytes, mtu: int, sender_uid: int = 1000, transfer_id: int = 42, priority: int = 4
    ) -> list[tuple[_FrameHeader, bytes]]:
        """Generate (header, chunk) pairs from _segment_transfer output."""
        frames = _segment_transfer(priority, transfer_id, sender_uid, payload, mtu)
        result = []
        for frame in frames:
            hdr = _header_deserialize(frame[:HEADER_SIZE])
            assert hdr is not None
            chunk = frame[HEADER_SIZE:]
            result.append((hdr, chunk))
        return result

    def test_single_frame(self):
        payload = b"hello world"
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=1400)
        assert len(frame_pairs) == 1
        result = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result is not None
        assert result.payload == payload
        assert result.sender_uid == 1000
        assert result.priority == 4

    def test_multi_frame_in_order(self):
        payload = os.urandom(300)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)
        assert len(frame_pairs) == 3

        for i, (hdr, chunk) in enumerate(frame_pairs[:-1]):
            result = reasm.accept(hdr, chunk)
            assert result is None, f"Unexpected completion at frame {i}"

        result = reasm.accept(frame_pairs[-1][0], frame_pairs[-1][1])
        assert result is not None
        assert result.payload == payload

    def test_multi_frame_out_of_order(self):
        payload = os.urandom(300)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)
        assert len(frame_pairs) == 3

        # Deliver in reverse order
        result = reasm.accept(frame_pairs[2][0], frame_pairs[2][1])
        assert result is None
        result = reasm.accept(frame_pairs[1][0], frame_pairs[1][1])
        assert result is None
        result = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result is not None
        assert result.payload == payload

    def test_duplicate_frame(self):
        """Sending the same frame twice should not cause issues."""
        payload = os.urandom(300)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)

        # Send frame 0 twice
        reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        reasm.accept(frame_pairs[0][0], frame_pairs[0][1])

        # Complete with remaining frames
        reasm.accept(frame_pairs[1][0], frame_pairs[1][1])
        result = reasm.accept(frame_pairs[2][0], frame_pairs[2][1])
        assert result is not None
        assert result.payload == payload

    def test_transfer_id_dedup(self):
        """A completed transfer should not be delivered again."""
        payload = b"dedup test"
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=1400)

        result1 = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result1 is not None

        # Re-send the same transfer
        result2 = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result2 is None  # Dedup

    def test_crc_mismatch_first_frame(self):
        """Corrupted first-frame CRC should be rejected."""
        payload = b"corrupt me"
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=1400)
        hdr, chunk = frame_pairs[0]
        # Corrupt the payload chunk
        bad_chunk = bytes([chunk[0] ^ 0xFF]) + chunk[1:]
        result = reasm.accept(hdr, bad_chunk)
        assert result is None

    def test_crc_mismatch_reassembled(self):
        """Corrupted non-first frame should cause full-transfer CRC failure."""
        payload = os.urandom(200)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)
        assert len(frame_pairs) == 2

        # Good first frame
        reasm.accept(frame_pairs[0][0], frame_pairs[0][1])

        # Corrupted second frame payload
        hdr1, chunk1 = frame_pairs[1]
        bad_chunk = bytes([chunk1[0] ^ 0xFF]) + chunk1[1:]
        result = reasm.accept(hdr1, bad_chunk)
        assert result is None  # CRC mismatch on full payload

    def test_interleaved_transfers_same_sender(self):
        """Two concurrent transfers from the same sender with different transfer_ids."""
        payload_a = b"transfer A"
        payload_b = b"transfer B"
        reasm = _RxReassembler()

        frames_a = self._make_frames(payload_a, mtu=1400, transfer_id=10)
        frames_b = self._make_frames(payload_b, mtu=1400, transfer_id=20)

        result_b = reasm.accept(frames_b[0][0], frames_b[0][1])
        assert result_b is not None
        assert result_b.payload == payload_b

        result_a = reasm.accept(frames_a[0][0], frames_a[0][1])
        assert result_a is not None
        assert result_a.payload == payload_a

    def test_interleaved_transfers_multi_frame(self):
        """Interleaved multi-frame transfers from the same sender."""
        payload_a = os.urandom(200)
        payload_b = os.urandom(200)
        reasm = _RxReassembler()
        frames_a = self._make_frames(payload_a, mtu=100, transfer_id=10)
        frames_b = self._make_frames(payload_b, mtu=100, transfer_id=20)

        # Interleave: A0, B0, A1, B1
        assert reasm.accept(frames_a[0][0], frames_a[0][1]) is None
        assert reasm.accept(frames_b[0][0], frames_b[0][1]) is None

        result_a = reasm.accept(frames_a[1][0], frames_a[1][1])
        assert result_a is not None
        assert result_a.payload == payload_a

        result_b = reasm.accept(frames_b[1][0], frames_b[1][1])
        assert result_b is not None
        assert result_b.payload == payload_b

    def test_different_senders(self):
        """Frames from different senders reassembled independently."""
        payload_x = b"from sender X"
        payload_y = b"from sender Y"
        reasm = _RxReassembler()

        frames_x = self._make_frames(payload_x, mtu=1400, sender_uid=100, transfer_id=1)
        frames_y = self._make_frames(payload_y, mtu=1400, sender_uid=200, transfer_id=1)

        rx = reasm.accept(frames_x[0][0], frames_x[0][1])
        assert rx is not None and rx.payload == payload_x
        ry = reasm.accept(frames_y[0][0], frames_y[0][1])
        assert ry is not None and ry.payload == payload_y

    def test_empty_payload(self):
        payload = b""
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=1400)
        assert len(frame_pairs) == 1
        result = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result is not None
        assert result.payload == b""

    def test_bounds_violation_rejected(self):
        """Frame where offset + chunk_size > transfer_payload_size should be rejected."""
        reasm = _RxReassembler()
        # Manually create a bad header
        hdr = _FrameHeader(
            priority=4, transfer_id=1, sender_uid=1, frame_payload_offset=5, transfer_payload_size=6, prefix_crc=0
        )
        # 5 + 3 = 8 > 6
        result = reasm.accept(hdr, b"abc")
        assert result is None

    def test_conflicting_size_rejected(self):
        """Frames with same (uid, tid) but different transfer_payload_size are rejected."""
        reasm = _RxReassembler()
        payload = os.urandom(200)
        frames = self._make_frames(payload, mtu=100, transfer_id=42)
        # First frame establishes transfer_payload_size=200
        reasm.accept(frames[0][0], frames[0][1])
        # Create a frame with different size for same transfer
        bad_hdr = _FrameHeader(
            priority=4,
            transfer_id=42,
            sender_uid=1000,
            frame_payload_offset=100,
            transfer_payload_size=300,
            prefix_crc=0,
        )
        result = reasm.accept(bad_hdr, os.urandom(100))
        assert result is None

    def test_priority_mismatch_drops_transfer(self):
        payload = os.urandom(200)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)
        assert reasm.accept(frame_pairs[0][0], frame_pairs[0][1]) is None

        bad_hdr, bad_chunk = frame_pairs[1]
        bad_hdr = _FrameHeader(
            priority=Priority.HIGH,
            transfer_id=bad_hdr.transfer_id,
            sender_uid=bad_hdr.sender_uid,
            frame_payload_offset=bad_hdr.frame_payload_offset,
            transfer_payload_size=bad_hdr.transfer_payload_size,
            prefix_crc=bad_hdr.prefix_crc,
        )
        assert reasm.accept(bad_hdr, bad_chunk) is None
        assert reasm.accept(frame_pairs[1][0], frame_pairs[1][1]) is None

    def test_stale_slot_is_retired(self):
        payload = os.urandom(200)
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=100)
        first_ts = 1_000_000_000
        stale_ts = first_ts + 31_000_000_000
        assert reasm.accept(frame_pairs[0][0], frame_pairs[0][1], timestamp_ns=first_ts) is None
        fresh = self._make_frames(b"fresh", mtu=1400, transfer_id=43)
        result = reasm.accept(fresh[0][0], fresh[0][1], timestamp_ns=stale_ts)
        assert result is not None
        session = reasm._sessions[1000]
        slot_transfer_ids = {slot.transfer_id for slot in session.slots if slot is not None}
        assert slot_transfer_ids == set()

    def test_ninth_concurrent_transfer_sacrifices_oldest_slot(self):
        reasm = _RxReassembler()
        for transfer_id in range(1, 10):
            frames = self._make_frames(os.urandom(200), mtu=100, transfer_id=transfer_id)
            assert reasm.accept(frames[0][0], frames[0][1], timestamp_ns=transfer_id) is None
        session = reasm._sessions[1000]
        slot_transfer_ids = {slot.transfer_id for slot in session.slots if slot is not None}
        assert slot_transfer_ids == set(range(2, 10))

    def test_duplicate_history_window_is_32(self):
        reasm = _RxReassembler()
        for transfer_id in range(1, 34):
            frames = self._make_frames(f"msg{transfer_id}".encode(), mtu=1400, transfer_id=transfer_id)
            result = reasm.accept(frames[0][0], frames[0][1], timestamp_ns=transfer_id)
            assert result is not None
        replay = self._make_frames(b"msg1", mtu=1400, transfer_id=1)
        replay_result = reasm.accept(replay[0][0], replay[0][1], timestamp_ns=100)
        assert replay_result is not None
        assert replay_result.payload == b"msg1"


class TestTransferSlot:
    def test_coverage_tracking(self):
        slot = _TransferSlot.create(
            _FrameHeader(
                priority=4, transfer_id=1, sender_uid=1, frame_payload_offset=0, transfer_payload_size=200, prefix_crc=0
            ),
            0,
        )
        assert slot._accept_fragment(0, b"a" * 30, 0)
        assert slot.covered_prefix == 30
        assert slot._accept_fragment(50, b"b" * 30, 0)
        assert slot.covered_prefix == 30
        assert slot._accept_fragment(30, b"c" * 20, 0)
        assert slot.covered_prefix == 80
        assert slot._accept_fragment(80, b"d" * 20, 0)
        assert slot.covered_prefix == 100

    def test_contained_fragment_rejected(self):
        slot = _TransferSlot.create(
            _FrameHeader(
                priority=4, transfer_id=1, sender_uid=1, frame_payload_offset=0, transfer_payload_size=12, prefix_crc=0
            ),
            0,
        )
        assert slot._accept_fragment(0, b"A" * 4, 0)
        assert not slot._accept_fragment(1, b"B" * 2, 0)
        assert [(frag.offset, frag.data) for frag in slot.fragments] == [(0, b"AAAA")]

    def test_bridge_fragment_evicts_victim(self):
        slot = _TransferSlot.create(
            _FrameHeader(
                priority=4, transfer_id=1, sender_uid=1, frame_payload_offset=0, transfer_payload_size=12, prefix_crc=0
            ),
            0,
        )
        assert slot._accept_fragment(0, b"AAAA", 0)
        assert slot._accept_fragment(4, b"BB", 0)
        assert slot._accept_fragment(6, b"CCCC", 0)
        assert slot._accept_fragment(2, b"XXXXXX", 0)
        assert [(frag.offset, frag.data) for frag in slot.fragments] == [(0, b"AAAA"), (2, b"XXXXXX"), (6, b"CCCC")]

    def test_furthest_reaching_crc_is_used(self):
        payload = b"abcdef"
        slot = _TransferSlot.create(
            _FrameHeader(
                priority=4,
                transfer_id=1,
                sender_uid=1,
                frame_payload_offset=0,
                transfer_payload_size=len(payload),
                prefix_crc=0,
            ),
            0,
        )
        slot.update(
            0,
            _FrameHeader(
                priority=4, transfer_id=1, sender_uid=1, frame_payload_offset=0, transfer_payload_size=6, prefix_crc=0
            ),
            b"abcd",
        )
        result = slot.update(
            1,
            _FrameHeader(
                priority=4,
                transfer_id=1,
                sender_uid=1,
                frame_payload_offset=2,
                transfer_payload_size=6,
                prefix_crc=crc32c_full(payload),
            ),
            b"cdef",
        )
        assert result == payload


# =====================================================================================================================
# Multicast Address Tests
# =====================================================================================================================


class TestMulticastAddress:
    def test_subject_zero(self):
        ip, port = _make_subject_endpoint(0)
        assert ip == "239.0.0.0"
        assert port == UDP_PORT

    def test_subject_max(self):
        ip, port = _make_subject_endpoint(IPv4_SUBJECT_ID_MAX)
        assert ip == "239.127.255.255"
        assert port == UDP_PORT

    def test_subject_one(self):
        ip, port = _make_subject_endpoint(1)
        assert ip == "239.0.0.1"
        assert port == UDP_PORT

    def test_subject_masking(self):
        """Subject IDs beyond 23 bits are masked."""
        ip1, _ = _make_subject_endpoint(0x800000)  # Bit 23 set, masked to 0
        ip2, _ = _make_subject_endpoint(0)
        assert ip1 == ip2

    def test_various_subjects(self):
        ip, _ = _make_subject_endpoint(42)
        expected_int = IPv4_MCAST_PREFIX | 42
        assert ip == str(IPv4Address(expected_int))


# =====================================================================================================================
# UID Generation Tests
# =====================================================================================================================


class TestUID:
    def test_bit_57_set(self):
        uid = eui64()
        assert uid & (1 << 57), "U/L bit (bit 57) must be set"

    def test_bit_56_clear(self):
        uid = eui64()
        assert not (uid & (1 << 56)), "I/G bit (bit 56) must be clear"

    def test_nonzero(self):
        assert eui64() != 0

    def test_unique(self):
        """Two calls should produce different UIDs (random component)."""
        uid1 = eui64()
        uid2 = eui64()
        assert uid1 != uid2

    def test_fits_64_bits(self):
        uid = eui64()
        assert 0 < uid < (1 << 64)


# =====================================================================================================================
# Interface Enumeration Tests
# =====================================================================================================================


class TestInterfaces:
    def test_list_interfaces(self):
        ifaces = UDPTransport.list_interfaces()
        assert len(ifaces) >= 1, "At least one interface (loopback) expected"

    def test_loopback_present(self):
        ifaces = UDPTransport.list_interfaces()
        loopback = [i for i in ifaces if i.address.is_loopback]
        assert len(loopback) >= 1, "Loopback interface expected"

    def test_loopback_last(self):
        ifaces = UDPTransport.list_interfaces()
        if len(ifaces) > 1:
            assert ifaces[-1].address.is_loopback, "Loopback should be sorted last"

    def test_mtu_valid(self):
        ifaces = UDPTransport.list_interfaces()
        for iface in ifaces:
            assert iface.mtu_link >= 576, f"MTU too small: {iface.mtu_link}"
            assert iface.mtu_cyphal > 0
            assert iface.mtu_cyphal == iface.mtu_link - 100

    def test_interface_dataclass(self):
        iface = Interface(address=IPv4Address("127.0.0.1"), mtu_link=1500)
        assert iface.mtu_cyphal == 1400
        assert iface.address == IPv4Address("127.0.0.1")


# =====================================================================================================================
# Wire Compatibility Tests
# =====================================================================================================================


class TestWireCompatibility:
    def test_header_byte_layout(self):
        """Verify specific byte positions in a known header."""
        priority = 4
        transfer_id = 0x0000DEADBEEF
        sender_uid = 0x0200001234567890
        offset = 0
        size = 5
        prefix_crc = crc32c_full(b"hello")

        hdr = _header_serialize(priority, transfer_id, sender_uid, offset, size, prefix_crc)

        # Byte 0: version(5 low) | priority(3 high) = 2 | (4<<5) = 0x82
        assert hdr[0] == 0x82
        # Byte 1: 0 (no incompatibility)
        assert hdr[1] == 0x00
        # Bytes 2-7: transfer_id LE = EF BE AD DE 00 00
        assert hdr[2] == 0xEF
        assert hdr[3] == 0xBE
        assert hdr[4] == 0xAD
        assert hdr[5] == 0xDE
        assert hdr[6] == 0x00
        assert hdr[7] == 0x00
        # Bytes 8-15: sender_uid LE
        uid_bytes = struct.pack("<Q", sender_uid)
        assert hdr[8:16] == uid_bytes
        # Bytes 16-19: frame_payload_offset LE = 0
        assert hdr[16:20] == b"\x00\x00\x00\x00"
        # Bytes 20-23: transfer_payload_size LE = 5
        assert hdr[20:24] == b"\x05\x00\x00\x00"
        # Bytes 24-27: prefix_crc LE
        assert struct.unpack_from("<I", hdr, 24)[0] == prefix_crc
        # Bytes 28-31: header_crc LE
        assert struct.unpack_from("<I", hdr, 28)[0] == crc32c_full(hdr[:28])

    def test_frame_roundtrip_with_payload(self):
        """Complete frame (header + payload) serialized and deserialized."""
        payload = b"hello"
        frames = _segment_transfer(4, 0xDEADBEEF, 12345, payload, mtu=1400)
        assert len(frames) == 1
        frame = frames[0]
        hdr = _header_deserialize(frame[:HEADER_SIZE])
        assert hdr is not None
        assert frame[HEADER_SIZE:] == payload

    def test_multiframe_reassembly_matches_segmentation(self):
        """Segment then reassemble via the RX path; verify byte-identical output."""
        payload = os.urandom(1000)
        mtu = 200
        frames = _segment_transfer(3, 555, 9999, payload, mtu)
        reasm = _RxReassembler()
        result = None
        for frame in frames:
            hdr = _header_deserialize(frame[:HEADER_SIZE])
            assert hdr is not None
            r = reasm.accept(hdr, frame[HEADER_SIZE:])
            if r is not None:
                result = r
        assert result is not None
        assert result.payload == payload


# =====================================================================================================================
# Integration Tests (real loopback sockets)
# =====================================================================================================================


def _get_loopback_iface() -> Interface:
    ifaces = UDPTransport.list_interfaces()
    lo = [i for i in ifaces if i.address.is_loopback]
    if not lo:
        pytest.skip("No loopback interface available")
    return lo[0]


@pytest.fixture
def loopback_iface():
    return _get_loopback_iface()


class TestIntegrationPubSub:
    @pytest.mark.asyncio
    async def test_single_frame_pubsub(self):
        """Two transports on loopback: one publishes, the other subscribes."""
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            sub.subject_listen(42, received.append)

            writer = pub.subject_advertise(42)
            deadline = Instant.now() + 2.0
            await writer(deadline, Priority.NOMINAL, b"hello")

            await asyncio.sleep(0.1)

            assert len(received) == 1
            assert received[0].message == b"hello"
            assert received[0].priority == Priority.NOMINAL
            assert isinstance(pub, _UDPTransportImpl)
            assert received[0].remote_id == pub._uid
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_multi_frame_pubsub(self, loopback_iface):
        """Send payload larger than MTU, verify correct reassembly."""
        small_iface = Interface(address=loopback_iface.address, mtu_link=608)
        # mtu_cyphal = 508, so payload of 2000 bytes -> 4 frames
        pub = UDPTransport.new(interfaces=[small_iface])
        sub = UDPTransport.new(interfaces=[small_iface])
        try:
            received: list[TransportArrival] = []
            sub.subject_listen(100, received.append)

            writer = pub.subject_advertise(100)
            payload = os.urandom(2000)
            deadline = Instant.now() + 2.0
            await writer(deadline, Priority.FAST, payload)

            await asyncio.sleep(0.2)

            assert len(received) == 1
            assert received[0].message == payload
            assert received[0].priority == Priority.FAST
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_multiple_messages(self):
        """Send several messages, all received in order."""
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            sub.subject_listen(7, received.append)

            writer = pub.subject_advertise(7)
            deadline = Instant.now() + 2.0
            for i in range(5):
                await writer(deadline, Priority.NOMINAL, f"msg{i}".encode())
                await asyncio.sleep(0.02)

            await asyncio.sleep(0.1)

            assert len(received) == 5
            for i in range(5):
                assert received[i].message == f"msg{i}".encode()
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_empty_payload(self):
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            sub.subject_listen(99, received.append)

            writer = pub.subject_advertise(99)
            await writer(Instant.now() + 2.0, Priority.LOW, b"")

            await asyncio.sleep(0.1)

            assert len(received) == 1
            assert received[0].message == b""
        finally:
            pub.close()
            sub.close()


class TestIntegrationUnicast:
    @pytest.mark.asyncio
    async def test_unicast_roundtrip(self):
        """A publishes subject message -> B learns A's endpoint -> B unicasts to A."""
        a = UDPTransport.new_loopback()
        b = UDPTransport.new_loopback()
        try:
            # B subscribes to subject 50 (to learn A's endpoint)
            subject_received: list[TransportArrival] = []
            b.subject_listen(50, subject_received.append)

            # A registers unicast handler
            unicast_received: list[TransportArrival] = []
            a.unicast_listen(unicast_received.append)

            # A publishes on subject 50
            writer = a.subject_advertise(50)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"discover me")
            await asyncio.sleep(0.1)

            # B should have received the subject message and learned A's endpoint
            assert len(subject_received) == 1

            # B unicasts to A
            assert isinstance(a, _UDPTransportImpl)
            assert isinstance(b, _UDPTransportImpl)
            await b.unicast(Instant.now() + 2.0, Priority.HIGH, a._uid, b"unicast hello")
            await asyncio.sleep(0.1)

            assert len(unicast_received) == 1
            assert unicast_received[0].message == b"unicast hello"
            assert unicast_received[0].priority == Priority.HIGH
            assert unicast_received[0].remote_id == b._uid
        finally:
            a.close()
            b.close()


class TestIntegrationListenerLifecycle:
    @pytest.mark.asyncio
    async def test_listener_close_stops_delivery(self):
        """After closing a listener, no more messages are delivered to it."""
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            listener = sub.subject_listen(60, received.append)

            writer = pub.subject_advertise(60)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"before close")
            await asyncio.sleep(0.1)
            assert len(received) == 1

            # Close the listener
            listener.close()

            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"after close")
            await asyncio.sleep(0.1)
            # Should still be 1 (no new messages after close)
            assert len(received) == 1
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_duplicate_listener_same_subject_raises(self):
        sub = UDPTransport.new_loopback()
        try:
            listener = sub.subject_listen(70, lambda a: None)
            with pytest.raises(ValueError, match="active listener"):
                sub.subject_listen(70, lambda a: None)
            listener.close()
        finally:
            sub.close()

    @pytest.mark.asyncio
    async def test_listener_close_allows_relisten(self):
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received_before: list[TransportArrival] = []
            received_after: list[TransportArrival] = []
            listener = sub.subject_listen(80, received_before.append)

            writer = pub.subject_advertise(80)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"msg1")
            await asyncio.sleep(0.1)
            assert len(received_before) == 1
            assert len(received_after) == 0

            listener.close()
            listener = sub.subject_listen(80, received_after.append)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"msg2")
            await asyncio.sleep(0.1)
            assert len(received_before) == 1
            assert len(received_after) == 1

            listener.close()
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_duplicate_writer_same_subject_raises(self):
        t = UDPTransport.new_loopback()
        try:
            writer = t.subject_advertise(81)
            with pytest.raises(ValueError, match="active writer"):
                t.subject_advertise(81)
            writer.close()
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_writer_close_allows_readvertise(self):
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            sub.subject_listen(82, received.append)

            writer_a = pub.subject_advertise(82)
            await writer_a(Instant.now() + 2.0, Priority.NOMINAL, b"msg1")
            await asyncio.sleep(0.1)
            assert len(received) == 1

            writer_a.close()
            writer_b = pub.subject_advertise(82)
            await writer_b(Instant.now() + 2.0, Priority.NOMINAL, b"msg2")
            await asyncio.sleep(0.1)
            assert len(received) == 2
            assert received[1].message == b"msg2"

            writer_b.close()
        finally:
            pub.close()
            sub.close()


class TestIntegrationTransportClose:
    @pytest.mark.asyncio
    async def test_close_cleans_up(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        t.subject_listen(90, lambda a: None)
        t.subject_advertise(90)

        assert len(t._tx_socks) > 0
        assert len(t._mcast_socks) > 0

        t.close()

        assert len(t.tx_socks) == 0
        assert t.closed

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        t = UDPTransport.new_loopback()
        t.close()
        t.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_operations_after_close_fail(self):
        t = UDPTransport.new_loopback()
        writer = t.subject_advertise(91)
        t.close()
        with pytest.raises(SendError):
            await writer(Instant.now() + 1.0, Priority.NOMINAL, b"should fail")

    @pytest.mark.asyncio
    async def test_subject_id_modulus(self, loopback_iface):
        t = UDPTransport.new(interfaces=[loopback_iface], subject_id_modulus=_SUBJECT_ID_MODULUS_MAX)
        assert t.subject_id_modulus == _SUBJECT_ID_MODULUS_MAX
        t.close()

    @pytest.mark.asyncio
    async def test_subject_id_modulus_too_large_rejected(self, loopback_iface):
        with pytest.raises(ValueError, match="subject_id_modulus"):
            UDPTransport.new(interfaces=[loopback_iface], subject_id_modulus=_SUBJECT_ID_MODULUS_MAX + 1)


class TestIntegrationRXParity:
    @pytest.mark.asyncio
    async def test_malformed_frame_does_not_learn_endpoint(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            frame = _segment_transfer(4, 1, 0xAA, b"hello", mtu=1400)[0]
            bad = frame[:HEADER_SIZE] + bytes([frame[HEADER_SIZE] ^ 0xFF]) + frame[HEADER_SIZE + 1 :]
            t._process_subject_datagram(bad, "10.0.0.1", 9000, 55, 0, Instant(ns=1))
            assert t._remote_endpoints == {}
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_transfer_failure_still_learns_endpoint(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            frames = _segment_transfer(4, 1, 0xAB, os.urandom(200), mtu=100)
            t._process_subject_datagram(frames[0], "10.0.0.2", 9001, 56, 0, Instant(ns=1))
            bad = frames[1][:HEADER_SIZE] + bytes([frames[1][HEADER_SIZE] ^ 0xFF]) + frames[1][HEADER_SIZE + 1 :]
            t._process_subject_datagram(bad, "10.0.0.2", 9001, 56, 0, Instant(ns=2))
            assert t._remote_endpoints[(0xAB, 0)] == ("10.0.0.2", 9001)
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_transport_arrival_timestamp_uses_first_frame(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            received: list[TransportArrival] = []
            t.subject_listen(57, received.append)
            payload = os.urandom(200)
            frames = _segment_transfer(4, 1, 0xAC, payload, mtu=100)
            first = Instant(ns=100)
            second = Instant(ns=200)
            t._process_subject_datagram(frames[1], "10.0.0.3", 9002, 57, 0, second)
            t._process_subject_datagram(frames[0], "10.0.0.3", 9002, 57, 0, first)
            assert len(received) == 1
            assert received[0].timestamp == first
            assert received[0].message == payload
        finally:
            t.close()


class TestIntegrationSelfSendFilter:
    @pytest.mark.asyncio
    async def test_self_send_filtered(self):
        """A transport should NOT receive its own multicast messages."""
        t = UDPTransport.new_loopback()
        try:
            received: list[TransportArrival] = []
            t.subject_listen(55, received.append)
            writer = t.subject_advertise(55)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"self")
            await asyncio.sleep(0.1)
            assert len(received) == 0, "Self-sent messages should be filtered"
        finally:
            t.close()


class TestIntegrationDifferentSubjects:
    @pytest.mark.asyncio
    async def test_messages_isolated_by_subject(self):
        """Messages on different subjects don't cross-deliver."""
        pub = UDPTransport.new_loopback()
        sub = UDPTransport.new_loopback()
        try:
            received_10: list[TransportArrival] = []
            received_20: list[TransportArrival] = []
            sub.subject_listen(10, received_10.append)
            sub.subject_listen(20, received_20.append)

            w10 = pub.subject_advertise(10)
            w20 = pub.subject_advertise(20)

            await w10(Instant.now() + 2.0, Priority.NOMINAL, b"for subject 10")
            await w20(Instant.now() + 2.0, Priority.NOMINAL, b"for subject 20")
            await asyncio.sleep(0.1)

            assert len(received_10) == 1
            assert received_10[0].message == b"for subject 10"
            assert len(received_20) == 1
            assert received_20[0].message == b"for subject 20"
        finally:
            pub.close()
            sub.close()


# =====================================================================================================================
# Empty Interfaces Tests
# =====================================================================================================================


class TestEmptyInterfaces:
    @pytest.mark.asyncio
    async def test_empty_list_auto_discovers(self):
        """Empty list is treated as None — auto-discovers interfaces."""
        t = UDPTransport.new(interfaces=[])
        try:
            assert len(t.interfaces) >= 1
        finally:
            t.close()


# =====================================================================================================================
# Async Sendto Tests
# =====================================================================================================================


class TestAsyncSendto:
    @pytest.mark.asyncio
    async def test_deadline_already_expired(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            sock = t._tx_socks[0]
            expired = Instant(ns=0)
            with pytest.raises(SendError, match="Deadline exceeded"):
                await t.async_sendto(sock, b"data", ("127.0.0.1", 9999), expired)
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_sendto_immediate_success(self):
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            sock = t._tx_socks[0]
            deadline = Instant.now() + 2.0
            await t.async_sendto(sock, b"hello", ("127.0.0.1", sock.getsockname()[1]), deadline)
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_sendto_delegates_to_loop(self):
        """Verify _async_sendto delegates to loop.sock_sendto."""
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            sock = t._tx_socks[0]
            called = False

            async def mock_sock_sendto(s, data, addr):
                nonlocal called
                called = True

            deadline = Instant.now() + 2.0
            with patch.object(t._loop, "sock_sendto", mock_sock_sendto):
                await t.async_sendto(sock, b"retry", ("127.0.0.1", sock.getsockname()[1]), deadline)
            assert called
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_deadline_exceeded_during_wait(self):
        """sock_sendto hangs forever, short deadline -> SendError."""
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            sock = t._tx_socks[0]

            async def mock_sock_sendto(s, data, addr):
                await asyncio.sleep(100)

            deadline = Instant.now() + 0.05  # 50ms
            with patch.object(t._loop, "sock_sendto", mock_sock_sendto):
                with pytest.raises(SendError):
                    await t.async_sendto(sock, b"block", ("127.0.0.1", 9999), deadline)
        finally:
            t.close()

    @pytest.mark.asyncio
    async def test_sendto_os_error_propagates(self):
        """Non-BlockingIOError OSError propagated correctly."""
        t = UDPTransport.new_loopback()
        assert isinstance(t, _UDPTransportImpl)
        try:
            sock = t._tx_socks[0]

            async def mock_sock_sendto(s, data, addr):
                raise OSError("Network unreachable")

            deadline = Instant.now() + 2.0
            with patch.object(t._loop, "sock_sendto", mock_sock_sendto):
                with pytest.raises(OSError, match="Network unreachable"):
                    await t.async_sendto(sock, b"fail", ("127.0.0.1", 9999), deadline)
        finally:
            t.close()
