"""Comprehensive tests for pycyphal.udp -- Cyphal/UDP transport."""
from __future__ import annotations

import asyncio
import os
import struct
from ipaddress import IPv4Address

import pytest

from pycyphal import (
    Instant,
    Priority,
    SendError,
    TransportArrival,
    SUBJECT_ID_MODULUS_17bit,
    SUBJECT_ID_MODULUS_23bit,
    SUBJECT_ID_MODULUS_32bit,
)
from pycyphal.udp import (
    CRC_INITIAL,
    CRC_OUTPUT_XOR,
    CRC_RESIDUE,
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
    _TransferSlot,
    _crc_add,
    _header_deserialize,
    _header_serialize,
    _segment_transfer,
    crc32c,
    generate_uid,
    make_subject_endpoint,
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
        assert isinstance(crc32c(b"\xFF"), int)

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
            prefix_crc = crc32c(b"test")
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
        hdr = _header_serialize(4, 42, 12345, 0, 100, crc32c(b"x"))
        for byte_idx in range(HEADER_SIZE):
            for bit in range(8):
                corrupted = bytearray(hdr)
                corrupted[byte_idx] ^= 1 << bit
                assert _header_deserialize(bytes(corrupted)) is None, f"Bit flip at byte {byte_idx} bit {bit} not caught"

    def test_wrong_version(self):
        hdr = bytearray(_header_serialize(4, 42, 12345, 0, 100, 0))
        # Set version to 3 (clear bit 1, keep bit 0 set, set bit 1 to make version=3)
        hdr[0] = (hdr[0] & 0xE0) | 3  # version=3, keep priority
        # Re-compute header CRC
        struct.pack_into("<I", hdr, 28, crc32c(bytes(hdr[:28])))
        assert _header_deserialize(bytes(hdr)) is None

    def test_incompatibility_rejected(self):
        hdr = bytearray(_header_serialize(4, 42, 12345, 0, 100, 0))
        hdr[1] = 0xE0  # Set all 3 incompatibility bits
        struct.pack_into("<I", hdr, 28, crc32c(bytes(hdr[:28])))
        assert _header_deserialize(bytes(hdr)) is None

    def test_too_short(self):
        assert _header_deserialize(b"") is None
        assert _header_deserialize(b"\x00" * 31) is None

    def test_transfer_id_48bit_wrap(self):
        """transfer_id > 48 bits gets truncated to 48 bits."""
        big_tid = (1 << 48) + 42
        hdr = _header_serialize(0, big_tid, 0, 0, 0, crc32c(b""))
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
        assert hdr.prefix_crc == crc32c(payload)
        assert frames[0][HEADER_SIZE:] == payload

    def test_multi_frame(self):
        """Payload of 350 bytes with MTU 100 -> 4 frames."""
        payload = os.urandom(350)
        frames = _segment_transfer(2, 99, 200, payload, mtu=100)
        assert len(frames) == 4  # ceil(350/100) = 4

        offset = 0
        running_crc = CRC_INITIAL
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
            running_crc = _crc_add(running_crc, chunk)
            assert hdr.prefix_crc == (running_crc ^ CRC_OUTPUT_XOR)
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
        assert hdr.prefix_crc == crc32c(b"")

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
    def _make_frames(self, payload: bytes, mtu: int, sender_uid: int = 1000, transfer_id: int = 42,
                     priority: int = 4) -> list[tuple[_FrameHeader, bytes]]:
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
        sender_uid, priority, message = result
        assert message == payload
        assert sender_uid == 1000
        assert priority == 4

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
        assert result[2] == payload

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
        assert result[2] == payload

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
        assert result[2] == payload

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
        assert result_b[2] == payload_b

        result_a = reasm.accept(frames_a[0][0], frames_a[0][1])
        assert result_a is not None
        assert result_a[2] == payload_a

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
        assert result_a[2] == payload_a

        result_b = reasm.accept(frames_b[1][0], frames_b[1][1])
        assert result_b is not None
        assert result_b[2] == payload_b

    def test_different_senders(self):
        """Frames from different senders reassembled independently."""
        payload_x = b"from sender X"
        payload_y = b"from sender Y"
        reasm = _RxReassembler()

        frames_x = self._make_frames(payload_x, mtu=1400, sender_uid=100, transfer_id=1)
        frames_y = self._make_frames(payload_y, mtu=1400, sender_uid=200, transfer_id=1)

        rx = reasm.accept(frames_x[0][0], frames_x[0][1])
        assert rx is not None and rx[2] == payload_x
        ry = reasm.accept(frames_y[0][0], frames_y[0][1])
        assert ry is not None and ry[2] == payload_y

    def test_empty_payload(self):
        payload = b""
        reasm = _RxReassembler()
        frame_pairs = self._make_frames(payload, mtu=1400)
        assert len(frame_pairs) == 1
        result = reasm.accept(frame_pairs[0][0], frame_pairs[0][1])
        assert result is not None
        assert result[2] == b""

    def test_bounds_violation_rejected(self):
        """Frame where offset + chunk_size > transfer_payload_size should be rejected."""
        reasm = _RxReassembler()
        # Manually create a bad header
        hdr = _FrameHeader(priority=4, transfer_id=1, sender_uid=1,
                           frame_payload_offset=5, transfer_payload_size=6, prefix_crc=0)
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
        bad_hdr = _FrameHeader(priority=4, transfer_id=42, sender_uid=1000,
                               frame_payload_offset=100, transfer_payload_size=300,
                               prefix_crc=0)
        result = reasm.accept(bad_hdr, os.urandom(100))
        assert result is None


class TestTransferSlot:
    def test_coverage_tracking(self):
        slot = _TransferSlot(100)
        slot.add_fragment(0, b"a" * 30, 0)
        assert slot.covered == 30
        slot.add_fragment(50, b"b" * 30, 0)
        assert slot.covered == 30  # Gap at 30-50
        slot.add_fragment(30, b"c" * 20, 0)
        assert slot.covered == 80
        slot.add_fragment(80, b"d" * 20, 0)
        assert slot.covered == 100
        assert slot.is_complete()

    def test_overlapping_fragments(self):
        slot = _TransferSlot(50)
        slot.add_fragment(0, b"A" * 30, 0)
        slot.add_fragment(20, b"B" * 30, 0)
        assert slot.covered == 50
        assert slot.is_complete()
        payload = slot.assemble()
        assert len(payload) == 50
        assert payload[:20] == b"A" * 20
        assert payload[20:30] == b"B" * 10  # Overlap: B wins (later write)
        assert payload[30:50] == b"B" * 20


# =====================================================================================================================
# Multicast Address Tests
# =====================================================================================================================


class TestMulticastAddress:
    def test_subject_zero(self):
        ip, port = make_subject_endpoint(0)
        assert ip == "239.0.0.0"
        assert port == UDP_PORT

    def test_subject_max(self):
        ip, port = make_subject_endpoint(IPv4_SUBJECT_ID_MAX)
        assert ip == "239.127.255.255"
        assert port == UDP_PORT

    def test_subject_one(self):
        ip, port = make_subject_endpoint(1)
        assert ip == "239.0.0.1"
        assert port == UDP_PORT

    def test_subject_masking(self):
        """Subject IDs beyond 23 bits are masked."""
        ip1, _ = make_subject_endpoint(0x800000)  # Bit 23 set, masked to 0
        ip2, _ = make_subject_endpoint(0)
        assert ip1 == ip2

    def test_various_subjects(self):
        ip, _ = make_subject_endpoint(42)
        expected_int = IPv4_MCAST_PREFIX | 42
        assert ip == str(IPv4Address(expected_int))


# =====================================================================================================================
# UID Generation Tests
# =====================================================================================================================


class TestUID:
    def test_bit_57_set(self):
        uid = generate_uid()
        assert uid & (1 << 57), "U/L bit (bit 57) must be set"

    def test_bit_56_clear(self):
        uid = generate_uid()
        assert not (uid & (1 << 56)), "I/G bit (bit 56) must be clear"

    def test_nonzero(self):
        assert generate_uid() != 0

    def test_unique(self):
        """Two calls should produce different UIDs (random component)."""
        uid1 = generate_uid()
        uid2 = generate_uid()
        assert uid1 != uid2

    def test_fits_64_bits(self):
        uid = generate_uid()
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

    def test_interface_frozen(self):
        iface = Interface(address=IPv4Address("127.0.0.1"), mtu_link=1500)
        with pytest.raises(AttributeError):
            iface.mtu_link = 9000  # type: ignore[misc]


# =====================================================================================================================
# Modulus Constants Tests
# =====================================================================================================================


class TestModulusConstants:
    def test_values(self):
        assert SUBJECT_ID_MODULUS_17bit == 122743
        assert SUBJECT_ID_MODULUS_23bit == 8378431
        assert SUBJECT_ID_MODULUS_32bit == 4294954663

    def test_exportable(self):
        import pycyphal
        assert pycyphal.SUBJECT_ID_MODULUS_17bit == 122743
        assert pycyphal.SUBJECT_ID_MODULUS_23bit == 8378431
        assert pycyphal.SUBJECT_ID_MODULUS_32bit == 4294954663


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
        prefix_crc = crc32c(b"hello")

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
        assert struct.unpack_from("<I", hdr, 28)[0] == crc32c(hdr[:28])

    def test_frame_roundtrip_with_payload(self):
        """Complete frame (header + payload) serialized and deserialized."""
        payload = b"hello"
        frames = _segment_transfer(4, 0xDEADBEEF, 12345, payload, mtu=1400)
        assert len(frames) == 1
        frame = frames[0]
        hdr = _header_deserialize(frame[:HEADER_SIZE])
        assert hdr is not None
        assert frame[HEADER_SIZE:] == payload

    def test_known_crc_value(self):
        """Verify CRC-32C against a known value to ensure table correctness."""
        # CRC-32C("123456789") = 0xE3069283 is the standard test vector
        assert crc32c(b"123456789") == 0xE3069283

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
        assert result[2] == payload


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
    async def test_single_frame_pubsub(self, loopback_iface):
        """Two transports on loopback: one publishes, the other subscribes."""
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
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
            assert received[0].remote_id == pub._uid
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_multi_frame_pubsub(self, loopback_iface):
        """Send payload larger than MTU, verify correct reassembly."""
        small_iface = Interface(address=loopback_iface.address, mtu_link=608)
        # mtu_cyphal = 508, so payload of 2000 bytes -> 4 frames
        pub = UDPTransport(interfaces=[small_iface])
        sub = UDPTransport(interfaces=[small_iface])
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
    async def test_multiple_messages(self, loopback_iface):
        """Send several messages, all received in order."""
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
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
    async def test_empty_payload(self, loopback_iface):
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
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
    async def test_unicast_roundtrip(self, loopback_iface):
        """A publishes subject message -> B learns A's endpoint -> B unicasts to A."""
        a = UDPTransport(interfaces=[loopback_iface])
        b = UDPTransport(interfaces=[loopback_iface])
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
    async def test_listener_close_stops_delivery(self, loopback_iface):
        """After closing a listener, no more messages are delivered to it."""
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
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
    async def test_multiple_listeners_same_subject(self, loopback_iface):
        """Two listeners on the same subject both receive each message."""
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
        try:
            received_a: list[TransportArrival] = []
            received_b: list[TransportArrival] = []
            sub.subject_listen(70, received_a.append)
            sub.subject_listen(70, received_b.append)

            writer = pub.subject_advertise(70)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"to both")
            await asyncio.sleep(0.1)

            assert len(received_a) == 1
            assert len(received_b) == 1
            assert received_a[0].message == b"to both"
            assert received_b[0].message == b"to both"
        finally:
            pub.close()
            sub.close()

    @pytest.mark.asyncio
    async def test_close_one_listener_keeps_other(self, loopback_iface):
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
        try:
            received_a: list[TransportArrival] = []
            received_b: list[TransportArrival] = []
            listener_a = sub.subject_listen(80, received_a.append)
            sub.subject_listen(80, received_b.append)

            writer = pub.subject_advertise(80)
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"msg1")
            await asyncio.sleep(0.1)
            assert len(received_a) == 1
            assert len(received_b) == 1

            listener_a.close()
            await writer(Instant.now() + 2.0, Priority.NOMINAL, b"msg2")
            await asyncio.sleep(0.1)
            assert len(received_a) == 1  # No more deliveries
            assert len(received_b) == 2  # Still receiving
        finally:
            pub.close()
            sub.close()


class TestIntegrationTransportClose:
    @pytest.mark.asyncio
    async def test_close_cleans_up(self, loopback_iface):
        t = UDPTransport(interfaces=[loopback_iface])
        t.subject_listen(90, lambda a: None)
        t.subject_advertise(90)

        assert len(t._tx_socks) > 0
        assert len(t._mcast_socks) > 0

        t.close()

        assert len(t._tx_socks) == 0
        assert len(t._mcast_socks) == 0
        assert t._closed

    @pytest.mark.asyncio
    async def test_close_idempotent(self, loopback_iface):
        t = UDPTransport(interfaces=[loopback_iface])
        t.close()
        t.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_operations_after_close_fail(self, loopback_iface):
        t = UDPTransport(interfaces=[loopback_iface])
        writer = t.subject_advertise(91)
        t.close()
        with pytest.raises(SendError):
            await writer(Instant.now() + 1.0, Priority.NOMINAL, b"should fail")

    @pytest.mark.asyncio
    async def test_subject_id_modulus(self, loopback_iface):
        t = UDPTransport(interfaces=[loopback_iface], subject_id_modulus=SUBJECT_ID_MODULUS_17bit)
        assert t.subject_id_modulus == SUBJECT_ID_MODULUS_17bit
        t.close()

        t2 = UDPTransport(interfaces=[loopback_iface])
        assert t2.subject_id_modulus == SUBJECT_ID_MODULUS_23bit  # default
        t2.close()


class TestIntegrationSelfSendFilter:
    @pytest.mark.asyncio
    async def test_self_send_filtered(self, loopback_iface):
        """A transport should NOT receive its own multicast messages."""
        t = UDPTransport(interfaces=[loopback_iface])
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
    async def test_messages_isolated_by_subject(self, loopback_iface):
        """Messages on different subjects don't cross-deliver."""
        pub = UDPTransport(interfaces=[loopback_iface])
        sub = UDPTransport(interfaces=[loopback_iface])
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
