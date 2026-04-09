from __future__ import annotations

from pycyphal2.can import Filter
from pycyphal2.can._wire import (
    CRC_INITIAL,
    CRC_RESIDUE,
    DLC_TO_LENGTH,
    HEARTBEAT_SUBJECT_ID,
    LEGACY_NODE_STATUS_SUBJECT_ID,
    LENGTH_TO_DLC,
    MTU_CAN_FD,
    TransferKind,
    crc_add,
    ensure_forced_filters,
    make_can_id,
    make_filter,
    make_tail_byte,
    pack_u32_le,
    pack_u64_le,
    parse_frame,
    serialize_transfer,
)


def test_crc_check_value() -> None:
    assert crc_add(CRC_INITIAL, b"123456789") == 0x29B1


def test_dlc_tables_match_libcanard() -> None:
    assert DLC_TO_LENGTH == (0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64)
    assert LENGTH_TO_DLC[0] == 0
    assert LENGTH_TO_DLC[8] == 8
    assert LENGTH_TO_DLC[9] == 9
    assert LENGTH_TO_DLC[12] == 9
    assert LENGTH_TO_DLC[13] == 10
    assert LENGTH_TO_DLC[16] == 10
    assert LENGTH_TO_DLC[17] == 11
    assert LENGTH_TO_DLC[20] == 11
    assert LENGTH_TO_DLC[21] == 12
    assert LENGTH_TO_DLC[24] == 12
    assert LENGTH_TO_DLC[25] == 13
    assert LENGTH_TO_DLC[32] == 13
    assert LENGTH_TO_DLC[33] == 14
    assert LENGTH_TO_DLC[48] == 14
    assert LENGTH_TO_DLC[49] == 15
    assert LENGTH_TO_DLC[64] == 15


def test_can_id_layouts_roundtrip() -> None:
    msg16 = make_can_id(TransferKind.MESSAGE_16, 3, 0xABCD, 42)
    assert ((msg16 >> 26) & 0x07) == 3
    assert ((msg16 >> 25) & 0x01) == 0
    assert ((msg16 >> 24) & 0x01) == 0
    assert ((msg16 >> 8) & 0xFFFF) == 0xABCD
    assert ((msg16 >> 7) & 0x01) == 1
    assert (msg16 & 0x7F) == 42

    msg13 = make_can_id(TransferKind.MESSAGE_13, 4, 123, 17)
    parsed13 = parse_frame(msg13, b"xyz" + bytes([make_tail_byte(True, True, True, 5)]))
    assert parsed13 is not None
    assert parsed13.kind is TransferKind.MESSAGE_13
    assert parsed13.source_id == 17

    request = make_can_id(TransferKind.REQUEST, 2, 0x1FF, 10, 20)
    assert ((request >> 26) & 0x07) == 2
    assert ((request >> 25) & 0x01) == 1
    assert ((request >> 24) & 0x01) == 1
    assert ((request >> 14) & 0x1FF) == 0x1FF
    assert ((request >> 7) & 0x7F) == 20
    assert (request & 0x7F) == 10


def test_tail_byte_formula() -> None:
    for sot in (False, True):
        for eot in (False, True):
            for toggle in (False, True):
                for tid in range(32):
                    expected = (0x80 if sot else 0) | (0x40 if eot else 0) | (0x20 if toggle else 0) | (tid & 0x1F)
                    assert make_tail_byte(sot, eot, toggle, tid) == expected


def test_multiframe_layout_and_residue() -> None:
    payload = bytes(range(14))
    _, frames = serialize_transfer(
        kind=TransferKind.MESSAGE_16,
        priority=0,
        port_id=7,
        source_id=5,
        payload=payload,
        transfer_id=5,
        fd=False,
    )
    assert len(frames) == 3
    assert frames[0][:7] == payload[:7]
    assert frames[1][:7] == payload[7:14]
    assert (frames[0][-1], frames[1][-1], frames[2][-1]) == (
        make_tail_byte(True, False, True, 5),
        make_tail_byte(False, False, False, 5),
        make_tail_byte(False, True, True, 5),
    )
    crc = crc_add(CRC_INITIAL, payload)
    assert frames[2][:2] == bytes([(crc >> 8) & 0xFF, crc & 0xFF])

    running = CRC_INITIAL
    for frame in frames:
        running = crc_add(running, frame[:-1])
    assert running == CRC_RESIDUE


def test_pack_helpers() -> None:
    assert pack_u32_le(0x12345678) == b"\x78\x56\x34\x12"
    assert pack_u64_le(0x0123456789ABCDEF) == b"\xef\xcd\xab\x89\x67\x45\x23\x01"


def test_filter_masks_and_forced_heartbeat() -> None:
    assert make_filter(TransferKind.MESSAGE_16, 10, 1).mask == 0x03FFFF80
    assert make_filter(TransferKind.MESSAGE_13, 123, 1).mask == 0x029FFF80
    assert make_filter(TransferKind.V0_MESSAGE, LEGACY_NODE_STATUS_SUBJECT_ID, 1).mask == 0x00FFFF80
    assert make_filter(TransferKind.REQUEST, 511, 42).mask == 0x03FFFF80

    fused = Filter.coalesce(
        [
            make_filter(TransferKind.MESSAGE_16, 10, 1),
            make_filter(TransferKind.MESSAGE_16, 11, 1),
            make_filter(TransferKind.MESSAGE_13, 123, 1),
        ],
        2,
    )
    assert len(fused) == 2

    forced = ensure_forced_filters([make_filter(TransferKind.MESSAGE_16, 10, 1)], 1)
    heartbeat = make_filter(TransferKind.MESSAGE_13, HEARTBEAT_SUBJECT_ID, 1)
    assert any((heartbeat.id & flt.mask) == (flt.id & flt.mask) for flt in forced)
    legacy_node_status = make_filter(TransferKind.V0_MESSAGE, LEGACY_NODE_STATUS_SUBJECT_ID, 1)
    assert any((legacy_node_status.id & flt.mask) == (flt.id & flt.mask) for flt in forced)


def test_parse_frame_accepts_v0_start_frame() -> None:
    identifier = (0 << 26) | (LEGACY_NODE_STATUS_SUBJECT_ID << 8) | 5
    data = b"abc" + bytes([make_tail_byte(True, True, False, 0)])
    parsed = parse_frame(identifier, data)
    assert parsed is not None
    assert parsed.kind is TransferKind.V0_MESSAGE
    assert parsed.port_id == LEGACY_NODE_STATUS_SUBJECT_ID
    assert parsed.source_id == 5


def test_parse_frame_accepts_13_bit_reserved_variants() -> None:
    data = bytes([make_tail_byte(True, True, True, 0)])

    parsed = parse_frame(0x00002A01, data)
    assert parsed is not None
    assert parsed.kind is TransferKind.MESSAGE_13
    assert parsed.port_id == 42
    assert parsed.source_id == 1

    parsed = parse_frame(0x00602A01, data)
    assert parsed is not None
    assert parsed.kind is TransferKind.MESSAGE_13
    assert parsed.port_id == 42
    assert parsed.source_id == 1


def test_parse_frame_non_eot_fd_accepts_classic_sized_frame() -> None:
    identifier = make_can_id(TransferKind.MESSAGE_16, 0, 7, 5)
    short_fd = b"abcdefg" + bytes([make_tail_byte(True, False, True, 0)])
    parsed = parse_frame(identifier, short_fd, mtu=MTU_CAN_FD)
    assert parsed is not None
    assert parsed.payload == b"abcdefg"
