from __future__ import annotations

import pytest

from pycyphal2.can import Filter
from pycyphal2.can._wire import (
    CAN_EXT_ID_MASK,
    CRC_INITIAL,
    CRC_RESIDUE,
    HEARTBEAT_SUBJECT_ID,
    LEGACY_NODE_STATUS_SUBJECT_ID,
    MTU_CAN_CLASSIC,
    MTU_CAN_FD,
    NODE_ID_ANONYMOUS,
    ParsedFrame,
    TransferKind,
    ceil_frame_payload_size,
    crc_add,
    crc_add_byte,
    ensure_forced_filters,
    make_can_id,
    make_filter,
    make_tail_byte,
    match_filters,
    parse_frame,
    parse_frames,
    serialize_transfer,
)


def _frame(
    kind: TransferKind, *, start: bool, end: bool, toggle: bool, payload: bytes = b"x", **kwargs: int
) -> ParsedFrame:
    identifier = make_can_id(kind=kind, priority=kwargs.pop("priority", 0), **kwargs)
    data = payload + bytes([make_tail_byte(start, end, toggle, kwargs.pop("transfer_id", 0))])
    out = parse_frame(identifier, data, mtu=kwargs.pop("mtu", MTU_CAN_CLASSIC))
    assert out is not None
    return out


def test_crc_vectors() -> None:
    assert crc_add_byte(CRC_INITIAL, ord("1")) == crc_add(CRC_INITIAL, b"1")
    assert crc_add(CRC_INITIAL, b"") == CRC_INITIAL
    assert crc_add(0x1234, b"") == 0x1234

    payload = b"123456789"
    crc = crc_add(CRC_INITIAL, payload)
    augmented = payload + bytes([(crc >> 8) & 0xFF, crc & 0xFF])
    assert crc_add(CRC_INITIAL, augmented) == CRC_RESIDUE


def test_ceil_frame_payload_size_bounds() -> None:
    assert ceil_frame_payload_size(0) == 0
    assert ceil_frame_payload_size(9) == 12
    assert ceil_frame_payload_size(64) == 64

    with pytest.raises(ValueError, match="Invalid frame payload size"):
        ceil_frame_payload_size(-1)

    with pytest.raises(ValueError, match="Invalid frame payload size"):
        ceil_frame_payload_size(65)


def test_serialize_transfer_fd_padding_and_crc_split() -> None:
    payload = bytes(range(70))
    _, frames = serialize_transfer(
        kind=TransferKind.MESSAGE_16,
        priority=0,
        port_id=1000,
        source_id=10,
        payload=payload,
        transfer_id=17,
        fd=True,
    )

    assert len(frames) == 2
    assert len(frames[0]) == MTU_CAN_FD
    assert len(frames[1]) == 12
    assert frames[1][:7] == payload[63:]
    assert frames[1][7:9] == b"\x00\x00"

    running = CRC_INITIAL
    for frame in frames:
        running = crc_add(running, frame[:-1])
    assert running == CRC_RESIDUE


def test_parse_frames_validation_vectors() -> None:
    with pytest.raises(ValueError, match="Invalid MTU"):
        parse_frames(0, b"x", mtu=0)

    assert parse_frames(CAN_EXT_ID_MASK + 1, b"x") == ()
    assert parse_frames(0, b"") == ()
    assert parse_frames(0, bytes([make_tail_byte(False, False, False, 0)])) == ()
    assert parse_frames(0, b"x" + bytes([make_tail_byte(False, False, False, 0)])) == ()


def test_parse_frames_v0_service_and_message_vectors() -> None:
    valid_service_id = (2 << 26) | (0x12 << 16) | (1 << 15) | (23 << 8) | (1 << 7) | 42
    parsed = parse_frames(valid_service_id, b"svc" + bytes([make_tail_byte(False, True, False, 7)]))
    assert parsed[0] == ParsedFrame(
        kind=TransferKind.V0_REQUEST,
        priority=2,
        port_id=0x12,
        source_id=42,
        destination_id=23,
        transfer_id=7,
        start_of_transfer=False,
        end_of_transfer=True,
        toggle=False,
        payload=b"svc",
    )
    assert parsed[1].kind is TransferKind.MESSAGE_16

    # Zero destination/source and self-addressing are rejected for v0 services.
    for identifier in (
        (1 << 7) | 0,
        (1 << 7) | (10 << 8),
        (1 << 7) | (33 << 8) | 33,
    ):
        assert parse_frames(identifier, b"x" + bytes([make_tail_byte(True, True, False, 0)])) == ()

    anonymous_v0 = parse_frames(1 << 23, b"x" + bytes([make_tail_byte(False, True, False, 0)]))
    assert anonymous_v0 == ()

    valid_v0_message = parse_frames(0x0002347F, b"m" + bytes([make_tail_byte(True, True, False, 3)]))
    assert valid_v0_message[0].kind is TransferKind.V0_MESSAGE
    assert valid_v0_message[0].source_id == 0x7F


def test_parse_frames_v1_cases() -> None:
    service = parse_frame(
        make_can_id(TransferKind.REQUEST, 3, 511, 21, destination_id=42),
        b"rq" + bytes([make_tail_byte(True, True, True, 5)]),
    )
    assert service is not None
    assert service.kind is TransferKind.REQUEST
    assert service.destination_id == 42

    response = parse_frame(
        make_can_id(TransferKind.RESPONSE, 1, 111, 9, destination_id=77),
        b"rs" + bytes([make_tail_byte(True, True, True, 6)]),
    )
    assert response is not None
    assert response.kind is TransferKind.RESPONSE
    assert response.destination_id == 77

    dual_identifier = make_can_id(TransferKind.MESSAGE_16, 4, 0x1234, 42)
    dual = parse_frames(dual_identifier, b"ABCDEFG" + bytes([make_tail_byte(False, False, False, 1)]))
    assert [item.kind for item in dual] == [TransferKind.V0_RESPONSE, TransferKind.MESSAGE_16]
    preferred = parse_frame(dual_identifier, b"ABCDEFG" + bytes([make_tail_byte(False, False, False, 1)]))
    assert preferred is not None
    assert preferred.kind is TransferKind.MESSAGE_16

    start_false_toggle = parse_frames(dual_identifier, b"x" + bytes([make_tail_byte(True, True, False, 2)]))
    assert [item.kind for item in start_false_toggle] == [TransferKind.V0_RESPONSE]

    reserved_bit_23 = (1 << 23) | (42 << 8) | 1
    assert parse_frames(reserved_bit_23, b"x" + bytes([make_tail_byte(True, True, True, 0)])) == ()
    v0_only = parse_frames(reserved_bit_23, b"z" + bytes([make_tail_byte(False, True, False, 0)]))
    assert [item.kind for item in v0_only] == [TransferKind.V0_MESSAGE]

    bit24_rejected = (1 << 24) | (123 << 8) | (1 << 7) | 5
    assert parse_frames(bit24_rejected, b"x" + bytes([make_tail_byte(True, True, True, 0)])) == ()

    self_addressed = make_can_id(TransferKind.REQUEST, 0, 77, 33, destination_id=33)
    assert parse_frames(self_addressed, b"x" + bytes([make_tail_byte(True, True, True, 0)])) == ()

    anonymous_multiframe = (3 << 21) | (1 << 24)
    assert parse_frames(anonymous_multiframe, b"x" + bytes([make_tail_byte(False, True, True, 0)])) == ()

    valid_anonymous = parse_frames((3 << 21) | (1 << 24), b"a" + bytes([make_tail_byte(True, True, True, 31)]))
    assert valid_anonymous == (
        ParsedFrame(
            kind=TransferKind.MESSAGE_13,
            priority=0,
            port_id=0,
            source_id=NODE_ID_ANONYMOUS,
            destination_id=None,
            transfer_id=31,
            start_of_transfer=True,
            end_of_transfer=True,
            toggle=True,
            payload=b"a",
        ),
    )


def test_make_can_id_validation_vectors() -> None:
    with pytest.raises(ValueError, match="Invalid priority"):
        make_can_id(TransferKind.MESSAGE_16, -1, 0, 0)

    with pytest.raises(ValueError, match="Invalid source node-ID"):
        make_can_id(TransferKind.MESSAGE_16, 0, 0, 128)

    with pytest.raises(ValueError, match="Invalid 16-bit subject-ID"):
        make_can_id(TransferKind.MESSAGE_16, 0, 0x1_0000, 0)

    with pytest.raises(ValueError, match="Invalid 13-bit subject-ID"):
        make_can_id(TransferKind.MESSAGE_13, 0, 0x2000, 0)

    with pytest.raises(ValueError, match="Legacy v0 TX is not supported"):
        make_can_id(TransferKind.V0_MESSAGE, 0, 1, 1)

    with pytest.raises(ValueError, match="Invalid destination node-ID"):
        make_can_id(TransferKind.REQUEST, 0, 1, 1)

    with pytest.raises(ValueError, match="Invalid service-ID"):
        make_can_id(TransferKind.REQUEST, 0, 512, 1, destination_id=2)

    with pytest.raises(ValueError, match="Unsupported transfer kind"):
        make_can_id("bad", 0, 1, 1, destination_id=2)  # type: ignore[arg-type]


def test_make_filter_and_forced_filter_vectors() -> None:
    v0_request = make_filter(TransferKind.V0_REQUEST, 0xAB, 21)
    assert v0_request == Filter(id=(0xAB << 16) | (1 << 15) | (21 << 8) | (1 << 7), mask=0x00FFFF80)

    v0_response = make_filter(TransferKind.V0_RESPONSE, 0x12, 99)
    assert v0_response == Filter(id=(0x12 << 16) | (99 << 8) | (1 << 7), mask=0x00FFFF80)

    with pytest.raises(ValueError, match="Invalid local node-ID"):
        make_filter(TransferKind.MESSAGE_16, 0, 128)

    with pytest.raises(ValueError, match="Unsupported transfer kind"):
        make_filter("bad", 0, 1)  # type: ignore[arg-type]

    forced = ensure_forced_filters(
        [
            make_filter(TransferKind.MESSAGE_13, HEARTBEAT_SUBJECT_ID, 7),
            make_filter(TransferKind.V0_MESSAGE, LEGACY_NODE_STATUS_SUBJECT_ID, 7),
        ],
        7,
    )
    assert len(forced) == 2

    extra = ensure_forced_filters([make_filter(TransferKind.MESSAGE_16, 200, 7)], 7)
    assert match_filters(extra, make_filter(TransferKind.MESSAGE_13, HEARTBEAT_SUBJECT_ID, 7).id)
    assert match_filters(extra, make_filter(TransferKind.V0_MESSAGE, LEGACY_NODE_STATUS_SUBJECT_ID, 7).id)
