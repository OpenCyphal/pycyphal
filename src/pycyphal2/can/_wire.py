from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
import struct
from typing import Iterable, Sequence

from .._hash import (
    CRC16CCITT_FALSE_INITIAL,
    CRC16CCITT_FALSE_RESIDUE,
    crc16ccitt_false_add,
)
from ._interface import Filter

CAN_EXT_ID_MASK = (1 << 29) - 1
NODE_ID_MAX = 127
NODE_ID_ANONYMOUS = 0xFF
NODE_ID_CAPACITY = NODE_ID_MAX + 1
SUBJECT_ID_MAX_13 = 8191
SUBJECT_ID_MAX_16 = 0xFFFF
SERVICE_ID_MAX = 511
SERVICE_ID_MAX_V0 = 0xFF
PRIORITY_COUNT = 8
TRANSFER_ID_MODULO = 32
TRANSFER_ID_MAX = TRANSFER_ID_MODULO - 1
MTU_CAN_CLASSIC = 8
MTU_CAN_FD = 64
UNICAST_SERVICE_ID = 511
HEARTBEAT_SUBJECT_ID = 7509
LEGACY_NODE_STATUS_SUBJECT_ID = 341
TRANSFER_ID_TIMEOUT_NS = 2_000_000_000
RX_SESSION_TIMEOUT_NS = 30_000_000_000
RX_SESSION_RETENTION_NS = max(RX_SESSION_TIMEOUT_NS, TRANSFER_ID_TIMEOUT_NS)
CRC_INITIAL = CRC16CCITT_FALSE_INITIAL
CRC_RESIDUE = CRC16CCITT_FALSE_RESIDUE
CRC_BYTES = 2
TAIL_SOT = 0x80
TAIL_EOT = 0x40
TAIL_TOGGLE = 0x20
PRIO_SHIFT = 26
PADDING_BYTE = 0x00

DLC_TO_LENGTH: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64)


def _make_length_to_dlc() -> tuple[int, ...]:
    out = [0] * (MTU_CAN_FD + 1)
    dlc = 0
    for length in range(MTU_CAN_FD + 1):
        while DLC_TO_LENGTH[dlc] < length:
            dlc += 1
        out[length] = dlc
    return tuple(out)


LENGTH_TO_DLC = _make_length_to_dlc()


class TransferKind(Enum):
    MESSAGE_16 = auto()
    MESSAGE_13 = auto()
    REQUEST = auto()
    RESPONSE = auto()
    V0_MESSAGE = auto()
    V0_REQUEST = auto()
    V0_RESPONSE = auto()


@dataclass(frozen=True)
class ParsedFrame:
    kind: TransferKind
    priority: int
    port_id: int
    source_id: int
    destination_id: int | None
    transfer_id: int
    start_of_transfer: bool
    end_of_transfer: bool
    toggle: bool
    payload: bytes


def crc_add_byte(crc: int, value: int) -> int:
    return crc16ccitt_false_add(crc, bytes((value & 0xFF,)))


def crc_add(crc: int, data: bytes | bytearray | memoryview) -> int:
    return crc16ccitt_false_add(crc, memoryview(data))


def make_tail_byte(start_of_transfer: bool, end_of_transfer: bool, toggle: bool, transfer_id: int) -> int:
    return (
        (TAIL_SOT if start_of_transfer else 0)
        | (TAIL_EOT if end_of_transfer else 0)
        | (TAIL_TOGGLE if toggle else 0)
        | (transfer_id & TRANSFER_ID_MAX)
    )


def ceil_frame_payload_size(size: int) -> int:
    if not (0 <= size <= MTU_CAN_FD):
        raise ValueError(f"Invalid frame payload size: {size}")
    return DLC_TO_LENGTH[LENGTH_TO_DLC[size]]


def serialize_transfer(
    kind: TransferKind,
    priority: int,
    port_id: int,
    source_id: int,
    payload: bytes | memoryview,
    transfer_id: int,
    *,
    destination_id: int | None = None,
    fd: bool = False,
) -> tuple[int, list[bytes]]:
    payload_bytes = bytes(payload)
    mtu = MTU_CAN_FD if fd else MTU_CAN_CLASSIC
    can_id = make_can_id(kind, priority, port_id, source_id, destination_id=destination_id)
    toggle = True
    if len(payload_bytes) < mtu:
        frame_size = ceil_frame_payload_size(len(payload_bytes) + 1)
        tail = bytes((make_tail_byte(True, True, toggle, transfer_id),))
        return can_id, [payload_bytes + (bytes(frame_size - len(payload_bytes) - 1)) + tail]

    size_with_crc = len(payload_bytes) + CRC_BYTES
    crc = CRC_INITIAL
    offset = 0
    frames: list[bytes] = []
    while offset < size_with_crc:
        if (size_with_crc - offset) < (mtu - 1):
            frame_size_with_tail = ceil_frame_payload_size((size_with_crc - offset) + 1)
        else:
            frame_size_with_tail = mtu
        frame_size = frame_size_with_tail - 1
        buf = bytearray(frame_size_with_tail)
        frame_offset = 0
        if offset < len(payload_bytes):
            move_size = min(len(payload_bytes) - offset, frame_size)
            buf[0:move_size] = payload_bytes[offset : offset + move_size]
            crc = crc_add(crc, memoryview(buf)[:move_size])
            frame_offset += move_size
            offset += move_size
        if offset >= len(payload_bytes):
            while (frame_offset + CRC_BYTES) < frame_size:
                buf[frame_offset] = PADDING_BYTE
                crc = crc_add_byte(crc, PADDING_BYTE)
                frame_offset += 1
            if frame_offset < frame_size and offset == len(payload_bytes):
                buf[frame_offset] = (crc >> 8) & 0xFF
                frame_offset += 1
                offset += 1
            if frame_offset < frame_size and offset > len(payload_bytes):
                buf[frame_offset] = crc & 0xFF
                frame_offset += 1
                offset += 1
        assert frame_offset + 1 == frame_size_with_tail
        buf[frame_offset] = make_tail_byte(len(frames) == 0, offset >= size_with_crc, toggle, transfer_id)
        frames.append(bytes(buf))
        toggle = not toggle
    return can_id, frames


def parse_frame(identifier: int, data: bytes | memoryview, *, mtu: int = MTU_CAN_CLASSIC) -> ParsedFrame | None:
    parsed = parse_frames(identifier, data, mtu=mtu)
    for item in parsed:
        if item.kind in (
            TransferKind.MESSAGE_16,
            TransferKind.MESSAGE_13,
            TransferKind.REQUEST,
            TransferKind.RESPONSE,
        ):
            return item
    return parsed[0] if parsed else None


def parse_frames(identifier: int, data: bytes | memoryview, *, mtu: int = MTU_CAN_CLASSIC) -> tuple[ParsedFrame, ...]:
    payload_raw = bytes(data)
    if not (1 <= mtu <= MTU_CAN_FD):
        raise ValueError(f"Invalid MTU: {mtu}")
    if not (0 <= identifier <= CAN_EXT_ID_MASK):
        return ()
    if len(payload_raw) < 1:
        return ()
    tail = payload_raw[-1]
    start = (tail & TAIL_SOT) != 0
    end = (tail & TAIL_EOT) != 0
    toggle = (tail & TAIL_TOGGLE) != 0
    transfer_id = tail & TRANSFER_ID_MAX
    payload = payload_raw[:-1]
    payload_ok = (end or (len(payload_raw) >= MTU_CAN_CLASSIC)) and ((start and end) or (len(payload) > 0))
    if not payload_ok:
        return ()
    priority = (identifier >> PRIO_SHIFT) & 0x07
    source_id = identifier & NODE_ID_MAX
    out: list[ParsedFrame] = []

    if not (start and toggle):
        service_v0 = (identifier & (1 << 7)) != 0
        if service_v0:
            destination_id = (identifier >> 8) & NODE_ID_MAX
            port_id = (identifier >> 16) & SERVICE_ID_MAX_V0
            request = (identifier & (1 << 15)) != 0
            if destination_id != 0 and source_id != 0 and source_id != destination_id:
                out.append(
                    ParsedFrame(
                        kind=TransferKind.V0_REQUEST if request else TransferKind.V0_RESPONSE,
                        priority=priority,
                        port_id=port_id,
                        source_id=source_id,
                        destination_id=destination_id,
                        transfer_id=transfer_id,
                        start_of_transfer=start,
                        end_of_transfer=end,
                        toggle=toggle,
                        payload=payload,
                    )
                )
        else:
            source_id_v0 = NODE_ID_ANONYMOUS if source_id == 0 else source_id
            if source_id_v0 != NODE_ID_ANONYMOUS or (start and end):
                out.append(
                    ParsedFrame(
                        kind=TransferKind.V0_MESSAGE,
                        priority=priority,
                        port_id=(identifier >> 8) & SUBJECT_ID_MAX_16,
                        source_id=source_id_v0,
                        destination_id=None,
                        transfer_id=transfer_id,
                        start_of_transfer=start,
                        end_of_transfer=end,
                        toggle=toggle,
                        payload=payload,
                    )
                )

    if start and not toggle:
        return tuple(out)
    service = (identifier & (1 << 25)) != 0
    bit_23 = (identifier & (1 << 23)) != 0
    if service:
        destination_id = (identifier >> 7) & NODE_ID_MAX
        port_id = (identifier >> 14) & SERVICE_ID_MAX
        request = (identifier & (1 << 24)) != 0
        if not (bit_23 or (source_id == destination_id)):
            out.append(
                ParsedFrame(
                    kind=TransferKind.REQUEST if request else TransferKind.RESPONSE,
                    priority=priority,
                    port_id=port_id,
                    source_id=source_id,
                    destination_id=destination_id,
                    transfer_id=transfer_id,
                    start_of_transfer=start,
                    end_of_transfer=end,
                    toggle=toggle,
                    payload=payload,
                )
            )
        return tuple(out)
    destination_id_msg: int | None = None
    if (identifier & (1 << 7)) != 0:
        if (identifier & (1 << 24)) == 0:
            out.append(
                ParsedFrame(
                    kind=TransferKind.MESSAGE_16,
                    priority=priority,
                    port_id=(identifier >> 8) & SUBJECT_ID_MAX_16,
                    source_id=source_id,
                    destination_id=destination_id_msg,
                    transfer_id=transfer_id,
                    start_of_transfer=start,
                    end_of_transfer=end,
                    toggle=toggle,
                    payload=payload,
                )
            )
        return tuple(out)
    if bit_23:
        return tuple(out)
    anonymous = (identifier & (1 << 24)) != 0
    if anonymous:
        if not (start and end):
            return tuple(out)
        source_id = NODE_ID_ANONYMOUS
    out.append(
        ParsedFrame(
            kind=TransferKind.MESSAGE_13,
            priority=priority,
            port_id=(identifier >> 8) & SUBJECT_ID_MAX_13,
            source_id=source_id,
            destination_id=destination_id_msg,
            transfer_id=transfer_id,
            start_of_transfer=start,
            end_of_transfer=end,
            toggle=toggle,
            payload=payload,
        )
    )
    return tuple(out)


def make_can_id(
    kind: TransferKind, priority: int, port_id: int, source_id: int, destination_id: int | None = None
) -> int:
    if not (0 <= priority < PRIORITY_COUNT):
        raise ValueError(f"Invalid priority: {priority}")
    if not (0 <= source_id <= NODE_ID_MAX):
        raise ValueError(f"Invalid source node-ID: {source_id}")
    if kind is TransferKind.MESSAGE_16:
        if not (0 <= port_id <= SUBJECT_ID_MAX_16):
            raise ValueError(f"Invalid 16-bit subject-ID: {port_id}")
        return (priority << PRIO_SHIFT) | (port_id << 8) | (1 << 7) | source_id
    if kind is TransferKind.MESSAGE_13:
        if not (0 <= port_id <= SUBJECT_ID_MAX_13):
            raise ValueError(f"Invalid 13-bit subject-ID: {port_id}")
        return (priority << PRIO_SHIFT) | (3 << 21) | (port_id << 8) | source_id
    if kind in (TransferKind.V0_MESSAGE, TransferKind.V0_REQUEST, TransferKind.V0_RESPONSE):
        raise ValueError(f"Legacy v0 TX is not supported: {kind}")
    if destination_id is None or not (0 <= destination_id <= NODE_ID_MAX):
        raise ValueError(f"Invalid destination node-ID: {destination_id}")
    if not (0 <= port_id <= SERVICE_ID_MAX):
        raise ValueError(f"Invalid service-ID: {port_id}")
    request_not_response = 1 if kind is TransferKind.REQUEST else 0
    if kind not in (TransferKind.REQUEST, TransferKind.RESPONSE):
        raise ValueError(f"Unsupported transfer kind for service frame: {kind}")
    return (
        (priority << PRIO_SHIFT)
        | (1 << 25)
        | (request_not_response << 24)
        | (port_id << 14)
        | (destination_id << 7)
        | source_id
    )


def make_filter(kind: TransferKind, port_id: int, local_node_id: int) -> Filter:
    if not (0 <= local_node_id <= NODE_ID_MAX):
        raise ValueError(f"Invalid local node-ID: {local_node_id}")
    if kind is TransferKind.MESSAGE_16:
        return Filter(id=(port_id << 8) | (1 << 7), mask=0x03FFFF80)
    if kind is TransferKind.MESSAGE_13:
        return Filter(id=port_id << 8, mask=0x029FFF80)
    if kind is TransferKind.V0_MESSAGE:
        return Filter(id=port_id << 8, mask=0x00FFFF80)
    if kind in (TransferKind.REQUEST, TransferKind.RESPONSE):
        request_bit = 1 << 24 if kind is TransferKind.REQUEST else 0
        return Filter(id=(1 << 25) | request_bit | (port_id << 14) | (local_node_id << 7), mask=0x03FFFF80)
    if kind in (TransferKind.V0_REQUEST, TransferKind.V0_RESPONSE):
        request_bit = 1 << 15 if kind is TransferKind.V0_REQUEST else 0
        return Filter(id=((port_id & 0xFF) << 16) | request_bit | (local_node_id << 8) | (1 << 7), mask=0x00FFFF80)
    raise ValueError(f"Unsupported transfer kind: {kind}")


def match_filters(filters: Sequence[Filter], identifier: int) -> bool:
    return any((identifier & flt.mask) == (flt.id & flt.mask) for flt in filters)


def ensure_forced_filters(filters: Iterable[Filter], local_node_id: int) -> list[Filter]:
    out = list(filters)
    forced = (
        make_filter(TransferKind.MESSAGE_13, HEARTBEAT_SUBJECT_ID, local_node_id),
        make_filter(TransferKind.V0_MESSAGE, LEGACY_NODE_STATUS_SUBJECT_ID, local_node_id),
    )
    for flt in forced:
        if not match_filters(out, flt.id):
            out.append(flt)
    return out


def pack_u32_le(value: int) -> bytes:
    return struct.pack("<I", value & 0xFFFFFFFF)


def pack_u64_le(value: int) -> bytes:
    return struct.pack("<Q", value & 0xFFFFFFFFFFFFFFFF)
