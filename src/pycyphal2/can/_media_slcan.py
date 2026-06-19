"""
SLCAN text protocol for CAN media (frame codec + adapter handshake).
"""

from __future__ import annotations

import logging

from ._interface import Frame
from ._wire import CAN_EXT_ID_MASK, DLC_TO_LENGTH, MTU_CAN_CLASSIC

_logger = logging.getLogger(__name__)

_CAN_STD_ID_MASK = (1 << 11) - 1
_CR = 0x0D  # ACK / carriage return
_LF = 0x0A
_BEL = 0x07  # NACK / bell
_MAX_LINE_LENGTH = 256
_STRIP_CHARS = b" \t\r\n\x07\x03"

_CMD_TERMINATOR = bytes([_CR])
_CMD_CLOSE = b"C"
_CMD_OPEN = b"O"
_CMD_SET_BITRATE_PREFIX = b"S"
_BITRATE_TO_SPEED_CODE = {
    1_000_000: 8,
    800_000: 7,
    500_000: 6,
    250_000: 5,
    125_000: 4,
    100_000: 3,
    50_000: 2,
    20_000: 1,
    10_000: 0,
}


def encode_frame(identifier: int, data: bytes | bytearray | memoryview) -> bytes:
    """
    Encode an extended-ID Classic CAN data frame into one SLCAN ``T`` command line.
    """
    if not isinstance(identifier, int) or not (0 <= identifier <= CAN_EXT_ID_MASK):
        raise ValueError(f"Invalid CAN identifier: {identifier!r}")
    payload = bytes(data)
    if len(payload) > MTU_CAN_CLASSIC:
        raise ValueError(f"Invalid CAN data length: {len(payload)}")
    return f"T{identifier:08X}{len(payload):1d}{payload.hex().upper()}\r".encode()


def encode_deinit() -> bytes:
    """The close command line. Sent fire-and-forget before purging input to reset the adapter."""
    return _CMD_CLOSE + _CMD_TERMINATOR


def encode_init_sequence(bitrate: int | None) -> list[bytes]:
    """
    Command lines to bring the adapter up after deinit+purge: optionally set the bitrate, then open.
    If ``bitrate`` is None, the bitrate command is skipped, the old configured value (or adapter default) is kept.
    A bitrate not in the standard speed-code table is sent as-is (some adapters accept raw bitrates, e.g. Zubax).
    Each returned command line expects an ACK.
    """
    out: list[bytes] = []
    if bitrate is not None:
        code = _BITRATE_TO_SPEED_CODE.get(bitrate, bitrate)
        out.append(_CMD_SET_BITRATE_PREFIX + str(code).encode("ascii") + _CMD_TERMINATOR)
    out.append(_CMD_OPEN + _CMD_TERMINATOR)
    return out


def classify_init_response(chunk: bytes) -> bool | None:
    """Scan a chunk for the first ACK (True) or NACK (False); return None if neither appears."""
    for byte in chunk:
        if byte == _CR:
            return True
        if byte == _BEL:
            return False
    return None


class SLCANParser:
    """
    Incremental SLCAN parser.
    Only data frames are returned. Unsupported or malformed input is silently dropped with debug logging.
    Adapter-specific suffixes after the payload, such as timestamps or flags, are ignored.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._discarding = False

    def feed(self, chunk: bytes | bytearray | memoryview) -> list[Frame]:
        out: list[Frame] = []
        for byte in bytes(chunk):
            if byte == _BEL:
                if self._buffer or self._discarding:
                    _logger.debug("SLCAN drop adapter error len=%d", len(self._buffer))
                self._buffer.clear()
                self._discarding = False
                continue
            if byte in (_CR, _LF):
                if self._discarding:
                    self._buffer.clear()
                    self._discarding = False
                    continue
                if self._buffer:
                    frame = _parse_line(bytes(self._buffer))
                    if frame is not None:
                        out.append(frame)
                    self._buffer.clear()
                continue
            if self._discarding:
                continue
            if len(self._buffer) >= _MAX_LINE_LENGTH:
                _logger.debug("SLCAN drop overlong line len>%d", _MAX_LINE_LENGTH)
                self._buffer.clear()
                self._discarding = True
                continue
            self._buffer.append(byte)
        return out


def _parse_line(line: bytes) -> Frame | None:
    # Based on the original PyUAVCAN/PyDroneCAN implementation.
    # Strips surrounding whitespace and control characters like BEL/ETX.
    line = line.strip(_STRIP_CHARS)
    if not line:
        return None
    command = line[:1]
    if command in (b"T", b"x"):
        return _parse_data_frame(line, id_length=8, max_payload_length=MTU_CAN_CLASSIC)
    if command == b"t":
        return _parse_data_frame(line, id_length=3, max_payload_length=MTU_CAN_CLASSIC)
    if command == b"D":
        return _parse_data_frame(line, id_length=8, max_payload_length=64)
    if command in (b"r", b"R"):
        _logger.debug("SLCAN drop unsupported frame type cmd=%r", command)
        return None
    _logger.debug("SLCAN drop unknown line=%r", line)
    return None


def _parse_data_frame(line: bytes, *, id_length: int, max_payload_length: int) -> Frame | None:
    header_length = 2 + id_length
    if len(line) < header_length:
        _logger.debug("SLCAN drop short data line=%r", line)
        return None
    identifier = _parse_hex_int(line[1 : 1 + id_length])
    dlc = _parse_dlc(line[1 + id_length])
    if identifier is None or dlc is None:
        _logger.debug("SLCAN drop malformed data header line=%r", line)
        return None
    payload_length = DLC_TO_LENGTH[dlc]  # _parse_dlc guarantees dlc in [0, 15]
    if payload_length > max_payload_length:
        _logger.debug("SLCAN drop data dlc out of range dlc=%d max=%d line=%r", dlc, max_payload_length, line)
        return None
    expected = header_length + payload_length * 2
    if len(line) < expected:
        _logger.debug("SLCAN drop data dlc mismatch len=%d expected=%d", len(line), expected)
        return None
    if id_length == 3 and identifier > _CAN_STD_ID_MASK:
        _logger.debug("SLCAN drop invalid standard id=%x", identifier)
        return None
    data = _parse_hex_bytes(line[header_length:expected])
    if data is None:
        _logger.debug("SLCAN drop malformed data id=%08x", identifier)
        return None
    try:
        return Frame(id=identifier, data=data)
    except ValueError as ex:
        _logger.debug("SLCAN drop invalid frame: %s", ex)
        return None


def _parse_hex_int(value: bytes) -> int | None:
    if not value:
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None


def _parse_hex_bytes(value: bytes) -> bytes | None:
    if len(value) % 2 != 0:
        return None
    try:
        return bytes.fromhex(value.decode("ascii"))
    except (UnicodeDecodeError, ValueError):
        return None


def _parse_dlc(value: int) -> int | None:
    if ord("0") <= value <= ord("9"):
        return value - ord("0")
    if ord("A") <= value <= ord("F"):
        return 10 + value - ord("A")
    if ord("a") <= value <= ord("f"):
        return 10 + value - ord("a")
    return None
