"""SLCAN text codec for CAN frames."""

from __future__ import annotations

import logging

from ._interface import Frame

_logger = logging.getLogger(__name__)

_CAN_EXT_ID_MASK = (1 << 29) - 1
_CAN_STD_ID_MASK = (1 << 11) - 1
_CAN_CLASSIC_MTU = 8
SLCAN_ACK = 0x0D
SLCAN_NACK = 0x07
SLCAN_ACK_TIMEOUT = 1.0
SLCAN_DEFAULT_BITRATE = 1_000_000
SLCAN_BITRATE_TO_SPEED_CODE = {
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
SLCAN_COMMAND_TERMINATOR = b"\r"
SLCAN_COMMAND_CLOSE = b"C"
SLCAN_COMMAND_OPEN = b"O"
SLCAN_COMMAND_SET_BITRATE_PREFIX = b"S"
_CR = SLCAN_ACK
_LF = 0x0A
_BEL = SLCAN_NACK
_MAX_LINE_LENGTH = 256
_DLC_TO_LENGTH = (0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64)
_STRIP_CHARS = b" \t\r\n\x07\x03"


def encode_frame(identifier: int, data: bytes | bytearray | memoryview) -> bytes:
    """
    Encode an extended-ID Classic CAN data frame into one SLCAN ``T`` command line.
    """
    if not isinstance(identifier, int) or not (0 <= identifier <= _CAN_EXT_ID_MASK):
        raise ValueError(f"Invalid CAN identifier: {identifier!r}")
    payload = bytes(data)
    if len(payload) > _CAN_CLASSIC_MTU:
        raise ValueError(f"Invalid CAN data length: {len(payload)}")
    return f"T{identifier:08X}{len(payload):1d}{payload.hex().upper()}\r".encode()


class SLCANParser:
    """
    Incremental SLCAN parser.

    Only data frames are returned. Unsupported or malformed input is silently dropped with debug logging.
    Adapter-specific suffixes after the payload, such as timestamps or flags, are ignored.
    """

    def __init__(self, *, max_line_length: int = _MAX_LINE_LENGTH) -> None:
        if max_line_length < 10:
            raise ValueError(f"Invalid maximum SLCAN line length: {max_line_length}")
        self._max_line_length = int(max_line_length)
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
            if len(self._buffer) >= self._max_line_length:
                _logger.debug("SLCAN drop overlong line len>%d", self._max_line_length)
                self._buffer.clear()
                self._discarding = True
                continue
            self._buffer.append(byte)
        return out


def _parse_line(line: bytes) -> Frame | None:
    # REFERENCE PARITY: pydronecan strips surrounding whitespace and control characters like BEL/ETX.
    line = line.strip(_STRIP_CHARS)
    if not line:
        return None
    command = line[:1]
    if command in (b"T", b"x"):
        return _parse_data_frame(line, id_length=8, max_payload_length=_CAN_CLASSIC_MTU)
    if command == b"t":
        return _parse_data_frame(line, id_length=3, max_payload_length=_CAN_CLASSIC_MTU)
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
    payload_length = _dlc_to_length(dlc)
    if payload_length is None:
        _logger.debug("SLCAN drop malformed dlc=%r line=%r", dlc, line)
        return None
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
    return _make_frame(identifier, line[header_length:expected])


def _make_frame(identifier: int, data_hex: bytes) -> Frame | None:
    data = _parse_hex_bytes(data_hex)
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


def _dlc_to_length(dlc: int) -> int | None:
    return _DLC_TO_LENGTH[dlc] if 0 <= dlc < len(_DLC_TO_LENGTH) else None
