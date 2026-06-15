"""SLCAN text codec for CAN frames."""

from __future__ import annotations

import logging

from ._interface import Frame

_logger = logging.getLogger(__name__)

_CAN_EXT_ID_MASK = (1 << 29) - 1
_CAN_CLASSIC_MTU = 8
_HEX_CHARS = frozenset(b"0123456789abcdefABCDEF")
_CR = 0x0D
_LF = 0x0A
_BEL = 0x07
_MAX_LINE_LENGTH = 256
_TIMESTAMP_LENGTH = 4


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

    Only extended-ID data frames are returned. Unsupported or malformed input is silently dropped with debug logging.
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
    command = line[:1]
    if command in (b"T", b"x"):
        return _parse_classic_extended(line)
    if command in (b"t", b"r", b"R"):
        _logger.debug("SLCAN drop unsupported frame type cmd=%r", command)
        return None
    _logger.debug("SLCAN drop unknown line=%r", line)
    return None


def _parse_classic_extended(line: bytes) -> Frame | None:
    if len(line) < 10:
        _logger.debug("SLCAN drop short classic line=%r", line)
        return None
    identifier = _parse_hex_int(line[1:9])
    dlc = _parse_classic_dlc(line[9])
    if identifier is None or dlc is None:
        _logger.debug("SLCAN drop malformed classic header line=%r", line)
        return None
    expected = 10 + dlc * 2
    if len(line) == expected + _TIMESTAMP_LENGTH:
        if not _is_hex(line[expected:]):
            _logger.debug("SLCAN drop malformed timestamp line=%r", line)
            return None
    elif len(line) != expected:
        _logger.debug("SLCAN drop classic dlc mismatch len=%d expected=%d", len(line), expected)
        return None
    return _make_frame(identifier, line[10:expected])


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
    if not value or not _is_hex(value):
        return None
    return int(value, 16)


def _parse_hex_bytes(value: bytes) -> bytes | None:
    if len(value) % 2 != 0 or not _is_hex(value):
        return None
    return bytes.fromhex(value.decode("ascii"))


def _parse_classic_dlc(value: int) -> int | None:
    return value - ord("0") if ord("0") <= value <= ord("8") else None


def _is_hex(value: bytes) -> bool:
    return all(x in _HEX_CHARS for x in value)
