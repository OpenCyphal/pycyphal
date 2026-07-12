"""Wire types shared by the file RPC examples."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import ClassVar


class FileReadError(IntEnum):
    ERROR_OK = 0
    ERROR_SEEK = 1
    ERROR_SUPPORT = 2
    ERROR_EXISTENCE = 3
    ERROR_KIND = 4
    ERROR_CAPACITY = 5
    ERROR_PERMISSION = 6
    ERROR_RUNTIME = 7
    ERROR_OTHER = 15


_INT48_MIN = -(1 << 47)
_INT48_MAX = (1 << 47) - 1


def _validate_int(name: str, value: int, lower: int, upper: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ValueError(f"{name} must be in [{lower}, {upper}], got {value!r}")


def _serialize_int48(value: int) -> bytes:
    _validate_int("seek", value, _INT48_MIN, _INT48_MAX)
    return value.to_bytes(6, byteorder="little", signed=True)


def _deserialize_int48(payload: bytes) -> int:
    return int.from_bytes(payload, byteorder="little", signed=True)


@dataclass(frozen=True)
class FileReadRequest:
    seek: int
    size: int
    path: str

    _HEADER_SIZE: ClassVar[int] = 12
    _PATH_MAX_LEN: ClassVar[int] = 1024

    def serialize(self) -> bytes:
        _validate_int("size", self.size, 0, 0xFFFF)
        if not isinstance(self.path, str):
            raise ValueError(f"path must be str, got {self.path!r}")
        try:
            encoded_path = self.path.encode("utf8")
        except UnicodeEncodeError as ex:
            raise ValueError("path is not valid UTF-8") from ex
        if len(encoded_path) > self._PATH_MAX_LEN:
            raise ValueError(f"File path length {len(encoded_path)} is too long")
        return (
            _serialize_int48(self.seek)
            + self.size.to_bytes(2, "little")
            + b"\0\0"
            + len(encoded_path).to_bytes(2, "little")
            + encoded_path
        )

    @staticmethod
    def deserialize(payload: bytes) -> FileReadRequest | None:
        if not isinstance(payload, bytes) or len(payload) < FileReadRequest._HEADER_SIZE:
            return None
        path_len = int.from_bytes(payload[10:12], "little")
        if path_len > FileReadRequest._PATH_MAX_LEN:
            return None
        path_end = FileReadRequest._HEADER_SIZE + path_len
        if len(payload) != path_end:
            return None
        try:
            path = payload[FileReadRequest._HEADER_SIZE : path_end].decode("utf8")
        except UnicodeDecodeError:
            return None
        return FileReadRequest(_deserialize_int48(payload[:6]), int.from_bytes(payload[6:8], "little"), path)


@dataclass(frozen=True)
class FileReadResponse:
    seek: int
    error: int
    end: bool
    data: bytes

    _HEADER_SIZE: ClassVar[int] = 12
    DATA_CAPACITY: ClassVar[int] = 0xFFFF

    def serialize(self) -> bytes:
        _validate_int("seek", self.seek, 0, _INT48_MAX)
        _validate_int("error", self.error, 0, 0x0F)
        if not isinstance(self.end, bool):
            raise ValueError(f"end must be bool, got {self.end!r}")
        if not isinstance(self.data, bytes):
            raise ValueError(f"data must be bytes, got {self.data!r}")
        if len(self.data) > self.DATA_CAPACITY:
            raise ValueError(f"Response data is too large: {len(self.data)}")
        return (
            _serialize_int48(self.seek)
            + bytes((self.error, int(self.end), 0, 0))
            + len(self.data).to_bytes(2, "little")
            + self.data
        )

    @staticmethod
    def deserialize(payload: bytes) -> FileReadResponse | None:
        if not isinstance(payload, bytes) or len(payload) < FileReadResponse._HEADER_SIZE:
            return None
        data_len = int.from_bytes(payload[10:12], "little")
        data_end = FileReadResponse._HEADER_SIZE + data_len
        if len(payload) != data_end:
            return None
        seek = _deserialize_int48(payload[:6])
        if seek < 0:
            return None
        return FileReadResponse(
            seek=seek,
            error=payload[6] & 0x0F,
            end=bool(payload[7] & 1),
            data=payload[FileReadResponse._HEADER_SIZE : data_end],
        )
