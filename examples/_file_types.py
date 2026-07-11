"""Wire types shared by the file RPC examples."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class FileReadRequest:
    _read_offset: int
    _file_path: str

    _HEADER_FORMAT: ClassVar[str] = "<QH"
    _HEADER_SIZE: ClassVar[int] = struct.calcsize(_HEADER_FORMAT)
    _PATH_MAX_LEN: ClassVar[int] = 2048

    @property
    def read_offset(self) -> int:
        return self._read_offset

    @property
    def file_path(self) -> str:
        return self._file_path

    def serialize(self) -> bytes:
        encoded_path = self._file_path.encode("utf8")
        if len(encoded_path) > self._PATH_MAX_LEN:
            raise ValueError(f"File path length {len(encoded_path)} is too long")
        return struct.pack(self._HEADER_FORMAT, self._read_offset, len(encoded_path)) + encoded_path

    @staticmethod
    def deserialize(payload: bytes) -> FileReadRequest | None:
        if len(payload) < FileReadRequest._HEADER_SIZE:
            return None
        read_offset, path_len = struct.unpack_from(FileReadRequest._HEADER_FORMAT, payload)
        if path_len == 0 or path_len > FileReadRequest._PATH_MAX_LEN:
            return None
        path_end = FileReadRequest._HEADER_SIZE + path_len
        if len(payload) != path_end:
            return None
        try:
            file_path = payload[FileReadRequest._HEADER_SIZE : path_end].decode("utf8")
        except UnicodeDecodeError:
            return None
        return FileReadRequest(read_offset, file_path)


@dataclass(frozen=True)
class FileReadResponse:
    _error: int
    _data: bytes

    _HEADER_FORMAT: ClassVar[str] = "<IH"
    _HEADER_SIZE: ClassVar[int] = struct.calcsize(_HEADER_FORMAT)
    _DATA_MAX: ClassVar[int] = 4096

    @property
    def error(self) -> int:
        return self._error

    @property
    def data(self) -> bytes:
        return self._data

    @classmethod
    def data_capacity(cls) -> int:
        return cls._DATA_MAX

    def serialize(self) -> bytes:
        if len(self._data) > self._DATA_MAX:
            raise ValueError(f"Response data is too large: {len(self._data)}")
        return struct.pack(self._HEADER_FORMAT, self._error, len(self._data)) + self._data

    @staticmethod
    def deserialize(payload: bytes) -> FileReadResponse | None:
        if len(payload) < FileReadResponse._HEADER_SIZE:
            return None
        error, data_len = struct.unpack_from(FileReadResponse._HEADER_FORMAT, payload)
        if data_len > FileReadResponse._DATA_MAX:
            return None
        data_start = FileReadResponse._HEADER_SIZE
        data_end = data_start + data_len
        if len(payload) != data_end:
            return None
        return FileReadResponse(error, payload[data_start:data_end])
