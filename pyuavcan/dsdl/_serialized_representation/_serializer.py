#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import numpy
import typing
import struct


class SerializerBase:
    """
    All methods operating on scalars implicitly truncate the value if it exceeds the range.
    If saturation is desired, it must be implemented externally.
    """

    def __init__(self, buffer_size_in_bytes: int):
        self._buf: numpy.ndarray = numpy.zeros(int(buffer_size_in_bytes), dtype=numpy.ubyte)
        self._bit_offset = 0

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        raise NotImplementedError

    def add_aligned_bytes(self, x: typing.Union[bytes, bytearray, numpy.ndarray]) -> None:
        """Simply adds a sequence of bytes; the current bit offset must be byte-aligned."""
        assert self._bit_offset % 8 == 0
        self._buf[self._byte_offset:self._byte_offset + len(x)] = x
        self._bit_offset += len(x) * 8

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits."""
        self._bit_offset += bit_length

    #
    # Fast methods optimized for aligned primitive fields; they are byte order invariant.
    #
    def add_aligned_uint8(self, x: int) -> None:
        assert self._bit_offset % 8 == 0
        self._buf[self._byte_offset] = x
        self._bit_offset += 8

    def add_aligned_uint16(self, x: int) -> None:
        self.add_aligned_uint8(x & 0xFF)
        self.add_aligned_uint8((x >> 8) & 0xFF)

    def add_aligned_uint32(self, x: int) -> None:
        self.add_aligned_uint16(x)
        self.add_aligned_uint16(x >> 16)

    def add_aligned_uint64(self, x: int) -> None:
        self.add_aligned_uint32(x)
        self.add_aligned_uint32(x >> 32)

    def add_aligned_int8(self, x: int) -> None:
        self.add_aligned_uint8((256 + x) if x < 0 else x)

    def add_aligned_int16(self, x: int) -> None:
        self.add_aligned_uint16((65536 + x) if x < 0 else x)

    def add_aligned_int32(self, x: int) -> None:
        self.add_aligned_uint32((0xFFFF_FFFF + x) if x < 0 else x)

    def add_aligned_int64(self, x: int) -> None:
        self.add_aligned_uint64((0xFFFF_FFFF_FFFF_FFFF + x) if x < 0 else x)

    def add_aligned_float16(self, x: float) -> None:
        self._add_aligned_float('e', x)

    def add_aligned_float32(self, x: float) -> None:
        self._add_aligned_float('f', x)

    def add_aligned_float64(self, x: float) -> None:
        self._add_aligned_float('d', x)

    def _add_aligned_float(self, format_char: str, x: float) -> None:
        f = '<' + format_char
        try:
            self.add_aligned_bytes(struct.pack(f, x))
        except OverflowError:
            self.add_aligned_bytes(struct.pack(f, numpy.inf if x > 0 else -numpy.inf))


class LittleEndianSerializer(SerializerBase):
    # noinspection PyUnresolvedReferences
    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        """
        Accepts an array of (u?int|float)(8|16|32|64) and encodes it using memcpy.
        The current bit offset must be byte-aligned.
        """
        mw = memoryview(x)
        assert mw.contiguous, 'Fast serialization requires the source array to be contiguous'
        assert mw.nbytes == len(x) * x.dtype.itemsize

        assert self._bit_offset % 8 == 0, 'This method can only be used if the offset is byte-aligned'
        start = self._byte_offset
        self._bit_offset += mw.nbytes * 8
        end = self._byte_offset

        # This is equivalent to raw memcpy() from the source memory into the destination memory, which is very fast.
        # We assume that the target platform uses IEEE 754-compliant floating point representation; otherwise,
        # the generated serialized representation will be incorrect. NumPy seems to only support IEEE-754 compliant
        # platforms though so I don't expect any compatibility issues.
        self._buf[start:end] = mw.cast('B')


class BigEndianSerializer(SerializerBase):
    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        raise NotImplementedError('Pull requests are welcome')
