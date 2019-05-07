#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import numpy
import typing
import struct

# We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
_Byte = numpy.uint8
# noinspection PyShadowingBuiltins
_T = typing.TypeVar('_T')
_PrimitiveType = typing.Union[typing.Type[numpy.integer], typing.Type[numpy.inexact]]


class Deserializer:
    class InvalidSerializedRepresentationError(ValueError):
        """
        This exception class is used when an auto-generated deserialization routine is supplied with invalid input data;
        in other words, input that is not a valid serialized representation of its data type.
        """
        pass

    def __init__(self, source_bytes: numpy.ndarray):
        if issubclass(Deserializer, type(self)):
            raise TypeError('Deserializer cannot be instantiated directly; use the new() factory instead')

        if not isinstance(source_bytes, numpy.ndarray) or source_bytes.dtype != _Byte:
            raise ValueError(f'Unsupported buffer: {type(source_bytes)}')

        self._buf = source_bytes
        self._bit_offset = 0

    @staticmethod
    def new(source_bytes: numpy.ndarray) -> 'Deserializer':
        return {
            'little': _LittleEndianDeserializer,
            'big':       _BigEndianDeserializer,
        }[sys.byteorder](source_bytes)

    @property
    def remaining_bit_length(self) -> int:
        return len(self._buf) * 8 - self._bit_offset

    def require_remaining_bit_length(self, inclusive_minimum: int) -> None:
        """
        Raises Deserializer.InvalidSerializedRepresentationError if the remaining bit length is
        strictly less than the specified minimum. Users must invoke this method before beginning deserialization.
        Failure to invoke this method beforehand may result in IndexError being thrown later during
        deserialization if the serialized representation is shorter than expected.
        """
        if self.remaining_bit_length < inclusive_minimum:
            raise self.InvalidSerializedRepresentationError(
                f'The serialized representation is {len(self._buf)} bytes long ({len(self._buf) * 8} bits), '
                f'which is shorter than the expected minimum of {inclusive_minimum} bits.')

    @staticmethod
    def require_value_in_range(value: _T, closed_interval_min_max: typing.Tuple[_T, _T]) -> _T:
        """
        Raises Deserializer.InvalidSerializedRepresentationError if the value is outside of the range.
        Returns the value unmodified if the value is inside the range.
        """
        low, high = closed_interval_min_max
        assert low <= high
        if low <= value <= high:
            return value
        else:
            raise Deserializer.InvalidSerializedRepresentationError(
                f'Value {value} is outside of the expected range [{low}, {high}]')

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits."""
        self._bit_offset += bit_length

    #
    # Fast methods optimized for aligned primitive fields.
    # The most specialized methods must be used whenever possible for best performance.
    #
    def fetch_aligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        """
        Returns a new array which may directly refer to the underlying memory if the buffer containing the source
        serialized representation is writeable; otherwise, new memory will be allocated.
        """
        raise NotImplementedError

    def fetch_aligned_array_of_bits(self, count: int) -> numpy.ndarray:
        """
        Quickly decodes an aligned array of bits using the numpy's fast bit unpacking routine.
        A new array is always created (the memory cannot be shared with the buffer due to the layout transformation).
        The returned array is of dtype numpy.bool.
        """
        if count <= 0:
            raise ValueError('The number of elements in the bit array must be positive')
        assert self._bit_offset % 8 == 0
        bs = self._buf[self._byte_offset:self._byte_offset + (count + 7) // 8]
        out = numpy.unpackbits(bs)[:count]
        if len(out) != count:
            raise self.InvalidSerializedRepresentationError(f'Requested {count} bits, only {len(out)} are available')
        self._bit_offset += count
        return out.astype(dtype=numpy.bool)

    def fetch_aligned_bytes(self, how_many: int) -> numpy.ndarray:
        assert self._bit_offset % 8 == 0
        out = self._buf[self._byte_offset:self._byte_offset + how_many]
        if len(out) != how_many:
            raise self.InvalidSerializedRepresentationError(
                f'Could not fetch {how_many} bytes from the buffer, only {len(out)} are available')
        self._bit_offset += how_many * 8
        return out

    def fetch_aligned_u8(self) -> int:
        assert self._bit_offset % 8 == 0
        out = self._buf[self._byte_offset]
        self._bit_offset += 8
        return int(out)  # the array element access yields numpy.uint8, we don't want that

    def fetch_aligned_u16(self) -> int:
        out = self.fetch_aligned_u8()
        out |= self.fetch_aligned_u8() << 8
        return out

    def fetch_aligned_u32(self) -> int:
        out = self.fetch_aligned_u16()
        out |= self.fetch_aligned_u16() << 16
        return out

    def fetch_aligned_u64(self) -> int:
        out = self.fetch_aligned_u32()
        out |= self.fetch_aligned_u32() << 32
        return out

    def fetch_aligned_i8(self) -> int:
        x = self.fetch_aligned_u8()
        return (x - 256) if x >= 128 else x

    def fetch_aligned_i16(self) -> int:
        x = self.fetch_aligned_u16()
        return (x - 65536) if x >= 32768 else x

    def fetch_aligned_i32(self) -> int:
        x = self.fetch_aligned_u32()
        return (x - 2 ** 32) if x >= 2 ** 31 else x

    def fetch_aligned_i64(self) -> int:
        x = self.fetch_aligned_u64()
        return (x - 2 ** 64) if x >= 2 ** 63 else x

    def fetch_aligned_f16(self) -> float:
        return self._float_from_bytes(self.fetch_aligned_bytes(2))

    def fetch_aligned_f32(self) -> float:
        return self._float_from_bytes(self.fetch_aligned_bytes(4))

    def fetch_aligned_f64(self) -> float:
        return self._float_from_bytes(self.fetch_aligned_bytes(8))

    #
    # Less specialized methods: assuming that the value is aligned at the beginning, but its bit length
    # is non-standard and may not be an integer multiple of eight.
    # These must not be used if there is a suitable more specialized version defined above.
    #
    def fetch_aligned_unsigned(self, bit_length: int) -> int:
        assert self._bit_offset % 8 == 0
        bs = self._buf[self._byte_offset:self._byte_offset + (bit_length + 7) // 8]
        if len(bs) * 8 < bit_length:
            raise self.InvalidSerializedRepresentationError(f'Could not fetch {bit_length} bits from the buffer')
        self._bit_offset += bit_length
        return self._unsigned_from_bytes(bs, bit_length)

    def fetch_aligned_signed(self, bit_length: int) -> int:
        assert bit_length >= 2
        u = self.fetch_aligned_unsigned(bit_length)
        return (u - 2 ** bit_length) if u >= 2 ** (bit_length - 1) else u

    #
    # Private methods.
    #
    @staticmethod
    def _unsigned_from_bytes(x: numpy.ndarray, bit_length: int) -> int:
        assert bit_length >= 1
        num_bytes = (bit_length + 7) // 8
        assert num_bytes > 0
        last_byte_index = num_bytes - 1
        assert len(x) >= num_bytes, f'The source array {x} is not long enough to deserialize uint{bit_length}'
        out = 0
        for i in range(last_byte_index):
            out |= int(x[i]) << (i * 8)
        # The trailing bits must be shifted right because the most significant bit has index zero. If the bit length
        # is an integer multiple of eight, this won't be necessary and the operation will have no effect.
        backtrack = (8 - bit_length % 8) & 0b111
        out |= (int(x[last_byte_index]) >> backtrack) << (last_byte_index * 8)
        assert 0 <= out < (2 ** bit_length)
        return out

    @staticmethod
    def _float_from_bytes(x: numpy.ndarray) -> float:
        assert x.dtype == _Byte
        # noinspection PyTypeChecker
        out, = struct.unpack({2: '<e', 4: '<f', 8: '<d'}[len(x)], x)  # type: ignore
        assert isinstance(out, float)
        return out

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8


class _LittleEndianDeserializer(Deserializer):
    def fetch_aligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        assert dtype not in (numpy.bool, numpy.bool_, numpy.object), 'Invalid usage'
        assert self._bit_offset % 8 == 0
        out: numpy.ndarray = numpy.frombuffer(self._buf, dtype=dtype, count=count, offset=self._byte_offset)
        assert len(out) == count                # numpy should throw if there is not enough bytes in the source buffer
        self._bit_offset += out.nbytes * 8
        # The returned array must be writeable, which is possible only if the underlying buffer is writeable.
        # If not, we will have to clone the output array. Perhaps we should escalate this to error?
        out = out if out.flags.writeable else out.copy()
        assert out.flags.writeable
        return out

    def fetch_unaligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        pass


class _BigEndianDeserializer(Deserializer):
    def fetch_aligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        raise NotImplementedError('Pull requests are welcome')

    def fetch_unaligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        raise NotImplementedError('Pull requests are welcome')


def _unittest_deserializer_aligned() -> None:
    from pytest import raises, approx

    # The buffer is constructed from the corresponding serialization test.
    sample = bytes(map(lambda x: int(x, 2),
                       '10100111 11101111 11001101 10101011 10010000 01111000 01010110 00110100 00010010 10001000 '
                       '10101001 11001011 11101101 11111110 11111111 00000000 01111111 00000000 00000000 00000000 '
                       '00000000 00000000 00000000 11110000 00111111 00000000 00000000 10000000 00111111 00000000 '
                       '01111100 11011010 11100000 11011010 10111110 11111110 10000000 10101101 11011110 11101111 '
                       '10111110 10100011 11100110 10100011 11010000'.split()))
    assert len(sample) == 45

    des = Deserializer.new(numpy.frombuffer(sample, dtype=_Byte).copy())
    assert des.remaining_bit_length == 45 * 8
    des.require_remaining_bit_length(0)
    des.require_remaining_bit_length(45 * 8)
    with raises(Deserializer.InvalidSerializedRepresentationError):
        des.require_remaining_bit_length(45 * 8 + 1)

    assert 1 == des.require_value_in_range(1, (0, 2))
    assert 0 == des.require_value_in_range(0, (0, 1))
    assert 1 == des.require_value_in_range(1, (0, 1))
    with raises(Deserializer.InvalidSerializedRepresentationError):
        des.require_value_in_range(2, (0, 1))

    assert des.fetch_aligned_u8() == 0b1010_0111
    assert des.fetch_aligned_i64() == 0x1234_5678_90ab_cdef
    assert des.fetch_aligned_i32() == -0x1234_5678
    assert des.fetch_aligned_i16() == -2

    assert des.remaining_bit_length == 45 * 8 - 8 - 64 - 32 - 16
    des.skip_bits(8)
    assert des.remaining_bit_length == 45 * 8 - 8 - 64 - 32 - 16 - 8

    assert des.fetch_aligned_i8() == 127
    assert des.fetch_aligned_f64() == approx(1.0)
    assert des.fetch_aligned_f32() == approx(1.0)
    assert des.fetch_aligned_f16() == numpy.inf

    assert des.fetch_aligned_unsigned(12) == 0xEDA
    des.skip_bits(4)
    assert des.fetch_aligned_unsigned(16) == 0xBEDA
    assert des.fetch_aligned_signed(9) == -2
    des.skip_bits(7)

    assert all(des.fetch_aligned_array_of_standard_bit_length_primitives(numpy.uint16, 2) == [0xdead, 0xbeef])

    assert all(des.fetch_aligned_array_of_bits(16) == [
        True, False, True, False, False, False, True, True, True, True, True, False, False, True, True, False,
    ])

    assert all(des.fetch_aligned_array_of_bits(13) == [
        True, False, True, False, False, False, True, True, True, True, False, True, False,
    ])
