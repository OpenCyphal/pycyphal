#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import numpy
import typing
import struct
import base64

# We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
_Byte = numpy.uint8
# noinspection PyShadowingBuiltins
_T = typing.TypeVar('_T')
_PrimitiveType = typing.Union[typing.Type[numpy.integer], typing.Type[numpy.inexact]]


class Deserializer:
    class FormatError(ValueError):
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
    def consumed_bit_length(self) -> int:
        return self._bit_offset

    @property
    def remaining_bit_length(self) -> int:
        return len(self._buf) * 8 - self._bit_offset

    def require_remaining_bit_length(self, inclusive_minimum: int) -> None:
        """
        Raises Deserializer.FormatError if the remaining bit length is
        strictly less than the specified minimum. Users must invoke this method before beginning deserialization.
        Failure to invoke this method beforehand may result in IndexError being thrown later during
        deserialization if the serialized representation is shorter than expected.
        """
        if self.remaining_bit_length < inclusive_minimum:
            raise self.FormatError(
                f'The serialized representation is {len(self._buf)} bytes long ({len(self._buf) * 8} bits), '
                f'which is shorter than the expected minimum of {inclusive_minimum} bits.')

    @staticmethod
    def require_value_in_range(value: _T, closed_interval_min_max: typing.Tuple[_T, _T]) -> _T:
        """
        Raises Deserializer.FormatError if the value is outside of the range.
        Returns the value unmodified if the value is inside the range.
        """
        low, high = closed_interval_min_max
        assert low <= high          # type: ignore
        if low <= value <= high:    # type: ignore
            return value
        else:
            raise Deserializer.FormatError(f'Value {value} is outside of the expected range [{low}, {high}]')

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
        if count < 0:
            raise ValueError('The number of elements in the bit array cannot be negative')
        assert self._bit_offset % 8 == 0
        bs = self._buf[self._byte_offset:self._byte_offset + (count + 7) // 8]
        out = numpy.unpackbits(bs)[:count]
        if len(out) != count:   # Explicit check is required because numpy silently truncates slices
            raise IndexError(f'Requested {count} bits, only {len(out)} are available')
        self._bit_offset += count
        return out.astype(dtype=numpy.bool)

    def fetch_aligned_bytes(self, count: int) -> numpy.ndarray:
        assert self._bit_offset % 8 == 0
        if count < 0:
            raise ValueError('The number of elements in the byte array cannot be negative')
        out = self._buf[self._byte_offset:self._byte_offset + count]
        if len(out) != count:   # Explicit check is required because numpy silently truncates slices
            raise IndexError(f'Could not fetch {count} bytes from the buffer, only {len(out)} are available')
        self._bit_offset += count * 8
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

    def fetch_aligned_f16(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<e', self.fetch_aligned_bytes(2))
        assert isinstance(out, float)
        return out

    def fetch_aligned_f32(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<f', self.fetch_aligned_bytes(4))
        assert isinstance(out, float)
        return out

    def fetch_aligned_f64(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<d', self.fetch_aligned_bytes(8))
        assert isinstance(out, float)
        return out

    #
    # Less specialized methods: assuming that the value is aligned at the beginning, but its bit length
    # is non-standard and may not be an integer multiple of eight.
    # These must not be used if there is a suitable more specialized version defined above.
    #
    def fetch_aligned_unsigned(self, bit_length: int) -> int:
        assert self._bit_offset % 8 == 0
        bs = self._buf[self._byte_offset:self._byte_offset + (bit_length + 7) // 8]
        if len(bs) * 8 < bit_length:   # Explicit check is required because numpy silently truncates slices
            raise IndexError(f'Could not fetch {bit_length} bits from the buffer')
        self._bit_offset += bit_length
        return self._unsigned_from_bytes(bs, bit_length)

    def fetch_aligned_signed(self, bit_length: int) -> int:
        assert bit_length >= 2
        u = self.fetch_aligned_unsigned(bit_length)
        out = (u - 2 ** bit_length) if u >= 2 ** (bit_length - 1) else u
        assert isinstance(out, int)     # MyPy pls
        return out

    #
    # Least specialized methods: no assumptions about alignment are made.
    # These are the slowest and may be used only if none of the above (specialized) methods are suitable.
    #
    def fetch_unaligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        """See the aligned counterpart."""
        raise NotImplementedError

    def fetch_unaligned_array_of_bits(self, count: int) -> numpy.ndarray:
        byte_count = (count + 7) // 8
        bs = self.fetch_unaligned_bytes(byte_count)
        assert len(bs) == byte_count
        backtrack = byte_count * 8 - count
        assert 0 <= backtrack < 8
        self._bit_offset -= backtrack
        out: numpy.ndarray = numpy.unpackbits(bs)[:count].astype(dtype=numpy.bool)
        assert len(out) == count
        return out

    def fetch_unaligned_bytes(self, count: int) -> numpy.ndarray:
        # This is a faster variation of the Ben Dyer's unaligned bit copy algorithm:
        # https://github.com/UAVCAN/libuavcan/blob/fd8ba19bc9c09c05a/libuavcan/src/marshal/uc_bit_array_copy.cpp#L12
        # It is faster because here we are aware that the destination is always aligned, which we take advantage of.
        # This algorithm breaks for byte-aligned offset, so we have to delegate the aligned case (it's also faster):
        if self._bit_offset % 8 == 0:
            return self.fetch_aligned_bytes(count)
        else:
            out = numpy.empty(count, dtype=_Byte)
            left = self._bit_offset % 8
            right = 8 - left
            assert (1 <= right <= 7) and (1 <= left <= 7)
            for i in range(count):
                byte_offset = self._byte_offset
                out[i] = ((self._buf[byte_offset] << left) & 0xFF) | (self._buf[byte_offset + 1] >> right)
                self._bit_offset += 8
            assert len(out) == count
            return out

    def fetch_unaligned_unsigned(self, bit_length: int) -> int:
        byte_length = (bit_length + 7) // 8
        bs = self.fetch_unaligned_bytes(byte_length)
        assert len(bs) == byte_length
        backtrack = byte_length * 8 - bit_length
        assert 0 <= backtrack < 8
        self._bit_offset -= backtrack
        return self._unsigned_from_bytes(bs, bit_length)

    def fetch_unaligned_signed(self, bit_length: int) -> int:
        assert bit_length >= 2
        u = self.fetch_unaligned_unsigned(bit_length)
        out = (u - 2 ** bit_length) if u >= 2 ** (bit_length - 1) else u
        assert isinstance(out, int)     # MyPy pls
        return out

    def fetch_unaligned_f16(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<e', self.fetch_unaligned_bytes(2))
        assert isinstance(out, float)
        return out

    def fetch_unaligned_f32(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<f', self.fetch_unaligned_bytes(4))
        assert isinstance(out, float)
        return out

    def fetch_unaligned_f64(self) -> float:  # noinspection PyTypeChecker
        out, = struct.unpack('<d', self.fetch_unaligned_bytes(8))
        assert isinstance(out, float)
        return out

    def fetch_unaligned_bit(self) -> bool:
        mask = 1 << (7 - self._bit_offset % 8)
        assert 1 <= mask <= 128
        out = self._buf[self._byte_offset] & mask == mask
        self._bit_offset += 1
        return bool(out)

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
        shift = (8 - bit_length % 8) & 0b111
        out |= (int(x[last_byte_index]) >> shift) << (last_byte_index * 8)
        assert 0 <= out < (2 ** bit_length)
        return out

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    def __str__(self) -> str:
        return f'{type(self).__name__}(' \
            f'current_bit_offset={self._bit_offset}, ' \
            f'remaining_bit_length={self.remaining_bit_length}, ' \
            f'serialized_representation_base64={base64.b64encode(self._buf.tobytes()).decode()!r})'

    __repr__ = __str__


class _LittleEndianDeserializer(Deserializer):
    def fetch_aligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        assert dtype not in (numpy.bool, numpy.bool_, numpy.object), 'Invalid usage'
        assert self._bit_offset % 8 == 0
        # Interestingly, numpy doesn't care about alignment. If the source buffer is not properly aligned, it will
        # work anyway but slower.
        out: numpy.ndarray = numpy.frombuffer(self._buf, dtype=dtype, count=count, offset=self._byte_offset)
        assert len(out) == count                # numpy should throw if there is not enough bytes in the source buffer
        self._bit_offset += out.nbytes * 8
        return self._ensure_writeable(out)

    def fetch_unaligned_array_of_standard_bit_length_primitives(self, dtype: _PrimitiveType, count: int) \
            -> numpy.ndarray:
        assert dtype not in (numpy.bool, numpy.bool_, numpy.object), 'Invalid usage'
        bs = self.fetch_unaligned_bytes(numpy.dtype(dtype).itemsize * count)
        assert len(bs) >= count
        out: numpy.ndarray = numpy.frombuffer(bs, dtype=dtype, count=count)
        return self._ensure_writeable(out)

    @staticmethod
    def _ensure_writeable(array: numpy.ndarray) -> numpy.ndarray:
        # The returned array must be writeable, which is possible only if the underlying buffer is writeable.
        # If not, we will have to clone the output array. Perhaps we should escalate this to error?
        array = array if array.flags.writeable else array.copy()
        assert array.flags.writeable
        return array


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
    sample = bytes(map(
        lambda x: int(x, 2),
        '10100111 '                                                                 # u8
        '11101111 11001101 10101011 10010000 01111000 01010110 00110100 00010010 '  # i64
        '10001000 10101001 11001011 11101101 '                                      # i32 = -0x1234_5678
        '11111110 11111111 '                                                        # i16 = -2
        '00000000 '                                                                 # padding
        '01111111 '                                                                 # i8 = 127
        '00000000 00000000 00000000 00000000 00000000 00000000 11110000 00111111 '  # f64 = 1.0
        '00000000 00000000 10000000 00111111 '                                      # f32 = 1.0
        '00000000 01111100 '                                                        # f16 = +inf
        '11011010 1110'                                                             # u12 = 0xEDA
        '0000 '                                                                     # padding
        '11011010 10111110 '                                                        # u16 = 0xBEDA
        '11111110 1'                                                                # i9 = -2
        '0000000 '                                                                  # padding
        '10101101 11011110 11101111 10111110 '                                      # u16 [0xdead 0xbeef]
        '10100011 11100110 '                                                        # 16 bits
        '10100011 11010'                                                            # 13 bits
        '000'.split()))                                                             # auto trailing padding
    assert len(sample) == 45

    with raises(TypeError):
        Deserializer(numpy.array([1, 2, 3], dtype=_Byte))

    with raises(ValueError):
        Deserializer.new(numpy.array([1, 2, 3], dtype=numpy.int8))

    des = Deserializer.new(numpy.frombuffer(sample, dtype=_Byte).copy())
    assert des.remaining_bit_length == 45 * 8
    des.require_remaining_bit_length(0)
    des.require_remaining_bit_length(45 * 8)
    with raises(Deserializer.FormatError):
        des.require_remaining_bit_length(45 * 8 + 1)

    assert 1 == des.require_value_in_range(1, (0, 2))
    assert 0 == des.require_value_in_range(0, (0, 1))
    assert 1 == des.require_value_in_range(1, (0, 1))
    with raises(Deserializer.FormatError):
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

    print('repr(deserializer):', repr(des))

    des = Deserializer.new(numpy.array([1, 2, 3], dtype=_Byte))

    assert list(des.fetch_aligned_array_of_bits(0)) == []
    assert list(des.fetch_aligned_bytes(0)) == []
    assert des.remaining_bit_length == 3 * 8

    with raises(ValueError):
        des.fetch_aligned_array_of_bits(-1)

    with raises(ValueError):
        des.fetch_aligned_bytes(-1)

    with raises(IndexError):
        des.fetch_aligned_array_of_bits(100)

    with raises(IndexError):
        des.fetch_aligned_bytes(10)

    with raises(IndexError):
        des.fetch_aligned_unsigned(64)

    print('repr(deserializer):', repr(des))


def _unittest_deserializer_unaligned() -> None:
    from pytest import raises, approx

    des = Deserializer.new(numpy.array([0b10101010, 0b01011101, 0b11001100, 0b10010001], dtype=_Byte))
    assert des.consumed_bit_length == 0
    assert des.consumed_bit_length % 8 == 0
    assert list(des.fetch_aligned_array_of_bits(3)) == [True, False, True]
    assert des.consumed_bit_length == 3
    assert des.consumed_bit_length % 8 == 3
    assert list(des.fetch_unaligned_bytes(0)) == []
    assert list(des.fetch_unaligned_bytes(2)) == [0b01010_010, 0b11101_110]
    assert list(des.fetch_unaligned_bytes(1)) == [0b01100_100]
    assert des.consumed_bit_length == 27
    assert des.consumed_bit_length % 8 == 3
    assert des.remaining_bit_length == 5
    with raises(IndexError):
        des.fetch_unaligned_bytes(1)
    assert des.consumed_bit_length == 27

    des = Deserializer.new(numpy.array([0b10101010, 0b01011101, 0b11001100, 0b10010001], dtype=_Byte))
    assert list(des.fetch_unaligned_bytes(0)) == []
    assert list(des.fetch_unaligned_bytes(2)) == [0b10101010, 0b01011101]  # Actually aligned
    assert list(des.fetch_unaligned_bytes(1)) == [0b11001100]
    assert des.remaining_bit_length == 8
    with raises(IndexError):
        des.fetch_unaligned_bytes(2)

    # The buffer is constructed from the corresponding serialization test.
    sample = bytes(map(
        lambda x: int(x, 2),
        '10100011 111'                          # 11 bits
        '10100 11101'                           # 10 bits
        '000 10010001 10100010 10110'           # u8 [0x12, 0x34, 0x56]
        '011 '                                  # 3 bits
        '00010010 00110100 01010110 '           # u8 [0x12, 0x34, 0x56]
        '10011'                                 # 5 bits
        '111 11110'                             # u8 = -2
        '011 00101111 '                         # u11 = 0b111_0110_0101
        '110'                                   # u3 = 0b110
        '00000 00000000 00000000 00000000 00000000 00000000 00011110 00000111 111'  # f64 = 1.0
        '00000 00000000 00010000 00000111 111'                                      # f32 = 1.0
        '00000 00011111 100'                                                        # f16 = -inf
        '10101 10111011 11011101 11110111 110'                                      # u16 [0xdead, 0xbeef]
        '00000'                                                                     # padding
        ''.split()))
    assert len(sample) == 31

    des = Deserializer.new(numpy.frombuffer(sample, dtype=_Byte))       # Operating on the read-only buffer
    assert des.remaining_bit_length == 31 * 8
    des.require_remaining_bit_length(31 * 8)

    assert list(des.fetch_unaligned_array_of_bits(11)) == [
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, True,                                       # 111
    ]
    assert list(des.fetch_unaligned_array_of_bits(10)) == [
        True, False, True, False, False,                        # ???10100 (byte alignment restored here)
        True, True, True, False, True,                          # 11101 (byte alignment lost, three bits short)
    ]

    assert list(des.fetch_unaligned_bytes(3)) == [0x12, 0x34, 0x56]
    assert list(des.fetch_unaligned_array_of_bits(3)) == [False, True, True]
    assert list(des.fetch_unaligned_bytes(3)) == [0x12, 0x34, 0x56]

    assert des.fetch_unaligned_bit()
    assert not des.fetch_unaligned_bit()
    assert not des.fetch_unaligned_bit()
    assert des.fetch_unaligned_bit()
    assert des.fetch_unaligned_bit()

    assert des.fetch_unaligned_signed(8) == -2
    assert des.fetch_unaligned_unsigned(11) == 0b111_0110_0101
    assert des.fetch_unaligned_unsigned(3) == 0b110

    assert des.consumed_bit_length % 8 > 0             # not aligned
    assert des.fetch_unaligned_f64() == approx(1.0)
    assert des.fetch_unaligned_f32() == approx(1.0)
    assert des.fetch_unaligned_f16() == -numpy.inf

    assert list(des.fetch_unaligned_array_of_standard_bit_length_primitives(numpy.uint16, 2)) == [0xdead, 0xbeef]
    des.skip_bits(5)
    assert des.consumed_bit_length % 8 == 0
    assert des.remaining_bit_length == 0

    print('repr(deserializer):', repr(des))
