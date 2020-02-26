#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import sys
import typing
import struct

import numpy

# We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
_Byte = numpy.uint8


class Serializer(abc.ABC):
    """
    All methods operating on scalars implicitly truncate the value if it exceeds the range,
    excepting signed integers, for which overflow handling is not implemented (DSDL does not permit truncation
    of signed integers anyway so it doesn't matter). Saturation must be implemented externally.
    Methods that expect an unsigned integer will raise ValueError if the supplied integer is negative.
    """

    def __init__(self, buffer_size_in_bytes: int):
        """
        Do not call this directly. Use :meth:`new` to instantiate.
        """
        # We extend the requested buffer size by one because some of the non-byte-aligned write operations
        # require us to temporarily use one extra byte after the current byte.
        buffer_size_in_bytes = int(buffer_size_in_bytes) + 1
        self._buf: numpy.ndarray = numpy.zeros(buffer_size_in_bytes, dtype=_Byte)
        self._bit_offset = 0

    @staticmethod
    def new(buffer_size_in_bytes: int) -> Serializer:
        return _PlatformSpecificSerializer(buffer_size_in_bytes)

    @property
    def current_bit_length(self) -> int:
        return self._bit_offset

    @property
    def buffer(self) -> numpy.ndarray:
        """Returns a properly sized read-only slice of the destination buffer zero-bit-padded to byte."""
        out = self._buf[:(self._bit_offset + 7) // 8]
        out.flags.writeable = False
        assert out.base is self._buf    # Making sure we're not creating a copy, that might be costly
        return out

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits."""
        self._bit_offset += bit_length

    #
    # Fast methods optimized for aligned primitive fields.
    # The most specialized methods must be used whenever possible for best performance.
    #
    @abc.abstractmethod
    def add_aligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        """
        Accepts an array of ``(u?int|float)(8|16|32|64)`` and encodes it into the destination.
        On little-endian platforms this may be implemented virtually through ``memcpy()``.
        The current bit offset must be byte-aligned.
        """
        raise NotImplementedError

    def add_aligned_array_of_bits(self, x: numpy.ndarray) -> None:
        """
        Accepts an array of bools and encodes it into the destination using fast native serialization routine
        implemented in numpy. The current bit offset must be byte-aligned.
        """
        assert x.dtype in (numpy.bool, numpy.bool_)
        assert self._bit_offset % 8 == 0
        packed = numpy.packbits(x, bitorder='little')
        assert len(packed) * 8 >= len(x)
        self._buf[self._byte_offset:self._byte_offset + len(packed)] = packed
        self._bit_offset += len(x)

    def add_aligned_bytes(self, x: numpy.ndarray) -> None:
        """Simply adds a sequence of bytes; the current bit offset must be byte-aligned."""
        assert self._bit_offset % 8 == 0
        assert x.dtype == _Byte
        self._buf[self._byte_offset:self._byte_offset + len(x)] = x
        self._bit_offset += len(x) * 8

    def add_aligned_u8(self, x: int) -> None:
        assert self._bit_offset % 8 == 0
        self._ensure_not_negative(x)
        self._buf[self._byte_offset] = x
        self._bit_offset += 8

    def add_aligned_u16(self, x: int) -> None:
        self._ensure_not_negative(x)
        self.add_aligned_u8(x & 0xFF)
        self.add_aligned_u8((x >> 8) & 0xFF)

    def add_aligned_u32(self, x: int) -> None:
        self.add_aligned_u16(x)
        self.add_aligned_u16(x >> 16)

    def add_aligned_u64(self, x: int) -> None:
        self.add_aligned_u32(x)
        self.add_aligned_u32(x >> 32)

    def add_aligned_i8(self, x: int) -> None:
        self.add_aligned_u8((256 + x) if x < 0 else x)

    def add_aligned_i16(self, x: int) -> None:
        self.add_aligned_u16((65536 + x) if x < 0 else x)

    def add_aligned_i32(self, x: int) -> None:
        self.add_aligned_u32((2 ** 32 + x) if x < 0 else x)

    def add_aligned_i64(self, x: int) -> None:
        self.add_aligned_u64((2 ** 64 + x) if x < 0 else x)

    def add_aligned_f16(self, x: float) -> None:
        self.add_aligned_bytes(self._float_to_bytes('e', x))

    def add_aligned_f32(self, x: float) -> None:
        self.add_aligned_bytes(self._float_to_bytes('f', x))

    def add_aligned_f64(self, x: float) -> None:
        self.add_aligned_bytes(self._float_to_bytes('d', x))

    #
    # Less specialized methods: assuming that the value is aligned at the beginning, but its bit length
    # is non-standard and may not be an integer multiple of eight.
    # These must not be used if there is a suitable more specialized version defined above.
    #
    def add_aligned_unsigned(self, value: int, bit_length: int) -> None:
        assert self._bit_offset % 8 == 0
        self._ensure_not_negative(value)
        bs = self._unsigned_to_bytes(value, bit_length)
        self._buf[self._byte_offset:self._byte_offset + len(bs)] = bs
        self._bit_offset += bit_length

    def add_aligned_signed(self, value: int, bit_length: int) -> None:
        assert bit_length >= 2
        self.add_aligned_unsigned((2 ** bit_length + value) if value < 0 else value, bit_length)

    #
    # Least specialized methods: no assumptions about alignment are made.
    # These are the slowest and may be used only if none of the above (specialized) methods are suitable.
    #
    @abc.abstractmethod
    def add_unaligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        """See the aligned counterpart."""
        raise NotImplementedError

    def add_unaligned_array_of_bits(self, x: numpy.ndarray) -> None:
        assert x.dtype in (numpy.bool, numpy.bool_)
        packed = numpy.packbits(x, bitorder='little')
        backtrack = len(packed) * 8 - len(x)
        assert backtrack >= 0
        self.add_unaligned_bytes(packed)
        self._bit_offset -= backtrack

    def add_unaligned_bytes(self, value: numpy.ndarray) -> None:
        assert value.dtype == _Byte
        # This is a faster variant of Ben Dyer's unaligned bit copy algorithm:
        # https://github.com/UAVCAN/libuavcan/blob/fd8ba19bc9c09c05a/libuavcan/src/marshal/uc_bit_array_copy.cpp#L12
        # It is faster because here we are aware that the source is always aligned, which we take advantage of.
        left = self._bit_offset % 8
        right = 8 - left
        for b in value:
            self._buf[self._byte_offset] |= (b << left) & 0xFF
            self._bit_offset += 8
            self._buf[self._byte_offset] = b >> right

    def add_unaligned_unsigned(self, value: int, bit_length: int) -> None:
        self._ensure_not_negative(value)
        bs = self._unsigned_to_bytes(value, bit_length)
        backtrack = len(bs) * 8 - bit_length
        assert backtrack >= 0
        self.add_unaligned_bytes(bs)
        self._bit_offset -= backtrack

    def add_unaligned_signed(self, value: int, bit_length: int) -> None:
        assert bit_length >= 2
        self.add_unaligned_unsigned((2 ** bit_length + value) if value < 0 else value, bit_length)

    def add_unaligned_f16(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('e', x))

    def add_unaligned_f32(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('f', x))

    def add_unaligned_f64(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('d', x))

    def add_unaligned_bit(self, x: bool) -> None:
        self._buf[self._byte_offset] |= bool(x) << (self._bit_offset % 8)
        self._bit_offset += 1

    #
    # Private methods.
    #
    @staticmethod
    def _unsigned_to_bytes(value: int, bit_length: int) -> numpy.ndarray:
        assert bit_length >= 1
        assert value >= 0, 'This operation is undefined for negative integers'
        value &= 2 ** bit_length - 1
        num_bytes = (bit_length + 7) // 8
        out = numpy.zeros(num_bytes, dtype=_Byte)
        for i in range(num_bytes):      # Oh, why is my life like this?
            out[i] = value & 0xFF
            value >>= 8
        return out

    @staticmethod
    def _float_to_bytes(format_char: str, x: float) -> numpy.ndarray:
        f = '<' + format_char
        try:
            out = struct.pack(f, x)
        except OverflowError:  # Oops, let's truncate (saturation must be implemented by the caller if needed)
            out = struct.pack(f, numpy.inf if x > 0 else -numpy.inf)
        return numpy.frombuffer(out, dtype=_Byte)  # Note: this operation does not copy the underlying bytes

    @staticmethod
    def _ensure_not_negative(x: int) -> None:
        if x < 0:
            raise ValueError(f'The requested serialization method is not defined on negative integers ({x})')

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    def __str__(self) -> str:
        s = ' '.join(map(_byte_as_bit_string, self.buffer))
        if self._bit_offset % 8 != 0:
            s, tail = s.rsplit(maxsplit=1)
            bits_to_cut_off = 8 - self._bit_offset % 8
            tail = ('x' * bits_to_cut_off) + tail[bits_to_cut_off:]
            return s + ' ' + tail
        else:
            return s

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self})'


class _LittleEndianSerializer(Serializer):
    # noinspection PyUnresolvedReferences
    def add_aligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        # This is close to direct memcpy() from the source memory into the destination memory, which is very fast.
        # We assume that the local platform uses IEEE 754-compliant floating point representation; otherwise,
        # the generated serialized representation may be incorrect. NumPy seems to only support IEEE-754 compliant
        # platforms though so I don't expect any compatibility issues.
        assert x.dtype not in (numpy.bool, numpy.bool_, numpy.object)
        self.add_aligned_bytes(x.view(_Byte))

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        # This is much slower than the aligned version because we have to manually copy and shift each byte,
        # but still better than manual elementwise serialization.
        assert x.dtype not in (numpy.bool, numpy.bool_, numpy.object)
        self.add_unaligned_bytes(x.view(_Byte))


class _BigEndianSerializer(Serializer):
    def add_aligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:  # pragma: no cover
        raise NotImplementedError('Pull requests are welcome')

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        raise NotImplementedError('Pull requests are welcome')


_PlatformSpecificSerializer = {
    'little': _LittleEndianSerializer,
    'big':       _BigEndianSerializer,
}[sys.byteorder]


def _byte_as_bit_string(x: int) -> str:
    return bin(x)[2:].zfill(8)


def _unittest_serializer_to_str() -> None:
    ser = Serializer.new(50)
    assert str(ser) == ''
    ser.add_aligned_u8(0b11001110)
    assert str(ser) == '11001110'
    ser.add_aligned_i16(-1)
    assert str(ser) == '11001110 11111111 11111111'
    ser.add_aligned_unsigned(0, 1)
    assert str(ser) == '11001110 11111111 11111111 xxxxxxx0'
    ser.add_unaligned_signed(-1, 3)
    assert str(ser) == '11001110 11111111 11111111 xxxx1110'


def _unittest_serializer_aligned() -> None:
    from pytest import raises

    def unseparate(s: typing.Any) -> str:
        return str(s).replace(' ', '')

    bs = _byte_as_bit_string
    ser = Serializer.new(50)
    expected = ''
    assert str(ser) == ''

    with raises(ValueError):
        ser.add_aligned_u8(-42)

    ser.add_aligned_u8(0b1010_0111)
    expected += '1010 0111'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i64(0x1234_5678_90ab_cdef)
    expected += bs(0xef) + bs(0xcd) + bs(0xab) + bs(0x90)
    expected += bs(0x78) + bs(0x56) + bs(0x34) + bs(0x12)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i32(-0x1234_5678)                           # Two's complement: 0xedcb_a988
    expected += bs(0x88) + bs(0xa9) + bs(0xcb) + bs(0xed)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i16(-2)                                     # Two's complement: 0xfffe
    ser.skip_bits(8)
    ser.add_aligned_i8(127)
    expected += bs(0xfe) + bs(0xff) + bs(0x00) + bs(0x7f)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_f64(1)                                      # IEEE 754: 0x3ff0_0000_0000_0000
    expected += bs(0x00) * 6 + bs(0xf0) + bs(0x3f)
    ser.add_aligned_f32(1)                                      # IEEE 754: 0x3f80_0000
    expected += bs(0x00) * 2 + bs(0x80) + bs(0x3f)
    ser.add_aligned_f16(99999.9)                                # IEEE 754: overflow, degenerates to +inf: 0x7c00
    expected += bs(0x00) * 1 + bs(0x7c)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_unsigned(0xBEDA, 12)                        # 0xBxxx will be truncated away
    expected += '1101 1010 xxxx1110'
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(4)                                            # Bring back into alignment
    expected = expected[:-8] + '00001110'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_unsigned(0xBEDA, 16)                        # Making sure byte-size-aligned are handled well, too
    expected += bs(0xda) + bs(0xbe)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_signed(-2, 9)                               # Two's complement: 510 = 0b1_1111_1110
    expected += '11111110 xxxxxxx1'                             # MSB is at the end
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(7)                                            # Bring back into alignment
    expected = expected[:-8] + '00000001'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_standard_bit_length_primitives(numpy.array([0xdead, 0xbeef], numpy.uint16))
    expected += bs(0xad) + bs(0xde) + bs(0xef) + bs(0xbe)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(numpy.array([
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, True, False, False, True, True, False,      # 11100110
    ], numpy.bool))
    expected += '11000101 01100111'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(numpy.array([
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, False, True, False,                         # 11010
    ], numpy.bool))
    expected += '11000101 xxx01011'
    assert unseparate(ser) == unseparate(expected)

    print('repr(serializer):', repr(ser))

    with raises(ValueError, match='.*read-only.*'):
        ser.buffer[0] = 123                                     # The buffer is read-only for safety reasons


# noinspection PyProtectedMember
def _unittest_serializer_unaligned() -> None:                   # Tricky cases with unaligned fields (very tricky)
    ser = Serializer.new(40)

    ser.add_unaligned_array_of_bits(numpy.array([
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, True,                                       # 111
    ], numpy.bool))
    assert str(ser) == '11000101 xxxxx111'

    ser.add_unaligned_array_of_bits(numpy.array([
        True, False, True, False, False,                        # ???10100 (byte alignment restored here)
        True, True, True, False, True,                          # 11101 (byte alignment lost, three bits short)
    ], numpy.bool))
    assert str(ser) == '11000101 00101111 xxx10111'

    # Adding '00010010 00110100 01010110'
    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=_Byte))
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 xxx01010'

    ser.add_unaligned_array_of_bits(numpy.array([False, True, True], numpy.bool))
    assert ser._bit_offset % 8 == 0, 'Byte alignment is not restored'
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010'

    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=_Byte))     # We're actually aligned here
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110'

    ser.add_unaligned_bit(True)
    ser.add_unaligned_bit(False)
    ser.add_unaligned_bit(False)
    ser.add_unaligned_bit(True)
    ser.add_unaligned_bit(True)   # Three bits short until alignment
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 xxx11001'

    ser.add_unaligned_signed(-2, 8)                             # Two's complement: 254 = 1111 1110
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       'xxx11111'

    ser.add_unaligned_unsigned(0b11101100101, 11)             # Tricky, eh? Eleven bits, unaligned write
    assert ser._bit_offset % 8 == 0, 'Byte alignment is not restored'
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100'

    ser.add_unaligned_unsigned(0b1110, 3)                       # MSB truncated away
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 xxxxx110'

    # Adding '00000000 00000000 00000000 00000000 00000000 00000000 11110000 00111111'
    ser.add_unaligned_f64(1)
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 ' \
                       'xxxxx001'

    # Adding '00000000 00000000 10000000 00111111'
    ser.add_unaligned_f32(1)
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 ' \
                       '00000001 00000000 00000000 11111100 xxxxx001'

    # Adding '00000000 11111100'
    ser.add_unaligned_f16(-99999.9)
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 ' \
                       '00000001 00000000 00000000 11111100 00000001 11100000 xxxxx111'

    # Adding '10101101 11011110 11101111 10111110'
    ser.add_unaligned_array_of_standard_bit_length_primitives(numpy.array([0xdead, 0xbeef], numpy.uint16))
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 ' \
                       '00000001 00000000 00000000 11111100 00000001 11100000 01101111 11110101 01111110 11110111 ' \
                       'xxxxx101'

    ser.skip_bits(5)
    assert ser._bit_offset % 8 == 0, 'Byte alignment is not restored'
    assert str(ser) == '11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 ' \
                       '10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 ' \
                       '00000001 00000000 00000000 11111100 00000001 11100000 01101111 11110101 01111110 11110111 ' \
                       '00000101'

    print('repr(serializer):', repr(ser))
