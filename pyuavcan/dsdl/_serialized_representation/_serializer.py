#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
#

import numpy
import typing
import struct


class SerializerBase:
    """
    All methods operating on scalars implicitly truncate the value if it exceeds the range,
    excepting signed integers, for which overflow handling is not implemented (DSDL does not permit truncation
    of signed integers anyway so it doesn't matter). Saturation must be implemented externally.
    """

    def __init__(self, buffer_size_in_bytes: int):
        self._buf: numpy.ndarray = numpy.zeros(int(buffer_size_in_bytes), dtype=numpy.ubyte)
        self._bit_offset = 0

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    @property
    def buffer(self) -> numpy.ndarray:
        """Returns a properly sized read-only slice of the destination buffer padded to byte."""
        out = self._buf[:(self._bit_offset + 7) // 8]
        out.flags.writeable = False
        assert out.base is self._buf    # Making sure we're not creating a copy, that might be costly
        return out

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits."""
        self._bit_offset += bit_length

    def __str__(self) -> str:
        s = ' '.join(map(lambda b: bin(b)[2:].zfill(8), self.buffer))
        if self._bit_offset % 8 != 0:
            bits_to_cut_off = 8 - self._bit_offset % 8
            return s[:-bits_to_cut_off]
        else:
            return s

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self})'

    #
    # Fast methods optimized for aligned primitive fields; they are byte order invariant.
    # The most specialized methods must be used whenever possible for best performance.
    #
    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        """
        Accepts an array of (u?int|float)(8|16|32|64) and encodes it into the destination.
        On little-endian platforms this may be implemented as memcpy(). The current bit offset must be byte-aligned.
        """
        raise NotImplementedError

    def add_aligned_array_of_bits(self, x: numpy.ndarray) -> None:
        assert self._bit_offset % 8 == 0
        packed = numpy.packbits(x)  # Fortunately, numpy uses same bit ordering as DSDL, no additional transforms needed
        assert len(packed) * 8 >= len(x)
        self._buf[self._byte_offset:self._byte_offset + len(packed)] = packed
        self._bit_offset += len(x)

    def add_aligned_bytes(self, x: numpy.ndarray) -> None:
        """Simply adds a sequence of bytes; the current bit offset must be byte-aligned."""
        assert self._bit_offset % 8 == 0
        assert x.dtype == numpy.ubyte
        self._buf[self._byte_offset:self._byte_offset + len(x)] = x
        self._bit_offset += len(x) * 8

    def add_aligned_u8(self, x: int) -> None:
        assert self._bit_offset % 8 == 0
        self._buf[self._byte_offset] = x
        self._bit_offset += 8

    def add_aligned_u16(self, x: int) -> None:
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
        self.add_aligned_u32((2**32 + x) if x < 0 else x)

    def add_aligned_i64(self, x: int) -> None:
        self.add_aligned_u64((2**64 + x) if x < 0 else x)

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
        bs = self._unsigned_to_bytes(value, bit_length)
        self._buf[self._byte_offset:self._byte_offset + len(bs)] = bs
        self._bit_offset += bit_length
        self._buf[self._byte_offset] <<= (8 - bit_length % 8) & 0b111  # Most significant bit has index zero

    def add_aligned_signed(self, value: int, bit_length: int) -> None:
        self.add_aligned_unsigned((2**bit_length + value) if value < 0 else value, bit_length)

    #
    # Least specialized methods: no assumptions about alignment or element size are made.
    # These are the slowest and may be used only if none of the above (specialized) methods are suitable.
    #
    def add_unaligned_bytes(self, value: numpy.ndarray) -> None:
        assert value.dtype == numpy.ubyte
        # This is a faster alternative of the Ben Dyer's unaligned bit copy algorithm:
        # https://github.com/UAVCAN/libuavcan/blob/fd8ba19bc9c09c05a/libuavcan/src/marshal/uc_bit_array_copy.cpp#L12
        # It is faster because here we are aware that the source is always aligned, which we take advantage of.
        right = self._bit_offset % 8
        left = 8 - right
        for b in value:
            self._buf[self._byte_offset] |= b >> right
            self._bit_offset += 8
            self._buf[self._byte_offset] = (b << left) & 0xFF  # Does nothing if aligned

    def add_unaligned_unsigned(self, value: int, bit_length: int) -> None:
        bs = self._unsigned_to_bytes(value, bit_length)
        self.add_unaligned_bytes(bs)
        # TODO self._buf[self._byte_offset] <<= 8 - bit_length % 8

    def add_unaligned_signed(self, value: int, bit_length: int) -> None:
        self.add_unaligned_unsigned((2**bit_length + value) if value < 0 else value, bit_length)

    def add_unaligned_f16(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('e', x))

    def add_unaligned_f32(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('f', x))

    def add_unaligned_f64(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes('d', x))

    def add_unaligned_bit(self, x: bool) -> None:
        self._buf[self._byte_offset] |= bool(x) << ((8 - self._bit_offset % 8) & 0b111)
        self._bit_offset += 1

    def add_unaligned_array_of_bits(self, x: numpy.ndarray) -> None:
        packed = numpy.packbits(x)  # Fortunately, numpy uses same bit ordering as DSDL
        raise NotImplementedError

    #
    # Internal primitive conversion methods.
    #
    @staticmethod
    def _unsigned_to_bytes(value: int, bit_length: int) -> numpy.ndarray:
        assert bit_length >= 1
        return numpy.frombuffer(struct.pack('<Q', value & (2 ** bit_length - 1))[:(bit_length + 7) // 8],
                                dtype=numpy.ubyte)

    @staticmethod
    def _float_to_bytes(format_char: str, x: float) -> numpy.ndarray:
        f = '<' + format_char
        try:
            out = struct.pack(f, x)
        except OverflowError:  # Oops, let's truncate (saturation must be implemented by the caller if needed)
            out = struct.pack(f, numpy.inf if x > 0 else -numpy.inf)
        return numpy.frombuffer(out, dtype=numpy.ubyte)


def _unittest_serializer_aligned() -> None:
    import sys

    def new(buffer_size_in_bytes: int) -> SerializerBase:
        return {
            'little': LittleEndianSerializer,
            'big': BigEndianSerializer,
        }[sys.byteorder](buffer_size_in_bytes)

    def unseparate(s: typing.Any) -> str:
        return str(s).replace(' ', '').replace('_', '')

    def bit_str(s: int) -> str:
        return bin(s)[2:].zfill(8)

    ser = new(50)
    expected = ''
    assert str(ser) == ''

    ser.add_aligned_u8(0b1010_0111)
    expected += '1010_0111'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i64(0x1234_5678_90ab_cdef)
    expected += bit_str(0xef) + bit_str(0xcd) + bit_str(0xab) + bit_str(0x90)
    expected += bit_str(0x78) + bit_str(0x56) + bit_str(0x34) + bit_str(0x12)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i32(-0x1234_5678)  # two's complement: 0xedcb_a988
    expected += bit_str(0x88) + bit_str(0xa9) + bit_str(0xcb) + bit_str(0xed)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i16(-2)  # two's complement: 0xfffe
    ser.skip_bits(8)
    ser.add_aligned_i8(127)
    expected += bit_str(0xfe) + bit_str(0xff) + bit_str(0x00) + bit_str(0x7f)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_f64(1)                              # IEEE 754: 0x3ff0_0000_0000_0000
    expected += bit_str(0x00) * 6 + bit_str(0xf0) + bit_str(0x3f)
    ser.add_aligned_f32(1)                              # IEEE 754: 0x3f80_0000
    expected += bit_str(0x00) * 2 + bit_str(0x80) + bit_str(0x3f)
    ser.add_aligned_f16(99999.9)                        # IEEE 754: overflow, degenerates to positive infinity: 0x7c00
    expected += bit_str(0x00) * 1 + bit_str(0x7c)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_unsigned(0xBEDA, 12)        # 0xBxxx will be truncated away
    expected += '1101 1010 1110'                # This case is from the examples from the specification
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(4)                            # Bring back into alignment
    expected += '0000'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_signed(-2, 9)               # Two's complement: 510 = 0b1_1111_1110
    expected += '11111110 1'                    # MSB is at the end
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(7)                            # Bring back into alignment
    expected += '0' * 7
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_standard_size_primitives(numpy.array([0xdead, 0xbeef], numpy.uint16))
    expected += bit_str(0xad) + bit_str(0xde) + bit_str(0xef) + bit_str(0xbe)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(numpy.array([
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, True, False, False, True, True, False,      # 11100110
    ], numpy.bool))
    expected += '10100011 11100110'
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(numpy.array([
        True, False, True, False, False, False, True, True,     # 10100011
        True, True, False, True, False,                         # 11010
    ], numpy.bool))
    expected += '10100011 11010'
    assert unseparate(ser) == unseparate(expected)

    print('repr(serializer):', repr(ser))


class LittleEndianSerializer(SerializerBase):
    # noinspection PyUnresolvedReferences
    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        # This is close to raw memcpy() from the source memory into the destination memory, which is very fast.
        # We assume that the target platform uses IEEE 754-compliant floating point representation; otherwise,
        # the generated serialized representation may be incorrect. NumPy seems to only support IEEE-754 compliant
        # platforms though so I don't expect any compatibility issues.
        self.add_aligned_bytes(x.view(numpy.ubyte))


class BigEndianSerializer(SerializerBase):
    def add_aligned_array_of_standard_size_primitives(self, x: numpy.ndarray) -> None:
        raise NotImplementedError('Pull requests are welcome')
