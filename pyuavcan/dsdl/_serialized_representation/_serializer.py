# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import abc
import sys
import typing
import struct

import numpy

_Byte = numpy.uint8
"""
We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
"""

_EXTRA_BUFFER_CAPACITY_BYTES = 1
"""
We extend the requested buffer size by one because some of the non-byte-aligned write operations
require us to temporarily use one extra byte after the current byte.
"""


class Serializer(abc.ABC):
    """
    All methods operating on scalars implicitly truncate the value if it exceeds the range,
    excepting signed integers, for which overflow handling is not implemented (DSDL does not permit truncation
    of signed integers anyway so it doesn't matter). Saturation must be implemented externally.
    Methods that expect an unsigned integer will raise ValueError if the supplied integer is negative.
    """

    def __init__(self, buffer: numpy.ndarray):
        """
        Do not call this directly. Use :meth:`new` to instantiate.
        """
        self._buf = buffer
        self._bit_offset = 0

    @staticmethod
    def new(buffer_size_in_bytes: int) -> Serializer:
        buffer_size_in_bytes = int(buffer_size_in_bytes) + _EXTRA_BUFFER_CAPACITY_BYTES
        buf: numpy.ndarray = numpy.zeros(buffer_size_in_bytes, dtype=_Byte)
        return _PlatformSpecificSerializer(buf)

    @property
    def current_bit_length(self) -> int:
        return self._bit_offset

    @property
    def buffer(self) -> numpy.ndarray:
        """Returns a properly sized read-only slice of the destination buffer zero-bit-padded to byte."""
        out = self._buf[: (self._bit_offset + 7) // 8]
        out.flags.writeable = False
        # Here we used to check if out.base is self._buf to make sure we're not creating a copy because that might
        # be costly. We no longer do that because it doesn't work with forked serializers: forks don't own their
        # buffers so this check would be failing; also, with MyPy v1.19 this expression used to segfault the
        # interpreter. Very dangerous.
        return out

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits and for skipping fragments written by forked serializers."""
        self._bit_offset += bit_length

    def pad_to_alignment(self, bit_length: int) -> None:
        while self._bit_offset % bit_length != 0:
            self.add_unaligned_bit(False)

    def fork_bytes(self, forked_buffer_size_in_bytes: int) -> Serializer:
        """
        Creates another serializer that uses the same underlying serialization destination buffer
        but offset by :prop:`current_bit_length`. This is intended for delimited serialization.
        The algorithm is simple:

        - Fork the main serializer (M) at the point where the delimited nested instance needs to be serialized.
        - Having obtained the forked serializer (F), skip the size of the delimited header and serialize the object.
        - Take the offset of F (in bytes) sans the size of the delimiter header and serialize the value using M.
        - Skip M by the above number of bytes to avoid overwriting the fragment written by F.
        - Discard F. The job is done.

        This may be unnecessary if the nested object is of a fixed size. In this case, since its length is known,
        the delimiter header can be serialized as a constant, and then the nested object can be serialized trivially
        as if it was sealed.

        This method raises a :class:`ValueError` if the forked instance is not byte-aligned or if the requested buffer
        size is too large.
        """
        if self._bit_offset % 8 != 0:
            raise ValueError("Cannot fork unaligned serializer")
        forked_buffer = self._buf[self._bit_offset // 8 :]
        forked_buffer_size_in_bytes += _EXTRA_BUFFER_CAPACITY_BYTES
        if len(forked_buffer) < forked_buffer_size_in_bytes:
            raise ValueError(
                f"The required forked buffer size of {forked_buffer_size_in_bytes} bytes is less "
                f"than the available remaining buffer space of {len(forked_buffer)} bytes"
            )
        forked_buffer = forked_buffer[:forked_buffer_size_in_bytes]
        assert len(forked_buffer) == forked_buffer_size_in_bytes
        return _PlatformSpecificSerializer(forked_buffer)

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
        assert x.dtype in (bool, numpy.bool_)
        assert self._bit_offset % 8 == 0
        packed = numpy.packbits(x, bitorder="little")
        assert len(packed) * 8 >= len(x)
        self._buf[self._byte_offset : self._byte_offset + len(packed)] = packed
        self._bit_offset += len(x)

    def add_aligned_bytes(self, x: numpy.ndarray) -> None:
        """Simply adds a sequence of bytes; the current bit offset must be byte-aligned."""
        assert self._bit_offset % 8 == 0
        assert x.dtype == _Byte
        self._buf[self._byte_offset : self._byte_offset + len(x)] = x
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
        self.add_aligned_bytes(self._float_to_bytes("e", x))

    def add_aligned_f32(self, x: float) -> None:
        self.add_aligned_bytes(self._float_to_bytes("f", x))

    def add_aligned_f64(self, x: float) -> None:
        self.add_aligned_bytes(self._float_to_bytes("d", x))

    #
    # Less specialized methods: assuming that the value is aligned at the beginning, but its bit length
    # is non-standard and may not be an integer multiple of eight.
    # These must not be used if there is a suitable more specialized version defined above.
    #
    def add_aligned_unsigned(self, value: int, bit_length: int) -> None:
        assert self._bit_offset % 8 == 0
        self._ensure_not_negative(value)
        bs = self._unsigned_to_bytes(value, bit_length)
        self._buf[self._byte_offset : self._byte_offset + len(bs)] = bs
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
        assert x.dtype in (bool, numpy.bool_)
        packed = numpy.packbits(x, bitorder="little")
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
        self.add_unaligned_bytes(self._float_to_bytes("e", x))

    def add_unaligned_f32(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes("f", x))

    def add_unaligned_f64(self, x: float) -> None:
        self.add_unaligned_bytes(self._float_to_bytes("d", x))

    def add_unaligned_bit(self, x: bool) -> None:
        self._buf[self._byte_offset] |= bool(x) << (self._bit_offset % 8)
        self._bit_offset += 1

    #
    # Private methods.
    #
    @staticmethod
    def _unsigned_to_bytes(value: int, bit_length: int) -> numpy.ndarray:
        assert bit_length >= 1
        assert value >= 0, "This operation is undefined for negative integers"
        value &= 2 ** bit_length - 1
        num_bytes = (bit_length + 7) // 8
        out = numpy.zeros(num_bytes, dtype=_Byte)
        for i in range(num_bytes):  # Oh, why is my life like this?
            out[i] = value & 0xFF
            value >>= 8
        return out

    @staticmethod
    def _float_to_bytes(format_char: str, x: float) -> numpy.ndarray:
        f = "<" + format_char
        try:
            out = struct.pack(f, x)
        except OverflowError:  # Oops, let's truncate (saturation must be implemented by the caller if needed)
            out = struct.pack(f, numpy.inf if x > 0 else -numpy.inf)
        return numpy.frombuffer(out, dtype=_Byte)  # Note: this operation does not copy the underlying bytes

    @staticmethod
    def _ensure_not_negative(x: int) -> None:
        if x < 0:
            raise ValueError(f"The requested serialization method is not defined on negative integers ({x})")

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    def __str__(self) -> str:
        s = " ".join(map(_byte_as_bit_string, self.buffer))
        if self._bit_offset % 8 != 0:
            s, tail = s.rsplit(maxsplit=1)
            bits_to_cut_off = 8 - self._bit_offset % 8
            tail = ("x" * bits_to_cut_off) + tail[bits_to_cut_off:]
            return s + " " + tail
        return s

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self})"


class _LittleEndianSerializer(Serializer):
    # noinspection PyUnresolvedReferences
    def add_aligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        # This is close to direct memcpy() from the source memory into the destination memory, which is very fast.
        # We assume that the local platform uses IEEE 754-compliant floating point representation; otherwise,
        # the generated serialized representation may be incorrect. NumPy seems to only support IEEE-754 compliant
        # platforms though so I don't expect any compatibility issues.
        assert x.dtype not in (bool, numpy.bool_, object)
        self.add_aligned_bytes(x.view(_Byte))

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        # This is much slower than the aligned version because we have to manually copy and shift each byte,
        # but still better than manual elementwise serialization.
        assert x.dtype not in (bool, numpy.bool_, object)
        self.add_unaligned_bytes(x.view(_Byte))


class _BigEndianSerializer(Serializer):
    def add_aligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:  # pragma: no cover
        raise NotImplementedError("Pull requests are welcome")

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: numpy.ndarray) -> None:
        raise NotImplementedError("Pull requests are welcome")


_PlatformSpecificSerializer = {
    "little": _LittleEndianSerializer,
    "big": _BigEndianSerializer,
}[sys.byteorder]


def _byte_as_bit_string(x: int) -> str:
    return bin(x)[2:].zfill(8)


def _unittest_serializer_to_str() -> None:
    ser = Serializer.new(50)
    assert str(ser) == ""
    ser.add_aligned_u8(0b11001110)
    assert str(ser) == "11001110"
    ser.add_aligned_i16(-1)
    assert str(ser) == "11001110 11111111 11111111"
    ser.add_aligned_unsigned(0, 1)
    assert str(ser) == "11001110 11111111 11111111 xxxxxxx0"
    ser.add_unaligned_signed(-1, 3)
    assert str(ser) == "11001110 11111111 11111111 xxxx1110"


def _unittest_serializer_aligned() -> None:
    from pytest import raises

    def unseparate(s: typing.Any) -> str:
        return str(s).replace(" ", "")

    bs = _byte_as_bit_string
    ser = Serializer.new(50)
    expected = ""
    assert str(ser) == ""

    with raises(ValueError):
        ser.add_aligned_u8(-42)

    ser.add_aligned_u8(0b1010_0111)
    expected += "1010 0111"
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i64(0x1234_5678_90AB_CDEF)
    expected += bs(0xEF) + bs(0xCD) + bs(0xAB) + bs(0x90)
    expected += bs(0x78) + bs(0x56) + bs(0x34) + bs(0x12)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i32(-0x1234_5678)  # Two's complement: 0xedcb_a988
    expected += bs(0x88) + bs(0xA9) + bs(0xCB) + bs(0xED)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_i16(-2)  # Two's complement: 0xfffe
    ser.skip_bits(8)
    ser.add_aligned_i8(127)
    expected += bs(0xFE) + bs(0xFF) + bs(0x00) + bs(0x7F)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_f64(1)  # IEEE 754: 0x3ff0_0000_0000_0000
    expected += bs(0x00) * 6 + bs(0xF0) + bs(0x3F)
    ser.add_aligned_f32(1)  # IEEE 754: 0x3f80_0000
    expected += bs(0x00) * 2 + bs(0x80) + bs(0x3F)
    ser.add_aligned_f16(99999.9)  # IEEE 754: overflow, degenerates to +inf: 0x7c00
    expected += bs(0x00) * 1 + bs(0x7C)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_unsigned(0xBEDA, 12)  # 0xBxxx will be truncated away
    expected += "1101 1010 xxxx1110"
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(4)  # Bring back into alignment
    expected = expected[:-8] + "00001110"
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_unsigned(0xBEDA, 16)  # Making sure byte-size-aligned are handled well, too
    expected += bs(0xDA) + bs(0xBE)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_signed(-2, 9)  # Two's complement: 510 = 0b1_1111_1110
    expected += "11111110 xxxxxxx1"  # MSB is at the end
    assert unseparate(ser) == unseparate(expected)

    ser.skip_bits(7)  # Bring back into alignment
    expected = expected[:-8] + "00000001"
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_standard_bit_length_primitives(numpy.array([0xDEAD, 0xBEEF], numpy.uint16))
    expected += bs(0xAD) + bs(0xDE) + bs(0xEF) + bs(0xBE)
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(
        numpy.array(
            [
                True,
                False,
                True,
                False,
                False,
                False,
                True,
                True,  # 10100011
                True,
                True,
                True,
                False,
                False,
                True,
                True,
                False,  # 11100110
            ],
            bool,
        )
    )
    expected += "11000101 01100111"
    assert unseparate(ser) == unseparate(expected)

    ser.add_aligned_array_of_bits(
        numpy.array(
            [
                True,
                False,
                True,
                False,
                False,
                False,
                True,
                True,  # 10100011
                True,
                True,
                False,
                True,
                False,  # 11010
            ],
            bool,
        )
    )
    expected += "11000101 xxx01011"
    assert unseparate(ser) == unseparate(expected)

    print("repr(serializer):", repr(ser))

    with raises(ValueError, match=".*read-only.*"):
        ser.buffer[0] = 123  # The buffer is read-only for safety reasons


def _unittest_serializer_unaligned() -> None:  # Tricky cases with unaligned fields (very tricky)
    ser = Serializer.new(40)

    ser.add_unaligned_array_of_bits(
        numpy.array(
            [
                True,
                False,
                True,
                False,
                False,
                False,
                True,
                True,  # 10100011
                True,
                True,
                True,  # 111
            ],
            bool,
        )
    )
    assert str(ser) == "11000101 xxxxx111"

    ser.add_unaligned_array_of_bits(
        numpy.array(
            [
                True,
                False,
                True,
                False,
                False,  # ???10100 (byte alignment restored here)
                True,
                True,
                True,
                False,
                True,  # 11101 (byte alignment lost, three bits short)
            ],
            bool,
        )
    )
    assert str(ser) == "11000101 00101111 xxx10111"

    # Adding '00010010 00110100 01010110'
    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=_Byte))
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 xxx01010"

    ser.add_unaligned_array_of_bits(numpy.array([False, True, True], bool))
    assert ser._bit_offset % 8 == 0, "Byte alignment is not restored"  # pylint: disable=protected-access
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010"

    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=_Byte))  # We're actually aligned here
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110"

    ser.add_unaligned_bit(True)
    ser.add_unaligned_bit(False)
    ser.add_unaligned_bit(False)
    ser.add_unaligned_bit(True)
    ser.add_unaligned_bit(True)  # Three bits short until alignment
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 xxx11001"

    ser.add_unaligned_signed(-2, 8)  # Two's complement: 254 = 1111 1110
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "xxx11111"
    )

    ser.add_unaligned_unsigned(0b11101100101, 11)  # Tricky, eh? Eleven bits, unaligned write
    assert ser._bit_offset % 8 == 0, "Byte alignment is not restored"  # pylint: disable=protected-access
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100"
    )

    ser.add_unaligned_unsigned(0b1110, 3)  # MSB truncated away
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 xxxxx110"
    )

    # Adding '00000000 00000000 00000000 00000000 00000000 00000000 11110000 00111111'
    ser.add_unaligned_f64(1)
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 "
        "xxxxx001"
    )

    # Adding '00000000 00000000 10000000 00111111'
    ser.add_unaligned_f32(1)
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 "
        "00000001 00000000 00000000 11111100 xxxxx001"
    )

    # Adding '00000000 11111100'
    ser.add_unaligned_f16(-99999.9)
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 "
        "00000001 00000000 00000000 11111100 00000001 11100000 xxxxx111"
    )

    # Adding '10101101 11011110 11101111 10111110'
    ser.add_unaligned_array_of_standard_bit_length_primitives(numpy.array([0xDEAD, 0xBEEF], numpy.uint16))
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 "
        "00000001 00000000 00000000 11111100 00000001 11100000 01101111 11110101 01111110 11110111 "
        "xxxxx101"
    )

    ser.skip_bits(5)
    assert ser._bit_offset % 8 == 0, "Byte alignment is not restored"  # pylint: disable=protected-access
    assert (
        str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 "
        "10111111 11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 "
        "00000001 00000000 00000000 11111100 00000001 11100000 01101111 11110101 01111110 11110111 "
        "00000101"
    )

    print("repr(serializer):", repr(ser))


def _unittest_serializer_fork_bytes() -> None:
    import pytest

    r = Serializer.new(16)
    m = Serializer.new(16)
    assert str(r) == str(m)

    r.add_aligned_u8(123)
    m.add_aligned_u8(123)
    assert str(r) == str(m)

    with pytest.raises(ValueError):
        m.fork_bytes(16)  # Out of range

    f = m.fork_bytes(15)
    assert str(f) == ""
    r.add_aligned_u8(42)
    f.add_aligned_u8(42)
    assert str(r) != str(m)
    m.skip_bits(8)
    assert str(r) == str(m)  # M updated even though we didn't write in it!

    r.add_aligned_u8(11)
    m.add_aligned_u8(11)
    assert str(r) == str(m)

    f.skip_bits(8)
    ff = f.fork_bytes(1)
    r.add_aligned_u8(22)
    ff.add_aligned_u8(22)
    assert str(r) != str(m)
    m.skip_bits(8)
    assert str(r) == str(m)  # M updated even though we didn't write in it! Double indirection.

    ff.add_unaligned_bit(True)  # Break alignment
    with pytest.raises(ValueError):
        ff.fork_bytes(1)  # Bad alignment
