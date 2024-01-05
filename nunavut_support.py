# Copyright (C) 2019  OpenCyphal Development Team  <opencyphal.org>
# This software is distributed under the terms of the MIT License.
# This code has been migrated from PyCyphal: https://github.com/OpenCyphal/pycyphal
# Author: Pavel Kirienko <pavel@opencyphal.org>, Kalyan Sriram <coder.kalyan@gmail.com>

"""
This module contains various definitions and logic that the code emitted by Nunavut relies on.
It shall be distributed along with the generated code and be importable by it.
No API stability guarantees are made for this module except for those entities that are listed in __all__.
The generated code currently requires the following dependencies to be available at runtime:

- NumPy
- PyDSDL

This module also contains built-in unit test functions (named "test_*") and doctests.
To execute the tests, simply invoke PyTest on this file as follows::

    pytest --doctest-modules nunavut_support.py
"""

from __future__ import annotations
import abc
import sys
from typing import TypeVar, Type, Sequence, cast, Any, Iterable
import importlib
import struct
import string
import base64
import logging

# Dependencies not from the standard library:
import numpy  # This is not a dependency of Nunavut but is used in the generated code.
from numpy.typing import NDArray
import pydsdl

if sys.byteorder != "little":  # pragma: no cover
    raise RuntimeError(
        "BIG-ENDIAN PLATFORMS ARE NOT YET SUPPORTED. "
        "The current serialization code assumes that the native byte order is little-endian. Since Cyphal uses "
        "little-endian byte order in its serialized data representations, this assumption allows us to bypass data "
        "transformation in many cases, resulting in zero-cost serialization and deserialization. "
        "Big-endian platforms are unable to take advantage of that, requiring byte swapping for multi-byte entities; "
        "fortunately, nowadays such platforms are uncommon. If you need to use this code on a big-endian platform, "
        "please implement the missing code and submit a pull request upstream, then remove this exception."
    )

API_VERSION = (1, 0, 0)
"""
This is the version of the API that the generated code expects to be available at runtime.
"""

__all__ = [
    "serialize",
    "deserialize",
    "get_model",
    "get_class",
    "get_extent_bytes",
    "get_fixed_port_id",
    "get_attribute",
    "set_attribute",
    "is_serializable",
    "is_message_type",
    "is_service_type",
    "to_builtin",
    "update_from_builtin",
]
"""
No API stability guarantees are made for this module except for those entities that are listed in __all__.
"""

Byte = numpy.uint8
"""
We must use uint8 instead of ubyte because uint8 is platform-invariant whereas (u)byte is platform-dependent.
"""

StdPrimitive = TypeVar(
    "StdPrimitive",
    numpy.float64,
    numpy.float32,
    numpy.float16,
    numpy.uint8,
    numpy.uint16,
    numpy.uint32,
    numpy.uint64,
    numpy.int8,
    numpy.int16,
    numpy.int32,
    numpy.int64,
)

T = TypeVar("T")

_logger = logging.getLogger(__name__)

# ==================================================  SERIALIZER  ==================================================

class Serializer(abc.ABC):
    """
    All methods operating on scalars implicitly truncate the value if it exceeds the range,
    excepting signed integers, for which overflow handling is not implemented (DSDL does not permit truncation
    of signed integers anyway so it doesn't matter). Saturation must be implemented externally.
    Methods that expect an unsigned integer will raise ValueError if the supplied integer is negative.
    """

    _EXTRA_BUFFER_CAPACITY_BYTES = 1
    """
    We extend the requested buffer size by one because some of the non-byte-aligned write operations
    require us to temporarily use one extra byte after the current byte.
    """

    def __init__(self, buffer: NDArray[Byte]):
        """
        Do not call this directly. Use :meth:`new` to instantiate.
        """
        self._buf = buffer
        self._bit_offset = 0

    @staticmethod
    def new(buffer_size_in_bytes: int) -> Serializer:
        buffer_size_in_bytes = int(buffer_size_in_bytes) + Serializer._EXTRA_BUFFER_CAPACITY_BYTES
        buf: NDArray[Byte] = numpy.zeros(buffer_size_in_bytes, dtype=Byte)
        return _PlatformSpecificSerializer(buf)

    @property
    def current_bit_length(self) -> int:
        return self._bit_offset

    @property
    def buffer(self) -> NDArray[Byte]:
        """Returns a properly sized read-only slice of the destination buffer zero-bit-padded to byte."""
        out: NDArray[Byte] = self._buf[: (self._bit_offset + 7) // 8]
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
        forked_buffer_size_in_bytes += Serializer._EXTRA_BUFFER_CAPACITY_BYTES
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
    def add_aligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        """
        Accepts an array of ``(u?int|float)(8|16|32|64)`` and encodes it into the destination.
        On little-endian platforms this may be implemented virtually through ``memcpy()``.
        The current bit offset must be byte-aligned.
        """
        raise NotImplementedError

    def add_aligned_array_of_bits(self, x: NDArray[numpy.bool_]) -> None:
        """
        Accepts an array of bools and encodes it into the destination using fast native serialization routine
        implemented in numpy. The current bit offset must be byte-aligned.
        """
        assert self._bit_offset % 8 == 0
        packed = numpy.packbits(x, bitorder="little")
        assert len(packed) * 8 >= len(x)
        self._buf[self._byte_offset : self._byte_offset + len(packed)] = packed
        self._bit_offset += len(x)

    def add_aligned_bytes(self, x: NDArray[Byte]) -> None:
        """Simply adds a sequence of bytes; the current bit offset must be byte-aligned."""
        assert self._bit_offset % 8 == 0
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
        self.add_aligned_u32((2**32 + x) if x < 0 else x)

    def add_aligned_i64(self, x: int) -> None:
        self.add_aligned_u64((2**64 + x) if x < 0 else x)

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
        self.add_aligned_unsigned((2**bit_length + value) if value < 0 else value, bit_length)

    #
    # Least specialized methods: no assumptions about alignment are made.
    # These are the slowest and may be used only if none of the above (specialized) methods are suitable.
    #
    @abc.abstractmethod
    def add_unaligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        """See the aligned counterpart."""
        raise NotImplementedError

    def add_unaligned_array_of_bits(self, x: NDArray[numpy.bool_]) -> None:
        packed = numpy.packbits(x, bitorder="little")
        backtrack = len(packed) * 8 - len(x)
        assert backtrack >= 0
        self.add_unaligned_bytes(packed)
        self._bit_offset -= backtrack

    def add_unaligned_bytes(self, value: NDArray[Byte]) -> None:
        # This is a faster variant of Ben Dyer's unaligned bit copy algorithm:
        # https://github.com/OpenCyphal/libuavcan/blob/fd8ba19bc9c09c05a/libuavcan/src/marshal/uc_bit_array_copy.cpp#L12
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
        self.add_unaligned_unsigned((2**bit_length + value) if value < 0 else value, bit_length)

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
    def _unsigned_to_bytes(value: int, bit_length: int) -> NDArray[Byte]:
        assert bit_length >= 1
        assert value >= 0, "This operation is undefined for negative integers"
        value &= 2**bit_length - 1
        num_bytes = (bit_length + 7) // 8
        out: NDArray[Byte] = numpy.zeros(num_bytes, dtype=Byte)
        for i in range(num_bytes):  # Oh, why is my life like this?
            out[i] = value & 0xFF
            value >>= 8
        return out

    @staticmethod
    def _float_to_bytes(format_char: str, x: float) -> NDArray[Byte]:
        f = "<" + format_char
        try:
            out = struct.pack(f, x)
        except OverflowError:  # Oops, let's truncate (saturation must be implemented by the caller if needed)
            out = struct.pack(f, numpy.inf if x > 0 else -numpy.inf)
        # Note: this operation does not copy the underlying bytes
        return numpy.frombuffer(out, dtype=Byte)

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
    def add_aligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        # This is close to direct memcpy() from the source memory into the destination memory, which is very fast.
        # We assume that the local platform uses IEEE 754-compliant floating point representation; otherwise,
        # the generated serialized representation may be incorrect. NumPy seems to only support IEEE-754 compliant
        # platforms though so I don't expect any compatibility issues.
        self.add_aligned_bytes(x.view(Byte))

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        # This is much slower than the aligned version because we have to manually copy and shift each byte,
        # but still better than manual elementwise serialization.
        self.add_unaligned_bytes(x.view(Byte))

class _BigEndianSerializer(Serializer):
    def add_aligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        raise NotImplementedError("Pull requests are welcome")  # pragma: no cover

    def add_unaligned_array_of_standard_bit_length_primitives(self, x: NDArray[StdPrimitive]) -> None:
        raise NotImplementedError("Pull requests are welcome")  # pragma: no cover

_PlatformSpecificSerializer = {
    "little": _LittleEndianSerializer,
    "big": _BigEndianSerializer,
}[sys.byteorder]

def _byte_as_bit_string(x: int) -> str:
    return bin(x)[2:].zfill(8)

def test_serializer_to_str() -> None:
    """This is a unit test, not for production use."""
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

def test_serializer_aligned() -> None:
    """This is a unit test, not for production use."""
    from pytest import raises

    def unseparate(s: Any) -> str:
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

    ser.add_aligned_i32(-0x1234_5678)  # Two's complement: 0xEDCB_A988
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

def test_serializer_unaligned() -> None:  # Tricky cases with unaligned fields (very tricky)
    """This is a unit test, not for production use."""
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
    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=Byte))
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 xxx01010"

    ser.add_unaligned_array_of_bits(numpy.array([False, True, True], bool))
    assert ser._bit_offset % 8 == 0, "Byte alignment is not restored"  # pylint: disable=protected-access
    assert str(ser) == "11000101 00101111 01010111 10000010 11000110 11001010"

    ser.add_unaligned_bytes(numpy.array([0x12, 0x34, 0x56], dtype=Byte))  # We're actually aligned here
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

def test_serializer_fork_bytes() -> None:
    """This is a unit test, not for production use."""
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

# ==================================================  DESERIALIZER  ==================================================

class Deserializer(abc.ABC):
    """
    The deserializer class is used for deconstruction of serialized representations of DSDL objects into Python objects.
    It implements the implicit zero extension rule as described in the Specification.
    """

    class FormatError(ValueError):
        """
        This exception class is used when an auto-generated deserialization routine is supplied with invalid input data;
        in other words, input that is not a valid serialized representation of its data type.

        Deserialization logic (auto-generated or manually written) may use this exception type.
        When thrown from a deserialization method, it may be intercepted by its caller,
        which then returns None instead of a valid deserialized instance,
        indicating that the serialized representation is invalid.
        """

    def __init__(self, fragmented_buffer: Sequence[memoryview]):
        """
        Do not call this directly. Use :meth:`new` to instantiate.
        """
        self._buf = ZeroExtendingBuffer(fragmented_buffer)
        self._bit_offset = 0
        assert self.consumed_bit_length + self.remaining_bit_length == self._buf.bit_length

    @staticmethod
    def new(fragmented_buffer: Sequence[memoryview]) -> Deserializer:
        """
        :param fragmented_buffer: The source serialized representation. The deserializer will attempt to avoid copying
            any data from the serialized representation, establishing direct references to its memory instead.
            If any of the source buffer fragments are read-only, some of the deserialized array-typed values
            may end up being read-only as well. If that is undesirable, use writeable buffer.

        :return: A new instance of Deserializer, either little-endian or big-endian, depending on the platform.
        """
        return _PlatformSpecificDeserializer(fragmented_buffer)

    @property
    def consumed_bit_length(self) -> int:
        return self._bit_offset

    @property
    def remaining_bit_length(self) -> int:
        """Returns negative if out of bounds (zero extension rule in effect)."""
        return self._buf.bit_length - self._bit_offset

    def skip_bits(self, bit_length: int) -> None:
        """This is used for padding bits."""
        _ensure_cardinal(bit_length)
        self._bit_offset += bit_length

    def pad_to_alignment(self, bit_length: int) -> None:
        while self._bit_offset % bit_length != 0:
            self._bit_offset += 1

    def fork_bytes(self, forked_buffer_size_in_bytes: int) -> Deserializer:
        """
        This is the counterpart of fork_bytes() defined in the serializer intended for deserializing delimited types.
        Forking is necessary to support implicit truncation and implicit zero extension of nested objects.
        The algorithm is as follows:

        - Before forking, using the main deserializer (M), read the delimiter header.
        - If the value of the delimiter header exceeds the number of bytes remaining in the deserialization buffer,
          raise :class:`FormatError`, thereby declaring the serialized representation invalid, as prescribed by the
          Specification.
        - Fork M.
        - Skip M by the size reported by the delimiter header.
        - Using the forked deserializer (F), deserialize the nested object. F will apply implicit truncation
          and the implicit zero extension rules as necessary regardless of the amount of data remaining in M.
        - Discard F.

        This method raises a :class:`ValueError` if the forked instance is not byte-aligned or if the requested buffer
        size is too large. The latter is because it is a class usage error, not a deserialization error.
        """
        if self._bit_offset % 8 != 0:
            raise ValueError("Cannot fork unaligned deserializer")
        remaining_bit_length = self.remaining_bit_length
        assert remaining_bit_length % 8 == 0
        remaining_byte_length = remaining_bit_length // 8
        if remaining_byte_length < forked_buffer_size_in_bytes:
            raise ValueError(
                f"Invalid usage: the required forked buffer size of {forked_buffer_size_in_bytes} bytes "
                f"is less than the available remaining buffer space of {remaining_byte_length} bytes"
            )
        out = _PlatformSpecificDeserializer(self._buf.fork_bytes(self._byte_offset, forked_buffer_size_in_bytes))
        assert out.remaining_bit_length == forked_buffer_size_in_bytes * 8
        return out

    #
    # Fast methods optimized for aligned primitive fields.
    # The most specialized methods must be used whenever possible for best performance.
    #
    @abc.abstractmethod
    def fetch_aligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        """
        Returns a new array which may directly refer to the underlying memory.
        The returned array may be read-only if the source buffer is read-only.
        """
        raise NotImplementedError

    def fetch_aligned_array_of_bits(self, count: int) -> NDArray[numpy.bool_]:
        """
        Quickly decodes an aligned array of bits using the numpy's fast bit unpacking routine.
        A new array is always created (the memory cannot be shared with the buffer due to the layout transformation).
        The returned array is of dtype :class:`bool`.
        """
        _ensure_cardinal(count)
        assert self._bit_offset % 8 == 0
        bs = self._buf.get_unsigned_slice(self._byte_offset, self._byte_offset + (count + 7) // 8)
        out = numpy.unpackbits(bs, bitorder="little")[:count]
        self._bit_offset += count
        assert len(out) == count
        return cast(NDArray[numpy.bool_], out.astype(dtype=bool))

    def fetch_aligned_bytes(self, count: int) -> NDArray[Byte]:
        _ensure_cardinal(count)
        assert self._bit_offset % 8 == 0
        out = self._buf.get_unsigned_slice(self._byte_offset, self._byte_offset + count)
        self._bit_offset += count * 8
        assert len(out) == count
        return out

    def fetch_aligned_u8(self) -> int:
        assert self._bit_offset % 8 == 0
        out = self._buf.get_byte(self._byte_offset)
        assert isinstance(out, int)  # Make sure it's not a NumPy's integer type like numpy.uint8. We need native int.
        self._bit_offset += 8
        return out

    def fetch_aligned_u16(
        self,
    ) -> int:  # TODO: here and below, consider using int.from_bytes()?
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
        return int(x - 2**32) if x >= 2**31 else x  # wrapped in int() to appease MyPy

    def fetch_aligned_i64(self) -> int:
        x = self.fetch_aligned_u64()
        return int(x - 2**64) if x >= 2**63 else x  # wrapped in int() to appease MyPy

    def fetch_aligned_f16(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<e", self.fetch_aligned_bytes(2))  # type: ignore
        assert isinstance(out, float)
        return out

    def fetch_aligned_f32(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<f", self.fetch_aligned_bytes(4))  # type: ignore
        assert isinstance(out, float)
        return out

    def fetch_aligned_f64(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<d", self.fetch_aligned_bytes(8))  # type: ignore
        assert isinstance(out, float)
        return out

    #
    # Less specialized methods: assuming that the value is aligned at the beginning, but its bit length
    # is non-standard and may not be an integer multiple of eight.
    # These must not be used if there is a suitable more specialized version defined above.
    #
    def fetch_aligned_unsigned(self, bit_length: int) -> int:
        _ensure_cardinal(bit_length)
        assert self._bit_offset % 8 == 0
        bs = self._buf.get_unsigned_slice(self._byte_offset, self._byte_offset + (bit_length + 7) // 8)
        self._bit_offset += bit_length
        return self._unsigned_from_bytes(bs, bit_length)

    def fetch_aligned_signed(self, bit_length: int) -> int:
        assert bit_length >= 2
        u = self.fetch_aligned_unsigned(bit_length)
        out = (u - 2**bit_length) if u >= 2 ** (bit_length - 1) else u
        assert isinstance(out, int)  # MyPy pls
        return out

    #
    # Least specialized methods: no assumptions about alignment are made.
    # These are the slowest and may be used only if none of the above (specialized) methods are suitable.
    #
    @abc.abstractmethod
    def fetch_unaligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        """See the aligned counterpart."""
        raise NotImplementedError

    def fetch_unaligned_array_of_bits(self, count: int) -> NDArray[numpy.bool_]:
        _ensure_cardinal(count)
        byte_count = (count + 7) // 8
        bs = self.fetch_unaligned_bytes(byte_count)
        assert len(bs) == byte_count
        backtrack = byte_count * 8 - count
        assert 0 <= backtrack < 8
        self._bit_offset -= backtrack
        out: NDArray[numpy.bool_] = numpy.unpackbits(bs, bitorder="little")[:count].astype(dtype=bool)
        assert len(out) == count
        return out

    def fetch_unaligned_bytes(self, count: int) -> NDArray[Byte]:
        if count > 0:
            if self._bit_offset % 8 != 0:
                # This is a faster variant of Ben Dyer's unaligned bit copy algorithm:
                # https://github.com/OpenCyphal/libuavcan/blob/fd8ba19bc9c09/libuavcan/src/marshal/uc_bit_array_copy.cpp#L12
                # It is faster because here we are aware that the destination is always aligned, which we take
                # advantage of. This algorithm breaks for byte-aligned offset, so we have to delegate the aligned
                # case to the aligned copy method (which is also much faster).
                out: NDArray[Byte] = numpy.empty(count, dtype=Byte)
                right = self._bit_offset % 8
                left = 8 - right
                assert (1 <= right <= 7) and (1 <= left <= 7)
                # The last byte is a special case because if we're reading the last few unaligned bits, the very last
                # byte access will be always out of range. We don't care because of the implicit zero extension rule.
                for i in range(count):
                    byte_offset = self._byte_offset
                    out[i] = (self._buf.get_byte(byte_offset) >> right) | (
                        (self._buf.get_byte(byte_offset + 1) << left) & 0xFF
                    )
                    self._bit_offset += 8
                assert len(out) == count
                return out
            return self.fetch_aligned_bytes(count)
        return numpy.zeros(0, dtype=Byte)

    def fetch_unaligned_unsigned(self, bit_length: int) -> int:
        _ensure_cardinal(bit_length)
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
        out = (u - 2**bit_length) if u >= 2 ** (bit_length - 1) else u
        assert isinstance(out, int)  # MyPy pls
        return out

    def fetch_unaligned_f16(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<e", self.fetch_unaligned_bytes(2))  # type: ignore
        assert isinstance(out, float)
        return out

    def fetch_unaligned_f32(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<f", self.fetch_unaligned_bytes(4))  # type: ignore
        assert isinstance(out, float)
        return out

    def fetch_unaligned_f64(self) -> float:  # noinspection PyTypeChecker
        (out,) = struct.unpack("<d", self.fetch_unaligned_bytes(8))  # type: ignore
        assert isinstance(out, float)
        return out

    def fetch_unaligned_bit(self) -> bool:
        mask = 1 << (self._bit_offset % 8)
        assert 1 <= mask <= 128
        out = self._buf.get_byte(self._byte_offset) & mask == mask
        self._bit_offset += 1
        return bool(out)

    #
    # Private methods.
    #
    @staticmethod
    def _unsigned_from_bytes(x: NDArray[Byte], bit_length: int) -> int:
        assert bit_length >= 1
        num_bytes = (bit_length + 7) // 8
        assert num_bytes > 0
        last_byte_index = num_bytes - 1
        assert len(x) >= num_bytes
        out = 0
        for i in range(last_byte_index):
            out |= int(x[i]) << (i * 8)
        msb_mask = (2 ** (bit_length % 8) - 1) if bit_length % 8 != 0 else 0xFF
        assert msb_mask in (1, 3, 7, 15, 31, 63, 127, 255)
        out |= (int(x[last_byte_index]) & msb_mask) << (last_byte_index * 8)
        assert 0 <= out < (2**bit_length)
        return out

    @property
    def _byte_offset(self) -> int:
        return self._bit_offset // 8

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"consumed_bit_length={self.consumed_bit_length}, "
            f"remaining_bit_length={self.remaining_bit_length}, "
            f"serialized_representation_base64={self._buf.to_base64()!r})"
        )

class _LittleEndianDeserializer(Deserializer):
    def fetch_aligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        assert dtype not in (bool, numpy.bool_, object), "Invalid usage"
        assert self._bit_offset % 8 == 0
        bo = self._byte_offset
        # Interestingly, numpy doesn't care about alignment. If the source buffer is not properly aligned, it will
        # work anyway but slower.
        out: NDArray[StdPrimitive] = numpy.frombuffer(
            self._buf.get_unsigned_slice(bo, bo + count * numpy.dtype(dtype).itemsize),
            dtype=dtype,
        )
        assert len(out) == count
        self._bit_offset += out.nbytes * 8
        return out

    def fetch_unaligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        assert dtype not in (bool, numpy.bool_, object), "Invalid usage"
        bs = self.fetch_unaligned_bytes(numpy.dtype(dtype).itemsize * count)
        assert len(bs) >= count
        return numpy.frombuffer(bs, dtype=dtype, count=count)

class _BigEndianDeserializer(Deserializer):
    def fetch_aligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        raise NotImplementedError("Pull requests are welcome")

    def fetch_unaligned_array_of_standard_bit_length_primitives(
        self, dtype: Type[StdPrimitive], count: int
    ) -> NDArray[StdPrimitive]:
        raise NotImplementedError("Pull requests are welcome")

_PlatformSpecificDeserializer = {
    "little": _LittleEndianDeserializer,
    "big": _BigEndianDeserializer,
}[sys.byteorder]

class ZeroExtendingBuffer:
    """
    This class implements the implicit zero extension logic as described in the Specification.
    A read beyond the end of the buffer returns zero bytes.
    """

    def __init__(self, fragmented_buffer: Sequence[memoryview]):
        # TODO: Concatenation is a tentative measure. Add proper support for fragmented buffers for speed.
        if len(fragmented_buffer) == 1:
            contiguous: bytearray | memoryview = fragmented_buffer[0]  # Fast path.
        else:
            contiguous = bytearray().join(fragmented_buffer)

        self._buf: NDArray[Byte] = numpy.frombuffer(contiguous, dtype=Byte)
        assert self._buf.dtype == Byte and self._buf.ndim == 1

    @property
    def bit_length(self) -> int:
        return len(self._buf) * 8

    def get_byte(self, index: int) -> int:
        """
        Like the standard ``x[i]`` except that i may not be negative and out of range access returns zero.
        """
        if index < 0:
            raise ValueError("Byte index may not be negative because the end of a zero-extended buffer is undefined.")
        try:
            return int(self._buf[index])
        except IndexError:
            return 0  # Implicit zero extension rule

    def get_unsigned_slice(self, left: int, right: int) -> NDArray[Byte]:
        """
        Like the standard ``x[left:right]`` except that neither index may be negative,
        left may not exceed right (otherwise it's a :class:`ValueError`),
        and the returned value is always of size ``right-left`` right-zero-padded if necessary.
        """
        if not (0 <= left <= right):
            raise ValueError(f"Invalid slice boundary specification: [{left}:{right}]")
        count = int(right - left)
        assert count >= 0
        out: NDArray[Byte] = self._buf[left:right]  # Slicing never raises an IndexError.
        if len(out) < count:  # Implicit zero extension rule
            out = numpy.concatenate((out, numpy.zeros(count - len(out), dtype=Byte)))
        assert len(out) == count
        return out

    def fork_bytes(self, offset_bytes: int, length_bytes: int) -> Sequence[memoryview]:
        """
        This is intended for use with :meth:`Deserializer.fork_bytes`.
        Given an offset from the beginning and length (both in bytes), yields a list of compliant memory fragments
        that can be fed into the forked deserializer instance.
        The requested (offset + length) shall not exceeded the buffer length; this is because per the Specification,
        a delimiter header cannot exceed the amount of remaining space in the deserialization buffer.
        """
        # Currently, we use a contiguous buffer, but when scattered buffers are supported, this method will need
        # to discard the fragments before the requested offset and then return the following subset of fragments.
        if offset_bytes + length_bytes > len(self._buf):
            raise ValueError(f"Invalid fork: offset ({offset_bytes}) + length ({length_bytes}) > {len(self._buf)}")
        out = memoryview(self._buf[offset_bytes : offset_bytes + length_bytes])  # type: ignore
        assert len(out) == length_bytes
        return [out]

    def to_base64(self) -> str:
        return base64.b64encode(self._buf.tobytes()).decode()

def _ensure_cardinal(i: int) -> None:
    if i < 0:
        raise ValueError(f"Cardinal may not be negative: {i}")

def test_deserializer_aligned() -> None:
    """This is a unit test, not for production use."""
    from pytest import raises, approx

    # The buffer is constructed from the corresponding serialization test.
    # The final bit padding is done with 1's to ensure that they are correctly discarded.
    sample = bytes(
        map(
            lambda x: int(x, 2),
            "10100111 11101111 11001101 10101011 10010000 01111000 01010110 00110100 00010010 10001000 10101001 "
            "11001011 11101101 11111110 11111111 00000000 01111111 00000000 00000000 00000000 00000000 00000000 "
            "00000000 11110000 00111111 00000000 00000000 10000000 00111111 00000000 01111100 11011010 00001110 "
            "11011010 10111110 11111110 00000001 10101101 11011110 11101111 10111110 11000101 01100111 11000101 "
            "11101011".split(),
        )
    )
    assert len(sample) == 45

    des = Deserializer.new([memoryview(sample)])
    assert des.remaining_bit_length == 45 * 8

    assert des.fetch_aligned_u8() == 0b1010_0111
    assert des.fetch_aligned_i64() == 0x1234_5678_90AB_CDEF
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

    assert all(des.fetch_aligned_array_of_standard_bit_length_primitives(numpy.uint16, 2) == [0xDEAD, 0xBEEF])

    assert all(
        des.fetch_aligned_array_of_bits(16)
        == [
            True,
            False,
            True,
            False,
            False,
            False,
            True,
            True,
            True,
            True,
            True,
            False,
            False,
            True,
            True,
            False,
        ]
    )

    assert all(
        des.fetch_aligned_array_of_bits(13)
        == [
            True,
            False,
            True,
            False,
            False,
            False,
            True,
            True,
            True,
            True,
            False,
            True,
            False,
        ]
    )

    print("repr(deserializer):", repr(des))

    des = Deserializer.new([memoryview(bytes([1, 2, 3]))])

    assert list(des.fetch_aligned_array_of_bits(0)) == []
    assert list(des.fetch_aligned_bytes(0)) == []
    assert des.remaining_bit_length == 3 * 8

    with raises(ValueError):
        des.fetch_aligned_array_of_bits(-1)

    with raises(ValueError):
        des.fetch_aligned_bytes(-1)

    des.skip_bits(3 * 8)
    assert des.remaining_bit_length == 0

    assert all([False] * 100 == des.fetch_aligned_array_of_bits(100))  # type: ignore
    assert des.remaining_bit_length == -100
    des.skip_bits(4)
    assert des.remaining_bit_length == -104
    assert b"\x00" * 10 == des.fetch_aligned_bytes(10).tobytes()
    assert des.remaining_bit_length == -184
    des.skip_bits(64)
    assert des.remaining_bit_length == -248
    assert 0 == des.fetch_aligned_unsigned(64)
    assert des.remaining_bit_length == -312

    print("repr(deserializer):", repr(des))

def test_deserializer_unaligned() -> None:
    """This is a unit test, not for production use."""
    from pytest import approx

    des = Deserializer.new([memoryview(bytearray([0b10101010, 0b01011101, 0b11001100, 0b10010001]))])
    assert des.consumed_bit_length == 0
    assert des.consumed_bit_length % 8 == 0
    assert list(des.fetch_aligned_array_of_bits(3)) == [False, True, False]
    assert des.consumed_bit_length == 3
    assert des.consumed_bit_length % 8 == 3
    assert list(des.fetch_unaligned_bytes(0)) == []
    assert list(des.fetch_unaligned_bytes(2)) == [0b10110101, 0b10001011]
    assert list(des.fetch_unaligned_bytes(1)) == [0b00111001]
    assert des.consumed_bit_length == 27
    assert des.consumed_bit_length % 8 == 3
    assert des.remaining_bit_length == 5
    assert all(numpy.array([0b00010010, 0], dtype=Byte) == des.fetch_unaligned_bytes(2))
    assert des.consumed_bit_length == 43
    assert des.remaining_bit_length == -11

    des = Deserializer.new([memoryview(bytearray([0b10101010, 0b01011101, 0b11001100, 0b10010001]))])
    assert list(des.fetch_unaligned_bytes(0)) == []
    assert list(des.fetch_unaligned_bytes(2)) == [
        0b10101010,
        0b01011101,
    ]  # Actually aligned
    assert list(des.fetch_unaligned_bytes(1)) == [0b11001100]
    assert des.remaining_bit_length == 8
    assert list(des.fetch_unaligned_bytes(2)) == [0b10010001, 0]
    assert des.remaining_bit_length == -8

    # The buffer is constructed from the corresponding serialization test.
    sample = bytearray(
        map(
            lambda x: int(x, 2),
            "11000101 00101111 01010111 10000010 11000110 11001010 00010010 00110100 01010110 11011001 10111111 "
            "11101100 00000110 00000000 00000000 00000000 00000000 00000000 10000000 11111111 00000001 00000000 "
            "00000000 11111100 00000001 11100000 01101111 11110101 01111110 11110111 00000101".split(),
        )
    )
    assert len(sample) == 31

    des = Deserializer.new([memoryview(sample[:])])
    assert des.remaining_bit_length == 31 * 8

    assert list(des.fetch_unaligned_array_of_bits(11)) == [
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
    ]
    assert list(des.fetch_unaligned_array_of_bits(10)) == [
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

    assert des.consumed_bit_length % 8 > 0  # not aligned
    assert des.fetch_unaligned_f64() == approx(1.0)
    assert des.fetch_unaligned_f32() == approx(1.0)
    assert des.fetch_unaligned_f16() == -numpy.inf

    assert list(des.fetch_unaligned_array_of_standard_bit_length_primitives(numpy.uint16, 2)) == [0xDEAD, 0xBEEF]
    des.skip_bits(5)
    assert des.consumed_bit_length % 8 == 0
    assert des.remaining_bit_length == 0

    print("repr(deserializer):", repr(des))

def test_deserializer_fork_bytes() -> None:
    """This is a unit test, not for production use."""
    import pytest

    m = Deserializer.new(
        [
            memoryview(
                bytes(
                    [
                        0b10100111,
                        0b11101111,
                        0b11001101,
                        0b10101011,
                        0b10010000,
                        0b01111000,
                        0b01010110,
                        0b00110100,
                    ]
                )
            )
        ]
    )
    with pytest.raises(ValueError):
        m.fork_bytes(9)

    f = m.fork_bytes(8)
    assert f.consumed_bit_length == 0
    assert f.remaining_bit_length == 8 * 8
    assert f.fetch_aligned_u8() == 0b10100111
    assert f.remaining_bit_length == 7 * 8
    assert f.fetch_aligned_u8() == 0b11101111
    assert f.remaining_bit_length == 6 * 8
    assert f.consumed_bit_length == 16

    assert m.remaining_bit_length == 8 * 8
    m.skip_bits(6 * 8)
    assert m.remaining_bit_length == 2 * 8
    assert m.fetch_aligned_u8() == 0b01010110
    assert m.fetch_aligned_u8() == 0b00110100
    assert m.remaining_bit_length == 0
    assert m.fetch_aligned_u8() == 0
    assert m.fetch_aligned_u16() == 0
    assert m.fetch_aligned_u32() == 0
    assert m.fetch_aligned_u64() == 0

    assert f.remaining_bit_length == 6 * 8
    ff = f.fork_bytes(2)
    assert ff.consumed_bit_length == 0
    assert ff.remaining_bit_length == 16
    assert ff.fetch_aligned_u8() == 0b11001101
    assert ff.fetch_aligned_u8() == 0b10101011
    assert ff.remaining_bit_length == 0
    assert ff.consumed_bit_length == 16
    assert ff.fetch_aligned_u8() == 0
    assert ff.fetch_aligned_u16() == 0
    assert ff.fetch_aligned_u32() == 0
    assert ff.fetch_aligned_u64() == 0

    f.skip_bits(40)
    assert f.consumed_bit_length == 56
    assert f.remaining_bit_length == 8
    assert f.fetch_aligned_u8() == 0b00110100
    assert f.remaining_bit_length == 0

# ================================================== USER CODE API ==================================================

def serialize(obj: Any) -> Iterable[memoryview]:
    """
    Constructs a serialized representation of the provided top-level object.
    The resulting serialized representation is padded to one byte in accordance with the Cyphal specification.
    The constructed serialized representation is returned as a sequence of byte-aligned fragments which must be
    concatenated in order to obtain the final representation.
    The objective of this model is to avoid copying data into a temporary buffer when possible.
    Each yielded fragment is of type :class:`memoryview` pointing to raw unsigned bytes.
    It is guaranteed that at least one fragment is always returned (which may be empty).
    """
    try:
        fun = obj._serialize_
    except AttributeError:
        raise TypeError(f"Cannot serialize object of type {type(obj)}") from None
    # TODO: update the Serializer class to emit an iterable of fragments.
    ser = Serializer.new(obj._EXTENT_BYTES_)
    fun(ser)
    yield ser.buffer.data

def deserialize(dtype: Type[T], fragmented_serialized_representation: Sequence[memoryview]) -> T | None:
    """
    Constructs an instance of the supplied DSDL-generated data type from its serialized representation.
    Returns None if the provided serialized representation is invalid.

    This function will never raise an exception for invalid input data; the only possible outcome of an invalid data
    being supplied is None at the output. A raised exception can only indicate an error in the deserialization logic.

    .. important:: The constructed object may contain arrays referencing the memory allocated for the serialized
        representation. Therefore, in order to avoid unintended data corruption, the caller should destroy all
        references to the serialized representation after the invocation.

    .. important:: The supplied fragments of the serialized representation should be writeable.
        If they are not, some of the array-typed fields of the constructed object may be read-only.
    """
    try:
        fun = dtype._deserialize_  # type: ignore
    except AttributeError:
        raise TypeError(f"Cannot deserialize using type {dtype}") from None
    deserializer = Deserializer.new(fragmented_serialized_representation)
    try:
        return cast(T, fun(deserializer))
    except Deserializer.FormatError:
        _logger.info(
            "Invalid serialized representation of %s: %s",
            get_model(dtype),
            deserializer,
            exc_info=True,
        )
        return None

def get_model(class_or_instance: Any) -> pydsdl.CompositeType:
    """
    Obtains a PyDSDL model of the supplied DSDL-generated class or its instance.
    This is the inverse of :func:`get_class`.
    """
    out = class_or_instance._MODEL_
    assert isinstance(out, pydsdl.CompositeType)
    return out

def get_class(model: pydsdl.CompositeType) -> type:
    """
    Returns a generated native class implementing the specified DSDL type represented by its PyDSDL model object.
    Promotes the model to delimited type automatically if necessary.
    This is the inverse of :func:`get_model`.

    :raises:
        - :class:`ImportError` if the generated package or subpackage cannot be found.

        - :class:`AttributeError` if the package is found but it does not contain the requested type.

        - :class:`TypeError` if the requested type is found, but its model does not match the input argument.
          This error may occur if the DSDL source has changed since the type was generated.
          To fix this, regenerate the package and make sure that all components of the application use identical
          or compatible DSDL source files.
    """

    def do_import(name_components: list[str]) -> Any:
        mod = None
        for comp in name_components:
            name = (mod.__name__ + "." + comp) if mod else comp  # type: ignore
            try:
                mod = importlib.import_module(name)
            except ImportError:  # We seem to have hit a reserved word; try with an underscore.
                mod = importlib.import_module(name + "_")
        return mod

    if model.has_parent_service:  # uavcan.node.GetInfo.Request --> uavcan.node.GetInfo then Request
        parent_name, child_name = model.name_components[-2:]
        mod = do_import(model.name_components[:-2])
        out = getattr(mod, f"{parent_name}_{model.version.major}_{model.version.minor}")
        out = getattr(out, child_name)
    else:
        mod = do_import(model.name_components[:-1])
        out = getattr(mod, f"{model.short_name}_{model.version.major}_{model.version.minor}")

    out_model = get_model(out)
    if out_model.inner_type != model.inner_type:
        raise TypeError(
            f"The class has been generated using an incompatible DSDL definition. "
            f"Requested model: {model} defined in {model.source_file_path}. "
            f"Model found in the class: {out_model} defined in {out_model.source_file_path}."
        )

    assert str(get_model(out)) == str(model)
    assert isinstance(out, type)
    return out

def get_extent_bytes(class_or_instance: Any) -> int:
    return int(class_or_instance._EXTENT_BYTES_)

def get_fixed_port_id(class_or_instance: Any) -> int | None:
    """
    Returns None if the supplied type has no fixed port-ID.
    """
    try:
        out = int(class_or_instance._FIXED_PORT_ID_)
    except (TypeError, AttributeError):
        return None
    else:
        assert 0 <= out < 2**16
        return out

def get_attribute(obj: Any, name: str) -> Any:
    """
    DSDL type attributes whose names can't be represented in Python (such as ``def`` or ``type``)
    are suffixed with an underscore.
    This function allows the caller to read arbitrary attributes referring to them by their original
    DSDL names, e.g., ``def`` instead of ``def_``.

    This function behaves like :func:`getattr` if the attribute does not exist.
    """
    try:
        return getattr(obj, name)
    except AttributeError:
        return getattr(obj, name + "_")

def set_attribute(obj: Any, name: str, value: Any) -> None:
    """
    DSDL type attributes whose names can't be represented in Python (such as ``def`` or ``type``)
    are suffixed with an underscore.
    This function allows the caller to assign arbitrary attributes referring to them by their original DSDL names,
    e.g., ``def`` instead of ``def_``.

    If the attribute does not exist, raises :class:`AttributeError`.
    """
    suffixed = name + "_"
    # We can't call setattr() without asking first because if it doesn't exist it will be created,
    # which would be disastrous.
    if hasattr(obj, name):
        setattr(obj, name, value)
    elif hasattr(obj, suffixed):
        setattr(obj, suffixed, value)
    else:
        raise AttributeError(name)

def is_serializable(dtype: Any) -> bool:
    """
    Whether the passed type is a DSDL-generated serializable type.
    """
    return (
        hasattr(dtype, "_MODEL_")
        and hasattr(dtype, "_EXTENT_BYTES_")
        and hasattr(dtype, "_serialize_")
        and hasattr(dtype, "_deserialize_")
    )

def is_message_type(dtype: Any) -> bool:
    """
    Whether the passed type is generated from a DSDL message type.
    """
    return is_serializable(dtype) and not hasattr(dtype, "Request") and not hasattr(dtype, "Response")

def is_service_type(dtype: Any) -> bool:
    """
    Whether the passed type is generated from a DSDL service type, excluding its nested Request and Response types.
    """
    return (
        hasattr(dtype, "_MODEL_")
        and is_serializable(getattr(dtype, "Request", None))
        and is_serializable(getattr(dtype, "Response", None))
    )

def to_builtin(obj: object) -> dict[str, Any]:
    """
    Accepts a DSDL object (an instance of a Python class auto-generated from a DSDL definition),
    returns its value represented using only native built-in types: dict, list, bool, int, float, str.
    Ordering of dict elements is guaranteed to match the field ordering of the source definition.
    Keys of dicts representing DSDL objects use the original unstropped names from the source DSDL definition;
    e.g., ``if``, not ``if_``.

    This is intended for use with JSON, YAML, and other serialization formats.

    >>> import json
    >>> import uavcan.primitive.array
    >>> json.dumps(to_builtin(uavcan.primitive.array.Integer32_1_0([-123, 456, 0])))
    '{"value": [-123, 456, 0]}'
    >>> import uavcan.register
    >>> request = uavcan.register.Access_1_0.Request(
    ...     uavcan.register.Name_1_0('my.register'),
    ...     uavcan.register.Value_1_0(integer16=uavcan.primitive.array.Integer16_1_0([1, 2, +42, -10_000]))
    ... )
    >>> to_builtin(request)  # doctest: +NORMALIZE_WHITESPACE
    {'name':  {'name': 'my.register'},
     'value': {'integer16': {'value': [1, 2, 42, -10000]}}}
    """
    model = get_model(obj)
    if isinstance(model, pydsdl.ServiceType):  # pragma: no cover
        raise TypeError(
            f"Built-in form is not defined for service types. "
            f"Did you mean to use Request or Response? Input type: {model}"
        )
    out = _to_builtin_impl(obj, model)
    assert isinstance(out, dict)
    return out

def _to_builtin_impl(
    obj: object | NDArray[Any] | str | bool | int | float, model: pydsdl.SerializableType
) -> dict[str, Any] | list[Any] | str | bool | int | float:
    if isinstance(model, pydsdl.CompositeType):
        return {
            f.name: _to_builtin_impl(get_attribute(obj, f.name), f.data_type)
            for f in model.fields_except_padding
            if get_attribute(obj, f.name) is not None  # The check is to hide inactive union variants.
        }

    if isinstance(model, pydsdl.ArrayType):
        assert isinstance(obj, numpy.ndarray)
        # TODO: drop this special case when strings are natively supported in DSDL.
        printable = set(map(ord, string.printable))
        if model.string_like and all(map(lambda x: x in printable, obj.tobytes())):
            try:
                return bytes(e for e in obj).decode()
            except UnicodeError:
                return list(map(int, obj))
        return [_to_builtin_impl(e, model.element_type) for e in obj]

    if isinstance(model, pydsdl.PrimitiveType):
        # The explicit conversions are needed to get rid of NumPy scalar types.
        if isinstance(model, pydsdl.IntegerType):
            return int(obj)  # type: ignore
        if isinstance(model, pydsdl.FloatType):
            return float(obj)  # type: ignore
        if isinstance(model, pydsdl.BooleanType):
            return bool(obj)
        assert isinstance(obj, str)
        return obj

    assert False, "Unexpected inputs"

def update_from_builtin(destination: T, source: Any) -> T:
    """
    Updates the provided DSDL object (an instance of a Python class auto-generated from a DSDL definition)
    with the values from a native representation, where DSDL objects are represented as dicts, arrays
    are lists, and primitives are represented as int/float/bool. This is the reverse of :func:`to_builtin`.
    Values that are not specified in the source are not updated (left at their original values),
    so an empty source will leave the input object unchanged.

    Source field names shall match the original unstropped names provided in the DSDL definition;
    e.g., `if`, not `if_`. If there is more than one variant specified for a union type, the last
    specified variant takes precedence.
    If the structure of the source does not match the destination object, the correct representation
    may be deduced automatically as long as it can be done unambiguously.

    :param destination: The object to update. The update will be done in-place. If you don't want the source
        object modified, clone it beforehand.

    :param source: The :class:`dict` instance containing the values to update the destination object with.

    :return: A reference to destination (not a copy).

    :raises: :class:`ValueError` if the provided source values cannot be applied to the destination object,
        or if the source contains fields that are not present in the destination object.
        :class:`TypeError` if an entity of the source cannot be converted into the type expected by the destination.

    >>> import uavcan.primitive.array
    >>> import uavcan.register
    >>> request = uavcan.register.Access_1_0.Request(
    ...     uavcan.register.Name_1_0('my.register'),
    ...     uavcan.register.Value_1_0(string=uavcan.primitive.String_1_0('Hello world!'))
    ... )
    >>> request
    uavcan.register.Access.Request...name='my.register'...value='Hello world!'...
    >>> update_from_builtin(request, {  # Switch the Value union from string to int16; keep the name unchanged.
    ...     'value': {
    ...         'integer16': {
    ...             'value': [1, 2, 42, -10000]
    ...         }
    ...     }
    ... })  # doctest: +NORMALIZE_WHITESPACE
    uavcan.register.Access.Request...name='my.register'...value=[ 1, 2, 42,-10000]...

    The following examples showcase positional initialization:

    >>> from uavcan.node import Heartbeat_1
    >>> update_from_builtin(Heartbeat_1(), [123456, 1, 2])  # doctest: +NORMALIZE_WHITESPACE
    uavcan.node.Heartbeat.1.0(uptime=123456,
                              health=uavcan.node.Health.1.0(value=1),
                              mode=uavcan.node.Mode.1.0(value=2),
                              vendor_specific_status_code=0)
    >>> update_from_builtin(Heartbeat_1(), 123456)  # doctest: +NORMALIZE_WHITESPACE
    uavcan.node.Heartbeat.1.0(uptime=123456,
                              health=uavcan.node.Health.1.0(value=0),
                              mode=uavcan.node.Mode.1.0(value=0),
                              vendor_specific_status_code=0)
    >>> update_from_builtin(Heartbeat_1(), [0, 0, 0, 0, 0])  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError: ...

    >>> update_from_builtin(uavcan.primitive.array.Real64_1(), 123.456) # doctest: +NORMALIZE_WHITESPACE
    uavcan.primitive.array.Real64.1.0(value=[123.456])
    >>> update_from_builtin(uavcan.primitive.array.Real64_1(), [123.456]) # doctest: +NORMALIZE_WHITESPACE
    uavcan.primitive.array.Real64.1.0(value=[123.456])
    >>> update_from_builtin(uavcan.primitive.array.Real64_1(), [123.456, -9]) # doctest: +NORMALIZE_WHITESPACE
    uavcan.primitive.array.Real64.1.0(value=[123.456, -9. ])

    >>> update_from_builtin(uavcan.register.Access_1_0.Request(), ["X", {"integer8": 99}])  # Same as the next one!
    uavcan.register.Access.Request...name='X'...value=[99]...
    >>> update_from_builtin(uavcan.register.Access_1_0.Request(), {'name': 'X', 'value': {'integer8': {'value': [99]}}})
    uavcan.register.Access.Request...name='X'...value=[99]...
    """
    _logger.debug("update_from_builtin: destination/source on the next lines:\n%r\n%r", destination, source)
    model = get_model(destination)
    if isinstance(model, pydsdl.ServiceType):  # pragma: no cover
        raise TypeError(
            f"Built-in form is not defined for service types. "
            f"Did you mean to use Request or Response? Input type: {model}"
        )
    fields = model.fields_except_padding

    # UX improvement: https://github.com/OpenCyphal/pycyphal/issues/147 -- match the source against the data type.
    if not isinstance(source, dict):
        if not isinstance(source, (list, tuple)):  # Assume positional initialization.
            source = (source,)
        can_propagate = fields and isinstance(fields[0].data_type, (pydsdl.ArrayType, pydsdl.CompositeType))
        too_many_values = len(source) > (1 if isinstance(model.inner_type, pydsdl.UnionType) else len(fields))
        if can_propagate and too_many_values:
            _logger.debug(
                "update_from_builtin: %d source values cannot be applied to %s but the first field accepts "
                "positional initialization -- propagating down",
                len(source),
                type(destination).__name__,
            )
            source = [source]
        if len(source) > len(fields):
            raise TypeError(
                f"Cannot apply {len(source)} values to {len(fields)} fields in {type(destination).__name__}"
            )
        source = {f.name: v for f, v in zip(fields, source)}
        return update_from_builtin(destination, source)

    source = dict(source)  # Create copy to prevent mutation of the original

    for f in fields:
        field_type = f.data_type
        try:
            value = source.pop(f.name)
        except LookupError:
            continue  # No value specified, keep original value

        if isinstance(field_type, pydsdl.CompositeType):
            field_obj = get_attribute(destination, f.name)
            if field_obj is None:  # Oh, this is a union
                field_obj = get_class(field_type)()  # The variant was not selected, construct a default
                set_attribute(destination, f.name, field_obj)  # Switch the union to the new variant
            update_from_builtin(field_obj, value)

        elif isinstance(field_type, pydsdl.ArrayType):
            element_type = field_type.element_type
            if isinstance(element_type, pydsdl.PrimitiveType):
                set_attribute(destination, f.name, value)
            elif isinstance(element_type, pydsdl.CompositeType):
                dtype = get_class(element_type)
                set_attribute(destination, f.name, [update_from_builtin(dtype(), s) for s in value])
            else:
                assert False, f"Unexpected array element type: {element_type!r}"

        elif isinstance(field_type, pydsdl.PrimitiveType):
            set_attribute(destination, f.name, value)

        else:
            assert False, f"Unexpected field type: {field_type!r}"

    if source:
        raise ValueError(f"No such fields in {model}: {list(source.keys())}")

    return destination
