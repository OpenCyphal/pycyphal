# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import sys
from typing import TypeVar, Type, Sequence, Union, cast
import struct
import base64

import numpy
from numpy.typing import NDArray
from ._common import StdPrimitive, Byte

# noinspection PyShadowingBuiltins
_T = TypeVar("_T")


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
        When thrown from a deserialization method, it is intercepted by :func:`pycyphal.dsdl.deserialize`
        which then returns None instead of a valid instance, indicating that the serialized representation is invalid.
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

    def fetch_aligned_u16(self) -> int:  # TODO: here and below, consider using int.from_bytes()?
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
            self._buf.get_unsigned_slice(bo, bo + count * numpy.dtype(dtype).itemsize), dtype=dtype
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
            contiguous: Union[bytearray, memoryview] = fragmented_buffer[0]  # Fast path.
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


def _unittest_deserializer_aligned() -> None:
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


def _unittest_deserializer_unaligned() -> None:
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
    assert list(des.fetch_unaligned_bytes(2)) == [0b10101010, 0b01011101]  # Actually aligned
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


def _unittest_deserializer_fork_bytes() -> None:
    import pytest

    m = Deserializer.new(
        [
            memoryview(
                bytes([0b10100111, 0b11101111, 0b11001101, 0b10101011, 0b10010000, 0b01111000, 0b01010110, 0b00110100])
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
