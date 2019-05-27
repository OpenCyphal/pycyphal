#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing


def refragment(input_fragments: typing.Iterable[memoryview], output_fragment_size: int) -> typing.Iterable[memoryview]:
    """
    Repackages the data from the arbitrarily-sized input fragments into fixed-size output fragments while minimizing
    the amount of data copying. The last fragment is allowed to be smaller than the requested size.
    If the input iterable contains no fragments or all of them are empty, nothing will be yielded.
    """
    if output_fragment_size < 1:
        raise ValueError(f'Invalid output fragment size: {output_fragment_size}')

    carry: typing.Union[bytearray, memoryview] = memoryview(b'')
    for frag in input_fragments:
        # First, emit the leftover carry from the previous iteration(s), and update the fragment.
        # After this operation either the carry or the fragment (or both) will be empty.
        if carry:
            offset = output_fragment_size - len(carry)
            assert len(carry) < output_fragment_size and offset < output_fragment_size
            if isinstance(carry, bytearray):
                carry += frag[:offset]                                                  # Expensive copy!
            else:
                carry = bytearray().join((carry, frag[:offset]))                        # Expensive copy!

            frag = frag[offset:]
            if len(carry) >= output_fragment_size:
                assert len(carry) == output_fragment_size
                yield memoryview(carry)
                carry = memoryview(b'')

        assert not carry or not frag

        # Process the remaining data in the current fragment excepting the last incomplete section.
        for offset in range(0, len(frag), output_fragment_size):
            assert not carry
            chunk = frag[offset:offset + output_fragment_size]
            if len(chunk) < output_fragment_size:
                carry = chunk
            else:
                assert len(chunk) == output_fragment_size
                yield chunk

    if carry:
        assert len(carry) < output_fragment_size
        yield memoryview(carry)


def _unittest_util_refragment_manual() -> None:
    from pytest import raises

    with raises(ValueError):
        _ = list(refragment([memoryview(b'')], 0))

    assert b'' == _to_bytes(refragment([], 1000))
    assert b'' == _to_bytes(refragment([memoryview(b'')], 1000))

    assert b'012345' == _to_bytes(refragment([memoryview(b'012345')], 1000))

    assert b'0123456789' == _to_bytes(refragment([memoryview(b'012345'), memoryview(b'6789')], 1000))
    assert b'0123456789' == _to_bytes(refragment([memoryview(b'012345'), memoryview(b'6789')], 6))
    assert b'0123456789' == _to_bytes(refragment([memoryview(b'012345'), memoryview(b'6789')], 3))
    assert b'0123456789' == _to_bytes(refragment([memoryview(b'012345'), memoryview(b'6789'), memoryview(b'')], 1))

    tiny = [
        memoryview(b'0'),
        memoryview(b'1'),
        memoryview(b'2'),
        memoryview(b'3'),
        memoryview(b'4'),
        memoryview(b'5'),
    ]
    assert b'012345' == _to_bytes(refragment(tiny, 1000))
    assert b'012345' == _to_bytes(refragment(tiny, 1))


def _unittest_util_refragment_automatic() -> None:
    import math
    import random

    def once(input_fragments: typing.Iterable[memoryview], output_fragment_size: int) -> None:
        input_fragments = list(input_fragments)
        reference = _to_bytes(input_fragments)
        expected_frags = math.ceil(len(reference) / output_fragment_size)
        out = list(refragment(input_fragments, output_fragment_size))
        assert len(out) == expected_frags
        assert _to_bytes(out) == reference
        if expected_frags > 0:
            sizes = list(map(len, out))
            assert all([x == output_fragment_size for x in sizes[:-1]])
            assert 0 < sizes[-1] <= output_fragment_size

    def once_all(input_fragments: typing.Iterable[memoryview]) -> None:
        input_fragments = list(input_fragments)
        longest = max(map(len, input_fragments)) if len(input_fragments) > 0 else 1
        for size in range(1, longest + 2):
            once(input_fragments, size)

    once_all([])
    once_all([memoryview(b'012345'), memoryview(b'6789')])

    num_iterations = 100
    max_fragments = 100
    max_fragment_size = 100

    def make_random_fragment() -> memoryview:
        size = random.randint(0, max_fragment_size)
        return memoryview(bytes(random.getrandbits(8) for _ in range(size)))

    for _ in range(num_iterations):
        num_fragments = random.randint(0, max_fragments)
        frags = [make_random_fragment() for _ in range(num_fragments)]
        once_all(frags)


def _to_bytes(fragments: typing.Iterable[memoryview]) -> bytes:
    return bytes().join(fragments)


def _unittest_util_refragment_to_bytes() -> None:
    assert _to_bytes([]) == b''
    assert _to_bytes([memoryview(b'')]) == b''
    assert _to_bytes([memoryview(b'')] * 3) == b''
    assert _to_bytes([memoryview(b''), memoryview(b'123'), memoryview(b'')]) == b'123'
    assert _to_bytes([memoryview(b'123')]) == b'123'
    assert _to_bytes([memoryview(b'123'), memoryview(b'456')]) == b'123456'
    assert _to_bytes([memoryview(b'123'), memoryview(b''), memoryview(b'456')]) == b'123456'
