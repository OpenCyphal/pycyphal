# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing


def refragment(input_fragments: typing.Iterable[memoryview], output_fragment_size: int) -> typing.Iterable[memoryview]:
    """
    Repackages the data from the arbitrarily-sized input fragments into fixed-size output fragments while minimizing
    the amount of data copying. The last fragment is allowed to be smaller than the requested size.
    If the input iterable contains no fragments or all of them are empty, nothing will be yielded.

    This function is designed for use in transfer emission logic where it's often needed to split a large
    payload into several frames while avoiding unnecessary copying. The best case scenario is when the size
    of input blocks is a multiple of the output fragment size -- in this case no copy will be done.

    >>> list(map(bytes, refragment([memoryview(b'0123456789'), memoryview(b'abcdef')], 7)))
    [b'0123456', b'789abcd', b'ef']

    The above example shows a marginally suboptimal case where one copy is required:

    - ``b'0123456789'[0:7]``  --> output    ``b'0123456'``       (slicing, no copy)
    - ``b'0123456789'[7:10]`` --> temporary ``b'789'``           (slicing, no copy)
    - ``b'abcdef'[0:4]``      --> output    ``b'789' + b'abcd'`` (copied into the temporary, which is then yielded)
    - ``b'abcdef'[4:6]``      --> output    ``b'ef'``            (slicing, no copy)
    """
    if output_fragment_size < 1:
        raise ValueError(f"Invalid output fragment size: {output_fragment_size}")

    carry: typing.Union[bytearray, memoryview] = memoryview(b"")
    for frag in input_fragments:
        # First, emit the leftover carry from the previous iteration(s), and update the fragment.
        # After this operation either the carry or the fragment (or both) will be empty.
        if carry:
            offset = output_fragment_size - len(carry)
            assert len(carry) < output_fragment_size and offset < output_fragment_size
            if isinstance(carry, bytearray):
                carry += frag[:offset]  # Expensive copy!
            else:
                carry = bytearray().join((carry, frag[:offset]))  # Expensive copy!

            frag = frag[offset:]
            if len(carry) >= output_fragment_size:
                assert len(carry) == output_fragment_size
                yield memoryview(carry)
                carry = memoryview(b"")

        assert not carry or not frag

        # Process the remaining data in the current fragment excepting the last incomplete section.
        for offset in range(0, len(frag), output_fragment_size):
            assert not carry
            chunk = frag[offset : offset + output_fragment_size]
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
        _ = list(refragment([memoryview(b"")], 0))

    assert [] == list(refragment([], 1000))
    assert [] == list(refragment([memoryview(b"")], 1000))

    def lby(it: typing.Iterable[memoryview]) -> typing.List[bytes]:
        return list(map(bytes, it))

    assert [b"012345"] == lby(refragment([memoryview(b"012345")], 1000))

    assert [b"0123456789"] == lby(refragment([memoryview(b"012345"), memoryview(b"6789")], 1000))
    assert [b"012345", b"6789"] == lby(refragment([memoryview(b"012345"), memoryview(b"6789")], 6))
    assert [b"012", b"345", b"678", b"9"] == lby(refragment([memoryview(b"012345"), memoryview(b"6789")], 3))
    assert [b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9"] == lby(
        refragment([memoryview(b"012345"), memoryview(b"6789"), memoryview(b"")], 1)
    )

    tiny = [
        memoryview(b"0"),
        memoryview(b"1"),
        memoryview(b"2"),
        memoryview(b"3"),
        memoryview(b"4"),
        memoryview(b"5"),
    ]
    assert [b"012345"] == lby(refragment(tiny, 1000))
    assert [b"0", b"1", b"2", b"3", b"4", b"5"] == lby(refragment(tiny, 1))


def _unittest_slow_util_refragment_automatic() -> None:
    import math
    import random

    def once(input_fragments: typing.List[memoryview], output_fragment_size: int) -> None:
        reference = _to_bytes(input_fragments)
        expected_frags = math.ceil(len(reference) / output_fragment_size)
        out = list(refragment(input_fragments, output_fragment_size))
        assert all(map(lambda x: isinstance(x, memoryview), out))
        assert len(out) == expected_frags
        assert _to_bytes(out) == reference
        if expected_frags > 0:
            sizes = list(map(len, out))
            assert all([x == output_fragment_size for x in sizes[:-1]])
            assert 0 < sizes[-1] <= output_fragment_size

    def once_all(input_fragments: typing.List[memoryview]) -> None:
        longest = max(map(len, input_fragments)) if len(input_fragments) > 0 else 1
        for size in range(1, longest + 2):
            once(input_fragments, size)

        # Manual check for the edge case where all fragments are assembled into one chunk
        total_size = sum(map(len, input_fragments))
        if total_size > 0:
            out_list = list(refragment(input_fragments, total_size))
            assert len(out_list) in (0, 1)
            out = out_list[0] if out_list else b""
            assert out == _to_bytes(input_fragments)

    once_all([])
    once_all([memoryview(b"012345"), memoryview(b"6789")])

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
    assert _to_bytes([]) == b""
    assert _to_bytes([memoryview(b"")]) == b""
    assert _to_bytes([memoryview(b"")] * 3) == b""
    assert _to_bytes([memoryview(b""), memoryview(b"123"), memoryview(b"")]) == b"123"
    assert _to_bytes([memoryview(b"123")]) == b"123"
    assert _to_bytes([memoryview(b"123"), memoryview(b"456")]) == b"123456"
    assert _to_bytes([memoryview(b"123"), memoryview(b""), memoryview(b"456")]) == b"123456"
