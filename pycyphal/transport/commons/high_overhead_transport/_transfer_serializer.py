# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import itertools
import pycyphal
from ._frame import Frame
from ._common import TransferCRC


FrameType = typing.TypeVar("FrameType", bound=Frame)


def serialize_transfer(
    fragmented_payload: typing.Sequence[memoryview],
    max_frame_payload_bytes: int,
    frame_factory: typing.Callable[[int, bool, memoryview], FrameType],
) -> typing.Iterable[FrameType]:
    r"""
    Constructs an ordered sequence of frames ready for transmission from the provided data fragments.
    Compatible with any high-overhead transport.

    :param fragmented_payload: The transfer payload we're going to be sending.

    :param max_frame_payload_bytes: Max payload per transport-layer frame.

    :param frame_factory: A callable that accepts (frame index, end of transfer, payload) and returns a frame.
        Normally this would be a closure.

    :return: An iterable that yields frames.

    >>> import dataclasses
    >>> from pycyphal.transport.commons.high_overhead_transport import Frame
    >>> @dataclasses.dataclass(frozen=True)
    ... class MyFrameType(Frame):
    ...     pass    # Transport-specific definition goes here.
    >>> priority = pycyphal.transport.Priority.NOMINAL
    >>> transfer_id = 12345
    >>> def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> MyFrameType:
    ...     return MyFrameType(priority=priority,
    ...                        transfer_id=transfer_id,
    ...                        index=index,
    ...                        end_of_transfer=end_of_transfer,
    ...                        payload=payload)
    >>> frames = list(serialize_transfer(
    ...     fragmented_payload=[
    ...         memoryview(b'He thought about the Horse: '),         # The CRC of this quote is 0xDDD1FF3A
    ...         memoryview(b'how was she doing there, in the fog?'),
    ...     ],
    ...     max_frame_payload_bytes=53,
    ...     frame_factory=construct_frame,
    ... ))
    >>> frames
    [MyFrameType(..., index=0, end_of_transfer=False, ...), MyFrameType(..., index=1, end_of_transfer=True, ...)]
    >>> bytes(frames[0].payload)    # 53 bytes long, as configured.
    b'He thought about the Horse: how was she doing there, '
    >>> bytes(frames[1].payload)    # The stuff at the end is the four bytes of multi-frame transfer CRC.
    b'in the fog?:\xff\xd1\xdd'

    >>> single_frame = list(serialize_transfer(
    ...     fragmented_payload=[
    ...         memoryview(b'FOUR'),
    ...         ],
    ...         max_frame_payload_bytes=8,
    ...         frame_factory=construct_frame,
    ... ))
    >>> single_frame
    [MyFrameType(..., index=0, end_of_transfer=True, ...)]
    >>> bytes(single_frame[0].payload)    # 8 bytes long, as configured.
    b'FOUR-\xb8\xa4\x81'
    """
    assert max_frame_payload_bytes > 0
    payload_length = sum(map(len, fragmented_payload))
    # SINGLE-FRAME TRANSFER
    if payload_length <= max_frame_payload_bytes - 4:  # 4 bytes for crc!
        crc_bytes = TransferCRC.new(*fragmented_payload).value_as_bytes
        payload_with_crc = memoryview(b"".join(list(fragmented_payload) + [memoryview(crc_bytes)]))
        assert len(payload_with_crc) == payload_length + 4
        assert max_frame_payload_bytes >= len(payload_with_crc)
        yield frame_factory(0, True, payload_with_crc)
    # MULTI-FRAME TRANSFER
    else:
        crc_bytes = TransferCRC.new(*fragmented_payload).value_as_bytes
        refragmented = pycyphal.transport.commons.refragment(
            itertools.chain(fragmented_payload, (memoryview(crc_bytes),)), max_frame_payload_bytes
        )
        for frame_index, (end_of_transfer, frag) in enumerate(pycyphal.util.mark_last(refragmented)):
            yield frame_factory(frame_index, end_of_transfer, frag)


def _unittest_serialize_transfer() -> None:
    from pycyphal.transport import Priority

    priority = Priority.NOMINAL
    transfer_id = 12345678901234567890

    def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> Frame:
        return Frame(
            priority=priority, transfer_id=transfer_id, index=index, end_of_transfer=end_of_transfer, payload=payload
        )

    hello_world_crc = pycyphal.transport.commons.crc.CRC32C()
    hello_world_crc.add(b"hello world")

    empty_crc = pycyphal.transport.commons.crc.CRC32C()
    empty_crc.add(b"")

    assert [
        construct_frame(0, True, memoryview(b"hello world" + hello_world_crc.value_as_bytes)),
    ] == list(serialize_transfer([memoryview(b"hello"), memoryview(b" "), memoryview(b"world")], 100, construct_frame))

    assert [
        construct_frame(0, True, memoryview(b"" + empty_crc.value_as_bytes)),
    ] == list(serialize_transfer([], 100, construct_frame))

    assert [
        construct_frame(0, False, memoryview(b"hello")),
        construct_frame(1, False, memoryview(b" worl")),
        construct_frame(2, True, memoryview(b"d" + hello_world_crc.value_as_bytes)),
    ] == list(serialize_transfer([memoryview(b"hello"), memoryview(b" "), memoryview(b"world")], 5, construct_frame))
