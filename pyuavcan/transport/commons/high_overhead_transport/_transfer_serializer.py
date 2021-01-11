# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import itertools
import pyuavcan
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
    >>> from pyuavcan.transport.commons.high_overhead_transport import Frame
    >>> @dataclasses.dataclass(frozen=True)
    ... class MyFrameType(Frame):
    ...     pass    # Transport-specific definition goes here.
    >>> priority = pyuavcan.transport.Priority.NOMINAL
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
    """
    assert max_frame_payload_bytes > 0
    payload_length = sum(map(len, fragmented_payload))
    if payload_length <= max_frame_payload_bytes:  # SINGLE-FRAME TRANSFER
        payload = fragmented_payload[0] if len(fragmented_payload) == 1 else memoryview(b"".join(fragmented_payload))
        assert len(payload) == payload_length
        assert max_frame_payload_bytes >= len(payload)
        yield frame_factory(0, True, payload)
    else:  # MULTI-FRAME TRANSFER
        crc_bytes = TransferCRC.new(*fragmented_payload).value_as_bytes
        refragmented = pyuavcan.transport.commons.refragment(
            itertools.chain(fragmented_payload, (memoryview(crc_bytes),)), max_frame_payload_bytes
        )
        for frame_index, (end_of_transfer, frag) in enumerate(pyuavcan.util.mark_last(refragmented)):
            yield frame_factory(frame_index, end_of_transfer, frag)


def _unittest_serialize_transfer() -> None:
    from pyuavcan.transport import Priority

    priority = Priority.NOMINAL
    transfer_id = 12345678901234567890

    def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> Frame:
        return Frame(
            priority=priority, transfer_id=transfer_id, index=index, end_of_transfer=end_of_transfer, payload=payload
        )

    assert [
        construct_frame(0, True, memoryview(b"hello world")),
    ] == list(serialize_transfer([memoryview(b"hello"), memoryview(b" "), memoryview(b"world")], 100, construct_frame))

    assert [
        construct_frame(0, True, memoryview(b"")),
    ] == list(serialize_transfer([], 100, construct_frame))

    hello_world_crc = pyuavcan.transport.commons.crc.CRC32C()
    hello_world_crc.add(b"hello world")

    assert [
        construct_frame(0, False, memoryview(b"hello")),
        construct_frame(1, False, memoryview(b" worl")),
        construct_frame(2, True, memoryview(b"d" + hello_world_crc.value_as_bytes)),
    ] == list(serialize_transfer([memoryview(b"hello"), memoryview(b" "), memoryview(b"world")], 5, construct_frame))
