#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
from . import _frame, media as _media


def serialize_transfer(can_identifier:        int,
                       transfer_id:           int,
                       fragmented_payload:    typing.Sequence[memoryview],
                       max_data_field_length: int,
                       loopback:              bool) -> typing.Iterable[_media.Frame]:
    chunks_iter = _rechunk(fragmented_payload, max_data_field_length - 1)
    # TODO: CRC computation - append padding and then CRC to the fragmented payload?
    for index, (last, chunk) in enumerate(_mark_last(chunks_iter)):
        padded_payload: memoryview = _frame.UAVCANFrame.pad_payload(chunk) if last else chunk
        ufr = _frame.UAVCANFrame(identifier=can_identifier,
                                 padded_payload=padded_payload,
                                 transfer_id=transfer_id,
                                 start_of_transfer=index == 0,
                                 end_of_transfer=last,
                                 toggle_bit=index % 2 != 0,
                                 loopback=loopback)
        yield ufr.compile()


def _rechunk(fragmented_payload: typing.Sequence[memoryview], chunk_size: int) -> typing.Iterable[memoryview]:
    """
    Repackages the fragmented payload into fixed-size chunks. The last chunk is allowed to be smaller.
    It is GUARANTEED that at least one item will be returned, which may be empty.
    """
    if len(fragmented_payload) == 1:
        buf = fragmented_payload[0]    # Small payload optimization
    else:
        # TODO: contiguous copy is not really necessary, optimizations possible
        buf = memoryview(bytearray().join(fragmented_payload))

    if len(buf) > chunk_size:
        for i in range(0, len(buf), chunk_size):
            chunk = buf[i:i + chunk_size]
            yield chunk
    else:
        yield buf


_ML = typing.TypeVar('_ML')


def _mark_last(it: typing.Iterable[_ML]) -> typing.Iterable[typing.Tuple[bool, _ML]]:
    it = iter(it)
    last = next(it)
    for val in it:
        yield False, last
        last = val
    yield True, last
