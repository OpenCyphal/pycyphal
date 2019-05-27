#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import itertools
import pyuavcan.util
from . import _frame, media as _media


_PADDING_PATTERN = b'\x55'
_CRC_LENGTH = 2


def serialize_transfer(can_identifier:        int,
                       transfer_id:           int,
                       fragmented_payload:    typing.Sequence[memoryview],
                       max_data_field_length: int,
                       loopback:              bool) -> typing.Iterable[_media.DataFrame]:
    max_single_frame_payload_length = max_data_field_length - 1
    payload_length = sum(map(len, fragmented_payload))

    if payload_length <= max_single_frame_payload_length:
        # SINGLE-FRAME TRANSFER
        payload, = tuple(pyuavcan.util.refragment(fragmented_payload, max_single_frame_payload_length))
        assert len(payload) <= max_single_frame_payload_length and len(payload) == payload_length

        padding_length = _media.DataFrame.get_required_padding(len(payload) + 1)            # +1 for the tail byte
        if padding_length > 0:
            payload = memoryview(b''.join((payload, _PADDING_PATTERN * padding_length)))    # Payload copied here!

        yield _frame.UAVCANFrame(identifier=can_identifier,
                                 padded_payload=payload,
                                 transfer_id=transfer_id,
                                 start_of_transfer=True,
                                 end_of_transfer=True,
                                 toggle_bit=True,
                                 loopback=loopback)
    else:
        # MULTI-FRAME TRANSFER
        last_frame_payload_length = payload_length % max_single_frame_payload_length
        if last_frame_payload_length + _CRC_LENGTH >= max_single_frame_payload_length:
            padding = b''
        else:
            last_frame_data_length = last_frame_payload_length + _CRC_LENGTH + 1
            assert last_frame_data_length <= max_data_field_length
            padding = _PADDING_PATTERN * _media.DataFrame.get_required_padding(last_frame_data_length)

        crc = pyuavcan.util.hash.CRC16CCITT()
        for frag in fragmented_payload:
            crc.add(frag)
        crc.add(padding)

        trailing_bytes = padding + bytes([crc.value >> 8, crc.value & 0xFF])

        fragments = pyuavcan.util.refragment(itertools.chain(fragmented_payload, (memoryview(trailing_bytes),)),
                                             max_single_frame_payload_length)

        for index, (last, chunk) in enumerate(pyuavcan.util.mark_last(fragments)):
            ufr = _frame.UAVCANFrame(identifier=can_identifier,
                                     padded_payload=chunk,
                                     transfer_id=transfer_id,
                                     start_of_transfer=index == 0,
                                     end_of_transfer=last,
                                     toggle_bit=index % 2 != 0,
                                     loopback=loopback)
            yield ufr.compile()
