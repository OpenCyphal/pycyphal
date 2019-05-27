#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.util
from . import _frame, media as _media


def serialize_transfer(can_identifier:        int,
                       transfer_id:           int,
                       fragmented_payload:    typing.Sequence[memoryview],
                       max_data_field_length: int,
                       loopback:              bool) -> typing.Iterable[_media.DataFrame]:
    frags = pyuavcan.util.refragment(fragmented_payload, max_data_field_length - 1)
    # TODO: CRC computation - append padding and then CRC to the fragmented payload?
    for index, (last, chunk) in enumerate(pyuavcan.util.mark_last(frags)):
        padded_payload: memoryview = _frame.UAVCANFrame.pad_payload(chunk) if last else chunk
        ufr = _frame.UAVCANFrame(identifier=can_identifier,
                                 padded_payload=padded_payload,
                                 transfer_id=transfer_id,
                                 start_of_transfer=index == 0,
                                 end_of_transfer=last,
                                 toggle_bit=index % 2 != 0,
                                 loopback=loopback)
        yield ufr.compile()
