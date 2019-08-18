#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import pyuavcan


class HighOverheadTransferReassembler:
    TRANSFER_CRC_LENGTH_BYTES = 4

    class ErrorID(enum.Enum):
        MISSED_START_OF_TRANSFER = enum.auto()
        MISSING_FRAMES           = enum.auto()
        UNEXPECTED_TRANSFER_ID   = enum.auto()
        TRANSFER_CRC_MISMATCH    = enum.auto()
        PAYLOAD_TOO_LARGE        = enum.auto()

    def __init__(self,
                 source_node_id:         int,
                 max_payload_size_bytes: int):
        self._source_node_id = int(source_node_id)

        max_payload_size_bytes = int(max_payload_size_bytes)
        assert max_payload_size_bytes >= 0
        self._max_payload_size_bytes_with_crc = max_payload_size_bytes + self.TRANSFER_CRC_LENGTH_BYTES

        self._sparse_payload_fragments: typing.List[typing.Optional[memoryview]] = []
        self._timestamp = pyuavcan.transport.Timestamp(0, 0)
        self._transfer_id = 0

    def process_frame(self,
                      timestamp:              pyuavcan.transport.Timestamp,
                      priority:               pyuavcan.transport.Priority,
                      transfer_id:            int,
                      frame_index:            int,
                      end_of_transfer:        bool,
                      payload:                memoryview,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, ErrorID, pyuavcan.transport.TransferFrom]:
        """
        Occasionally, newer frames may have lower timestamp values due to error variations in the timestamping
        algorithms. This class therefore does not check if the timestamp values are monotonically increasing.
        The timestamp of a transfer will be the lowest (earliest) timestamp value of its frames.
        """
        # FIRST STAGE - DETECTION OF NEW TRANSFERS.
        tid_timed_out = \
            timestamp.monotonic_ns - self._timestamp.monotonic_ns > transfer_id_timeout_ns or \
            self._timestamp.monotonic_ns == 0

        raise NotImplementedError
