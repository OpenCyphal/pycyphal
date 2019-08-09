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
    class ErrorID(enum.Enum):
        MISSED_START_OF_TRANSFER = enum.auto()
        MISSING_FRAMES           = enum.auto()
        UNEXPECTED_TRANSFER_ID   = enum.auto()
        TRANSFER_CRC_MISMATCH    = enum.auto()
        PAYLOAD_TOO_LARGE        = enum.auto()

    def __init__(self, max_payload_size_bytes: int):
        self._max_payload_size_bytes_with_crc = int(max_payload_size_bytes) + 4

    def process_frame(self,
                      timestamp:              pyuavcan.transport.Timestamp,
                      priority:               pyuavcan.transport.Priority,
                      source_node_id:         int,
                      transfer_id:            int,
                      frame_index:            int,
                      end_of_transfer:        bool,
                      payload:                memoryview,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, ErrorID, pyuavcan.transport.TransferFrom]:
        raise NotImplementedError
