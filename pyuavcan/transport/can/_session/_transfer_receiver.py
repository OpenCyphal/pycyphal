#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import pyuavcan.util
import pyuavcan.transport
from .. import _frame


class TransferReceptionError(enum.Enum):
    MISSED_START_OF_TRANSFER = enum.auto()
    UNEXPECTED_TOGGLE_BIT    = enum.auto()
    UNEXPECTED_TRANSFER_ID   = enum.auto()
    TRANSFER_CRC_MISMATCH    = enum.auto()
    PAYLOAD_TOO_LARGE        = enum.auto()


class TransferReceiver:
    def __init__(self, max_payload_size_bytes: int):
        self._fragmented_payload: typing.List[memoryview] = []
        self._timestamp = pyuavcan.transport.Timestamp(0, 0)
        self._transfer_id = 0
        self._toggle_bit = False
        self._max_payload_size_bytes = int(max_payload_size_bytes)

    def process_frame(self,
                      priority:               pyuavcan.transport.Priority,
                      source_node_id:         int,
                      frame:                  _frame.TimestampedUAVCANFrame,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, TransferReceptionError, pyuavcan.transport.Transfer]:
        # FIRST STAGE - DETECTION OF NEW TRANSFERS.
        # Decide if we need to begin a new transfer.
        tid_timed_out = \
            frame.timestamp.monotonic_ns - self._timestamp.monotonic_ns > transfer_id_timeout_ns or \
            self._timestamp.monotonic_ns == 0

        not_previous_tid = _frame.compute_transfer_id_forward_distance(frame.transfer_id, self._transfer_id) > 1

        if tid_timed_out or (frame.start_of_transfer and not_previous_tid):
            self._transfer_id = frame.transfer_id
            self._toggle_bit = frame.toggle_bit
            if not frame.start_of_transfer:
                return TransferReceptionError.MISSED_START_OF_TRANSFER

        # SECOND STAGE - DROP UNEXPECTED FRAMES.
        # A properly functioning CAN bus may occasionally replicate frames (see the Specification for background).
        # Here we combat these issues by checking the transfer ID and the toggle bit.
        if frame.toggle_bit != self._toggle_bit:
            return TransferReceptionError.UNEXPECTED_TOGGLE_BIT

        if frame.transfer_id != self._transfer_id:
            return TransferReceptionError.UNEXPECTED_TRANSFER_ID

        # THIRD STAGE - PAYLOAD REASSEMBLY AND VERIFICATION.
        # Collect the data and check its correctness.
        if frame.start_of_transfer:
            self._fragmented_payload.clear()
            self._timestamp = frame.timestamp

        self._toggle_bit = not self._toggle_bit
        self._fragmented_payload.append(frame.padded_payload)

        if frame.end_of_transfer:
            fragmented_payload = self._fragmented_payload.copy()
            self._increment_transfer_id()
            self._fragmented_payload.clear()

            if frame.start_of_transfer:
                assert len(fragmented_payload) == 1     # Single-frame transfer, additional checks not needed
            else:
                assert len(fragmented_payload) > 1      # Multi-frame transfer, check and remove the trailing CRC
                crc = pyuavcan.util.hash.CRC16CCITT()
                for frag in fragmented_payload:
                    crc.add(frag)
                if crc.value != crc.RESIDUE:
                    return TransferReceptionError.TRANSFER_CRC_MISMATCH

                fragmented_payload[-1] = fragmented_payload[-1][:-_frame.TRANSFER_CRC_LENGTH_BYTES]  # Cut off the CRC

            return pyuavcan.transport.TransferFrom(timestamp=self._timestamp,
                                                   priority=priority,
                                                   transfer_id=frame.transfer_id,
                                                   fragmented_payload=fragmented_payload,
                                                   source_node_id=source_node_id)
        else:
            if sum(map(len, self._fragmented_payload)) > self._max_payload_size_bytes:
                self._increment_transfer_id()
                self._fragmented_payload.clear()
                return TransferReceptionError.PAYLOAD_TOO_LARGE

            return None     # Expect more frames to come

    def _increment_transfer_id(self) -> None:
        self._transfer_id = (self._transfer_id + 1) % _frame.TRANSFER_ID_MODULO
