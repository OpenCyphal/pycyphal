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
        self._max_payload_size_bytes_with_crc = int(max_payload_size_bytes) + _frame.TRANSFER_CRC_LENGTH_BYTES

    def process_frame(self,
                      priority:               pyuavcan.transport.Priority,
                      source_node_id:         int,
                      frame:                  _frame.TimestampedUAVCANFrame,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, TransferReceptionError, pyuavcan.transport.TransferFrom]:
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
        # We combat these issues by checking the transfer ID and the toggle bit.
        if frame.transfer_id != self._transfer_id:
            return TransferReceptionError.UNEXPECTED_TRANSFER_ID

        if frame.toggle_bit != self._toggle_bit:
            return TransferReceptionError.UNEXPECTED_TOGGLE_BIT

        # THIRD STAGE - PAYLOAD REASSEMBLY AND VERIFICATION.
        # Collect the data and check its correctness.
        if frame.start_of_transfer:
            self._fragmented_payload.clear()
            self._timestamp = frame.timestamp

        self._toggle_bit = not self._toggle_bit
        self._fragmented_payload.append(frame.padded_payload)

        if frame.end_of_transfer:
            fragmented_payload = self._fragmented_payload.copy()
            self._prepare_for_next_transfer()
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

                # Cut off the CRC
                expected_length = sum(map(len, fragmented_payload)) - _frame.TRANSFER_CRC_LENGTH_BYTES
                if len(fragmented_payload[-1]) > _frame.TRANSFER_CRC_LENGTH_BYTES:
                    fragmented_payload[-1] = fragmented_payload[-1][:-_frame.TRANSFER_CRC_LENGTH_BYTES]
                else:
                    cutoff = _frame.TRANSFER_CRC_LENGTH_BYTES - len(fragmented_payload[-1])
                    assert cutoff >= 0
                    fragmented_payload = fragmented_payload[:-1]                # Drop the last fragment
                    fragmented_payload[-1] = fragmented_payload[-1][:-cutoff]   # Truncate the previous fragment
                assert expected_length == sum(map(len, fragmented_payload))

            return pyuavcan.transport.TransferFrom(timestamp=self._timestamp,
                                                   priority=priority,
                                                   transfer_id=frame.transfer_id,
                                                   fragmented_payload=fragmented_payload,
                                                   source_node_id=source_node_id)
        else:
            if sum(map(len, self._fragmented_payload)) > self._max_payload_size_bytes_with_crc:
                self._prepare_for_next_transfer()
                self._fragmented_payload.clear()
                return TransferReceptionError.PAYLOAD_TOO_LARGE

            return None     # Expect more frames to come

    def _prepare_for_next_transfer(self) -> None:
        self._transfer_id = (self._transfer_id + 1) % _frame.TRANSFER_ID_MODULO
        self._toggle_bit = True


def _unittest_can_transfer_receiver_manual() -> None:
    priority = pyuavcan.transport.Priority.IMMEDIATE
    source_node_id: typing.Optional[int] = 123
    transfer_id_timeout_ns = 900
    can_identifier = 0xbadc0fe

    err = TransferReceptionError

    def go(frame: _frame.TimestampedUAVCANFrame) \
            -> typing.Union[None, TransferReceptionError, pyuavcan.transport.TransferFrom]:
        return rx.process_frame(priority=priority,
                                source_node_id=source_node_id,
                                frame=frame,
                                transfer_id_timeout_ns=transfer_id_timeout_ns)

    def fr(monotonic_ns:      int,
           padded_payload:    typing.Union[bytes, str],
           transfer_id:       int,
           start_of_transfer: bool,
           end_of_transfer:   bool,
           toggle_bit:        bool) -> _frame.TimestampedUAVCANFrame:
        return _frame.TimestampedUAVCANFrame(
            identifier=can_identifier,
            padded_payload=memoryview(padded_payload if isinstance(padded_payload, bytes) else padded_payload.encode()),
            transfer_id=transfer_id,
            start_of_transfer=start_of_transfer,
            end_of_transfer=end_of_transfer,
            toggle_bit=toggle_bit,
            loopback=False,
            timestamp=pyuavcan.transport.Timestamp(wall_ns=0, monotonic_ns=monotonic_ns))

    def tr(monotonic_ns:       int,
           transfer_id:        int,
           fragmented_payload: typing.Sequence[typing.Union[bytes, str, memoryview]]) \
            -> pyuavcan.transport.TransferFrom:
        return pyuavcan.transport.TransferFrom(
            timestamp=pyuavcan.transport.Timestamp(wall_ns=0, monotonic_ns=monotonic_ns),
            priority=priority,
            transfer_id=transfer_id,
            fragmented_payload=[
                memoryview(x if isinstance(x, (bytes, memoryview)) else x.encode()) for x in fragmented_payload
            ],
            source_node_id=source_node_id)

    rx = TransferReceiver(100)

    assert go(fr(1000, 'Hello', 0, True, True, True)) == tr(1000, 0, ['Hello'])
    assert go(fr(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2000, 'Hello', 0, True, True, True)) == tr(2000, 0, ['Hello'])         # TID timeout

    assert go(fr(2000, b'\x00\x01\x02\x03\x04\x05\x06', 1, True, False, True)) is None
    assert go(fr(2001, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 1, False, False, False)) is None
    assert go(fr(2002, b'\x0e\x0f\x10\x11\x12\x13\x14', 1, False, False, True)) is None
    assert go(fr(2003, b'\x15\x16\x17\x18\x19\x1a\x1b', 1, False, False, False)) is None
    assert go(fr(2004, b'\x1c\x1d' b'\x35\x54', 1, False, True, True)) == tr(2000, 1, [
        b'\x00\x01\x02\x03\x04\x05\x06',
        b'\x07\x08\x09\x0a\x0b\x0c\x0d',
        b'\x0e\x0f\x10\x11\x12\x13\x14',
        b'\x15\x16\x17\x18\x19\x1a\x1b',
        b'\x1c\x1d',
    ])

    assert go(fr(2100, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 9, True, False, True)) is None
    assert go(fr(2101, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 9, False, False, False)) is None
    assert go(fr(2102, b'\x6f', 9, False, True, True)) == tr(2100, 9, [
        b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e',
        b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c',      # Third fragment is gone - used to contain CRC
    ])
