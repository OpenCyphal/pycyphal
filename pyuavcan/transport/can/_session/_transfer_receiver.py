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
        """
        Observe that occasionally newer frames may have lower timestamp values due to error variations in the time
        recovery algorithms, depending on the methods of timestamping. This class therefore does not check if the
        timestamp values are monotonically increasing. The timestamp of a transfer will be the lowest (earliest)
        timestamp value of its frames (ignoring frames with mismatching transfer ID or toggle bit).
        """
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
            self._timestamp = frame.timestamp   # Initialization from the first frame

        if self._timestamp.monotonic_ns > frame.timestamp.monotonic_ns or \
                self._timestamp.system_ns > frame.timestamp.system_ns:
            # The timestamping algorithm may have corrected the time error since the first frame, accept lower value
            self._timestamp = pyuavcan.transport.Timestamp.combine_oldest(self._timestamp, frame.timestamp)

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
                # Observe that padding bytes at the end of the last frame are not counted towards the maximum
                # transfer length because when we receive the last frame we blindly accept it, not checking the
                # resulting transfer size.
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
        away = rx.process_frame(priority=priority,
                                source_node_id=source_node_id,
                                frame=frame,
                                transfer_id_timeout_ns=transfer_id_timeout_ns)
        assert away is None or isinstance(away, (TransferReceptionError, pyuavcan.transport.TransferFrom))
        return away

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
            timestamp=pyuavcan.transport.Timestamp(system_ns=0, monotonic_ns=monotonic_ns))

    def tr(monotonic_ns:       int,
           transfer_id:        int,
           fragmented_payload: typing.Sequence[typing.Union[bytes, str, memoryview]]) \
            -> pyuavcan.transport.TransferFrom:
        return pyuavcan.transport.TransferFrom(
            timestamp=pyuavcan.transport.Timestamp(system_ns=0, monotonic_ns=monotonic_ns),
            priority=priority,
            transfer_id=transfer_id,
            fragmented_payload=[
                memoryview(x if isinstance(x, (bytes, memoryview)) else x.encode()) for x in fragmented_payload
            ],
            source_node_id=source_node_id)

    rx = TransferReceiver(50)

    # Correct single-frame transfers.
    assert go(fr(1000, 'Hello', 0, True, True, True)) == tr(1000, 0, ['Hello'])
    assert go(fr(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2000, 'Hello', 0, True, True, True)) == tr(2000, 0, ['Hello'])         # TID timeout

    # Correct multi-frame transfer.
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

    # Correct transfer with the old transfer ID will be ignored.
    assert go(fr(2010, b'\x00\x01\x02\x03\x04\x05\x06', 1, True, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2011, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2012, b'\x0e\x0f\x10\x11\x12\x13\x14', 1, False, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2013, b'\x15\x16\x17\x18\x19\x1a\x1b', 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert go(fr(2014, b'\x1c\x1d' b'\x35\x54', 1, False, True, True)) == err.UNEXPECTED_TRANSFER_ID

    # Correct reception where the CRC spills over into the next frame.
    assert go(fr(2100, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 9, True, False, True)) is None
    assert go(fr(2101, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 9, False, False, False)) is None
    assert go(fr(2102, b'\x6f', 9, False, True, True)) == tr(2100, 9, [
        b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e',
        b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c',      # Third fragment is gone - used to contain CRC
    ])

    # Transfer ID rolled back but should be accepted anyway; CRC is invalid
    assert go(fr(2200, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 8, True, False, True)) is None
    assert go(fr(2201, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 8, False, False, False)) is None
    assert go(fr(2202, b'\x00', 8, False, True, True)) == err.TRANSFER_CRC_MISMATCH

    # Transfer ID timeout and the new frame is not a start of new transfer --> missed start error
    assert go(fr(4000, b'123456', 8, False, False, True)) == err.MISSED_START_OF_TRANSFER

    # New transfer; same TID is accepted anyway due to the timeout condition; repeated frames (bad toggles)
    assert go(fr(4000, b'\x00\x01\x02\x03\x04\x05\x06', 8, True, False, True)) is None
    assert go(fr(4010, b'123456', 8, True, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert go(fr(3500, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 8, False, False, False)) is None    # Timestamp update!
    assert go(fr(3000, b'', 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT           # Timestamp ignored
    assert go(fr(4022, b'\x0e\x0f\x10\x11\x12\x13\x14', 8, False, False, True)) is None
    assert go(fr(4002, b'\x0e\x0f\x10\x11\x12\x13\x14', 8, False, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert go(fr(4013, b'\x15\x16\x17\x18\x19\x1a\x1b', 8, False, False, False)) is None
    assert go(fr(4003, b'\x15\x16\x17\x18\x19\x1a\x1b' * 2, 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT
    assert go(fr(4004, b'\x1c\x1d' b'\x35\x54', 8, False, True, True)) == tr(3500, 8, [
        b'\x00\x01\x02\x03\x04\x05\x06',
        b'\x07\x08\x09\x0a\x0b\x0c\x0d',
        b'\x0e\x0f\x10\x11\x12\x13\x14',
        b'\x15\x16\x17\x18\x19\x1a\x1b',
        b'\x1c\x1d',
    ])
    assert go(fr(4004, b'\x1c\x1d' b'\x35\x54', 8, False, True, True)) == err.UNEXPECTED_TRANSFER_ID  # TID, not toggle

    # Transfer that is too large (above the configured limit) and rejected. Time goes back but it's fine.
    assert go(fr(1000, b'0123456789abcdefghi', 0, True, False, True)) is None       # 19
    assert go(fr(1001, b'0123456789abcdefghi', 0, False, False, False)) is None     # 38
    assert go(fr(1001, b'0123456789abcdefghi', 0, False, False, True)) == err.PAYLOAD_TOO_LARGE

    # Transfer above the limit but accepted nevertheless because the overflow induced by the last frame is not checked.
    assert go(fr(1000, b'0123456789abcdefghi', 31, True, False, True)) is None       # 19
    assert go(fr(1001, b'0123456789abcdefghi', 31, False, False, False)) is None     # 38
    assert go(fr(1001, b'0123456789abcdefghi' b'\xa9\x72', 31, False, True, True)) == tr(1000, 31, [
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
    ])
