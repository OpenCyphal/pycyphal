#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import pyuavcan
from .. import _frame


class TransferReassemblyErrorID(enum.Enum):
    """
    Transfer reassembly error codes. Used in the extended error statistics.
    See the UAVCAN specification for background info.
    We have ``ID`` in the name to make clear that this is not an exception type.
    """
    MISSED_START_OF_TRANSFER = enum.auto()
    UNEXPECTED_TOGGLE_BIT    = enum.auto()
    UNEXPECTED_TRANSFER_ID   = enum.auto()
    TRANSFER_CRC_MISMATCH    = enum.auto()


class TransferReassembler:
    def __init__(self, source_node_id: int, extent_bytes: int):
        self._source_node_id = int(source_node_id)
        self._timestamp = pyuavcan.transport.Timestamp(0, 0)
        self._transfer_id = 0
        self._toggle_bit = False
        self._max_payload_size_bytes_with_crc = int(extent_bytes) + _frame.TRANSFER_CRC_LENGTH_BYTES
        self._crc = pyuavcan.transport.commons.crc.CRC16CCITT()
        self._payload_truncated = False
        self._fragmented_payload: typing.List[memoryview] = []

    def process_frame(self,
                      priority:               pyuavcan.transport.Priority,
                      frame:                  _frame.TimestampedUAVCANFrame,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, TransferReassemblyErrorID, pyuavcan.transport.TransferFrom]:
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
                return TransferReassemblyErrorID.MISSED_START_OF_TRANSFER

        # SECOND STAGE - DROP UNEXPECTED FRAMES.
        # A properly functioning CAN bus may occasionally replicate frames (see the Specification for background).
        # We combat these issues by checking the transfer ID and the toggle bit.
        if frame.transfer_id != self._transfer_id:
            return TransferReassemblyErrorID.UNEXPECTED_TRANSFER_ID

        if frame.toggle_bit != self._toggle_bit:
            return TransferReassemblyErrorID.UNEXPECTED_TOGGLE_BIT

        # THIRD STAGE - PAYLOAD REASSEMBLY AND VERIFICATION.
        # Collect the data and check its correctness.
        if frame.start_of_transfer:
            self._crc = pyuavcan.transport.commons.crc.CRC16CCITT()
            self._payload_truncated = False
            self._fragmented_payload.clear()
            self._timestamp = frame.timestamp   # Initialization from the first frame

        if self._timestamp.monotonic_ns > frame.timestamp.monotonic_ns or \
                self._timestamp.system_ns > frame.timestamp.system_ns:
            # The timestamping algorithm may have corrected the time error since the first frame, accept lower value
            self._timestamp = pyuavcan.transport.Timestamp.combine_oldest(self._timestamp, frame.timestamp)

        self._toggle_bit = not self._toggle_bit
        # Implicit truncation rule - discard the unexpected data at the end of the payload but compute the CRC anyway.
        self._crc.add(frame.padded_payload)
        if sum(map(len, self._fragmented_payload)) < self._max_payload_size_bytes_with_crc:
            self._fragmented_payload.append(frame.padded_payload)
        else:
            self._payload_truncated = True

        if frame.end_of_transfer:
            fragmented_payload = self._fragmented_payload.copy()
            self._prepare_for_next_transfer()
            self._fragmented_payload.clear()

            if frame.start_of_transfer:
                assert len(fragmented_payload) == 1     # Single-frame transfer, additional checks not needed
            else:
                assert len(fragmented_payload) > 1      # Multi-frame transfer, check and remove the trailing CRC
                if not self._crc.check_residue():
                    return TransferReassemblyErrorID.TRANSFER_CRC_MISMATCH

                # Cut off the CRC, unless it's already been removed by the implicit payload truncation rule.
                if not self._payload_truncated:
                    expected_length = sum(map(len, fragmented_payload)) - _frame.TRANSFER_CRC_LENGTH_BYTES
                    if len(fragmented_payload[-1]) > _frame.TRANSFER_CRC_LENGTH_BYTES:
                        fragmented_payload[-1] = fragmented_payload[-1][:-_frame.TRANSFER_CRC_LENGTH_BYTES]
                    else:
                        cutoff = _frame.TRANSFER_CRC_LENGTH_BYTES - len(fragmented_payload[-1])
                        assert cutoff >= 0
                        fragmented_payload = fragmented_payload[:-1]                    # Drop the last fragment
                        if cutoff > 0:
                            fragmented_payload[-1] = fragmented_payload[-1][:-cutoff]   # Truncate the previous fragment
                    assert expected_length == sum(map(len, fragmented_payload))

            return pyuavcan.transport.TransferFrom(timestamp=self._timestamp,
                                                   priority=priority,
                                                   transfer_id=frame.transfer_id,
                                                   fragmented_payload=fragmented_payload,
                                                   source_node_id=self._source_node_id)
        else:
            return None     # Expect more frames to come

    def _prepare_for_next_transfer(self) -> None:
        self._transfer_id = (self._transfer_id + 1) % _frame.TRANSFER_ID_MODULO
        self._toggle_bit = True


def _unittest_can_transfer_reassembler_manual() -> None:
    priority = pyuavcan.transport.Priority.IMMEDIATE
    source_node_id = 123
    transfer_id_timeout_ns = 900
    can_identifier = 0xbadc0fe

    err = TransferReassemblyErrorID

    def proc(frame: _frame.TimestampedUAVCANFrame) \
            -> typing.Union[None, TransferReassemblyErrorID, pyuavcan.transport.TransferFrom]:
        away = rx.process_frame(priority=priority,
                                frame=frame,
                                transfer_id_timeout_ns=transfer_id_timeout_ns)
        assert away is None or isinstance(away, (TransferReassemblyErrorID, pyuavcan.transport.TransferFrom))
        return away

    def frm(monotonic_ns:      int,
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

    def trn(monotonic_ns:       int,
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

    rx = TransferReassembler(source_node_id, 50)

    # Correct single-frame transfers.
    assert proc(frm(1000, 'Hello', 0, True, True, True)) == trn(1000, 0, ['Hello'])
    assert proc(frm(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(1000, 'Hello', 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(2000, 'Hello', 0, True, True, True)) == trn(2000, 0, ['Hello'])         # TID timeout

    # Correct multi-frame transfer.
    assert proc(frm(2000, b'\x00\x01\x02\x03\x04\x05\x06', 1, True, False, True)) is None
    assert proc(frm(2001, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 1, False, False, False)) is None
    assert proc(frm(2002, b'\x0e\x0f\x10\x11\x12\x13\x14', 1, False, False, True)) is None
    assert proc(frm(2003, b'\x15\x16\x17\x18\x19\x1a\x1b', 1, False, False, False)) is None
    assert proc(frm(2004, b'\x1c\x1d\x35\x54',             1, False, True, True)) == trn(2000, 1, [
        b'\x00\x01\x02\x03\x04\x05\x06',
        b'\x07\x08\x09\x0a\x0b\x0c\x0d',
        b'\x0e\x0f\x10\x11\x12\x13\x14',
        b'\x15\x16\x17\x18\x19\x1a\x1b',
        b'\x1c\x1d',
    ])

    # Correct transfer with the old transfer ID will be ignored.
    assert proc(frm(2010, b'\x00\x01\x02\x03\x04\x05\x06', 1, True, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(2011, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(2012, b'\x0e\x0f\x10\x11\x12\x13\x14', 1, False, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(2013, b'\x15\x16\x17\x18\x19\x1a\x1b', 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(frm(2014, b'\x1c\x1d\x35\x54',             1, False, True, True)) == err.UNEXPECTED_TRANSFER_ID

    # Correct reassembly where the CRC spills over into the next frame.
    assert proc(frm(2100, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 9, True, False, True)) \
        is None
    assert proc(frm(2101, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 9, False, False, False)) \
        is None
    assert proc(frm(2102, b'\x6f', 9, False, True, True)) == trn(2100, 9, [
        b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e',
        b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c',      # Third fragment is gone - used to contain CRC
    ])

    # Transfer ID rolled back but should be accepted anyway; CRC is invalid
    assert proc(frm(2200, b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e', 8, True, False, True)) \
        is None
    assert proc(frm(2201, b'\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4', 8, False, False, False)) \
        is None
    assert proc(frm(2202, b'\x00', 8, False, True, True)) == err.TRANSFER_CRC_MISMATCH

    # Transfer ID timeout and the new frame is not a start of new transfer --> missed start error
    assert proc(frm(4000, b'123456', 8, False, False, True)) == err.MISSED_START_OF_TRANSFER

    # New transfer; same TID is accepted anyway due to the timeout condition; repeated frames (bad toggles)
    assert proc(frm(4000, b'\x00\x01\x02\x03\x04\x05\x06', 8, True, False, True)) is None
    assert proc(frm(4010, b'123456', 8, True, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(frm(3500, b'\x07\x08\x09\x0a\x0b\x0c\x0d', 8, False, False, False)) is None    # Timestamp update!
    assert proc(frm(3000, b'', 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT           # Timestamp ignored
    assert proc(frm(4022, b'\x0e\x0f\x10\x11\x12\x13\x14', 8, False, False, True)) is None
    assert proc(frm(4002, b'\x0e\x0f\x10\x11\x12\x13\x14', 8, False, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(frm(4013, b'\x15\x16\x17\x18\x19\x1a\x1b', 8, False, False, False)) is None
    assert proc(frm(4003, b'\x15\x16\x17\x18\x19\x1a\x1b' * 2, 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(frm(4004, b'\x1c\x1d\x35\x54',             8, False, True, True)) == trn(3500, 8, [
        b'\x00\x01\x02\x03\x04\x05\x06',
        b'\x07\x08\x09\x0a\x0b\x0c\x0d',
        b'\x0e\x0f\x10\x11\x12\x13\x14',
        b'\x15\x16\x17\x18\x19\x1a\x1b',
        b'\x1c\x1d',
    ])
    assert proc(frm(4004, b'\x1c\x1d\x35\x54', 8, False, True, True)) == err.UNEXPECTED_TRANSFER_ID  # Not toggle!

    # Transfer that is too large (above the configured limit) is implicitly truncated. Time goes back but it's fine.
    assert proc(frm(1000, b'0123456789abcdefghi', 0, True, False, True)) is None       # 19
    assert proc(frm(1001, b'0123456789abcdefghi', 0, False, False, False)) is None     # 38
    assert proc(frm(1001, b'0123456789abcdefghi', 0, False, False, True)) is None      # 57
    assert proc(frm(1001, b'0123456789abcdefghi', 0, False, False, False)) is None     # 76
    assert proc(frm(1001, b':B',                  0, False, True, True)) == trn(1000, 0, [
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
        # Last two are truncated away.
    ])

    # Transfer above the limit but accepted nevertheless because the overflow induced by the last frame is not checked.
    assert proc(frm(1000, b'0123456789abcdefghi',         31, True, False, True)) is None       # 19
    assert proc(frm(1001, b'0123456789abcdefghi',         31, False, False, False)) is None     # 38
    assert proc(frm(1001, b'0123456789abcdefghi\xa9\x72', 31, False, True, True)) == trn(1000, 31, [
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
        b'0123456789abcdefghi',
    ])
