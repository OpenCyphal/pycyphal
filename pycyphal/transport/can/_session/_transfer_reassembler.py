# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import enum
from typing import Sequence
import pycyphal
from pycyphal.transport import Timestamp, TransferFrom
from .._frame import CyphalFrame, compute_transfer_id_forward_distance, TRANSFER_CRC_LENGTH_BYTES, TRANSFER_ID_MODULO


class TransferReassemblyErrorID(enum.Enum):
    """
    Transfer reassembly error codes. Used in the extended error statistics.
    See the Cyphal specification for background info.
    We have ``ID`` in the name to make clear that this is not an exception type.
    """

    MISSED_START_OF_TRANSFER = enum.auto()
    UNEXPECTED_TOGGLE_BIT = enum.auto()
    UNEXPECTED_TRANSFER_ID = enum.auto()
    TRANSFER_CRC_MISMATCH = enum.auto()


class TransferReassembler:
    def __init__(self, source_node_id: int, extent_bytes: int):
        self._source_node_id = int(source_node_id)
        self._timestamp = Timestamp(0, 0)
        self._transfer_id = 0
        self._toggle_bit = False
        self._max_payload_size_bytes_with_crc = int(extent_bytes) + TRANSFER_CRC_LENGTH_BYTES
        self._crc = pycyphal.transport.commons.crc.CRC16CCITT()
        self._payload_truncated = False
        self._fragmented_payload: list[memoryview] = []

    def process_frame(
        self,
        timestamp: Timestamp,
        priority: pycyphal.transport.Priority,
        frame: CyphalFrame,
        transfer_id_timeout_ns: int,
    ) -> None | TransferReassemblyErrorID | TransferFrom:
        """
        Observe that occasionally newer frames may have lower timestamp values due to error variations in the time
        recovery algorithms, depending on the methods of timestamping. This class therefore does not check if the
        timestamp values are monotonically increasing. The timestamp of a transfer will be the lowest (earliest)
        timestamp value of its frames (ignoring frames with mismatching transfer ID or toggle bit).
        """
        # FIRST STAGE - DETECTION OF NEW TRANSFERS.
        # Decide if we need to begin a new transfer.
        tid_timed_out = (
            timestamp.monotonic_ns - self._timestamp.monotonic_ns > transfer_id_timeout_ns
            or self._timestamp.monotonic_ns == 0
        )

        not_previous_tid = compute_transfer_id_forward_distance(frame.transfer_id, self._transfer_id) > 1

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
            self._crc = pycyphal.transport.commons.crc.CRC16CCITT()
            self._payload_truncated = False
            self._fragmented_payload.clear()
            self._timestamp = timestamp  # Initialization from the first frame

        if self._timestamp.monotonic_ns > timestamp.monotonic_ns or self._timestamp.system_ns > timestamp.system_ns:
            # The timestamping algorithm may have corrected the time error since the first frame, accept lower value
            self._timestamp = Timestamp.combine_oldest(self._timestamp, timestamp)

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
                assert len(fragmented_payload) == 1  # Single-frame transfer, additional checks not needed
            else:
                # Multi-frame transfer, check and remove the trailing CRC.
                # We don't bother checking the CRC if we received fewer than 2 frames because that implies that there
                # was a TID wraparound mid-transfer.
                # This happens when the reassembler that has just been reset is fed with the last frame of another
                # transfer, whose TOGGLE and TRANSFER-ID happen to match the expectations of the reassembler:
                #   1. Wait for the reassembler to be reset. Let: expected transfer-ID = X, expected toggle bit = Y.
                #   2. Construct a frame with SOF=0, EOF=1, TID=X, TOGGLE=Y.
                #   3. Feed the frame into the reassembler.
                # See https://github.com/OpenCyphal/pycyphal/issues/198. There is a dedicated test covering this case.
                if len(fragmented_payload) < 2 or not self._crc.check_residue():
                    return TransferReassemblyErrorID.TRANSFER_CRC_MISMATCH

                # Cut off the CRC, unless it's already been removed by the implicit payload truncation rule.
                if not self._payload_truncated:
                    expected_length = sum(map(len, fragmented_payload)) - TRANSFER_CRC_LENGTH_BYTES
                    if len(fragmented_payload[-1]) > TRANSFER_CRC_LENGTH_BYTES:
                        fragmented_payload[-1] = fragmented_payload[-1][:-TRANSFER_CRC_LENGTH_BYTES]
                    else:
                        cutoff = TRANSFER_CRC_LENGTH_BYTES - len(fragmented_payload[-1])
                        assert cutoff >= 0
                        fragmented_payload = fragmented_payload[:-1]  # Drop the last fragment
                        if cutoff > 0:
                            fragmented_payload[-1] = fragmented_payload[-1][:-cutoff]  # Truncate the previous fragment
                    assert expected_length == sum(map(len, fragmented_payload))

            return TransferFrom(
                timestamp=self._timestamp,
                priority=priority,
                transfer_id=frame.transfer_id,
                fragmented_payload=fragmented_payload,
                source_node_id=self._source_node_id,
            )

        return None  # Expect more frames to come

    def _prepare_for_next_transfer(self) -> None:
        self._transfer_id = (self._transfer_id + 1) % TRANSFER_ID_MODULO
        self._toggle_bit = True


def _unittest_can_transfer_reassembler_manual() -> None:
    priority = pycyphal.transport.Priority.IMMEDIATE
    source_node_id = 123
    transfer_id_timeout_ns = 900
    can_identifier = 0xBADC0FE

    err = TransferReassemblyErrorID

    def proc(monotonic_ns: int, frame: CyphalFrame) -> None | TransferReassemblyErrorID | TransferFrom:
        away = rx.process_frame(
            timestamp=Timestamp(system_ns=0, monotonic_ns=monotonic_ns),
            priority=priority,
            frame=frame,
            transfer_id_timeout_ns=transfer_id_timeout_ns,
        )
        assert away is None or isinstance(away, (TransferReassemblyErrorID, TransferFrom))
        return away

    def frm(
        padded_payload: bytes | str,
        transfer_id: int,
        start_of_transfer: bool,
        end_of_transfer: bool,
        toggle_bit: bool,
    ) -> CyphalFrame:
        return CyphalFrame(
            identifier=can_identifier,
            padded_payload=memoryview(padded_payload if isinstance(padded_payload, bytes) else padded_payload.encode()),
            transfer_id=transfer_id,
            start_of_transfer=start_of_transfer,
            end_of_transfer=end_of_transfer,
            toggle_bit=toggle_bit,
        )

    def trn(
        monotonic_ns: int, transfer_id: int, fragmented_payload: Sequence[bytes | str | memoryview]
    ) -> TransferFrom:
        return TransferFrom(
            timestamp=Timestamp(system_ns=0, monotonic_ns=monotonic_ns),
            priority=priority,
            transfer_id=transfer_id,
            fragmented_payload=[
                memoryview(x if isinstance(x, (bytes, memoryview)) else x.encode()) for x in fragmented_payload
            ],
            source_node_id=source_node_id,
        )

    rx = TransferReassembler(source_node_id, 50)

    # Correct single-frame transfers.
    assert proc(1000, frm("Hello", 0, True, True, True)) == trn(1000, 0, ["Hello"])
    assert proc(1000, frm("Hello", 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(1000, frm("Hello", 0, True, True, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(2000, frm("Hello", 0, True, True, True)) == trn(2000, 0, ["Hello"])  # TID timeout

    # Correct multi-frame transfer.
    assert proc(2000, frm(b"\x00\x01\x02\x03\x04\x05\x06", 1, True, False, True)) is None
    assert proc(2001, frm(b"\x07\x08\x09\x0a\x0b\x0c\x0d", 1, False, False, False)) is None
    assert proc(2002, frm(b"\x0e\x0f\x10\x11\x12\x13\x14", 1, False, False, True)) is None
    assert proc(2003, frm(b"\x15\x16\x17\x18\x19\x1a\x1b", 1, False, False, False)) is None
    assert proc(2004, frm(b"\x1c\x1d\x35\x54", 1, False, True, True)) == trn(
        2000,
        1,
        [
            b"\x00\x01\x02\x03\x04\x05\x06",
            b"\x07\x08\x09\x0a\x0b\x0c\x0d",
            b"\x0e\x0f\x10\x11\x12\x13\x14",
            b"\x15\x16\x17\x18\x19\x1a\x1b",
            b"\x1c\x1d",
        ],
    )

    # Correct transfer with the old transfer ID will be ignored.
    assert proc(2010, frm(b"\x00\x01\x02\x03\x04\x05\x06", 1, True, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(2011, frm(b"\x07\x08\x09\x0a\x0b\x0c\x0d", 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(2012, frm(b"\x0e\x0f\x10\x11\x12\x13\x14", 1, False, False, True)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(2013, frm(b"\x15\x16\x17\x18\x19\x1a\x1b", 1, False, False, False)) == err.UNEXPECTED_TRANSFER_ID
    assert proc(2014, frm(b"\x1c\x1d\x35\x54", 1, False, True, True)) == err.UNEXPECTED_TRANSFER_ID

    # Correct reassembly where the CRC spills over into the next frame.
    assert (
        proc(2100, frm(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e", 9, True, False, True)) is None
    )
    assert (
        proc(2101, frm(b"\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4", 9, False, False, False)) is None
    )
    assert proc(2102, frm(b"\x6f", 9, False, True, True)) == trn(
        2100,
        9,
        [
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e",
            b"\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c",  # Third fragment is gone - used to contain CRC
        ],
    )

    # Transfer ID rolled back but should be accepted anyway; CRC is invalid
    assert (
        proc(2200, frm(b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e", 8, True, False, True)) is None
    )
    assert (
        proc(2201, frm(b"\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\xc4", 8, False, False, False)) is None
    )
    assert proc(2202, frm(b"\x00", 8, False, True, True)) == err.TRANSFER_CRC_MISMATCH

    # Transfer ID timeout and the new frame is not a start of new transfer --> missed start error
    assert proc(4000, frm(b"123456", 8, False, False, True)) == err.MISSED_START_OF_TRANSFER

    # New transfer; same TID is accepted anyway due to the timeout condition; repeated frames (bad toggles)
    assert proc(4000, frm(b"\x00\x01\x02\x03\x04\x05\x06", 8, True, False, True)) is None
    assert proc(4010, frm(b"123456", 8, True, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(3500, frm(b"\x07\x08\x09\x0a\x0b\x0c\x0d", 8, False, False, False)) is None  # Timestamp update!
    assert proc(3000, frm(b"", 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT  # Timestamp ignored
    assert proc(4022, frm(b"\x0e\x0f\x10\x11\x12\x13\x14", 8, False, False, True)) is None
    assert proc(4002, frm(b"\x0e\x0f\x10\x11\x12\x13\x14", 8, False, False, True)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(4013, frm(b"\x15\x16\x17\x18\x19\x1a\x1b", 8, False, False, False)) is None
    assert proc(4003, frm(b"\x15\x16\x17\x18\x19\x1a\x1b" * 2, 8, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT
    assert proc(4004, frm(b"\x1c\x1d\x35\x54", 8, False, True, True)) == trn(
        3500,
        8,
        [
            b"\x00\x01\x02\x03\x04\x05\x06",
            b"\x07\x08\x09\x0a\x0b\x0c\x0d",
            b"\x0e\x0f\x10\x11\x12\x13\x14",
            b"\x15\x16\x17\x18\x19\x1a\x1b",
            b"\x1c\x1d",
        ],
    )
    assert proc(4004, frm(b"\x1c\x1d\x35\x54", 8, False, True, True)) == err.UNEXPECTED_TRANSFER_ID  # Not toggle!

    # Transfer that is too large (above the configured limit) is implicitly truncated. Time goes back but it's fine.
    assert proc(1000, frm(b"0123456789abcdefghi", 0, True, False, True)) is None  # 19
    assert proc(1001, frm(b"0123456789abcdefghi", 0, False, False, False)) is None  # 38
    assert proc(1001, frm(b"0123456789abcdefghi", 0, False, False, True)) is None  # 57
    assert proc(1001, frm(b"0123456789abcdefghi", 0, False, False, False)) is None  # 76
    assert proc(1001, frm(b":B", 0, False, True, True)) == trn(
        1000,
        0,
        [
            b"0123456789abcdefghi",
            b"0123456789abcdefghi",
            b"0123456789abcdefghi",
            # Last two are truncated away.
        ],
    )

    # Transfer above the limit but accepted nevertheless because the overflow induced by the last frame is not checked.
    assert proc(1000, frm(b"0123456789abcdefghi", 31, True, False, True)) is None  # 19
    assert proc(1001, frm(b"0123456789abcdefghi", 31, False, False, False)) is None  # 38
    assert proc(1001, frm(b"0123456789abcdefghi\xa9\x72", 31, False, True, True)) == trn(
        1000,
        31,
        [
            b"0123456789abcdefghi",
            b"0123456789abcdefghi",
            b"0123456789abcdefghi",
        ],
    )


def _unittest_issue_198() -> None:
    source_node_id = 88
    transfer_id_timeout_ns = 900

    def mk_frame(
        padded_payload: bytes | str,
        transfer_id: int,
        start_of_transfer: bool,
        end_of_transfer: bool,
        toggle_bit: bool,
    ) -> CyphalFrame:
        return CyphalFrame(
            identifier=0xBADC0FE,
            padded_payload=memoryview(padded_payload if isinstance(padded_payload, bytes) else padded_payload.encode()),
            transfer_id=transfer_id,
            start_of_transfer=start_of_transfer,
            end_of_transfer=end_of_transfer,
            toggle_bit=toggle_bit,
        )

    rx = TransferReassembler(source_node_id, 50)

    # First, ensure that the reassembler is initialized, by feeding it a valid transfer at least once.
    assert rx.process_frame(
        timestamp=Timestamp(system_ns=0, monotonic_ns=1000),
        priority=pycyphal.transport.Priority.SLOW,
        frame=mk_frame("123", 0, True, True, True),
        transfer_id_timeout_ns=transfer_id_timeout_ns,
    ) == TransferFrom(
        timestamp=Timestamp(system_ns=0, monotonic_ns=1000),
        priority=pycyphal.transport.Priority.SLOW,
        transfer_id=0,
        fragmented_payload=[memoryview(x if isinstance(x, (bytes, memoryview)) else x.encode()) for x in ["123"]],
        source_node_id=source_node_id,
    )

    # Next, feed the last frame of another transfer whose TID/TOG match the expected state of the reassembler.
    # This should be recognized as a CRC error.
    assert (
        rx.process_frame(
            timestamp=Timestamp(system_ns=0, monotonic_ns=1000),
            priority=pycyphal.transport.Priority.SLOW,
            frame=mk_frame("456", 1, False, True, True),
            transfer_id_timeout_ns=transfer_id_timeout_ns,
        )
        == TransferReassemblyErrorID.TRANSFER_CRC_MISMATCH
    )
