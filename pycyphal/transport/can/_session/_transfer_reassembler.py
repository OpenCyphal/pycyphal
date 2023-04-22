# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import enum
import dataclasses
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
    @dataclasses.dataclass
    class _State:
        crc: pycyphal.transport.commons.crc.CRC16CCITT = dataclasses.field(
            default_factory=pycyphal.transport.commons.crc.CRC16CCITT
        )
        truncated: bool = False
        payload: list[memoryview] = dataclasses.field(default_factory=list)

        @property
        def payload_size(self) -> int:
            return sum(map(len, self.payload))

    def __init__(self, source_node_id: int, extent_bytes: int):
        self._source_node_id = int(source_node_id)
        self._transfer_id = 0
        self._toggle_bit = False
        self._max_payload_size_bytes_with_crc = int(extent_bytes) + TRANSFER_CRC_LENGTH_BYTES
        self._state: TransferReassembler._State | None = None
        self._ts: Timestamp | None = None

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
        tid_timed_out = self._ts is None or (
            (frame.transfer_id != self._transfer_id)
            and (timestamp.monotonic_ns - self._ts.monotonic_ns > transfer_id_timeout_ns)
        )
        not_previous_tid = compute_transfer_id_forward_distance(frame.transfer_id, self._transfer_id) > 1
        need_restart = frame.start_of_transfer and (tid_timed_out or not_previous_tid)
        # Restarting the transfer reassembly only makes sense if the new frame is a start of transfer.
        # Otherwise, the new transfer would be impossible to reassemble anyway since the first frame is lost.
        if need_restart:
            self._state = None
            self._transfer_id = frame.transfer_id
            self._toggle_bit = frame.toggle_bit
            assert frame.start_of_transfer
        # A properly functioning CAN bus may occasionally replicate frames (see the Specification for background).
        # We combat these issues by checking the transfer ID and the toggle bit.
        if frame.transfer_id != self._transfer_id:
            return TransferReassemblyErrorID.UNEXPECTED_TRANSFER_ID
        if frame.toggle_bit != self._toggle_bit:
            return TransferReassemblyErrorID.UNEXPECTED_TOGGLE_BIT
        if frame.start_of_transfer:
            self._ts = timestamp  # Timestamp inited from the first frame.
            self._state = TransferReassembler._State()
        # Drop the frame if it's not the first one and the transfer is not yet started.
        # This condition protects against a TID wraparound mid-transfer,
        # see https://github.com/OpenCyphal/pycyphal/issues/198.
        # This happens when the reassembler that has just been reset is fed with the last frame of another
        # transfer, whose TOGGLE and TRANSFER-ID happen to match the expectations of the reassembler:
        #   1. Wait for the reassembler to be reset. Let: expected transfer-ID = X, expected toggle bit = Y.
        #   2. Construct a frame with SOF=0, EOF=1, TID=X, TOGGLE=Y.
        #   3. Feed the frame into the reassembler.
        # See https://github.com/OpenCyphal/pycyphal/issues/198. There is a dedicated test covering this case.
        if not self._state:
            return TransferReassemblyErrorID.MISSED_START_OF_TRANSFER
        # The timestamping algorithm may have corrected the time error since the first frame, accept lower values.
        assert self._ts is not None
        self._ts = Timestamp.combine_oldest(self._ts, timestamp)
        self._toggle_bit = not self._toggle_bit
        # Implicit truncation rule - discard the unexpected data at the end of the payload but compute the CRC anyway.
        assert self._state
        self._state.crc.add(frame.padded_payload)
        if self._state.payload_size < self._max_payload_size_bytes_with_crc:
            self._state.payload.append(frame.padded_payload)
        else:
            self._state.truncated = True
        if frame.end_of_transfer:
            fin, self._state = self._state, None
            self._transfer_id = (self._transfer_id + 1) % TRANSFER_ID_MODULO
            self._toggle_bit = True
            assert self._state is None and fin is not None
            if frame.start_of_transfer:
                assert len(fin.payload) == 1  # Single-frame transfer, additional checks not needed
            else:
                if not fin.crc.check_residue():
                    return TransferReassemblyErrorID.TRANSFER_CRC_MISMATCH
                # Cut off the CRC, unless it's already been removed by the implicit payload truncation rule.
                if not fin.truncated:
                    assert len(fin.payload) >= 2
                    expected_length = fin.payload_size - TRANSFER_CRC_LENGTH_BYTES
                    if len(fin.payload[-1]) > TRANSFER_CRC_LENGTH_BYTES:
                        fin.payload[-1] = fin.payload[-1][:-TRANSFER_CRC_LENGTH_BYTES]
                    else:
                        cutoff = TRANSFER_CRC_LENGTH_BYTES - len(fin.payload[-1])
                        assert cutoff >= 0
                        fin.payload = fin.payload[:-1]  # Drop the last fragment
                        if cutoff > 0:
                            fin.payload[-1] = fin.payload[-1][:-cutoff]  # Truncate previous
                    assert expected_length == fin.payload_size
            return TransferFrom(
                timestamp=self._ts,
                priority=priority,
                transfer_id=frame.transfer_id,
                fragmented_payload=fin.payload,
                source_node_id=self._source_node_id,
            )
        return None  # Expect more frames to come


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

    # Unexpected transfer-ID after a timeout; timeout ignored because not a new transfer.
    assert proc(4000, frm(b"123456", 8, False, False, True)) == err.UNEXPECTED_TRANSFER_ID
    # Unexpected toggle after a timeout; timeout ignored because not a new transfer.
    assert proc(4000, frm(b"123456", 9, False, False, False)) == err.UNEXPECTED_TOGGLE_BIT

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
        == TransferReassemblyErrorID.MISSED_START_OF_TRANSFER
    )


def _unittest_issue_288() -> None:  # https://github.com/OpenCyphal/pycyphal/issues/288
    from pytest import approx

    source_node_id = 127
    transfer_id_timeout_ns = int(2 * 1e9)

    def mk_frame(can_id: int, hex_string: str) -> CyphalFrame:
        from ..media import DataFrame, FrameFormat

        df = DataFrame(FrameFormat.EXTENDED, can_id, bytearray(bytes.fromhex(hex_string)))
        out = CyphalFrame.parse(df)
        assert out is not None
        return out

    # In the original repo instructions, the subscription type was uavcan.primitive.scalar.Real16 with extent 2 bytes.
    rx = TransferReassembler(source_node_id, 2)

    def process_frame(time_s: float, frame: CyphalFrame) -> None | TransferReassemblyErrorID | TransferFrom:
        return rx.process_frame(
            timestamp=Timestamp(system_ns=0, monotonic_ns=int(time_s * 1e9)),
            priority=pycyphal.transport.Priority.SLOW,
            frame=frame,
            transfer_id_timeout_ns=transfer_id_timeout_ns,
        )

    # Feed the frames from the capture one by one.
    assert None is process_frame(1681243583.288644, mk_frame(0x10644C7F, "09 30 00 00 00 00 00 B1"))
    assert None is process_frame(1681243583.291624, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243583.294662, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243583.297647, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243583.300635, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243583.303616, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243583.306614, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243583.309578, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243583.312569, mk_frame(0x10644C7F, "00 00 00 00 00 00 10 31"))
    transfer = process_frame(1681243583.315564, mk_frame(0x10644C7F, "4A 51"))

    # The reassembler should have returned a valid transfer.
    assert isinstance(transfer, TransferFrom)
    assert transfer.source_node_id == source_node_id
    assert transfer.transfer_id == 17
    assert len(transfer.fragmented_payload) == 1
    assert bytes(transfer.fragmented_payload[0]).startswith(b"\x09\x30")
    assert float(transfer.timestamp.monotonic) == approx(1681243583.288644, abs=1e-6)
    assert transfer.priority == pycyphal.transport.Priority.SLOW


def _unittest_issue_290() -> None:
    source_node_id = 127
    transfer_id_timeout_ns = 1  # A very low value.

    rx = TransferReassembler(source_node_id, 2)

    def process_frame(time_s: float, frame: CyphalFrame) -> None | TransferReassemblyErrorID | TransferFrom:
        return rx.process_frame(
            timestamp=Timestamp(system_ns=0, monotonic_ns=int(time_s * 1e9)),
            priority=pycyphal.transport.Priority.SLOW,
            frame=frame,
            transfer_id_timeout_ns=transfer_id_timeout_ns,
        )

    def mk_frame(can_id: int, hex_string: str) -> CyphalFrame:
        from ..media import DataFrame, FrameFormat

        df = DataFrame(FrameFormat.EXTENDED, can_id, bytearray(bytes.fromhex(hex_string)))
        out = CyphalFrame.parse(df)
        assert out is not None
        return out

    # Feed a transfer with a large time interval between its frames. Ensure it is accepted.
    assert None is process_frame(1681243583, mk_frame(0x10644C7F, "09 30 00 00 00 00 00 B1"))
    assert None is process_frame(1681243584, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243585, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243586, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243587, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243588, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243589, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 31"))
    assert None is process_frame(1681243590, mk_frame(0x10644C7F, "00 00 00 00 00 00 00 11"))
    assert None is process_frame(1681243591, mk_frame(0x10644C7F, "00 00 00 00 00 00 10 31"))
    transfer = process_frame(1681243592, mk_frame(0x10644C7F, "4A 51"))

    # The reassembler should have returned a valid transfer.
    assert isinstance(transfer, TransferFrom)
    assert transfer.source_node_id == source_node_id
    assert transfer.transfer_id == 17
    assert len(transfer.fragmented_payload) == 1
    assert bytes(transfer.fragmented_payload[0]).startswith(b"\x09\x30")
    assert transfer.priority == pycyphal.transport.Priority.SLOW
