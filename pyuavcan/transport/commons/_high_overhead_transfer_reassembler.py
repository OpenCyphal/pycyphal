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
    """
    In this format we don't know the number of frames in the transfer until the last frame is received.
    This complicates the reception logic somewhat, but the upside is that it enables zero-copy
    serialization and transmission where the number of frames is now known until the last frame is reached.
    This approach differs from other message fragmentation formats such as UDPROS, for example, where the
    number of frames is determined BEFORE the first frame is transmitted.

    The receiver can reassemble transfers where the frames are received out-of-order.
    This also includes the complicated edge case where the first frame of a transfer is not received first.
    Part of the reason why having the number of frames in the transfer reported in the first frame is suboptimal.
    """
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
        # Constant configuration.
        self._source_node_id = int(source_node_id)
        max_payload_size_bytes = int(max_payload_size_bytes)
        assert max_payload_size_bytes >= 0
        self._max_payload_size_bytes_with_crc = max_payload_size_bytes + self.TRANSFER_CRC_LENGTH_BYTES
        # Internal state.
        self._sparse_payload_fragments: typing.List[typing.Optional[memoryview]] = []
        self._last_frame_received = False
        self._timestamp = pyuavcan.transport.Timestamp(0, 0)
        self._transfer_id = 0
        self._inside_transfer = False

    def process_frame(self,
                      timestamp:              pyuavcan.transport.Timestamp,
                      priority:               pyuavcan.transport.Priority,
                      transfer_id:            int,
                      frame_index:            int,
                      end_of_transfer:        bool,
                      payload:                memoryview,
                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, ErrorID, pyuavcan.transport.TransferFrom]:
        output: typing.Union[None, HighOverheadTransferReassembler.ErrorID, pyuavcan.transport.TransferFrom] = None
        if not self._inside_transfer:
            output = self._handle_state_waiting_for_transfer(timestamp=timestamp,
                                                             transfer_id=transfer_id,
                                                             frame_index=frame_index,
                                                             transfer_id_timeout_ns=transfer_id_timeout_ns)
        if self._inside_transfer:
            output = self._handle_state_inside_transfer(timestamp=timestamp,
                                                        priority=priority,
                                                        transfer_id=transfer_id,
                                                        frame_index=frame_index,
                                                        end_of_transfer=end_of_transfer,
                                                        payload=payload,
                                                        transfer_id_timeout_ns=transfer_id_timeout_ns)
        return output

    def _handle_state_waiting_for_transfer(self,
                                           timestamp: pyuavcan.transport.Timestamp,
                                           transfer_id: int,
                                           frame_index: int,
                                           transfer_id_timeout_ns: int) \
            -> typing.Optional[ErrorID]:
        assert not self._sparse_payload_fragments
        if frame_index == 0:
            tid_timed_out = timestamp.monotonic_ns - self._timestamp.monotonic_ns > transfer_id_timeout_ns
            if transfer_id >= self._transfer_id or tid_timed_out:
                self._activate_state_inside_transfer(timestamp, transfer_id)
                return None
            else:
                return self.ErrorID.UNEXPECTED_TRANSFER_ID
        else:
            return self.ErrorID.MISSED_START_OF_TRANSFER

    def _handle_state_inside_transfer(self,
                                      timestamp: pyuavcan.transport.Timestamp,
                                      priority: pyuavcan.transport.Priority,
                                      transfer_id: int,
                                      frame_index: int,
                                      end_of_transfer: bool,
                                      payload: memoryview,
                                      transfer_id_timeout_ns: int) \
            -> typing.Union[None, ErrorID, pyuavcan.transport.TransferFrom]:
        output: typing.Union[None, HighOverheadTransferReassembler.ErrorID, pyuavcan.transport.TransferFrom] = None

        tid_timed_out = timestamp.monotonic_ns - self._timestamp.monotonic_ns > transfer_id_timeout_ns
        if transfer_id > self._transfer_id or tid_timed_out:
            self._activate_state_inside_transfer(timestamp, transfer_id)
            output = self.ErrorID.MISSING_FRAMES

        if self._transfer_id == transfer_id:
            self._last_frame_received = self._last_frame_received or end_of_transfer
            while len(self._sparse_payload_fragments) <= frame_index:
                self._sparse_payload_fragments.append(None)
            self._sparse_payload_fragments[frame_index] = payload

            if self._payload_length <= self._max_payload_size_bytes_with_crc:
                if self._last_frame_received and all(x is not None for x in self._sparse_payload_fragments):
                    fragmented_payload: typing.List[memoryview] = self._sparse_payload_fragments  # type: ignore
                    if len(self._sparse_payload_fragments) == 1:
                        success = True
                    else:
                        success = pyuavcan.transport.commons.crc.CRC32C.new(*fragmented_payload).check_residue()
                        self._cutoff_crc(fragmented_payload)
                    if success:
                        output = pyuavcan.transport.TransferFrom(timestamp=timestamp,
                                                                 priority=priority,
                                                                 transfer_id=transfer_id,
                                                                 fragmented_payload=fragmented_payload,
                                                                 source_node_id=self._source_node_id)
                    else:
                        output = self.ErrorID.TRANSFER_CRC_MISMATCH
            else:
                self._activate_state_waiting_for_transfer()
                output = self.ErrorID.PAYLOAD_TOO_LARGE
        else:
            output = self.ErrorID.UNEXPECTED_TRANSFER_ID

        return output

    def _activate_state_inside_transfer(self,
                                        timestamp: pyuavcan.transport.Timestamp,
                                        transfer_id: int) -> None:
        self._inside_transfer = True
        self._timestamp = timestamp
        self._transfer_id = transfer_id
        self._sparse_payload_fragments = []
        self._last_frame_received = False

    def _activate_state_waiting_for_transfer(self) -> None:
        self._inside_transfer = False
        self._transfer_id += 1
        self._sparse_payload_fragments = []
        self._last_frame_received = False

    @property
    def _payload_length(self) -> int:
        return sum(map(len, filter(None, self._sparse_payload_fragments)))

    @staticmethod
    def _cutoff_crc(fragmented_payload: typing.List[memoryview]) -> None:
        remaining = HighOverheadTransferReassembler.TRANSFER_CRC_LENGTH_BYTES
        expected_length = sum(map(len, fragmented_payload)) - remaining
        assert expected_length >= 0
        while fragmented_payload and remaining > 0:
            if len(fragmented_payload[-1]) <= remaining:
                remaining -= len(fragmented_payload[-1])
                fragmented_payload.pop()
            else:
                fragmented_payload[-1] = fragmented_payload[-1][:-remaining]
                remaining = 0
        assert remaining == 0
        assert sum(map(len, fragmented_payload)) == expected_length


# noinspection PyProtectedMember
def _unittest_cutoff_crc() -> None:
    fp = [memoryview(b'0123456789')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'012345')]

    fp = [memoryview(b'0123456789'), memoryview(b'abcde')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'0123456789'), memoryview(b'a')]

    fp = [memoryview(b'0123456789'), memoryview(b'abcd')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'0123456789')]

    fp = [memoryview(b'0123456789'), memoryview(b'abc')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'012345678')]

    fp = [memoryview(b'0123456789'), memoryview(b'ab')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'01234567')]

    fp = [memoryview(b'0123456789'), memoryview(b'a')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'0123456')]

    fp = [memoryview(b'0123456789'), memoryview(b'')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'012345')]

    fp = [memoryview(b'0123456789'), memoryview(b''), memoryview(b'a'), memoryview(b'b')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == [memoryview(b'01234567')]

    fp = [memoryview(b'01'), memoryview(b''), memoryview(b'a'), memoryview(b'b')]
    HighOverheadTransferReassembler._cutoff_crc(fp)
    assert fp == []
