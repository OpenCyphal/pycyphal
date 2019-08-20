#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import logging
import pyuavcan
from ._frame_base import FrameBase
from ._common import TransferCRC


_logger = logging.getLogger(__name__)


_CRC_SIZE_BYTES = len(TransferCRC().value_as_bytes)


class TransferReassembler:
    """
    Multi-frame transfer reassembly logic is arguably the most complex part of any UAVCAN transport implementation.
    This class implements a highly transport-agnostic transfer reassembly state machine designed for use
    with high-overhead transports, such as UDP, Serial, IEEE 802.15.4, etc.
    Any transport whose frame dataclass implementation derives from :class:`FrameBase` can use this class.

    Out-of-order (OOO) frame reception is supported, and therefore the reassembler can be used with
    redundant interfaces directly, without preliminary frame deduplication procedures or explicit
    interface index assignment.
    Distantly relevant: https://github.com/UAVCAN/specification/issues/8.

    A multi-frame transfer shall not contain frames with empty payload.

    We don't know the number of frames in the transfer until the last frame is received.
    This complicates the reception logic somewhat, but the upside is that it enables zero-copy
    serialization and transmission where the number of frames is not known until the last frame is emitted.
    This approach differs from other message fragmentation formats such as UDPROS, for example, where the
    number of frames is determined BEFORE the first frame is transmitted.

    The receiver can reassemble transfers where the frames are received out-of-order.
    This also includes the edge cases where the first frame of a transfer is not received first
    and the last frame of a transfer is not received last.
    This is another reason why having the number of frames in the transfer reported in the first frame is suboptimal.
    """
    class Error(enum.Enum):
        """
        Error states that the transfer reassembly state machine may encounter.
        Whenever an error is encountered, the corresponding error counter is incremented by one,
        and a verbose report is dumped into the log at the DEBUG level.
        """
        MISSING_FRAMES                  = enum.auto()
        MULTIFRAME_INTEGRITY_ERROR      = enum.auto()
        FRAME_PAST_END_OF_TRANSFER      = enum.auto()
        INCONSISTENT_END_OF_TRANSFER    = enum.auto()
        LARGE_PAYLOAD                   = enum.auto()

    def __init__(self,
                 source_node_id:         int,
                 max_payload_size_bytes: int):
        """
        :param source_node_id: The remote node-ID whose transfers this instance will be listening for.
            Anonymous transfers cannot be multi-frame transfers, so they are to be accepted as-is without any
            reassembly activities.

        :param max_payload_size_bytes: The maximum number of payload bytes per transfer.
            This value can be derived from the corresponding DSDL definition.
        """
        # Constant configuration.
        self._source_node_id = int(source_node_id)
        self._max_payload_size_bytes = int(max_payload_size_bytes)
        if self._source_node_id < 0 or self._max_payload_size_bytes < 0:
            raise ValueError('Invalid parameters')

        # Internal state.
        self._payloads: typing.List[memoryview] = []            # Payload fragments from the received frames.
        self._max_index: typing.Optional[int] = None            # Max frame index in transfer, None if unknown.
        self._timestamp = pyuavcan.transport.Timestamp(0, 0)    # First frame timestamp.
        self._transfer_id = 0                                   # Transfer-ID of the current transfer.
        self._error_counters = {e: 0 for e in self.Error}

    def process_frame(self,
                      frame:               FrameBase,
                      transfer_id_timeout: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        """
        Updates the transfer reassembly state machine with the new frame.
        :param frame: The new frame. Standard deviation of the reception timestamp error should be under 10 ms.
        :param transfer_id_timeout: The current value of the transfer-ID timeout.
        :return: A new transfer if the new frame completed one. None if the new frame did not complete a transfer.
        :raises: Nothing.
        """
        # DETECT NEW TRANSFERS. Either a newer TID or TID-timeout is reached.
        if frame.transfer_id > self._transfer_id or \
                frame.timestamp.monotonic - self._timestamp.monotonic > transfer_id_timeout:
            self._begin_transfer(frame.timestamp,
                                 frame.transfer_id,
                                 self.Error.MISSING_FRAMES if self._payloads else None)

        # DROP FRAMES FROM NON-MATCHING TRANSFERS. E.g., duplicates. This is not an error.
        if frame.transfer_id < self._transfer_id:
            return None
        assert frame.transfer_id == self._transfer_id

        # DETERMINE MAX FRAME INDEX FOR THIS TRANSFER. Frame N with EOT, then frame M with EOT, where N != M.
        if frame.end_of_transfer:
            if self._max_index is not None and self._max_index != frame.index:
                self._begin_transfer(frame.timestamp,
                                     frame.transfer_id + 1,
                                     self.Error.INCONSISTENT_END_OF_TRANSFER)
                return None
            assert self._max_index is None or self._max_index == frame.index
            self._max_index = frame.index

        # DETECT UNEXPECTED FRAMES PAST THE END OF TRANSFER. If EOT is set on index N, then indexes > N are invalid.
        if self._max_index is not None and max(frame.index, len(self._payloads) - 1) > self._max_index:
            self._begin_transfer(frame.timestamp,
                                 frame.transfer_id + 1,
                                 self.Error.FRAME_PAST_END_OF_TRANSFER)
            return None

        # ACCEPT THE PAYLOAD. Duplicates are accepted too, assuming they carry the same payload.
        while len(self._payloads) <= frame.index:
            self._payloads.append(memoryview(b''))
        self._payloads[frame.index] = frame.payload

        # ENFORCE PAYLOAD SIZE. Don't let a babbling sender exhaust our memory quota.
        if self._pure_payload_size_bytes > self._max_payload_size_bytes:
            self._begin_transfer(frame.timestamp,
                                 frame.transfer_id + 1,
                                 self.Error.LARGE_PAYLOAD)
            return None

        # CHECK IF ALL FRAMES ARE RECEIVED. If not, simply wait for next frame.
        if self._max_index is None or not all(self._payloads):
            return None

        # FINALIZE THE TRANSFER. All frames are received here.
        result = _validate_and_finalize_transfer(timestamp=self._timestamp,
                                                 priority=frame.priority,
                                                 transfer_id=frame.transfer_id,
                                                 frame_payloads=self._payloads,
                                                 source_node_id=self._source_node_id)
        self._begin_transfer(frame.timestamp,
                             frame.transfer_id + 1,
                             self.Error.MULTIFRAME_INTEGRITY_ERROR if result is None else None)
        return result

    def _begin_transfer(self,
                        timestamp:   pyuavcan.transport.Timestamp,
                        transfer_id: int,
                        error:       typing.Optional[TransferReassembler.Error] = None) -> None:
        if error is not None:
            self._error_counters[error] += 1
            if _logger.isEnabledFor(logging.DEBUG):  # pragma: no branch
                context = {
                    'TS':   self._timestamp,
                    'TID':  self._transfer_id,
                    'MI':   self._max_index,
                    'PAY':  f'{len(list(x for x in self._payloads if x))}/{len(self._payloads)}',
                }
                _logger.debug(f'{self}: {error}: ' + ' '.join(f'{k}={v}' for k, v in context.items()))
                _logger.debug(f'{self}: {self._error_counters}')
        # The error must be processed before the state is reset because when the state is destroyed
        # the useful diagnostic information becomes unavailable.
        self._timestamp = timestamp
        self._transfer_id = transfer_id
        self._max_index = None
        self._payloads = []

    @property
    def error_counters(self) -> typing.Dict[TransferReassembler.Error, int]:
        """
        Error statistics. The returned value is a clone, so it can be modified without affecting the origin.
        """
        return self._error_counters.copy()

    @property
    def _pure_payload_size_bytes(self) -> int:
        """May return a negative if the transfer is malformed."""
        size = sum(map(len, self._payloads))
        if len(self._payloads) > 1:
            size -= _CRC_SIZE_BYTES
        return size

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes_noexcept(self,
                                                      source_node_id=self._source_node_id,
                                                      max_payload_size_bytes=self._max_payload_size_bytes)


def _validate_and_finalize_transfer(timestamp:      pyuavcan.transport.Timestamp,
                                    priority:       pyuavcan.transport.Priority,
                                    transfer_id:    int,
                                    frame_payloads: typing.List[memoryview],
                                    source_node_id: int) -> typing.Optional[pyuavcan.transport.TransferFrom]:
    assert all(isinstance(x, memoryview) for x in frame_payloads)
    assert frame_payloads

    def package(fragmented_payload: typing.Sequence[memoryview]) -> pyuavcan.transport.TransferFrom:
        return pyuavcan.transport.TransferFrom(timestamp=timestamp,
                                               priority=priority,
                                               transfer_id=transfer_id,
                                               fragmented_payload=fragmented_payload,
                                               source_node_id=source_node_id)

    if len(frame_payloads) > 1:
        size_ok = sum(map(len, frame_payloads)) > _CRC_SIZE_BYTES
        crc_ok = TransferCRC.new(*frame_payloads).check_residue()
        return package(_drop_crc(frame_payloads)) if size_ok and crc_ok else None
    else:
        return package(frame_payloads)


def _drop_crc(fragments: typing.List[memoryview]) -> typing.Sequence[memoryview]:
    remaining = _CRC_SIZE_BYTES
    while fragments and remaining > 0:
        if len(fragments[-1]) <= remaining:
            remaining -= len(fragments[-1])
            fragments.pop()
        else:
            fragments[-1] = fragments[-1][:-remaining]
            remaining = 0
    return fragments


# noinspection PyProtectedMember
def _unittest_drop_crc() -> None:
    fp = [memoryview(b'0123456789')]
    assert _drop_crc(fp) == [memoryview(b'012345')]

    fp = [memoryview(b'0123456789'), memoryview(b'abcde')]
    assert _drop_crc(fp) == [memoryview(b'0123456789'), memoryview(b'a')]

    fp = [memoryview(b'0123456789'), memoryview(b'abcd')]
    assert _drop_crc(fp) == [memoryview(b'0123456789')]

    fp = [memoryview(b'0123456789'), memoryview(b'abc')]
    assert _drop_crc(fp) == [memoryview(b'012345678')]

    fp = [memoryview(b'0123456789'), memoryview(b'ab')]
    assert _drop_crc(fp) == [memoryview(b'01234567')]

    fp = [memoryview(b'0123456789'), memoryview(b'a')]
    assert _drop_crc(fp) == [memoryview(b'0123456')]

    fp = [memoryview(b'0123456789'), memoryview(b'')]
    assert _drop_crc(fp) == [memoryview(b'012345')]

    fp = [memoryview(b'0123456789'), memoryview(b''), memoryview(b'a'), memoryview(b'b')]
    assert _drop_crc(fp) == [memoryview(b'01234567')]

    fp = [memoryview(b'01'), memoryview(b''), memoryview(b'a'), memoryview(b'b')]
    assert _drop_crc(fp) == []

    fp = [memoryview(b'0'), memoryview(b''), memoryview(b'a'), memoryview(b'b')]  # Too short
    assert _drop_crc(fp) == []

    fp = [memoryview(b'')]  # Too short
    assert _drop_crc(fp) == []

    fp = []  # Too short
    assert _drop_crc(fp) == []
