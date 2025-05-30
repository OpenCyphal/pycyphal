# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import enum
import typing
import logging
import pycyphal
from pycyphal.transport import Timestamp, Priority, TransferFrom

from ._frame import Frame
from ._common import TransferCRC


_logger = logging.getLogger(__name__)


_CRC_SIZE_BYTES = len(TransferCRC().value_as_bytes)


class TransferReassembler:
    """
    Multi-frame transfer reassembly logic is arguably the most complex part of any Cyphal transport implementation.
    This class implements a highly transport-agnostic transfer reassembly state machine designed for use
    with high-overhead transports, such as UDP, Serial, IEEE 802.15.4, etc.
    Any transport whose frame dataclass implementation derives from :class:`Frame` can use this class.

    Out-of-order frame reception is supported, and therefore the reassembler can be used with
    redundant interfaces directly, without preliminary frame deduplication procedures or explicit
    interface index assignment, provided that all involved redundant interfaces share the same MTU setting.
    OOO support includes edge cases where the first frame of a transfer is not received first and/or the last
    frame is not received last.

    OOO is required for frame-level modular transport redundancy (more than one transport operating concurrently)
    and temporal transfer redundancy (every transfer repeated several times to mitigate frame loss).
    The necessity of OOO is due to the fact that frames sourced concurrently from multiple transport interfaces
    and/or frames of a temporally redundant transfer where some of the frames are lost
    result in an out-of-order arrival of the frames.
    Additionally, various non-vehicular and/or non-mission-critical networks
    (such as conventional IP networks) may deliver frames out-of-order even without redundancy.

    Distantly relevant discussion: https://github.com/OpenCyphal/specification/issues/8.

    A multi-frame transfer shall not contain frames with empty payload.
    """

    class Error(enum.Enum):
        """
        Error states that the transfer reassembly state machine may encounter.
        Whenever an error is encountered, the corresponding error counter is incremented by one,
        and a verbose report is dumped into the log at the DEBUG level.
        """

        INTEGRITY_ERROR = enum.auto()
        """
        A transfer payload did not pass integrity checks. Transfer discarded.
        """

        UNEXPECTED_TRANSFER_ID = enum.auto()
        """
        The transfer-ID of a frame does not match the anticipated value.
        """

        MULTIFRAME_MISSING_FRAMES = enum.auto()
        """
        New transfer started before the old one could be completed. Old transfer discarded.
        """

        MULTIFRAME_EMPTY_FRAME = enum.auto()
        """
        A frame without payload received as part of a multiframe transfer (not permitted by Specification).
        Only single-frame transfers can have empty payload.
        """

        MULTIFRAME_EOT_MISPLACED = enum.auto()
        """
        The end-of-transfer flag is set in a frame with index N,
        but the transfer contains at least one frame with index > N. Transfer discarded.
        """

        MULTIFRAME_EOT_INCONSISTENT = enum.auto()
        """
        The end-of-transfer flag is set in frames with indexes N and M, where N != M. Transfer discarded.
        """

    def __init__(
        self,
        source_node_id: int,
        extent_bytes: int,
        on_error_callback: typing.Callable[[TransferReassembler.Error], None],
    ):
        """
        :param source_node_id: The remote node-ID whose transfers this instance will be listening for.
            Anonymous transfers cannot be multi-frame transfers, so they are to be accepted as-is without any
            reassembly activities.

        :param extent_bytes: The maximum number of payload bytes per transfer.
            Payload that exceeds this size limit may be implicitly truncated (in the Specification this behavior
            is described as "implicit truncation rule").
            This value can be derived from the corresponding DSDL definition.
            Note that the reassembled payload may still be larger than this value.

        :param on_error_callback: The callback is invoked whenever an error is detected.
            This is intended for diagnostic purposes only; the error information is not actionable.
            The error is logged by the caller at the DEBUG verbosity level together with reassembly context info.
        """
        # Constant configuration.
        self._source_node_id = int(source_node_id)
        self._extent_bytes = int(extent_bytes)
        self._on_error_callback = on_error_callback
        if self._source_node_id < 0 or self._extent_bytes < 0 or not callable(self._on_error_callback):
            raise ValueError("Invalid parameters")

        # Internal state.
        self._payloads: typing.List[memoryview] = []  # Payload fragments from the received frames.
        self._max_index: typing.Optional[int] = None  # Max frame index in transfer, None if unknown.
        self._ts = Timestamp(0, 0)
        self._transfer_id = 0  # Transfer-ID of the current transfer.

    def process_frame(
        self, timestamp: Timestamp, frame: Frame, transfer_id_timeout: float
    ) -> typing.Optional[TransferFrom]:
        """
        Updates the transfer reassembly state machine with the new frame.

        :param timestamp: The reception timestamp from the transport layer.
        :param frame: The new frame.
        :param transfer_id_timeout: The current value of the transfer-ID timeout.
        :return: A new transfer if the new frame completed one. None if the new frame did not complete a transfer.
        :raises: Nothing.
        """
        # DROP MALFORMED FRAMES. A multi-frame transfer cannot contain frames with no payload.
        if not frame.single_frame_transfer and not frame.payload:
            self._on_error_callback(self.Error.MULTIFRAME_EMPTY_FRAME)
            return None

        # DETECT NEW TRANSFERS. Either a newer TID or TID-timeout is reached.
        # Restarting the transfer reassembly only makes sense if the new frame is a start of transfer.
        # Otherwise, the new transfer would be impossible to reassemble anyway since the first frame is lost.
        # As we can reassemble transfers with out-of-order frames, we need to also take into account the case
        # when the first frame arrives when we already have some data from this transfer stored,
        # in which case we must suppress the transfer-ID condition.
        is_future_transfer_id = frame.transfer_id > self._transfer_id
        is_tid_timeout = (
            frame.index == 0
            and frame.transfer_id != self._transfer_id
            and timestamp.monotonic - self._ts.monotonic > transfer_id_timeout
        )
        if is_future_transfer_id or is_tid_timeout:
            self._restart(frame.transfer_id, self.Error.MULTIFRAME_MISSING_FRAMES if self._payloads else None)
        if frame.transfer_id != self._transfer_id:
            self._on_error_callback(self.Error.UNEXPECTED_TRANSFER_ID)
            return None
        assert frame.transfer_id == self._transfer_id

        # DETERMINE MAX FRAME INDEX FOR THIS TRANSFER. Frame N with EOT, then frame M with EOT, where N != M.
        if frame.end_of_transfer:
            if self._max_index is not None and self._max_index != frame.index:
                self._restart(frame.transfer_id + 1, self.Error.MULTIFRAME_EOT_INCONSISTENT)
                return None
            assert self._max_index is None or self._max_index == frame.index
            self._max_index = frame.index

        # DETECT UNEXPECTED FRAMES PAST THE END OF TRANSFER. If EOT is set on index N, then indexes > N are invalid.
        if self._max_index is not None and max(frame.index, len(self._payloads) - 1) > self._max_index:
            self._restart(frame.transfer_id + 1, self.Error.MULTIFRAME_EOT_MISPLACED)
            return None

        # DETERMINE THE TRANSFER TIMESTAMP. It is the timestamp of the first frame in this implementation.
        # It may also be defined as the timestamp of the earliest frame in the transfer.
        if frame.index == 0:
            self._ts = timestamp

        # ACCEPT THE PAYLOAD. Duplicates are accepted too, assuming they carry the same payload.
        # Implicit truncation is implemented by not limiting the maximum payload size.
        # Real truncation is hard to implement if frames are delivered out-of-order, although it's not impossible:
        # instead of storing actual payload fragments above the limit, we can store their CRCs.
        # When the last fragment is received, CRC of all fragments are then combined to validate the final transfer-CRC.
        # This method, however, requires knowledge of the MTU to determine which fragments will be above the limit.
        while len(self._payloads) <= frame.index:
            self._payloads.append(memoryview(b""))
        self._payloads[frame.index] = frame.payload

        # CHECK IF ALL FRAMES ARE RECEIVED. If not, simply wait for next frame.
        # Single-frame transfers with empty payload are legal.
        if self._max_index is None or (self._max_index > 0 and not all(self._payloads)):
            return None
        assert self._max_index is not None
        assert self._max_index == len(self._payloads) - 1
        assert all(self._payloads) if self._max_index > 0 else True

        # FINALIZE THE TRANSFER. All frames are received here.
        result = _validate_and_finalize_transfer(
            timestamp=self._ts,
            priority=frame.priority,
            transfer_id=frame.transfer_id,
            frame_payloads=self._payloads,
            source_node_id=self._source_node_id,
        )

        self._restart(frame.transfer_id + 1, self.Error.INTEGRITY_ERROR if result is None else None)
        _logger.debug("Transfer reassembly completed: %s", result)
        # This implementation does not perform implicit truncation yet.
        # This may be changed in the future if it is found to benefit the performance.
        # The API contract does not provide any guarantees about whether the returned transfer is truncated or not.
        return result

    @property
    def source_node_id(self) -> int:
        return self._source_node_id

    def _restart(self, transfer_id: int, error: typing.Optional[TransferReassembler.Error] = None) -> None:
        if error is not None:
            self._on_error_callback(error)
            if _logger.isEnabledFor(logging.DEBUG):  # pragma: no branch
                context = {
                    "ts": self._ts,
                    "tid": self._transfer_id,
                    "max_idx": self._max_index,
                    "payload": f"{len(list(x for x in self._payloads if x))}/{len(self._payloads)}",
                }
                _logger.debug(  # pylint: disable=logging-not-lazy
                    f"{self}: {error.name}: " + " ".join(f"{k}={v}" for k, v in context.items())
                )
        # The error must be processed before the state is reset because when the state is destroyed
        # the useful diagnostic information becomes unavailable.
        self._transfer_id = transfer_id
        self._max_index = None
        self._payloads = []

    @property
    def _pure_payload_size_bytes(self) -> int:
        """May return a negative if the transfer is malformed."""
        size = sum(map(len, self._payloads))
        if len(self._payloads) > 1:
            size -= _CRC_SIZE_BYTES
        return size

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes_noexcept(
            self, source_node_id=self._source_node_id, extent_bytes=self._extent_bytes
        )

    @staticmethod
    def construct_anonymous_transfer(timestamp: Timestamp, frame: Frame) -> typing.Optional[TransferFrom]:
        """
        A minor helper that validates whether the frame is a valid anonymous transfer (it is if the index
        is zero, the end-of-transfer flag is set and crc checks out) and constructs a transfer instance if it is.
        Otherwise, returns None.
        Observe that this is a static method because anonymous transfers are fundamentally stateless.
        """
        if frame.single_frame_transfer:
            size_ok = frame.payload.nbytes > _CRC_SIZE_BYTES
            crc_ok = TransferCRC.new(frame.payload).check_residue()
            return (
                TransferFrom(
                    timestamp=timestamp,
                    priority=frame.priority,
                    transfer_id=frame.transfer_id,
                    fragmented_payload=_drop_crc([frame.payload]),
                    source_node_id=None,
                )
                if size_ok and crc_ok
                else None
            )
        return None


def _validate_and_finalize_transfer(
    timestamp: Timestamp,
    priority: Priority,
    transfer_id: int,
    frame_payloads: typing.List[memoryview],
    source_node_id: int,
) -> typing.Optional[TransferFrom]:
    assert all(isinstance(x, memoryview) for x in frame_payloads)
    assert frame_payloads

    def package(fragmented_payload: typing.Sequence[memoryview]) -> TransferFrom:
        return TransferFrom(
            timestamp=timestamp,
            priority=priority,
            transfer_id=transfer_id,
            fragmented_payload=fragmented_payload,
            source_node_id=source_node_id,
        )

    if len(frame_payloads) > 1:
        _logger.debug("Finalizing multiframe transfer...")
        size_ok = sum(map(len, frame_payloads)) > _CRC_SIZE_BYTES
    else:
        _logger.debug("Finalizing uniframe transfer...")
        # if equals _CRC_SIZE_BYTES, then it is an empty single-frame transfer
        size_ok = len(frame_payloads[0]) >= _CRC_SIZE_BYTES
    crc_ok = TransferCRC.new(*frame_payloads).check_residue()
    return package(_drop_crc(frame_payloads)) if size_ok and crc_ok else None


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


# ----------------------------------------  TESTS BELOW THIS LINE  ----------------------------------------


def _unittest_transfer_reassembler() -> None:
    from pytest import raises

    src_nid = 1234
    prio = Priority.SLOW
    transfer_id_timeout = 1.0

    error_counters = {e: 0 for e in TransferReassembler.Error}

    def on_error_callback(error: TransferReassembler.Error) -> None:
        error_counters[error] += 1

    def mk_frame(
        transfer_id: int, index: int, end_of_transfer: bool, payload: typing.Union[bytes, memoryview]
    ) -> Frame:
        return Frame(
            priority=prio,
            transfer_id=transfer_id,
            index=index,
            end_of_transfer=end_of_transfer,
            payload=memoryview(payload),
        )

    def mk_transfer(
        timestamp: Timestamp, transfer_id: int, fragmented_payload: typing.Sequence[typing.Union[bytes, memoryview]]
    ) -> TransferFrom:
        return TransferFrom(
            timestamp=timestamp,
            priority=prio,
            transfer_id=transfer_id,
            fragmented_payload=list(map(memoryview, fragmented_payload)),  # type: ignore
            source_node_id=src_nid,
        )

    def mk_ts(monotonic: float) -> Timestamp:
        monotonic_ns = round(monotonic * 1e9)
        return Timestamp(system_ns=monotonic_ns + 10**12, monotonic_ns=monotonic_ns)

    with raises(ValueError):
        _ = TransferReassembler(source_node_id=-1, extent_bytes=100, on_error_callback=on_error_callback)

    with raises(ValueError):
        _ = TransferReassembler(source_node_id=0, extent_bytes=-1, on_error_callback=on_error_callback)

    ta = TransferReassembler(source_node_id=src_nid, extent_bytes=100, on_error_callback=on_error_callback)
    assert ta.source_node_id == src_nid

    def push(timestamp: Timestamp, frame: Frame) -> typing.Optional[TransferFrom]:
        return ta.process_frame(timestamp, frame, transfer_id_timeout=transfer_id_timeout)

    hedgehog = b"In the evenings, the little Hedgehog went to the Bear Cub to count stars."
    horse = b"He thought about the Horse: how was she doing there, in the fog?"

    # Valid single-frame transfer.
    assert push(
        mk_ts(1000.0),
        mk_frame(
            transfer_id=0, index=0, end_of_transfer=True, payload=hedgehog + TransferCRC.new(hedgehog).value_as_bytes
        ),
    ) == mk_transfer(timestamp=mk_ts(1000.0), transfer_id=0, fragmented_payload=[hedgehog])

    # Same transfer-ID; transfer ignored, no error registered.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(
                transfer_id=0,
                index=0,
                end_of_transfer=True,
                payload=hedgehog + TransferCRC.new(hedgehog).value_as_bytes,
            ),
        )
        is None
    )

    # Same transfer-ID, different EOT; transfer ignored, no error registered.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(
                transfer_id=0,
                index=0,
                end_of_transfer=False,
                payload=hedgehog + TransferCRC.new(hedgehog).value_as_bytes,
            ),
        )
        is None
    )

    # Valid multi-frame transfer.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=2, index=0, end_of_transfer=False, payload=hedgehog[:50]),
        )
        is None
    )
    assert push(
        mk_ts(1000.0),
        mk_frame(
            transfer_id=2,
            index=1,
            end_of_transfer=True,
            payload=hedgehog[50:] + TransferCRC.new(hedgehog).value_as_bytes,
        ),
    ) == mk_transfer(timestamp=mk_ts(1000.0), transfer_id=2, fragmented_payload=[hedgehog[:50], hedgehog[50:]])

    # Same as above, but the frame ordering is reversed.
    assert (
        push(
            mk_ts(1000.0),  # LAST FRAME
            mk_frame(transfer_id=10, index=2, end_of_transfer=True, payload=TransferCRC.new(hedgehog).value_as_bytes),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert push(
        mk_ts(1000.0),  # FIRST FRAME
        mk_frame(transfer_id=10, index=0, end_of_transfer=False, payload=hedgehog[:50]),
    ) == mk_transfer(timestamp=mk_ts(1000.0), transfer_id=10, fragmented_payload=[hedgehog[:50], hedgehog[50:]])

    # Same as above, but one frame is duplicated and one is ignored with old TID, plus an empty frame in the middle.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),  # OLD TID
            mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),  # LAST FRAME
            mk_frame(transfer_id=11, index=2, end_of_transfer=True, payload=TransferCRC.new(hedgehog).value_as_bytes),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),  # DUPLICATE OF INDEX 1
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),  # OLD TID
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),  # MALFORMED FRAME (no payload), ignored
            mk_frame(transfer_id=9999999999, index=0, end_of_transfer=False, payload=b""),
        )
        is None
    )
    assert push(
        mk_ts(1000.0),  # FIRST FRAME
        mk_frame(transfer_id=11, index=0, end_of_transfer=False, payload=hedgehog[:50]),
    ) == mk_transfer(timestamp=mk_ts(1000.0), transfer_id=11, fragmented_payload=[hedgehog[:50], hedgehog[50:]])

    # Valid multi-frame transfer with payload size above the limit.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=102, index=0, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=102, index=1, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=102, index=2, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert push(
        mk_ts(1000.0),
        mk_frame(
            transfer_id=102,
            index=3,
            end_of_transfer=True,
            payload=hedgehog + TransferCRC.new(hedgehog * 4).value_as_bytes,
        ),
    ) == mk_transfer(
        timestamp=mk_ts(1000.0),
        transfer_id=102,
        fragmented_payload=[hedgehog] * 4,  # This implementation does not truncate the payload yet.
    )

    # Same as above, but the frames are reordered.
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=103, index=2, end_of_transfer=False, payload=horse),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(
                transfer_id=103,
                index=3,
                end_of_transfer=True,
                payload=horse + TransferCRC.new(horse * 4).value_as_bytes,
            ),
        )
        is None
    )
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=103, index=1, end_of_transfer=False, payload=horse),
        )
        is None
    )
    assert push(
        mk_ts(1000.0),
        mk_frame(transfer_id=103, index=0, end_of_transfer=False, payload=horse),
    ) == mk_transfer(
        timestamp=mk_ts(1000.0),
        transfer_id=103,
        fragmented_payload=[horse] * 4,  # This implementation does not truncate the payload yet.
    )

    # Transfer-ID timeout. No error registered.
    assert push(
        mk_ts(2000.0),
        mk_frame(
            transfer_id=0, index=0, end_of_transfer=True, payload=hedgehog + TransferCRC.new(hedgehog).value_as_bytes
        ),
    ) == mk_transfer(timestamp=mk_ts(2000.0), transfer_id=0, fragmented_payload=[hedgehog])
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 4,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 0,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }

    # Start a transfer, then start a new one with higher TID.
    assert (
        push(
            mk_ts(3000.0),  # Middle of a new transfer.
            mk_frame(transfer_id=2, index=1, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert (
        push(
            mk_ts(3000.0),  # Another transfer! The old one is discarded.
            mk_frame(transfer_id=3, index=1, end_of_transfer=False, payload=horse[50:]),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 4,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 1,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }
    assert (
        push(
            mk_ts(3000.0),
            mk_frame(transfer_id=3, index=2, end_of_transfer=True, payload=TransferCRC.new(horse).value_as_bytes),
        )
        is None
    )
    assert push(
        mk_ts(3000.0),
        mk_frame(transfer_id=3, index=0, end_of_transfer=False, payload=horse[:50]),
    ) == mk_transfer(timestamp=mk_ts(3000.0), transfer_id=3, fragmented_payload=[horse[:50], horse[50:]])
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 4,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 1,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }

    # Start a transfer, then start a new one with lower TID when a TID timeout is reached.
    # The new one will not be accepted.
    assert (
        push(
            mk_ts(3000.0),  # Middle of a new transfer.
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert (
        push(
            mk_ts(4000.0),  # Another transfer! Its TID is greater so it takes over.
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=horse[50:]),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 4,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 2,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }
    assert (
        push(
            mk_ts(4000.0),
            mk_frame(transfer_id=11, index=2, end_of_transfer=True, payload=TransferCRC.new(horse).value_as_bytes),
        )
        is None
    )
    assert push(
        mk_ts(4000.0),
        mk_frame(transfer_id=11, index=0, end_of_transfer=False, payload=horse[:50]),
    ) == mk_transfer(timestamp=mk_ts(4000.0), transfer_id=11, fragmented_payload=[horse[:50], horse[50:]])
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 4,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 2,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }

    # Start a transfer, then start a new one with lower TID when a TID timeout is reached.
    # The new one will not be accepted.
    assert (
        push(
            mk_ts(5000.0),  # Middle of a new transfer.
            mk_frame(transfer_id=13, index=1, end_of_transfer=False, payload=hedgehog),
        )
        is None
    )
    assert (
        push(
            mk_ts(6000.0),  # Another transfer! It is still ignored though because SOT is not set.
            mk_frame(transfer_id=3, index=1, end_of_transfer=False, payload=horse[50:]),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 0,
        ta.Error.UNEXPECTED_TRANSFER_ID: 5,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 2,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }
    assert (
        push(
            mk_ts(6000.0),
            mk_frame(transfer_id=3, index=2, end_of_transfer=True, payload=TransferCRC.new(horse).value_as_bytes),
        )
        is None
    )
    assert (
        push(
            mk_ts(6000.0),
            mk_frame(transfer_id=3, index=0, end_of_transfer=False, payload=horse[:50]),
        )
        is None
    )

    # Multi-frame transfer with bad CRC.
    assert (
        push(
            mk_ts(7000.0),
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(7000.0),  # LAST FRAME
            mk_frame(
                transfer_id=10, index=2, end_of_transfer=True, payload=TransferCRC.new(hedgehog).value_as_bytes[::-1]
            ),  # Bad CRC here.
        )
        is None
    )
    assert (
        push(
            mk_ts(7000.0),  # FIRST FRAME
            mk_frame(transfer_id=10, index=0, end_of_transfer=False, payload=hedgehog[:50]),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 1,
        ta.Error.UNEXPECTED_TRANSFER_ID: 6,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 4,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 0,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }

    # Frame past end of transfer.
    assert (
        push(
            mk_ts(8000.0),
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=hedgehog[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(8000.0),  # PAST THE END OF TRANSFER
            mk_frame(transfer_id=11, index=3, end_of_transfer=False, payload=horse),
        )
        is None
    )
    assert (
        push(
            mk_ts(8000.0),  # LAST FRAME
            mk_frame(
                transfer_id=11, index=2, end_of_transfer=True, payload=TransferCRC.new(hedgehog + horse).value_as_bytes
            ),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 1,
        ta.Error.UNEXPECTED_TRANSFER_ID: 6,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 4,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 1,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 0,
    }

    # Inconsistent end-of-transfer flag.
    assert (
        push(
            mk_ts(9000.0),
            mk_frame(transfer_id=12, index=0, end_of_transfer=False, payload=hedgehog[:50]),
        )
        is None
    )
    assert (
        push(
            mk_ts(9000.0),  # LAST FRAME A
            mk_frame(
                transfer_id=12, index=2, end_of_transfer=True, payload=TransferCRC.new(hedgehog + horse).value_as_bytes
            ),
        )
        is None
    )
    assert (
        push(
            mk_ts(9000.0),  # LAST FRAME B
            mk_frame(transfer_id=12, index=3, end_of_transfer=True, payload=horse),
        )
        is None
    )
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 1,
        ta.Error.UNEXPECTED_TRANSFER_ID: 6,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 4,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 1,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 1,
    }

    # Valid single-frame transfer with no payload.
    assert push(
        mk_ts(10000.0),
        mk_frame(transfer_id=0, index=0, end_of_transfer=True, payload=b"" + TransferCRC.new(b"").value_as_bytes),
    ) == mk_transfer(
        timestamp=mk_ts(10000.0), transfer_id=0, fragmented_payload=[]
    )  # fragmented_payload = [b""]?
    assert error_counters == {
        ta.Error.INTEGRITY_ERROR: 1,
        ta.Error.UNEXPECTED_TRANSFER_ID: 6,
        ta.Error.MULTIFRAME_MISSING_FRAMES: 4,
        ta.Error.MULTIFRAME_EMPTY_FRAME: 1,
        ta.Error.MULTIFRAME_EOT_MISPLACED: 1,
        ta.Error.MULTIFRAME_EOT_INCONSISTENT: 1,
    }


def _unittest_issue_290() -> None:
    src_nid = 1234
    prio = Priority.HIGH
    transfer_id_timeout = 1e-6  # A very low value.
    error_counters = {e: 0 for e in TransferReassembler.Error}

    def mk_frame(
        transfer_id: int, index: int, end_of_transfer: bool, payload: typing.Union[bytes, memoryview]
    ) -> Frame:
        return Frame(
            priority=prio,
            transfer_id=transfer_id,
            index=index,
            end_of_transfer=end_of_transfer,
            payload=memoryview(payload),
        )

    def mk_transfer(
        timestamp: Timestamp, transfer_id: int, fragmented_payload: typing.Sequence[typing.Union[bytes, memoryview]]
    ) -> TransferFrom:
        return TransferFrom(
            timestamp=timestamp,
            priority=prio,
            transfer_id=transfer_id,
            fragmented_payload=list(map(memoryview, fragmented_payload)),  # type: ignore
            source_node_id=src_nid,
        )

    def mk_ts(monotonic: float) -> Timestamp:
        monotonic_ns = round(monotonic * 1e9)
        return Timestamp(system_ns=monotonic_ns + 10**12, monotonic_ns=monotonic_ns)

    def on_error_callback(error: TransferReassembler.Error) -> None:
        error_counters[error] += 1

    ta = TransferReassembler(source_node_id=src_nid, extent_bytes=100, on_error_callback=on_error_callback)
    assert ta.source_node_id == src_nid

    def push(timestamp: Timestamp, frame: Frame) -> typing.Optional[TransferFrom]:
        return ta.process_frame(timestamp, frame, transfer_id_timeout=transfer_id_timeout)

    solipsism = b"The word you are looking for is Solipsism. But you are mistaken. This is not solipsism."

    # Valid multi-frame transfer with large interval between its frames (enough to trigger a TID timeout).
    assert (
        push(
            mk_ts(1000.0),
            mk_frame(transfer_id=2, index=0, end_of_transfer=False, payload=solipsism[:50]),
        )
        is None
    )
    assert push(
        mk_ts(1001.0),
        mk_frame(
            transfer_id=2,
            index=1,
            end_of_transfer=True,
            payload=solipsism[50:] + TransferCRC.new(solipsism).value_as_bytes,
        ),
    ) == mk_transfer(timestamp=mk_ts(1000.0), transfer_id=2, fragmented_payload=[solipsism[:50], solipsism[50:]])

    # Same as above, but the frame ordering is reversed.
    assert (
        push(
            mk_ts(1002.0),  # LAST FRAME
            mk_frame(transfer_id=10, index=2, end_of_transfer=True, payload=TransferCRC.new(solipsism).value_as_bytes),
        )
        is None
    )
    assert (
        push(
            mk_ts(1003.0),
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=solipsism[50:]),
        )
        is None
    )
    assert push(
        mk_ts(2000.0),  # FIRST FRAME
        mk_frame(transfer_id=10, index=0, end_of_transfer=False, payload=solipsism[:50]),
    ) == mk_transfer(timestamp=mk_ts(2000.0), transfer_id=10, fragmented_payload=[solipsism[:50], solipsism[50:]])

    # Same as above, but one frame is duplicated and one is ignored with old TID, plus an empty frame in the middle.
    assert (
        push(
            mk_ts(3000.0),
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=solipsism[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(3010.0),  # OLD TID
            mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=solipsism[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(3020.0),  # LAST FRAME
            mk_frame(transfer_id=11, index=2, end_of_transfer=True, payload=TransferCRC.new(solipsism).value_as_bytes),
        )
        is None
    )
    assert (
        push(
            mk_ts(3030.0),  # DUPLICATE OF INDEX 1
            mk_frame(transfer_id=11, index=1, end_of_transfer=False, payload=solipsism[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(3040.0),  # OLD TID
            mk_frame(transfer_id=10, index=1, end_of_transfer=False, payload=solipsism[50:]),
        )
        is None
    )
    assert (
        push(
            mk_ts(3050.0),  # MALFORMED FRAME (no payload), ignored
            mk_frame(transfer_id=9999999999, index=0, end_of_transfer=False, payload=b""),
        )
        is None
    )
    assert push(
        mk_ts(3060.0),  # FIRST FRAME
        mk_frame(transfer_id=11, index=0, end_of_transfer=False, payload=solipsism[:50]),
    ) == mk_transfer(timestamp=mk_ts(3060.0), transfer_id=11, fragmented_payload=[solipsism[:50], solipsism[50:]])


def _unittest_transfer_reassembler_anonymous() -> None:
    ts = Timestamp.now()
    prio = Priority.LOW

    # Correct single-frame transfer.
    assert TransferReassembler.construct_anonymous_transfer(
        ts,
        Frame(
            priority=prio,
            transfer_id=123456,
            index=0,
            end_of_transfer=True,
            payload=memoryview(b"abcdef" + b"\xf1\xef\xbcS"),
        ),
    ) == TransferFrom(
        timestamp=ts, priority=prio, transfer_id=123456, fragmented_payload=[memoryview(b"abcdef")], source_node_id=None
    )

    # Faulty: CRC is wrong.
    assert (
        TransferReassembler.construct_anonymous_transfer(
            ts,
            Frame(
                priority=prio,
                transfer_id=123456,
                index=0,
                end_of_transfer=True,
                payload=memoryview(b"abcdef" + b"\xf1\xef\xbdS"),
            ),
        )
        is None
    )

    # Faulty: single transfer has index 0.
    assert (
        TransferReassembler.construct_anonymous_transfer(
            ts,
            Frame(priority=prio, transfer_id=123456, index=1, end_of_transfer=True, payload=memoryview(b"abcdef")),
        )
        is None
    )

    # Faulty: single transfer has EOT flag.
    assert (
        TransferReassembler.construct_anonymous_transfer(
            ts,
            Frame(priority=prio, transfer_id=123456, index=0, end_of_transfer=False, payload=memoryview(b"abcdef")),
        )
        is None
    )


def _unittest_validate_and_finalize_transfer() -> None:
    ts = Timestamp.now()
    prio = Priority.FAST
    tid = 888888888
    src_nid = 1234

    def mk_transfer(fp: typing.Sequence[bytes]) -> TransferFrom:
        return TransferFrom(
            timestamp=ts,
            priority=prio,
            transfer_id=tid,
            fragmented_payload=list(map(memoryview, fp)),  # type: ignore
            source_node_id=src_nid,
        )

    def call(fp: typing.Sequence[bytes]) -> typing.Optional[TransferFrom]:
        return _validate_and_finalize_transfer(
            timestamp=ts,
            priority=prio,
            transfer_id=tid,
            frame_payloads=list(map(memoryview, fp)),  # type: ignore
            source_node_id=src_nid,
        )

    assert call([b"" + TransferCRC.new(b"").value_as_bytes]) == mk_transfer([])  # [b""]?
    assert call([b"hello world" + TransferCRC.new(b"hello world").value_as_bytes]) == mk_transfer([b"hello world"])
    assert call(
        [b"hello world", b"0123456789", TransferCRC.new(b"hello world", b"0123456789").value_as_bytes]
    ) == mk_transfer([b"hello world", b"0123456789"])
    assert call([b"hello world", b"0123456789"]) is None  # no CRC


def _unittest_drop_crc() -> None:
    mv = memoryview
    assert _drop_crc([mv(b"0123456789")]) == [mv(b"012345")]
    assert _drop_crc([mv(b"0123456789"), mv(b"abcde")]) == [mv(b"0123456789"), mv(b"a")]
    assert _drop_crc([mv(b"0123456789"), mv(b"abcd")]) == [mv(b"0123456789")]
    assert _drop_crc([mv(b"0123456789"), mv(b"abc")]) == [mv(b"012345678")]
    assert _drop_crc([mv(b"0123456789"), mv(b"ab")]) == [mv(b"01234567")]
    assert _drop_crc([mv(b"0123456789"), mv(b"a")]) == [mv(b"0123456")]
    assert _drop_crc([mv(b"0123456789"), mv(b"")]) == [mv(b"012345")]
    assert _drop_crc([mv(b"0123456789"), mv(b""), mv(b"a"), mv(b"b")]) == [mv(b"01234567")]
    assert _drop_crc([mv(b"01"), mv(b""), mv(b"a"), mv(b"b")]) == []
    assert _drop_crc([mv(b"0"), mv(b""), mv(b"a"), mv(b"b")]) == []
    assert _drop_crc([mv(b"")]) == []
    assert _drop_crc([]) == []
