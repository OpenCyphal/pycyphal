# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport
from pyuavcan.transport import Trace, TransferTrace, Capture, AlienSessionSpecifier, AlienTransferMetadata
from pyuavcan.transport import AlienTransfer, TransferFrom
from pyuavcan.transport.commons.high_overhead_transport import AlienTransferReassembler, TransferReassembler
from ._frame import SerialFrame


@dataclasses.dataclass(frozen=True)
class SerialCapture(pyuavcan.transport.Capture):
    """
    The set of subclasses may be extended in future versions.
    """
    @property
    def metadata(self) -> typing.Optional[AlienTransferMetadata]:
        return None


@dataclasses.dataclass(frozen=True)
class SerialTxCapture(SerialCapture):
    """
    Outgoing transfer emission event from the local node.
    It is guaranteed that all frames contained therein belong to the same transfer.

    The timestamp specifies the time when the first frame was sent (the other frames are not timestamped).
    If no frames could be sent due to an error, the list will be empty.
    Obtain bytes using :func:`SerialFrame.compile_into`.
    """
    frames: typing.List[SerialFrame]

    @property
    def metadata(self) -> typing.Optional[AlienTransferMetadata]:
        try:
            fr = self.frames[0]
        except LookupError:
            return None
        s = AlienSessionSpecifier(source_node_id=fr.source_node_id,
                                  destination_node_id=fr.destination_node_id,
                                  data_specifier=fr.data_specifier)
        return AlienTransferMetadata(fr.priority, fr.transfer_id, s)


@dataclasses.dataclass(frozen=True)
class SerialRxFrameCapture(SerialCapture):
    """
    Frame reception event.
    The timestamp reflects the time when the first byte was received.
    If necessary, obtain the original encoded byte representation using :func:`SerialFrame.compile_into`.
    """
    frame: SerialFrame

    @property
    def metadata(self) -> AlienTransferMetadata:
        fr = self.frame
        s = AlienSessionSpecifier(source_node_id=fr.source_node_id,
                                  destination_node_id=fr.destination_node_id,
                                  data_specifier=fr.data_specifier)
        return AlienTransferMetadata(fr.priority, fr.transfer_id, s)


@dataclasses.dataclass(frozen=True)
class SerialRxOutOfBandCapture(SerialCapture):
    """
    Out-of-band data or a malformed frame received. See :class:`pyuavcan.serial.StreamParser`.
    """
    data: memoryview


@dataclasses.dataclass(frozen=True)
class SerialErrorTrace(pyuavcan.transport.ErrorTrace):
    error: TransferReassembler.Error


@dataclasses.dataclass(frozen=True)
class SerialOutOfBandTrace(pyuavcan.transport.ErrorTrace):
    """
    This is a mirror of :class:`SerialRxOutOfBandCapture` at the transfer level rather than frame level.
    """
    data: memoryview


class SerialTracer(pyuavcan.transport.Tracer):
    """
    This tracer does not differentiate between sent and received transfers.
    If necessary, the user can distinguish them by checking the type of the supplied capture object.

    Return types from :meth:`update`:

    - :class:`pyuavcan.transport.TransferTrace`
    - :class:`SerialErrorTrace`
    - :class:`SerialOutOfBandTrace`
    """

    def __init__(self) -> None:
        self._sessions: typing.Dict[AlienSessionSpecifier, _AlienSession] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if isinstance(cap, SerialCapture):
            meta = cap.metadata
            if meta is not None:
                return self._get_session(meta.session_specifier).update(cap)
        return None

    def _get_session(self, specifier: AlienSessionSpecifier) -> _AlienSession:
        try:
            return self._sessions[specifier]
        except KeyError:
            self._sessions[specifier] = _AlienSession(specifier)
        return self._sessions[specifier]


class _AlienSession:
    def __init__(self, specifier: AlienSessionSpecifier) -> None:
        self._specifier = specifier
        src = specifier.source_node_id
        self._reassembler = AlienTransferReassembler(src) if src is not None else None

    def update(self, cap: SerialCapture) -> typing.Optional[Trace]:
        reasm = self._reassembler
        tid_timeout = reasm.transfer_id_timeout if reasm is not None else 0.0

        tr: typing.Union[TransferFrom, TransferReassembler.Error, None] = None
        if isinstance(cap, SerialRxFrameCapture):
            if reasm is not None:
                tr = reasm.process_frame(cap.timestamp, cap.frame)
            else:
                tr = TransferReassembler.construct_anonymous_transfer(cap.timestamp, cap.frame)
        elif isinstance(cap, SerialTxCapture):
            if reasm is not None:
                for frame in cap.frames:  # It is guaranteed that there is at most one transfer per capture.
                    tr = reasm.process_frame(cap.timestamp, frame)
            else:
                if len(cap.frames) == 1:  # Anonymous transfers are always single-frame transfers.
                    tr = TransferReassembler.construct_anonymous_transfer(cap.timestamp, cap.frames[0])
        elif isinstance(cap, SerialRxOutOfBandCapture):
            return SerialOutOfBandTrace(cap.timestamp, cap.data)
        else:
            return None

        if isinstance(tr, TransferReassembler.Error):
            return SerialErrorTrace(timestamp=cap.timestamp, error=tr)
        elif isinstance(tr, TransferFrom):
            meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
            return TransferTrace(cap.timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)
        else:
            assert tr is None
        return None
