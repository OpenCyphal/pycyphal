# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan
from pyuavcan.transport import Trace, TransferTrace, AlienSessionSpecifier, AlienTransferMetadata, Capture
from pyuavcan.transport import AlienTransfer, TransferFrom, Timestamp, Priority
from ._session import TransferReassemblyErrorID, TransferReassembler
from .media import DataFrame
from ._frame import UAVCANFrame
from ._identifier import CANID


@dataclasses.dataclass(frozen=True)
class CANCapture(Capture):
    """
    See :meth:`pyuavcan.transport.can.CANTransport.begin_capture` for details.
    """

    direction: Capture.Direction
    frame: DataFrame

    def parse(self) -> typing.Optional[typing.Tuple[AlienSessionSpecifier, Priority, UAVCANFrame]]:
        uf = UAVCANFrame.parse(self.frame)
        if not uf:
            return None
        ci = CANID.parse(self.frame.identifier)
        if not ci:
            return None
        ss = AlienSessionSpecifier(
            source_node_id=ci.source_node_id,
            destination_node_id=ci.get_destination_node_id(),
            data_specifier=ci.data_specifier,
        )
        return ss, ci.priority, uf

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self.timestamp, self.direction.name, self.frame)


@dataclasses.dataclass(frozen=True)
class CANErrorTrace(pyuavcan.transport.ErrorTrace):
    error: TransferReassemblyErrorID


class CANTracer(pyuavcan.transport.Tracer):
    """
    The CAN tracer does not differentiate between RX and TX frames, they are treated uniformly.
    Return types from :meth:`update`:

    - :class:`pyuavcan.transport.TransferTrace`
    - :class:`CANErrorTrace`
    """

    def __init__(self) -> None:
        self._sessions: typing.Dict[AlienSessionSpecifier, _AlienSession] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if not isinstance(cap, CANCapture):
            return None
        parsed = cap.parse()
        if not parsed:
            return None
        ss, prio, frame = parsed
        return self._get_session(ss).update(cap.timestamp, prio, frame)

    def _get_session(self, specifier: AlienSessionSpecifier) -> _AlienSession:
        try:
            return self._sessions[specifier]
        except KeyError:
            self._sessions[specifier] = _AlienSession(specifier)
        return self._sessions[specifier]


class _AlienSession:
    _MAX_INTERVAL = 1.0
    _TID_TIMEOUT_MULTIPLIER = 2.0  # TID = 2*interval as suggested in the Specification.
    _EXTENT_BYTES = 2 ** 32

    def __init__(self, specifier: AlienSessionSpecifier) -> None:
        assert specifier.source_node_id is not None
        self._specifier = specifier
        self._reassembler = TransferReassembler(
            source_node_id=specifier.source_node_id, extent_bytes=_AlienSession._EXTENT_BYTES
        )
        self._last_transfer_monotonic: float = 0.0
        self._interval = float(_AlienSession._MAX_INTERVAL)

    def update(self, timestamp: Timestamp, priority: Priority, frame: UAVCANFrame) -> typing.Optional[Trace]:
        tid_timeout = self.transfer_id_timeout
        tr = self._reassembler.process_frame(timestamp, priority, frame, int(tid_timeout * 1e9))
        if tr is None:
            return None
        if isinstance(tr, TransferReassemblyErrorID):
            return CANErrorTrace(timestamp=timestamp, error=tr)

        assert isinstance(tr, TransferFrom)
        meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
        out = TransferTrace(timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)

        # Update the transfer interval for automatic transfer-ID timeout deduction.
        delta = float(tr.timestamp.monotonic) - self._last_transfer_monotonic
        delta = min(_AlienSession._MAX_INTERVAL, max(0.0, delta))
        self._interval = (self._interval + delta) * 0.5
        self._last_transfer_monotonic = float(tr.timestamp.monotonic)

        return out

    @property
    def transfer_id_timeout(self) -> float:
        """
        The current value of the auto-deduced transfer-ID timeout.
        It is automatically adjusted whenever a new transfer is received.
        """
        return self._interval * _AlienSession._TID_TIMEOUT_MULTIPLIER
