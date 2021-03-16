# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan
import pyuavcan.transport.can
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

    frame: DataFrame

    own: bool
    """
    True if the captured frame was sent by the local transport instance.
    False if it was received from the bus.
    """

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
        direction = "tx" if self.own else "rx"
        return pyuavcan.util.repr_attributes(self, self.timestamp, direction, self.frame)

    @staticmethod
    def get_transport_type() -> typing.Type[pyuavcan.transport.can.CANTransport]:
        return pyuavcan.transport.can.CANTransport


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
        if ss.source_node_id is not None:
            return self._get_session(ss).update(cap.timestamp, prio, frame)
        # Anonymous transfer -- no reconstruction needed, no session.
        return TransferTrace(
            cap.timestamp,
            AlienTransfer(AlienTransferMetadata(prio, frame.transfer_id, ss), [frame.padded_payload]),
            0.0,
        )

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


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_can_capture() -> None:
    from pyuavcan.transport import MessageDataSpecifier
    from .media import FrameFormat
    from ._identifier import MessageCANID

    ts = Timestamp.now()
    payload = bytearray(b"123\x0A")
    cap = CANCapture(
        ts,
        DataFrame(
            FrameFormat.EXTENDED,
            MessageCANID(Priority.SLOW, 42, 3210).compile([memoryview(payload)]),
            payload,
        ),
        own=True,
    )
    print(cap)
    parsed = cap.parse()
    assert parsed is not None
    ss, prio, uf = parsed
    assert ss.source_node_id == 42
    assert ss.destination_node_id is None
    assert isinstance(ss.data_specifier, MessageDataSpecifier)
    assert ss.data_specifier.subject_id == 3210
    assert prio == Priority.SLOW
    assert uf.transfer_id == 0x0A
    assert uf.padded_payload == b"123"
    assert not uf.start_of_transfer
    assert not uf.end_of_transfer
    assert not uf.toggle_bit

    # Invalid CAN ID
    assert None is CANCapture(ts, DataFrame(FrameFormat.BASE, 123, payload), own=True).parse()

    # Invalid CAN payload
    assert (
        None
        is CANCapture(
            ts,
            DataFrame(FrameFormat.EXTENDED, MessageCANID(Priority.SLOW, 42, 3210).compile([]), bytearray()),
            own=True,
        ).parse()
    )


def _unittest_can_alien_session() -> None:
    from pytest import approx
    from pyuavcan.transport import MessageDataSpecifier
    from ._identifier import MessageCANID

    ts = Timestamp.now()
    can_identifier = MessageCANID(Priority.SLOW, 42, 3210).compile([])

    def frm(
        padded_payload: typing.Union[bytes, str],
        transfer_id: int,
        start_of_transfer: bool,
        end_of_transfer: bool,
        toggle_bit: bool,
    ) -> UAVCANFrame:
        return UAVCANFrame(
            identifier=can_identifier,
            padded_payload=memoryview(padded_payload if isinstance(padded_payload, bytes) else padded_payload.encode()),
            transfer_id=transfer_id,
            start_of_transfer=start_of_transfer,
            end_of_transfer=end_of_transfer,
            toggle_bit=toggle_bit,
        )

    spec = AlienSessionSpecifier(42, None, MessageDataSpecifier(3210))
    ses = _AlienSession(spec)

    # Valid multi-frame (test data copy-posted from the reassembler test).
    assert None is ses.update(ts, Priority.HIGH, frm(b"\x00\x01\x02\x03\x04\x05\x06", 11, True, False, True))
    assert None is ses.update(ts, Priority.HIGH, frm(b"\x07\x08\x09\x0a\x0b\x0c\x0d", 11, False, False, False))
    assert None is ses.update(ts, Priority.HIGH, frm(b"\x0e\x0f\x10\x11\x12\x13\x14", 11, False, False, True))
    assert None is ses.update(ts, Priority.HIGH, frm(b"\x15\x16\x17\x18\x19\x1a\x1b", 11, False, False, False))
    tr = ses.update(ts, Priority.HIGH, frm(b"\x1c\x1d\x35\x54", 11, False, True, True))
    assert isinstance(tr, TransferTrace)
    assert list(tr.transfer.fragmented_payload) == [
        b"\x00\x01\x02\x03\x04\x05\x06",
        b"\x07\x08\x09\x0a\x0b\x0c\x0d",
        b"\x0e\x0f\x10\x11\x12\x13\x14",
        b"\x15\x16\x17\x18\x19\x1a\x1b",
        b"\x1c\x1d",  # CRC stripped
    ]
    assert tr.transfer.metadata.priority == Priority.HIGH
    assert tr.transfer.metadata.transfer_id == 11
    assert tr.transfer.metadata.session_specifier.source_node_id == 42
    assert tr.transfer.metadata.session_specifier.destination_node_id is None
    assert isinstance(tr.transfer.metadata.session_specifier.data_specifier, MessageDataSpecifier)
    assert tr.transfer.metadata.session_specifier.data_specifier.subject_id == 3210
    assert tr.timestamp == ts
    assert tr.transfer_id_timeout == approx(2.0)  # Default value.

    # Missed start of transfer.
    tr = ses.update(ts, Priority.HIGH, frm(b"123456", 2, False, False, False))
    assert isinstance(tr, CANErrorTrace)

    # Valid single-frame; TID timeout updated.
    tr = ses.update(ts, Priority.LOW, frm(b"\x00\x01\x02\x03\x04\x05\x06", 12, True, True, True))
    assert isinstance(tr, TransferTrace)
    assert tr.transfer.metadata.priority == Priority.LOW
    assert tr.transfer.metadata.transfer_id == 12
    assert tr.transfer.metadata.session_specifier.source_node_id == 42
    assert tr.transfer.metadata.session_specifier.destination_node_id is None
    assert isinstance(tr.transfer.metadata.session_specifier.data_specifier, MessageDataSpecifier)
    assert tr.transfer.metadata.session_specifier.data_specifier.subject_id == 3210
    assert tr.timestamp == ts
    assert ses.transfer_id_timeout == approx(1.0)  # Shrunk twice because we're using the same timestamp here.


def _unittest_can_tracer() -> None:
    from .media import FrameFormat
    from ._identifier import MessageCANID

    ts = Timestamp.now()
    tracer = CANTracer()

    # Foreign capture ignored.
    assert None is tracer.update(Capture(ts))

    # Valid transfers.
    cap = CANCapture(
        ts,
        DataFrame(
            FrameFormat.EXTENDED,
            MessageCANID(Priority.FAST, 42, 3210).compile([]),
            bytearray(b"123\xFF"),
        ),
        own=True,
    )
    tr = tracer.update(cap)
    assert isinstance(tr, TransferTrace)
    assert tr.timestamp == ts
    assert tr.transfer.metadata.transfer_id == 31
    assert tr.transfer.metadata.priority == Priority.FAST
    assert tr.transfer.metadata.session_specifier.source_node_id == 42

    cap = CANCapture(
        ts,
        DataFrame(
            FrameFormat.EXTENDED,
            MessageCANID(Priority.SLOW, 42, 3210).compile([]),
            bytearray(b"123\xE0"),
        ),
        own=False,  # Direction is ignored.
    )
    tr = tracer.update(cap)
    assert isinstance(tr, TransferTrace)
    assert tr.timestamp == ts
    assert tr.transfer.metadata.transfer_id == 0
    assert tr.transfer.metadata.priority == Priority.SLOW
    assert tr.transfer.metadata.session_specifier.source_node_id == 42

    cap = CANCapture(
        ts,
        DataFrame(
            FrameFormat.EXTENDED,
            MessageCANID(Priority.SLOW, None, 3210).compile([]),
            bytearray(b"123\xE0"),
        ),
        own=False,  # Direction is ignored.
    )
    tr = tracer.update(cap)
    assert isinstance(tr, TransferTrace)
    assert tr.timestamp == ts
    assert tr.transfer.metadata.transfer_id == 0
    assert tr.transfer.metadata.priority == Priority.SLOW
    assert tr.transfer.metadata.session_specifier.source_node_id is None

    # Invalid captured frame.
    assert None is tracer.update(CANCapture(ts, DataFrame(FrameFormat.BASE, 123, bytearray(b"")), own=False))
