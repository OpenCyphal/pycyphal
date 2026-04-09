from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
import logging

from .._api import Instant, Priority
from ._wire import (
    NODE_ID_ANONYMOUS,
    PRIORITY_COUNT,
    RX_SESSION_RETENTION_NS,
    TRANSFER_ID_TIMEOUT_NS,
    ParsedFrame,
    TransferKind,
    crc_add,
)

_logger = logging.getLogger(__name__)


@dataclass
class RxSlot:
    start_ts_ns: int
    transfer_id: int
    iface_index: int
    expected_toggle: bool
    crc: int = 0xFFFF
    data: bytearray = field(default_factory=bytearray)

    def accept(self, payload: bytes) -> None:
        self.data.extend(payload)
        self.crc = crc_add(self.crc, payload)
        self.expected_toggle = not self.expected_toggle


@dataclass
class RxSession:
    last_admission_ts_ns: int
    last_admitted_transfer_id: int
    last_admitted_priority: int
    iface_index: int
    slots: list[RxSlot | None]

    @staticmethod
    def new(iface_index: int) -> RxSession:
        return RxSession(
            last_admission_ts_ns=-(1 << 62),
            last_admitted_transfer_id=0,
            last_admitted_priority=0,
            iface_index=iface_index,
            slots=[None] * PRIORITY_COUNT,
        )


@dataclass
class Endpoint:
    kind: TransferKind
    port_id: int
    on_transfer: Callable[[Instant, int, Priority, bytes], None]
    sessions: dict[int, RxSession] = field(default_factory=dict)


class Reassembler:
    @staticmethod
    def cleanup_sessions(endpoints: Iterable[Endpoint], now_ns: int) -> None:
        stale_deadline = now_ns - RX_SESSION_RETENTION_NS
        for endpoint in endpoints:
            for source_id, session in list(endpoint.sessions.items()):
                for priority, slot in enumerate(session.slots):
                    if slot is not None and slot.start_ts_ns < stale_deadline:
                        session.slots[priority] = None
                if all(slot is None for slot in session.slots) and session.last_admission_ts_ns < stale_deadline:
                    endpoint.sessions.pop(source_id, None)

    @staticmethod
    def ingest(endpoint: Endpoint, iface_index: int, timestamp: Instant, parsed: ParsedFrame) -> None:
        if parsed.source_id == NODE_ID_ANONYMOUS:
            if parsed.start_of_transfer and parsed.end_of_transfer:
                endpoint.on_transfer(timestamp, parsed.source_id, Priority(parsed.priority), parsed.payload)
            return

        session = endpoint.sessions.get(parsed.source_id)
        if session is None:
            if not parsed.start_of_transfer:
                return
            session = RxSession.new(iface_index)
            endpoint.sessions[parsed.source_id] = session
        if not Reassembler._solve_admission(
            session,
            timestamp.ns,
            parsed.priority,
            parsed.start_of_transfer,
            parsed.toggle,
            parsed.transfer_id,
            iface_index,
        ):
            return
        if parsed.start_of_transfer:
            if session.slots[parsed.priority] is not None:
                session.slots[parsed.priority] = None
            if not parsed.end_of_transfer:
                Reassembler._cleanup_session_slots(session, timestamp.ns)
                session.slots[parsed.priority] = RxSlot(
                    start_ts_ns=timestamp.ns,
                    transfer_id=parsed.transfer_id,
                    iface_index=iface_index,
                    expected_toggle=parsed.toggle,
                )
            session.last_admission_ts_ns = timestamp.ns
            session.last_admitted_transfer_id = parsed.transfer_id
            session.last_admitted_priority = parsed.priority
            session.iface_index = iface_index

        slot = session.slots[parsed.priority]
        if slot is None:
            endpoint.on_transfer(timestamp, parsed.source_id, Priority(parsed.priority), parsed.payload)
            return
        slot.accept(parsed.payload)
        if parsed.end_of_transfer:
            session.slots[parsed.priority] = None
            if len(slot.data) >= 2 and slot.crc == 0:
                endpoint.on_transfer(
                    Instant(ns=slot.start_ts_ns), parsed.source_id, Priority(parsed.priority), bytes(slot.data[:-2])
                )
            else:
                _logger.debug(
                    "CAN drop bad CRC kind=%s port=%d src=%d", endpoint.kind.name, endpoint.port_id, parsed.source_id
                )

    @staticmethod
    def _cleanup_session_slots(session: RxSession, now_ns: int) -> None:
        deadline = now_ns - RX_SESSION_RETENTION_NS
        for priority, slot in enumerate(session.slots):
            if slot is not None and slot.start_ts_ns < deadline:
                session.slots[priority] = None

    @staticmethod
    def _solve_admission(
        session: RxSession,
        timestamp_ns: int,
        priority: int,
        start_of_transfer: bool,
        toggle: bool,
        transfer_id: int,
        iface_index: int,
    ) -> bool:
        if not start_of_transfer:
            slot = session.slots[priority]
            return (
                slot is not None
                and slot.transfer_id == transfer_id
                and slot.iface_index == iface_index
                and slot.expected_toggle == toggle
            )
        fresh = (transfer_id != session.last_admitted_transfer_id) or (priority != session.last_admitted_priority)
        affine = session.iface_index == iface_index
        stale = (timestamp_ns - TRANSFER_ID_TIMEOUT_NS) > session.last_admission_ts_ns
        return (fresh and affine) or (affine and stale) or (stale and fresh)
