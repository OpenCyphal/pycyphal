from __future__ import annotations

import pytest

from pycyphal2 import Instant, Priority
from pycyphal2.can._reassembly import Endpoint, Reassembler, RxSession, RxSlot
from pycyphal2.can._wire import NODE_ID_ANONYMOUS, RX_SESSION_RETENTION_NS, ParsedFrame, TransferKind


def _parsed(
    *,
    kind: TransferKind = TransferKind.MESSAGE_16,
    priority: int = int(Priority.NOMINAL),
    port_id: int = 123,
    source_id: int = 42,
    transfer_id: int = 0,
    start: bool = True,
    end: bool = True,
    toggle: bool = True,
    payload: bytes = b"x",
) -> ParsedFrame:
    return ParsedFrame(
        kind=kind,
        priority=priority,
        port_id=port_id,
        source_id=source_id,
        destination_id=None,
        transfer_id=transfer_id,
        start_of_transfer=start,
        end_of_transfer=end,
        toggle=toggle,
        payload=payload,
    )


def test_anonymous_single_frame_is_accepted_but_multiframe_is_rejected() -> None:
    received: list[tuple[int, Priority, bytes]] = []
    endpoint = Endpoint(
        kind=TransferKind.MESSAGE_13,
        port_id=55,
        on_transfer=lambda _ts, src, prio, payload: received.append((src, prio, payload)),
    )

    Reassembler.ingest(endpoint, 0, Instant(ns=10), _parsed(kind=TransferKind.MESSAGE_13, source_id=NODE_ID_ANONYMOUS))
    Reassembler.ingest(
        endpoint,
        0,
        Instant(ns=11),
        _parsed(kind=TransferKind.MESSAGE_13, source_id=NODE_ID_ANONYMOUS, end=False),
    )

    assert received == [(NODE_ID_ANONYMOUS, Priority.NOMINAL, b"x")]


def test_cleanup_retains_fresh_session_while_dropping_stale_slots() -> None:
    endpoint = Endpoint(kind=TransferKind.MESSAGE_16, port_id=1, on_transfer=lambda *_: None)
    session = RxSession.new(0)
    session.last_admission_ts_ns = RX_SESSION_RETENTION_NS + 1
    session.slots[0] = RxSlot(start_ts_ns=0, transfer_id=0, iface_index=0, expected_toggle=False)
    endpoint.sessions[10] = session

    Reassembler.cleanup_sessions([endpoint], RX_SESSION_RETENTION_NS + 2)

    assert session.slots[0] is None
    assert endpoint.sessions[10] is session


def test_start_replaces_existing_slot_and_cleans_stale_slots() -> None:
    received: list[bytes] = []
    endpoint = Endpoint(
        kind=TransferKind.MESSAGE_16,
        port_id=7,
        on_transfer=lambda _ts, _src, _prio, payload: received.append(payload),
    )
    session = RxSession.new(0)
    session.last_admission_ts_ns = 0
    session.slots[int(Priority.NOMINAL)] = RxSlot(
        start_ts_ns=1,
        transfer_id=10,
        iface_index=0,
        expected_toggle=False,
    )
    session.slots[int(Priority.LOW)] = RxSlot(
        start_ts_ns=0,
        transfer_id=11,
        iface_index=0,
        expected_toggle=False,
    )
    endpoint.sessions[42] = session

    now = RX_SESSION_RETENTION_NS + 5
    Reassembler.ingest(
        endpoint,
        1,
        Instant(ns=now),
        _parsed(priority=int(Priority.NOMINAL), source_id=42, transfer_id=12, end=False),
    )

    slot = session.slots[int(Priority.NOMINAL)]
    assert slot is not None
    assert slot.transfer_id == 12
    assert slot.iface_index == 1
    assert session.slots[int(Priority.LOW)] is None
    assert received == []


@pytest.mark.parametrize(
    ("name", "slot", "timestamp_ns", "priority", "start", "toggle", "transfer_id", "iface_index", "expected"),
    [
        ("test_continuation_no_slot_rejected", None, 10, 0, False, False, 0, 0, False),
        (
            "test_continuation_wrong_tid_rejected",
            RxSlot(start_ts_ns=0, transfer_id=1, iface_index=0, expected_toggle=False),
            10,
            0,
            False,
            False,
            2,
            0,
            False,
        ),
        (
            "test_continuation_wrong_iface_rejected",
            RxSlot(start_ts_ns=0, transfer_id=1, iface_index=1, expected_toggle=False),
            10,
            0,
            False,
            False,
            1,
            0,
            False,
        ),
        (
            "test_continuation_frames",
            RxSlot(start_ts_ns=0, transfer_id=1, iface_index=0, expected_toggle=True),
            10,
            0,
            False,
            True,
            1,
            0,
            True,
        ),
        ("test_fresh_variants", None, 10, 0, True, True, 2, 0, True),
        ("test_stale_boundary", None, 2_000_000_001, 0, True, True, 1, 1, False),
    ],
    ids=lambda x: x if isinstance(x, str) else None,
)
def test_admission_cases(
    name: str,
    slot: RxSlot | None,
    timestamp_ns: int,
    priority: int,
    start: bool,
    toggle: bool,
    transfer_id: int,
    iface_index: int,
    expected: bool,
) -> None:
    del name
    session = RxSession.new(0)
    session.last_admitted_transfer_id = 1
    session.last_admitted_priority = 0
    session.last_admission_ts_ns = 0
    session.iface_index = 0
    session.slots[priority] = slot

    assert (
        Reassembler._solve_admission(session, timestamp_ns, priority, start, toggle, transfer_id, iface_index)
        is expected
    )


def test_multiframe_crc_failure_clears_slot_without_delivery() -> None:
    received: list[bytes] = []
    endpoint = Endpoint(
        kind=TransferKind.MESSAGE_16,
        port_id=99,
        on_transfer=lambda _ts, _src, _prio, payload: received.append(payload),
    )
    session = RxSession.new(0)
    session.slots[int(Priority.NOMINAL)] = RxSlot(
        start_ts_ns=1,
        transfer_id=0,
        iface_index=0,
        expected_toggle=False,
        crc=1,
        data=bytearray(b"bad\x00\x00"),
    )
    endpoint.sessions[42] = session

    Reassembler.ingest(
        endpoint,
        0,
        Instant(ns=2),
        _parsed(source_id=42, start=False, end=True, toggle=False, payload=b""),
    )

    assert session.slots[int(Priority.NOMINAL)] is None
    assert received == []
