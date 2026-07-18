from __future__ import annotations

from pycyphal2.can._reassembly import Endpoint, Reassembler, RxSession, RxSlot
from pycyphal2.can._wire import TransferKind


def test_cleanup_drops_slot_free_session_at_transfer_id_timeout() -> None:
    """A slot-free session dies at the 2 s transfer-ID timeout since last admission (reference:
    canard_poll); a session holding a slot lives for 30 s."""
    endpoint = Endpoint(kind=TransferKind.MESSAGE_16, port_id=7, on_transfer=lambda *_: None)
    stale = RxSession.new(0)
    stale.last_admission_ts_ns = 0
    fresh = RxSession.new(0)
    fresh.last_admission_ts_ns = 2_500_000_000
    endpoint.sessions[42] = stale
    endpoint.sessions[43] = fresh

    Reassembler.cleanup_sessions([endpoint], 3_000_000_000)  # Stale is 3 s idle, fresh only 0.5 s.

    assert 42 not in endpoint.sessions
    assert 43 in endpoint.sessions

    # A live slot keeps the session alive despite admission staleness, up to 30 s.
    occupied = RxSession.new(0)
    occupied.last_admission_ts_ns = 0
    occupied.slots[0] = RxSlot(start_ts_ns=0, transfer_id=0, iface_index=0, expected_toggle=False)
    endpoint.sessions[44] = occupied
    Reassembler.cleanup_sessions([endpoint], 25_000_000_000)  # Slot is 25 s old: below the 30 s purge.
    assert 44 in endpoint.sessions
    assert endpoint.sessions[44].slots[0] is not None


def test_cleanup_drops_session_after_30_seconds() -> None:
    received: list[bytes] = []
    endpoint = Endpoint(
        kind=TransferKind.MESSAGE_16,
        port_id=7,
        on_transfer=lambda _ts, _src, _prio, payload: received.append(payload),
    )
    session = RxSession.new(0)
    session.last_admission_ts_ns = 0
    session.slots[0] = RxSlot(start_ts_ns=0, transfer_id=0, iface_index=0, expected_toggle=False)
    endpoint.sessions[42] = session

    Reassembler.cleanup_sessions([endpoint], 30_000_000_001)

    assert received == []
    assert endpoint.sessions == {}
