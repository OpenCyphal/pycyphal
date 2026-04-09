from __future__ import annotations

from pycyphal2.can._reassembly import Endpoint, Reassembler, RxSession, RxSlot
from pycyphal2.can._wire import TransferKind


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
