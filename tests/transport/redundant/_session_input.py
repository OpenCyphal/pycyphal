# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import time
import asyncio
import pytest
import pyuavcan
from pyuavcan.transport import Transfer, Timestamp, Priority, ResourceClosedError
from pyuavcan.transport.loopback import LoopbackTransport
from pyuavcan.transport.redundant._session._base import RedundantSessionStatistics
from pyuavcan.transport.redundant._session._input import RedundantInputSession
from pyuavcan.transport.redundant._session._input import RedundantTransferFrom

pytestmark = pytest.mark.asyncio


async def _unittest_redundant_input_cyclic() -> None:
    asyncio.get_running_loop().slow_callback_duration = 5.0

    spec = pyuavcan.transport.InputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_tx = pyuavcan.transport.OutputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30)

    ts = Timestamp.now()

    tr_a = LoopbackTransport(111)
    tr_b = LoopbackTransport(111)
    tx_a = tr_a.get_output_session(spec_tx, meta)
    tx_b = tr_b.get_output_session(spec_tx, meta)
    inf_a = tr_a.get_input_session(spec, meta)
    inf_b = tr_b.get_input_session(spec, meta)

    inf_a.transfer_id_timeout = 1.1  # This is used to ensure that the transfer-ID timeout is handled correctly.

    is_retired = False

    def retire() -> None:
        nonlocal is_retired
        is_retired = True

    ses = RedundantInputSession(spec, meta, tid_modulo_provider=lambda: 32, finalizer=retire)  # Like CAN, for example.
    assert not is_retired
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()
    assert pytest.approx(0.0) == ses.transfer_id_timeout

    # Empty inferior set reception.
    time_before = asyncio.get_running_loop().time()
    assert not await (ses.receive(asyncio.get_running_loop().time() + 2.0))
    assert (
        1.0 < asyncio.get_running_loop().time() - time_before < 5.0
    ), "The method should have returned in about two seconds."

    # Begin reception, then add an inferior while the reception is in progress.
    assert await (
        tx_a.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=1,
                fragmented_payload=[memoryview(b"abc")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )

    async def add_inferior(inferior: pyuavcan.transport.InputSession) -> None:
        await asyncio.sleep(1.0)
        ses._add_inferior(inferior)  # pylint: disable=protected-access

    time_before = asyncio.get_running_loop().time()
    tr, _ = await (
        asyncio.gather(
            # Start reception here. It would stall for two seconds because no inferiors.
            ses.receive(asyncio.get_running_loop().time() + 2.0),
            # While the transmission is stalled, add one inferior with a delay.
            add_inferior(inf_a),
        )
    )
    assert (
        0.0 < asyncio.get_running_loop().time() - time_before < 5.0
    ), "The method should have returned in about one second."
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b"abc")]
    assert tr.inferior_session == inf_a

    # More inferiors
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    ses._add_inferior(inf_a)  # No change, added above    # pylint: disable=protected-access
    assert ses.inferiors == [inf_a]
    ses._add_inferior(inf_b)  # pylint: disable=protected-access
    assert ses.inferiors == [inf_a, inf_b]
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    assert inf_b.transfer_id_timeout == pytest.approx(1.1)

    # Redundant reception - new transfers accepted because the iface switch timeout is exceeded.
    time.sleep(ses.transfer_id_timeout)  # Just to make sure that it is REALLY exceeded.
    assert await (
        tx_b.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=2,
                fragmented_payload=[memoryview(b"def")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )
    assert await (
        tx_b.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=3,
                fragmented_payload=[memoryview(b"ghi")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )

    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 2
    assert tr.fragmented_payload == [memoryview(b"def")]
    assert tr.inferior_session == inf_b

    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 3
    assert tr.fragmented_payload == [memoryview(b"ghi")]
    assert tr.inferior_session == inf_b

    assert None is await (ses.receive(asyncio.get_running_loop().time() + 1.0))  # Nothing left to read now.

    # This one will be rejected because wrong iface and the switch timeout is not yet exceeded.
    assert await (
        tx_a.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=4,
                fragmented_payload=[memoryview(b"rej")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )
    assert None is await (ses.receive(asyncio.get_running_loop().time() + 0.1))

    # Transfer-ID timeout reconfiguration.
    ses.transfer_id_timeout = 3.0
    with pytest.raises(ValueError):
        ses.transfer_id_timeout = -0.0
    assert ses.transfer_id_timeout == pytest.approx(3.0)
    assert inf_a.transfer_id_timeout == pytest.approx(3.0)
    assert inf_a.transfer_id_timeout == pytest.approx(3.0)

    # Inferior removal resets the state of the deduplicator.
    ses._close_inferior(0)  # pylint: disable=protected-access
    ses._close_inferior(1)  # Out of range, no effect.  # pylint: disable=protected-access
    assert ses.inferiors == [inf_b]

    assert await (
        tx_b.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=1,
                fragmented_payload=[memoryview(b"acc")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )
    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b"acc")]
    assert tr.inferior_session == inf_b

    # Stats check.
    assert ses.sample_statistics() == RedundantSessionStatistics(
        transfers=4,
        frames=inf_b.sample_statistics().frames,
        payload_bytes=12,
        errors=0,
        drops=0,
        inferiors=[
            inf_b.sample_statistics(),
        ],
    )

    # Closure.
    assert not is_retired
    ses.close()
    assert is_retired
    is_retired = False
    ses.close()
    assert not is_retired
    assert not ses.inferiors
    with pytest.raises(ResourceClosedError):
        await (ses.receive(0))
    tr_a.close()
    tr_b.close()
    inf_a.close()
    inf_b.close()
    await asyncio.sleep(2.0)


async def _unittest_redundant_input_monotonic() -> None:
    asyncio.get_running_loop().slow_callback_duration = 5.0

    spec = pyuavcan.transport.InputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(4321), None)
    spec_tx = pyuavcan.transport.OutputSessionSpecifier(spec.data_specifier, None)
    meta = pyuavcan.transport.PayloadMetadata(30)

    ts = Timestamp.now()

    tr_a = LoopbackTransport(111)
    tr_b = LoopbackTransport(111)
    tx_a = tr_a.get_output_session(spec_tx, meta)
    tx_b = tr_b.get_output_session(spec_tx, meta)
    inf_a = tr_a.get_input_session(spec, meta)
    inf_b = tr_b.get_input_session(spec, meta)

    inf_a.transfer_id_timeout = 1.1  # This is used to ensure that the transfer-ID timeout is handled correctly.

    ses = RedundantInputSession(
        spec,
        meta,
        tid_modulo_provider=lambda: 2 ** 56,  # Like UDP or serial - infinite modulo.
        finalizer=lambda: None,
    )
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()
    assert pytest.approx(0.0) == ses.transfer_id_timeout

    # Add inferiors.
    ses._add_inferior(inf_a)  # No change, added above    # pylint: disable=protected-access
    assert ses.inferiors == [inf_a]
    ses._add_inferior(inf_b)  # pylint: disable=protected-access
    assert ses.inferiors == [inf_a, inf_b]

    ses.transfer_id_timeout = 1.1
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    assert inf_a.transfer_id_timeout == pytest.approx(1.1)
    assert inf_b.transfer_id_timeout == pytest.approx(1.1)

    # Redundant reception from multiple interfaces concurrently.
    for tx_x in (tx_a, tx_b):
        assert await (
            tx_x.send(
                Transfer(
                    timestamp=Timestamp.now(),
                    priority=Priority.HIGH,
                    transfer_id=2,
                    fragmented_payload=[memoryview(b"def")],
                ),
                asyncio.get_running_loop().time() + 1.0,
            )
        )
        assert await (
            tx_x.send(
                Transfer(
                    timestamp=Timestamp.now(),
                    priority=Priority.HIGH,
                    transfer_id=3,
                    fragmented_payload=[memoryview(b"ghi")],
                ),
                asyncio.get_running_loop().time() + 1.0,
            )
        )

    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 2
    assert tr.fragmented_payload == [memoryview(b"def")]

    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 3
    assert tr.fragmented_payload == [memoryview(b"ghi")]

    assert None is await (ses.receive(asyncio.get_running_loop().time() + 2.0))  # Nothing left to read now.

    # This one will be accepted despite a smaller transfer-ID because of the TID timeout.
    assert await (
        tx_a.send(
            Transfer(
                timestamp=Timestamp.now(),
                priority=Priority.HIGH,
                transfer_id=1,
                fragmented_payload=[memoryview(b"acc")],
            ),
            asyncio.get_running_loop().time() + 1.0,
        )
    )
    tr = await (ses.receive(asyncio.get_running_loop().time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (asyncio.get_running_loop().time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b"acc")]
    assert tr.inferior_session == inf_a

    # Stats check.
    assert ses.sample_statistics() == RedundantSessionStatistics(
        transfers=3,
        frames=inf_a.sample_statistics().frames + inf_b.sample_statistics().frames,
        payload_bytes=9,
        errors=0,
        drops=0,
        inferiors=[
            inf_a.sample_statistics(),
            inf_b.sample_statistics(),
        ],
    )

    ses.close()
    tr_a.close()
    tr_b.close()
    inf_a.close()
    inf_b.close()
    await asyncio.sleep(2.0)
