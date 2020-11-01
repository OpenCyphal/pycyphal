#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import dataclasses
import pyuavcan.transport
from ._base import RedundantSession, RedundantSessionStatistics
from .._deduplicator import Deduplicator, MonotonicDeduplicator, CyclicDeduplicator


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedundantTransferFrom(pyuavcan.transport.TransferFrom):
    inferior_session: pyuavcan.transport.InputSession


class RedundantInputSession(RedundantSession, pyuavcan.transport.InputSession):
    """
    This is a composite of a group of :class:`pyuavcan.transport.InputSession`.

    When an inferior session is removed, the state of the input transfer deduplicator is automatically reset.
    Therefore, removal of an inferior may temporarily disrupt the transfer flow by causing the session
    to skip or repeat several transfers.
    Applications where this is critical may prefer to avoid dynamic removal of inferiors.

    The transfer deduplication strategy is chosen between cyclic and monotonic automatically.
    """
    def __init__(self,
                 specifier:           pyuavcan.transport.InputSessionSpecifier,
                 payload_metadata:    pyuavcan.transport.PayloadMetadata,
                 tid_modulo_provider: typing.Callable[[], typing.Optional[int]],
                 loop:                asyncio.AbstractEventLoop,
                 finalizer:           typing.Callable[[], None]):
        """
        Do not call this directly! Use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._get_tid_modulo = tid_modulo_provider
        self._loop = loop
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert isinstance(self._get_tid_modulo(), (type(None), int))
        assert isinstance(self._loop, asyncio.AbstractEventLoop)
        assert callable(self._finalizer)

        self._inferiors: typing.List[pyuavcan.transport.InputSession] = []
        self._lock = asyncio.Lock(loop=self._loop)
        self._maybe_deduplicator: typing.Optional[Deduplicator] = None
        self._backlog: typing.List[RedundantTransferFrom] = []

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0

    def _add_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.InputSession)
        assert self._finalizer is not None, 'The session was supposed to be unregistered'
        assert session.specifier == self.specifier and session.payload_metadata == self.payload_metadata
        if session not in self._inferiors:
            if self._inferiors:  # Synchronize the settings.
                session.transfer_id_timeout = self.transfer_id_timeout
            self._inferiors.append(session)

    def _close_inferior(self, session_index: int) -> None:
        assert session_index >= 0, 'Negative indexes may lead to unexpected side effects'
        assert self._finalizer is not None, 'The session was supposed to be unregistered'
        try:
            session = self._inferiors.pop(session_index)
        except LookupError:
            pass
        else:
            self._maybe_deduplicator = None   # Removal of any inferior invalidates the state of the deduplicator.
            session.close()  # May raise.

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.InputSession]:
        return self._inferiors[:]

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        """
        Reads one deduplicated transfer using all inferiors concurrently. Returns None on timeout.
        If there are no inferiors, waits until the deadline, checks again, and returns if there are still none;
        otherwise, does a non-blocking read once.

        If any of the inferiors raises an exception, all reads are aborted and the exception is propagated immediately.
        """
        if self._finalizer is None:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed suka')

        try:
            async with self._lock:    # Serialize access to the inferiors.
                if not self._backlog:
                    if not self._inferiors:
                        await asyncio.sleep(monotonic_deadline - self._loop.time())
                        if not self._inferiors:
                            return None
                    # It is possible to optimize reads for the case of one inferior by invoking said inferior's
                    # receive_until() directly instead of dealing with sophisticated multiplexed reads here.
                    await self._receive_into_backlog(monotonic_deadline)
                    _logger.debug('%r new backlog (%d transfers): %r', self, len(self._backlog), self._backlog)

                if self._backlog:
                    out = self._backlog.pop(0)
                    self._stat_transfers += 1
                    self._stat_payload_bytes += sum(map(len, out.fragmented_payload))
                    return out
                else:
                    return None
        except Exception:
            self._stat_errors += 1
            raise

    @property
    def transfer_id_timeout(self) -> float:
        """
        Assignment of a new transfer-ID timeout is transferred to all inferior sessions,
        so that their settings are always kept consistent.
        When the transfer-ID timeout value is queried, the maximum value from the inferior sessions is returned;
        if there are no inferiors, zero is returned.
        The transfer-ID timeout is not kept by the redundant session itself.

        When a new inferior session is added, its transfer-ID timeout is assigned to match other inferiors.
        When all inferior sessions are removed, the transfer-ID timeout configuration becomes lost.
        Therefore, when the first inferior is added, the redundant session assumes its transfer-ID timeout
        configuration as its own; all inferiors added later will inherit the same setting.
        """
        if self._inferiors:
            return max(x.transfer_id_timeout for x in self._inferiors)
        else:
            return 0.0

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        value = float(value)
        if value <= 0.0:
            raise ValueError(f'Transfer-ID timeout shall be a positive number of seconds, got {value}')
        for s in self._inferiors:
            s.transfer_id_timeout = value

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> RedundantSessionStatistics:
        """
        - ``transfers``     - the number of successfully received deduplicated transfers (unique transfer count).
        - ``errors``        - the number of receive calls that could not be completed due to an exception.
        - ``payload_bytes`` - the number of payload bytes in successful deduplicated transfers counted in ``transfers``.
        - ``drops``         - the total number of drops summed from all inferiors (i.e., total drop count).
          This value is invalidated when the set of inferiors is changed. The semantics may change later.
        - ``frames``        - the total number of frames summed from all inferiors (i.e., replicated frame count).
          This value is invalidated when the set of inferiors is changed. The semantics may change later.
        """
        inferiors = [s.sample_statistics() for s in self._inferiors]
        return RedundantSessionStatistics(
            transfers=self._stat_transfers,
            frames=sum(s.frames for s in inferiors),
            payload_bytes=self._stat_payload_bytes,
            errors=self._stat_errors,
            drops=sum(s.drops for s in inferiors),
            inferiors=inferiors,
        )

    def close(self) -> None:
        for s in self._inferiors:
            try:
                s.close()
            except Exception as ex:
                _logger.exception('%s could not close inferior %s: %s', self, s, ex)
        self._inferiors.clear()

        fin, self._finalizer = self._finalizer, None
        if fin is not None:
            fin()

    @property
    def _deduplicator(self) -> Deduplicator:
        if self._maybe_deduplicator is None:
            tid_modulo = self._get_tid_modulo()
            if tid_modulo is None:
                self._maybe_deduplicator = MonotonicDeduplicator()
            else:
                assert 0 < tid_modulo < 2 ** 56, 'Sanity check'
                self._maybe_deduplicator = CyclicDeduplicator(tid_modulo)
        return self._maybe_deduplicator

    async def _receive_into_backlog(self, monotonic_deadline: float) -> None:
        assert self._lock.locked(), 'The mutex shall be locked to prevent concurrent reads'
        assert not self._backlog, 'This method need not be invoked if the backlog is not empty'
        assert self._inferiors, 'Some inferiors are required'

        inferiors = list(self._inferiors)  # Hold down a local copy to prevent concurrent mutation while waiting.

        async def do_receive(iface_index: int, session: pyuavcan.transport.InputSession) -> \
                typing.Tuple[int,
                             pyuavcan.transport.InputSession,
                             typing.Optional[pyuavcan.transport.TransferFrom]]:
            return iface_index, session, await session.receive_until(monotonic_deadline)

        pending = {self._loop.create_task(do_receive(if_idx, inf)) for if_idx, inf in enumerate(inferiors)}
        try:
            while True:
                assert len(pending) == len(inferiors)
                done, pending = await asyncio.wait(pending,  # type: ignore
                                                   loop=self._loop,
                                                   return_when=asyncio.FIRST_COMPLETED)
                _logger.debug('%r wait result: %d pending, %d done: %r', self, len(pending), len(done), done)

                # Process those that are done and push received transfers into the backlog.
                transfer_id_timeout = self.transfer_id_timeout  # May have been updated.
                for f in done:
                    if_idx, inf, tr = await f
                    assert isinstance(if_idx, int) and isinstance(inf, pyuavcan.transport.InputSession)
                    if tr is not None:  # Otherwise, the read has timed out.
                        assert isinstance(tr, pyuavcan.transport.TransferFrom)
                        if self._deduplicator.should_accept_transfer(if_idx, transfer_id_timeout, tr):
                            self._backlog.append(self._make_transfer(tr, inf))

                # Termination condition: success or timeout. We may have read more than one transfer.
                if self._backlog or self._loop.time() >= monotonic_deadline:
                    break

                # Not done yet - restart those reads that have completed; the pending ones remain pending, unchanged.
                for f in done:
                    if_idx, inf, _ = f.result()
                    assert isinstance(if_idx, int) and isinstance(inf, pyuavcan.transport.InputSession)
                    pending.add(self._loop.create_task(do_receive(if_idx, inf)))
        finally:
            if pending:
                _logger.debug('%r canceling %d pending reads', self, len(pending))
            for f in pending:
                f.cancel()

    @staticmethod
    def _make_transfer(origin:   pyuavcan.transport.TransferFrom,
                       inferior: pyuavcan.transport.InputSession) -> RedundantTransferFrom:
        return RedundantTransferFrom(timestamp=origin.timestamp,
                                     priority=origin.priority,
                                     transfer_id=origin.transfer_id,
                                     fragmented_payload=origin.fragmented_payload,
                                     source_node_id=origin.source_node_id,
                                     inferior_session=inferior)


def _unittest_redundant_input_cyclic() -> None:
    import time
    import pytest
    from pyuavcan.transport import Transfer, Timestamp, Priority, ResourceClosedError
    from pyuavcan.transport.loopback import LoopbackTransport

    loop = asyncio.get_event_loop()
    await_ = loop.run_until_complete

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

    ses = RedundantInputSession(spec, meta,
                                tid_modulo_provider=lambda: 32,  # Like CAN, for example.
                                loop=loop,
                                finalizer=retire)
    assert not is_retired
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()
    assert pytest.approx(0.0) == ses.transfer_id_timeout

    # Empty inferior set reception.
    time_before = loop.time()
    assert not await_(ses.receive_until(loop.time() + 2.0))
    assert 1.0 < loop.time() - time_before < 5.0, 'The method should have returned in about two seconds.'

    # Begin reception, then add an inferior while the reception is in progress.
    assert await_(tx_a.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=1,
                                           fragmented_payload=[memoryview(b'abc')]),
                                  loop.time() + 1.0))

    async def add_inferior(inferior: pyuavcan.transport.InputSession) -> None:
        await asyncio.sleep(1.0)
        # noinspection PyProtectedMember
        ses._add_inferior(inferior)

    time_before = loop.time()
    tr, _ = await_(asyncio.gather(
        # Start reception here. It would stall for two seconds because no inferiors.
        ses.receive_until(loop.time() + 2.0),
        # While the transmission is stalled, add one inferior with a delay.
        add_inferior(inf_a),
    ))
    assert 1.0 < loop.time() - time_before < 5.0, 'The method should have returned in about two seconds.'
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b'abc')]
    assert tr.inferior_session == inf_a

    # More inferiors
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    # noinspection PyProtectedMember
    ses._add_inferior(inf_a)  # No change, added above
    assert ses.inferiors == [inf_a]
    # noinspection PyProtectedMember
    ses._add_inferior(inf_b)
    assert ses.inferiors == [inf_a, inf_b]
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    assert inf_b.transfer_id_timeout == pytest.approx(1.1)

    # Redundant reception - new transfers accepted because the iface switch timeout is exceeded.
    time.sleep(ses.transfer_id_timeout)  # Just to make sure that it is REALLY exceeded.
    assert await_(tx_b.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=2,
                                           fragmented_payload=[memoryview(b'def')]),
                                  loop.time() + 1.0))
    assert await_(tx_b.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=3,
                                           fragmented_payload=[memoryview(b'ghi')]),
                                  loop.time() + 1.0))

    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 2
    assert tr.fragmented_payload == [memoryview(b'def')]
    assert tr.inferior_session == inf_b

    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 3
    assert tr.fragmented_payload == [memoryview(b'ghi')]
    assert tr.inferior_session == inf_b

    assert None is await_(ses.receive_until(loop.time() + 1.0))  # Nothing left to read now.

    # This one will be rejected because wrong iface and the switch timeout is not yet exceeded.
    assert await_(tx_a.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=4,
                                           fragmented_payload=[memoryview(b'rej')]),
                                  loop.time() + 1.0))
    assert None is await_(ses.receive_until(loop.time() + 0.1))

    # Transfer-ID timeout reconfiguration.
    ses.transfer_id_timeout = 3.0
    with pytest.raises(ValueError):
        ses.transfer_id_timeout = -0.0
    assert ses.transfer_id_timeout == pytest.approx(3.0)
    assert inf_a.transfer_id_timeout == pytest.approx(3.0)
    assert inf_a.transfer_id_timeout == pytest.approx(3.0)

    # Inferior removal resets the state of the deduplicator.
    # noinspection PyProtectedMember
    ses._close_inferior(0)
    # noinspection PyProtectedMember
    ses._close_inferior(1)  # Out of range, no effect.
    assert ses.inferiors == [inf_b]

    assert await_(tx_b.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=1,
                                           fragmented_payload=[memoryview(b'acc')]),
                                  loop.time() + 1.0))
    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b'acc')]
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
        await_(ses.receive_until(0))


def _unittest_redundant_input_monotonic() -> None:
    import pytest
    from pyuavcan.transport import Transfer, Timestamp, Priority
    from pyuavcan.transport.loopback import LoopbackTransport

    loop = asyncio.get_event_loop()
    await_ = loop.run_until_complete

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

    ses = RedundantInputSession(spec, meta,
                                tid_modulo_provider=lambda: None,  # Like UDP or serial - infinite modulo.
                                loop=loop,
                                finalizer=lambda: None)
    assert ses.specifier is spec
    assert ses.payload_metadata is meta
    assert not ses.inferiors
    assert ses.sample_statistics() == RedundantSessionStatistics()
    assert pytest.approx(0.0) == ses.transfer_id_timeout

    # Add inferiors.
    # noinspection PyProtectedMember
    ses._add_inferior(inf_a)  # No change, added above
    assert ses.inferiors == [inf_a]
    # noinspection PyProtectedMember
    ses._add_inferior(inf_b)
    assert ses.inferiors == [inf_a, inf_b]

    ses.transfer_id_timeout = 1.1
    assert ses.transfer_id_timeout == pytest.approx(1.1)
    assert inf_a.transfer_id_timeout == pytest.approx(1.1)
    assert inf_b.transfer_id_timeout == pytest.approx(1.1)

    # Redundant reception from multiple interfaces concurrently.
    for tx_x in (tx_a, tx_b):
        assert await_(tx_x.send_until(Transfer(timestamp=Timestamp.now(),
                                               priority=Priority.HIGH,
                                               transfer_id=2,
                                               fragmented_payload=[memoryview(b'def')]),
                                      loop.time() + 1.0))
        assert await_(tx_x.send_until(Transfer(timestamp=Timestamp.now(),
                                               priority=Priority.HIGH,
                                               transfer_id=3,
                                               fragmented_payload=[memoryview(b'ghi')]),
                                      loop.time() + 1.0))

    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 2
    assert tr.fragmented_payload == [memoryview(b'def')]

    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 3
    assert tr.fragmented_payload == [memoryview(b'ghi')]

    assert None is await_(ses.receive_until(loop.time() + 2.0))  # Nothing left to read now.

    # This one will be accepted despite a smaller transfer-ID because of the TID timeout.
    assert await_(tx_a.send_until(Transfer(timestamp=Timestamp.now(),
                                           priority=Priority.HIGH,
                                           transfer_id=1,
                                           fragmented_payload=[memoryview(b'acc')]),
                                  loop.time() + 1.0))
    tr = await_(ses.receive_until(loop.time() + 0.1))
    assert isinstance(tr, RedundantTransferFrom)
    assert ts.monotonic <= tr.timestamp.monotonic <= (loop.time() + 1e-3)
    assert tr.priority == Priority.HIGH
    assert tr.transfer_id == 1
    assert tr.fragmented_payload == [memoryview(b'acc')]
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
