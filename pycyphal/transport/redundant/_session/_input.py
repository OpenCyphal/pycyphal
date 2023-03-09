# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import asyncio
import logging
import dataclasses
import pycyphal.transport
import pycyphal.util
from ._base import RedundantSession, RedundantSessionStatistics
from .._deduplicator import Deduplicator


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, repr=False)
class RedundantTransferFrom(pycyphal.transport.TransferFrom):
    inferior_session: pycyphal.transport.InputSession


@dataclasses.dataclass(frozen=True)
class _Inferior:
    session: pycyphal.transport.InputSession
    worker: asyncio.Task[None]

    def close(self) -> None:
        try:
            self.session.close()
        finally:
            self.worker.cancel()

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self, session=self.session, iface_id=f"{id(self.session):016x}", worker=self.worker
        )


class RedundantInputSession(RedundantSession, pycyphal.transport.InputSession):
    """
    This is a composite of a group of :class:`pycyphal.transport.InputSession`.

    The transfer deduplication strategy is chosen between cyclic and monotonic automatically
    when the first inferior is added.
    """

    _READ_TIMEOUT = 1.0

    def __init__(
        self,
        specifier: pycyphal.transport.InputSessionSpecifier,
        payload_metadata: pycyphal.transport.PayloadMetadata,
        tid_modulo_provider: typing.Callable[[], int],
        finalizer: typing.Callable[[], None],
    ):
        """
        Do not call this directly! Use the factory method instead.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._get_tid_modulo = tid_modulo_provider
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pycyphal.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pycyphal.transport.PayloadMetadata)
        assert isinstance(self._get_tid_modulo(), (type(None), int))
        assert callable(self._finalizer)

        self._inferiors: typing.List[_Inferior] = []
        self._deduplicator: typing.Optional[Deduplicator] = None

        # The actual deduplicated transfers received by the inferiors.
        self._read_queue: asyncio.Queue[RedundantTransferFrom] = asyncio.Queue()
        # Queuing errors is meaningless because they lose relevance immediately, so the queue is only one item deep.
        self._error_queue: asyncio.Queue[Exception] = asyncio.Queue(1)

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0

    def _add_inferior(self, session: pycyphal.transport.Session) -> None:
        assert isinstance(session, pycyphal.transport.InputSession)
        assert self._finalizer is not None, "The session was supposed to be unregistered"
        assert session.specifier == self.specifier and session.payload_metadata == self.payload_metadata
        if session in self.inferiors:
            return
        _logger.debug("%s: Adding inferior %s id=%016x", self, session, id(session))

        # Ensure that the deduplicator is constructed when the first inferior is launched.
        if self._deduplicator is None:
            self._deduplicator = Deduplicator.new(self._get_tid_modulo())
            _logger.debug("%s: Constructed new deduplicator: %s", self, self._deduplicator)

        # Synchronize the settings for the newly added inferior with its siblings.
        # If there are no other inferiors, the first added one seeds the configuration for its future siblings.
        if self._inferiors:
            session.transfer_id_timeout = self.transfer_id_timeout

        # Launch the inferior's worker task in the last order and add that to the registry.
        task = asyncio.get_event_loop().create_task(self._inferior_worker_task(session))
        self._inferiors.append(_Inferior(session=session, worker=task))

    def _close_inferior(self, session_index: int) -> None:
        assert session_index >= 0, "Negative indexes may lead to unexpected side effects"
        assert self._finalizer is not None, "The session was supposed to be unregistered"
        try:
            inf = self._inferiors.pop(session_index)
        except LookupError:
            pass
        else:
            _logger.debug(
                "%s: Closing inferior %s that used to reside at index %d. Remaining siblings: %s",
                self,
                inf,
                session_index,
                self._inferiors,
            )
            inf.close()
        finally:
            if not self._inferiors:
                # Reset because inferiors we add later may require a different deduplication strategy.
                # When no inferiors are left, there are no consistency constraints to respect.
                self._deduplicator = None

    @property
    def inferiors(self) -> typing.Sequence[pycyphal.transport.InputSession]:
        return [x.session for x in self._inferiors]

    async def receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        """
        Reads one deduplicated transfer received from all inferiors concurrently. Returns None on timeout.
        If there are no inferiors at the time of the invocation and none appear by the expiration of the timeout,
        returns None.

        Exceptions raised by inferiors are propagated normally, but it is possible for an exception to be delayed
        until the next invocation of this method.
        """
        # First of all, handle pending errors, because removing the item from the queue might unblock reader tasks.
        try:
            exc = self._error_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        else:
            assert not isinstance(exc, (asyncio.CancelledError, pycyphal.transport.ResourceClosedError))
            raise exc
        # Check the read queue only if there are no pending errors.
        loop = asyncio.get_running_loop()
        try:
            timeout = monotonic_deadline - loop.time()
            if timeout > 0:
                tr = await asyncio.wait_for(self._read_queue.get(), timeout)
            else:
                tr = self._read_queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed transfers, allow the caller to read them even if the instance is closed.
            if self._finalizer is None:
                raise pycyphal.transport.ResourceClosedError(f"{self} is closed") from None
            return None
        # We do not re-check the error queue at the output because that would mean losing the received transfer.
        # If there are new errors, they will be handled at the next invocation.
        return tr

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
            return max(x.transfer_id_timeout for x in self.inferiors)
        return 0.0

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        value = float(value)
        if value <= 0.0:
            raise ValueError(f"Transfer-ID timeout shall be a positive number of seconds, got {value}")
        for s in self.inferiors:
            s.transfer_id_timeout = value

    @property
    def specifier(self) -> pycyphal.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pycyphal.transport.PayloadMetadata:
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
        inferiors = [s.sample_statistics() for s in self.inferiors]
        return RedundantSessionStatistics(
            transfers=self._stat_transfers,
            frames=sum(s.frames for s in inferiors),
            payload_bytes=self._stat_payload_bytes,
            errors=self._stat_errors,
            drops=sum(s.drops for s in inferiors),
            inferiors=inferiors,
        )

    def close(self) -> None:
        for inf in self._inferiors:
            try:
                inf.close()
            except Exception as ex:
                _logger.exception("%s: Could not close %s: %s", self, inf, ex)
        self._inferiors.clear()
        fin, self._finalizer = self._finalizer, None
        if fin is not None:
            fin()
        self._deduplicator = None

    async def _process_transfer(
        self, session: pycyphal.transport.InputSession, transfer: pycyphal.transport.TransferFrom
    ) -> None:
        assert self._deduplicator is not None
        iface_id = id(session)
        if self._deduplicator.should_accept_transfer(
            iface_id=iface_id,
            transfer_id_timeout=self.transfer_id_timeout,
            timestamp=transfer.timestamp,
            source_node_id=transfer.source_node_id,
            transfer_id=transfer.transfer_id,
        ):
            _logger.debug("%s: Accepting %s from %016x", self, transfer, iface_id)
            self._stat_transfers += 1
            self._stat_payload_bytes += sum(map(len, transfer.fragmented_payload))
            await self._read_queue.put(
                RedundantTransferFrom(
                    timestamp=transfer.timestamp,
                    priority=transfer.priority,
                    transfer_id=transfer.transfer_id,
                    fragmented_payload=transfer.fragmented_payload,
                    source_node_id=transfer.source_node_id,
                    inferior_session=session,
                )
            )
        else:
            _logger.debug("%s: Discarding redundant duplicate %s from %016x", self, transfer, iface_id)

    async def _inferior_worker_task(self, session: pycyphal.transport.InputSession) -> None:
        iface_id = id(session)
        loop = asyncio.get_running_loop()
        try:
            _logger.debug("%s: Task for inferior %016x is starting", self, iface_id)
            while self._deduplicator is not None:
                try:
                    deadline = loop.time() + RedundantInputSession._READ_TIMEOUT
                    tr = await session.receive(deadline)
                    if tr is not None and self._deduplicator is not None:
                        await self._process_transfer(session, tr)
                except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError):
                    break
                except Exception as ex:
                    # We block until the error is stored in the one-element error queue.
                    # This behavior allows us to avoid spinning broken inferiors that raise errors continuously.
                    _logger.debug("%s: Receive from %016x raised %s", self, iface_id, ex, exc_info=True)
                    self._stat_errors += 1
                    await self._error_queue.put(ex)
        except (asyncio.CancelledError, pycyphal.transport.ResourceClosedError):
            pass
        except Exception as ex:
            _logger.exception("%s: Task for %016x has encountered an unhandled exception: %s", self, iface_id, ex)
        finally:
            _logger.debug("%s: Task for %016x is stopping", self, iface_id)
