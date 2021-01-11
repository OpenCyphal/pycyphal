# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import logging
import dataclasses
import pyuavcan.transport
from ._session import RedundantInputSession, RedundantOutputSession, RedundantSession
from ._error import InconsistentInferiorConfigurationError


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedundantTransportStatistics(pyuavcan.transport.TransportStatistics):
    """
    Aggregate statistics for all inferior transports in a redundant group.
    This is an atomic immutable sample; it is not updated after construction.
    """

    inferiors: typing.List[pyuavcan.transport.TransportStatistics] = dataclasses.field(default_factory=list)
    """
    The ordering is guaranteed to match that of :attr:`RedundantTransport.inferiors`.
    """


class RedundantTransport(pyuavcan.transport.Transport):
    """
    This is a composite over a set of :class:`pyuavcan.transport.Transport`.
    Please read the module documentation for details.
    """

    MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD = int(2 ** 48)
    """
    An inferior transport whose transfer-ID modulo is less than this value is expected to experience
    transfer-ID overflows routinely during its operation. Otherwise, the transfer-ID is not expected to
    overflow for centuries.
    A transfer-ID counter that is expected to overflow is called "cyclic", otherwise it's "monotonic".
    Read https://forum.uavcan.org/t/alternative-transport-protocols/324.
    """

    def __init__(self, *, loop: typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        :param loop: All inferiors shall run on the same event loop,
            which is configured once here and cannot be changed after the instance is constructed.
            If not provided, defaults to :func:`asyncio.get_event_loop`.
        """
        self._cols: typing.List[pyuavcan.transport.Transport] = []
        self._rows: typing.Dict[pyuavcan.transport.SessionSpecifier, RedundantSession] = {}
        self._capture_handlers: typing.List[pyuavcan.transport.CaptureCallback] = []
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._check_matrix_consistency()

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        """
        Aggregate parameters constructed from all inferiors.
        If there are no inferiors (i.e., if the instance is closed), the value is all-zeros.
        Beware that if the set of inferiors is changed, this value may also be changed.

        The values are obtained from the set of inferiors by applying the following reductions:

        - min transfer-ID modulo
        - min max-nodes
        - min MTU
        """
        ipp = [t.protocol_parameters for t in self._cols] or [
            pyuavcan.transport.ProtocolParameters(
                transfer_id_modulo=0,
                max_nodes=0,
                mtu=0,
            )
        ]
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=min(t.transfer_id_modulo for t in ipp),
            max_nodes=min(t.max_nodes for t in ipp),
            mtu=min(t.mtu for t in ipp),
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        All inferiors run on the same event loop, which is configured statically once when the redundant transport
        is instantiated.
        The loop cannot be reassigned after instantiation.
        """
        return self._loop

    @property
    def local_node_id(self) -> typing.Optional[int]:
        """
        All inferiors share the same local node-ID.
        If there are no inferiors, the value is None (anonymous).
        """
        if self._cols:
            nid_set = set(x.local_node_id for x in self._cols)
            if len(nid_set) == 1:
                (out,) = nid_set
                return out
            # The following exception should not occur during normal operation unless one of the inferiors is
            # reconfigured sneakily.
            raise InconsistentInferiorConfigurationError(
                f"Redundant transports have different node-IDs: {[x.local_node_id for x in self._cols]}"
            )
        return None

    def get_input_session(
        self, specifier: pyuavcan.transport.InputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> RedundantInputSession:
        out = self._get_session(
            specifier,
            lambda fin: RedundantInputSession(specifier, payload_metadata, self._get_tid_modulo, self._loop, fin),
        )
        assert isinstance(out, RedundantInputSession)
        self._check_matrix_consistency()
        return out

    def get_output_session(
        self, specifier: pyuavcan.transport.OutputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> RedundantOutputSession:
        out = self._get_session(
            specifier, lambda fin: RedundantOutputSession(specifier, payload_metadata, self._loop, fin)
        )
        assert isinstance(out, RedundantOutputSession)
        self._check_matrix_consistency()
        return out

    def sample_statistics(self) -> RedundantTransportStatistics:
        return RedundantTransportStatistics(inferiors=[t.sample_statistics() for t in self._cols])

    @property
    def input_sessions(self) -> typing.Sequence[RedundantInputSession]:
        return [s for s in self._rows.values() if isinstance(s, RedundantInputSession)]

    @property
    def output_sessions(self) -> typing.Sequence[RedundantOutputSession]:
        return [s for s in self._rows.values() if isinstance(s, RedundantOutputSession)]

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Transport]:
        """
        Read-only access to the list of inferior transports.
        The inferiors are guaranteed to be ordered according to the temporal order of their attachment.
        """
        return self._cols[:]  # Return copy to prevent mutation

    def attach_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        """
        Adds a new transport to the redundant group. The new transport shall not be closed.

        If the transport is already added or it is the redundant transport itself (recursive attachment),
        a :class:`ValueError` will be raised.

        If the configuration of the new transport is not compatible with the other inferiors or with the
        redundant transport instance itself, an instance of :class:`InconsistentInferiorConfigurationError`
        will be raised.
        Specifically, the following preconditions are checked:

        - The new inferior shall operate on the same event loop as the redundant transport instance it is added to.
        - The local node-ID shall be the same for all inferiors, or all shall be anonymous.
        - The transfer-ID modulo shall meet *either* of the following conditions:

            - Identical for all inferiors.
            - Not less than :attr:`MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD` for all inferiors.

        If an exception is raised while the setup of the new inferior is in progress,
        the operation will be rolled back to ensure state consistency.
        """
        self._validate_inferior(transport)
        for ch in self._capture_handlers:
            transport.begin_capture(ch)
        self._cols.append(transport)
        try:
            for redundant_session in self._rows.values():
                self._construct_inferior_session(transport, redundant_session)
        except Exception:
            self.detach_inferior(transport)  # Roll back to ensure consistent states.
            raise
        finally:
            self._check_matrix_consistency()

    def detach_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        """
        Removes the specified transport from the redundant group.
        If there is no such transport, a :class:`ValueError` will be raised.

        All sessions of the removed inferior that are managed by the redundant transport instance
        will be automatically closed, but the inferior itself will not be
        (the caller will have to do that manually if desired).
        """
        if transport not in self._cols:
            raise ValueError(f"{transport} is not an inferior of {self}")
        index = self._cols.index(transport)
        self._cols.remove(transport)
        for owner in self._rows.values():
            try:
                owner._close_inferior(index)  # pylint: disable=protected-access
            except Exception as ex:
                _logger.exception("%s could not close inferior session #%d in %s: %s", self, index, owner, ex)
        self._check_matrix_consistency()

    def close(self) -> None:
        """
        Closes all redundant session instances, detaches and closes all inferior transports.
        Any exceptions occurring in the process will be suppressed and logged.

        Upon completion, the session matrix will be returned into its original empty state.
        It can be populated back by adding new transports and/or instantiating new redundant sessions
        if needed.
        In other words, closing is reversible here, which is uncommon for the library;
        consider this feature experimental.

        If the session matrix is empty, this method has no effect.
        """
        for s in list(self._rows.values()):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception("%s could not close %s: %s", self, s, ex)

        for t in self._cols:
            try:
                t.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception("%s could not close inferior %s: %s", self, t, ex)

        self._cols.clear()
        assert not self._rows, "All sessions should have been unregistered"
        self._check_matrix_consistency()

    def begin_capture(self, handler: pyuavcan.transport.CaptureCallback) -> None:
        """
        Stores the handler in the local list of handlers.
        Invokes :class:`pyuavcan.transport.Transport.begin_capture` on each inferior with the provided handler.
        If at least one inferior raises an exception, it is propagated immediately and the remaining inferiors
        will remain in an inconsistent state.
        When a new inferior is added later, the stored handlers will be automatically used to enable capture on it.
        If such auto-restoration behavior is undesirable, configure capture individually per-inferior instead.

        The redundant transport does not define its own capture events and does not wrap captured events reported
        by its inferiors.
        """
        self._capture_handlers.append(handler)
        for c in self._cols:
            c.begin_capture(handler)

    @staticmethod
    def make_tracer() -> pyuavcan.transport.Tracer:
        """
        This method is not implemented for redundant transport. Access the inferiors directly instead.
        """
        raise NotImplementedError

    async def spoof(self, transfer: pyuavcan.transport.AlienTransfer, monotonic_deadline: float) -> bool:
        """
        Simply propagates the call to every inferior.
        The return value is a logical AND for all inferiors; False if there are no inferiors.

        First exception to occur terminates the operation and is raised immediately.
        This is different from regular sending; the assumption is that the caller necessarily wants to ensure
        that spoofing takes place against every inferior.
        """
        if not self._cols:
            return False
        gather = asyncio.gather(*[inf.spoof(transfer, monotonic_deadline) for inf in self._cols])
        try:
            results = await gather
        except Exception:
            gather.cancel()
            raise
        return all(results)

    def _validate_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        # Ensure all inferiors run on the same event loop.
        if self.loop is not transport.loop:
            raise InconsistentInferiorConfigurationError(
                f"The inferior operates on a different event loop {transport.loop}, expected {self.loop}"
            )

        # Prevent double-add.
        if transport in self._cols:
            raise ValueError(f"{transport} is already an inferior of {self}")

        # Just out of abundance of paranoia.
        if transport is self:
            raise ValueError(f"A redundant transport cannot be an inferior of itself")

        # If there are no other inferiors, no further checks are necessary.
        if self._cols:
            # Ensure all inferiors have the same node-ID.
            if self.local_node_id != transport.local_node_id:
                raise InconsistentInferiorConfigurationError(
                    f"The inferior has a different node-ID {transport.local_node_id}, expected {self.local_node_id}"
                )

            # Ensure all inferiors use the same transfer-ID overflow policy.
            if self._get_tid_modulo() is None:
                if transport.protocol_parameters.transfer_id_modulo < self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
                    raise InconsistentInferiorConfigurationError(
                        f"The new inferior shall use monotonic transfer-ID counters in order to match the "
                        f"other inferiors in the redundant transport group"
                    )
            else:
                tid_modulo = self.protocol_parameters.transfer_id_modulo
                if transport.protocol_parameters.transfer_id_modulo != tid_modulo:
                    raise InconsistentInferiorConfigurationError(
                        f"The transfer-ID modulo {transport.protocol_parameters.transfer_id_modulo} of the new "
                        f"inferior is not compatible with the other inferiors ({tid_modulo})"
                    )

    def _get_session(
        self,
        specifier: pyuavcan.transport.SessionSpecifier,
        session_factory: typing.Callable[[typing.Callable[[], None]], RedundantSession],
    ) -> RedundantSession:
        if specifier not in self._rows:

            def retire() -> None:
                try:
                    del self._rows[specifier]
                except LookupError:
                    pass

            ses = session_factory(retire)
            try:
                for t in self._cols:
                    self._construct_inferior_session(t, ses)
            except Exception:
                ses.close()
                raise
            assert specifier not in self._rows
            self._rows[specifier] = ses

        return self._rows[specifier]

    @staticmethod
    def _construct_inferior_session(transport: pyuavcan.transport.Transport, owner: RedundantSession) -> None:
        assert isinstance(transport, pyuavcan.transport.Transport)
        if isinstance(owner, pyuavcan.transport.InputSession):
            inferior = transport.get_input_session(owner.specifier, owner.payload_metadata)
        elif isinstance(owner, pyuavcan.transport.OutputSession):
            inferior = transport.get_output_session(owner.specifier, owner.payload_metadata)
        else:
            assert False
        assert isinstance(owner, RedundantSession)  # MyPy makes me miss static typing so much.
        # If anything whatsoever goes wrong, just roll everything back and re-raise the exception.
        new_index = len(owner.inferiors)
        try:
            owner._add_inferior(inferior)  # pylint: disable=protected-access
        except Exception:
            # The inferior MUST be closed manually because in the case of failure it is not registered
            # in the redundant session.
            inferior.close()
            # If the inferior has not been added, this method will have no effect:
            owner._close_inferior(new_index)  # pylint: disable=protected-access
            raise

    def _get_tid_modulo(self) -> typing.Optional[int]:
        if self.protocol_parameters.transfer_id_modulo < self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
            return self.protocol_parameters.transfer_id_modulo
        return None

    def _check_matrix_consistency(self) -> None:
        for row in self._rows.values():
            assert len(row.inferiors) == len(self._cols)

    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        return list(self.inferiors), {}
