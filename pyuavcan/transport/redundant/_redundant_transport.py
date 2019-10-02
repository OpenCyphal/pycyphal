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
from ._session import RedundantInputSession, RedundantOutputSession, RedundantSession
from ._error import InconsistentInferiorConfigurationError


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedundantTransportStatistics(pyuavcan.transport.TransportStatistics):
    """
    Aggregate statistics for all inferior transports in a redundant group.
    """
    #: The ordering is guaranteed to match that of the inferiors.
    inferiors: typing.List[pyuavcan.transport.TransportStatistics] = dataclasses.field(default_factory=list)


class RedundantTransport(pyuavcan.transport.Transport):
    """
    This is a stub, not yet implemented. Come back later.

    This is a standard composite over a set of :class:`pyuavcan.transport.Transport`.

    Stub::

        top   /|    /|    /|    /
             / |   / |   / |   /
            /  |  /  |  /  |  /
           /   | /   | /   | /
        0 /    |/    |/    |/
          ---------------------->
                   time

    Stub::

        top >= 2**48         . `
                         . `
        ...          ...
                  . `
              . `
        0 . `
          ---------- ... ------->
                   time
    """

    #: An inferior transport whose transfer-ID modulo is less than this value is expected to experience
    #: transfer-ID overflows routinely during its operation. Otherwise, the transfer-ID is not expected to
    #: overflow for centuries. Read https://forum.uavcan.org/t/alternative-transport-protocols/324.
    MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD = 2 ** 48

    def __init__(self) -> None:
        self._inferiors: typing.List[pyuavcan.transport.Transport] = []
        self._sessions: typing.Dict[pyuavcan.transport.SessionSpecifier, RedundantSession] = {}

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        """
        Aggregate parameters constructed from all inferiors.
        If there are no inferiors (i.e., if the instance is closed), the value is all-zeros.
        Beware that if the set of inferiors is changed, this value may also be changed.
        """
        ipp = [t.protocol_parameters for t in self._inferiors] or [
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
        All inferiors run on the same event loop.
        If there are no inferiors, the value is sourced from :func:`asyncio.get_event_loop`.
        """
        if self._inferiors:
            out = self._inferiors[0].loop
            for it in self._inferiors:
                if it.loop is not out:
                    raise InconsistentInferiorConfigurationError(
                        f'Redundant transports operate on different event loops: {[x.loop for x in self._inferiors]}'
                    )
            return out
        else:
            return asyncio.get_event_loop()

    @property
    def local_node_id(self) -> typing.Optional[int]:
        """
        All inferiors share the same local node-ID.
        If there are no inferiors, the value is None (anonymous).
        """
        if self._inferiors:
            nid_set = set(x.local_node_id for x in self._inferiors)
            if len(nid_set) == 1:
                out, = nid_set
                return out
            else:
                raise InconsistentInferiorConfigurationError(
                    f'Redundant transports have different node-IDs: {[x.local_node_id for x in self._inferiors]}'
                )
        else:
            return None

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> RedundantInputSession:
        raise NotImplementedError

    def get_output_session(self,
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> RedundantOutputSession:
        if specifier not in self._sessions:
            def retire() -> None:
                try:
                    del self._sessions[specifier]
                except LookupError:
                    pass

            ses = RedundantOutputSession(specifier, payload_metadata, lambda: self.loop, retire)
            try:
                for t in self._inferiors:
                    # noinspection PyProtectedMember
                    ses._add_inferior(self._construct_session(t, specifier, payload_metadata))
            except Exception:
                ses.close()
                raise
            assert specifier not in self._sessions[specifier]
            self._sessions[specifier] = ses

        out = self._sessions[specifier]
        assert isinstance(out, RedundantOutputSession)
        return out

    def sample_statistics(self) -> pyuavcan.transport.TransportStatistics:
        return RedundantTransportStatistics(
            inferiors=[t.sample_statistics() for t in self._inferiors]
        )

    @property
    def input_sessions(self) -> typing.Sequence[RedundantInputSession]:
        return [s for s in self._sessions.values() if isinstance(s, RedundantInputSession)]

    @property
    def output_sessions(self) -> typing.Sequence[RedundantOutputSession]:
        return [s for s in self._sessions.values() if isinstance(s, RedundantOutputSession)]

    @property
    def descriptor(self) -> str:
        return '<redundant>' + ''.join(t.descriptor for t in self._inferiors) + '</redundant>'

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Transport]:
        return self._inferiors[:]  # Return copy to prevent mutation

    def attach_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        """
        Adds a new transport to the redundant set.
        The new transport shall not be closed.
        If the transport is already added, a :class:`ValueError` will be raised.
        If an exception is raised while the setup of the new inferior is in progress,
        the operation will be rolled back to ensure state consistency.
        """
        self._validate_inferior(transport)
        self._inferiors.append(transport)
        try:
            # Carefully create all session instances in the new inferior.
            # If anything whatsoever goes wrong, just roll everything back and re-raise the exception.
            for redundant_session in self._sessions.values():
                inferior_session = self._construct_session(transport,
                                                           redundant_session.specifier,
                                                           redundant_session.payload_metadata)
                try:  # noinspection PyProtectedMember
                    redundant_session._add_inferior(inferior_session)
                except Exception:
                    # The inferior MUST be closed manually because in the case of failure it is not
                    # registered in the redundant session.
                    inferior_session.close()
                    raise
        except Exception:
            self.detach_inferior(transport)  # Roll back to ensure consistent states.
            raise

    def detach_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        """
        Removes the specified transport from the redundant set.
        All session instances managed by the redundant transport instance will be automatically closed,
        but the transport itself will not be (the caller will have to do that manually if desired).
        If there is no such transport, a :class:`ValueError` will be raised.
        """
        if transport not in self._inferiors:
            raise ValueError(f'{transport} is not an inferior of {self}')
        self._inferiors.remove(transport)

        for inferior_session in (*transport.input_sessions, *transport.output_sessions):
            try:
                redundant_session = self._sessions[inferior_session.specifier]
            except LookupError:
                pass
            else:
                try:
                    # noinspection PyProtectedMember
                    redundant_session._close_inferior(inferior_session)
                except Exception as ex:
                    _logger.exception('%s could not close inferior session %s: %s', self, inferior_session, ex)

    def close(self) -> None:
        """
        Detaches and closes all inferior transports and their sessions.
        Upon completion, the current redundant transport instance will be returned into its original state (closed).
        It can be un-closed back by adding new transports if needed (closing is reversible here).
        """
        for s in self._sessions.values():
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s could not close %s: %s', self, s, ex)

        for t in self._inferiors:
            try:
                t.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s could not close inferior %s: %s', self, t, ex)

        self._inferiors.clear()
        assert not self._sessions, 'Internal logic error'

    def _validate_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        # If there are no other inferiors, we can accept anything.
        if not self._inferiors:
            return

        # Prevent double-add.
        if transport in self._inferiors:
            raise ValueError(f'{transport} is already an inferior of {self}')

        # Just out of abundance of paranoia.
        if transport is self:
            raise ValueError(f'A redundant transport cannot be an inferior of itself')

        # Ensure all inferiors run on the same event loop.
        if self.loop is not transport.loop:
            raise InconsistentInferiorConfigurationError(
                f'The inferior operates on a different event loop {transport.loop}, expected {self.loop}'
            )

        # Ensure all inferiors have the same node-ID.
        if self.local_node_id != transport.local_node_id:
            raise InconsistentInferiorConfigurationError(
                f'The inferior has a different node-ID {transport.local_node_id}, expected {self.local_node_id}'
            )

        # Ensure all inferiors use the same transfer-ID overflow policy.
        tid_modulo = self.protocol_parameters.transfer_id_modulo
        if tid_modulo >= self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
            if transport.protocol_parameters.transfer_id_modulo < self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
                raise InconsistentInferiorConfigurationError(
                    f'The new inferior shall use monotonic transfer-ID counters in order to match the '
                    f'other inferiors in the redundant transport group'
                )
        else:
            if transport.protocol_parameters.transfer_id_modulo != tid_modulo:
                raise InconsistentInferiorConfigurationError(
                    f'The transfer-ID modulo {transport.protocol_parameters.transfer_id_modulo} of the new '
                    f'inferior is not compatible with the other inferiors ({tid_modulo})'
                )

    @staticmethod
    def _construct_session(transport:        pyuavcan.transport.Transport,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> pyuavcan.transport.Session:
        assert isinstance(transport, pyuavcan.transport.Transport)
        assert isinstance(payload_metadata, pyuavcan.transport.PayloadMetadata)
        if isinstance(specifier, pyuavcan.transport.InputSessionSpecifier):
            return transport.get_input_session(specifier, payload_metadata)
        elif isinstance(specifier, pyuavcan.transport.OutputSessionSpecifier):
            return transport.get_output_session(specifier, payload_metadata)
        else:
            assert False, 'Internal logic error'
