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

    +-----------+---------------+---------------+---------------+---------------+
    |           |  Transport 0  |  Transport 1  |      ...      |  Transport M  |
    +===========+===============+===============+===============+===============+
    | Session 0 |     S0T0      |     S0T1      |      ...      |     S0Tm      |
    +-----------+---------------+---------------+---------------+---------------+
    | Session 1 |     S1T0      |     S1T1      |      ...      |     S1Tm      |
    +-----------+---------------+---------------+---------------+---------------+
    |    ...    |      ...      |      ...      |      ...      |      ...      |
    +-----------+---------------+---------------+---------------+---------------+
    | Session N |     SnT0      |     SnT1      |      ...      |     SnTm      |
    +-----------+---------------+---------------+---------------+---------------+

    - Attachment/detachment of a transport is modeled as an addition/removal of a column.
    - Construction/retirement of a session is modeled as an addition/removal of a row.

    While the construction of a row or a column is in progress, the matrix resides in an inconsistent state.
    If any error occurs in the process, the matrix is rolled back to the previous consistent state.

    Existing redundant sessions retain validity across any changes in the matrix configuration.
    Logic that relies on a redundant instance is completely shielded from any changes in the underlying transport
    configuration, meaning that the entire underlying transport structure may be swapped out with a completely
    different one without affecting the higher levels.
    An extreme case of this feature is an inverted logic where a redundant transport is constructed
    with zero inferior transports,
    its session instances are configured, and the inferior transports are added later.

    This is extremely useful for long-running applications that have to retain the presentation-level structure
    across any changes in the transport configurations done on-the-fly without stopping the application.
    An example of such application is a GUI tool where the user may want to switch an existing running setup
    from one transport configuration to another without stopping it.

    A redundant transport cannot be an inferior of itself,
    although it can be an inferior of another redundant transport (which is unlikely to be practical).

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
    MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD = int(2 ** 48)

    def __init__(self, loop: typing.Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        :param loop: All inferiors will have to run on the same event loop.
            If not provided, defaults to :func:`asyncio.get_event_loop`.
        """
        self._cols: typing.List[pyuavcan.transport.Transport] = []
        self._rows: typing.Dict[pyuavcan.transport.SessionSpecifier, RedundantSession] = {}
        self._loop = loop if loop is not None else asyncio.get_event_loop()

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        """
        Aggregate parameters constructed from all inferiors.
        If there are no inferiors (i.e., if the instance is closed), the value is all-zeros.
        Beware that if the set of inferiors is changed, this value may also be changed.
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
        is instantiated. The loop cannot be reassigned after instantiation.
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
                out, = nid_set
                return out
            else:
                raise InconsistentInferiorConfigurationError(
                    f'Redundant transports have different node-IDs: {[x.local_node_id for x in self._cols]}'
                )
        else:
            return None

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> RedundantInputSession:
        out = self._get_session(
            specifier,
            lambda fin: RedundantInputSession(specifier,
                                              payload_metadata,
                                              self._get_tid_modulo,
                                              self._loop,
                                              fin)
        )
        assert isinstance(out, RedundantInputSession)
        return out

    def get_output_session(self,
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> RedundantOutputSession:
        out = self._get_session(
            specifier,
            lambda fin: RedundantOutputSession(specifier,
                                               payload_metadata,
                                               self._loop,
                                               fin)
        )
        assert isinstance(out, RedundantOutputSession)
        return out

    def sample_statistics(self) -> pyuavcan.transport.TransportStatistics:
        return RedundantTransportStatistics(
            inferiors=[t.sample_statistics() for t in self._cols]
        )

    @property
    def input_sessions(self) -> typing.Sequence[RedundantInputSession]:
        return [s for s in self._rows.values() if isinstance(s, RedundantInputSession)]

    @property
    def output_sessions(self) -> typing.Sequence[RedundantOutputSession]:
        return [s for s in self._rows.values() if isinstance(s, RedundantOutputSession)]

    @property
    def descriptor(self) -> str:
        return '<redundant>' + ''.join(t.descriptor for t in self._cols) + '</redundant>'

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Transport]:
        """
        Read-only access to the list of inferior transports; ordering preserved.
        """
        return self._cols[:]  # Return copy to prevent mutation

    def attach_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        """
        Adds a new transport to the redundant set.
        The new transport shall not be closed.
        If the transport is already added or it is the redundant transport itself (recursive attachment),
        a :class:`ValueError` will be raised.
        If the new transport is not compatible with the other inferiors, a
        :class:`InconsistentInferiorConfigurationError` will be raised.
        If an exception is raised while the setup of the new inferior is in progress,
        the operation will be rolled back to ensure state consistency.
        """
        self._validate_inferior(transport)
        self._cols.append(transport)
        try:
            for redundant_session in self._rows.values():
                self._construct_inferior_session(transport, redundant_session)
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
        if transport not in self._cols:
            raise ValueError(f'{transport} is not an inferior of {self}')
        self._cols.remove(transport)

        for inferior_session in (*transport.input_sessions, *transport.output_sessions):
            try:
                redundant_session = self._rows[inferior_session.specifier]
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
        Upon completion, the session matrix will be returned into its original state (empty/closed).
        It can be un-closed back by adding new transports if needed (closing is reversible here).
        Invoking this method on an empty matrix has no effect.
        """
        for s in list(self._rows.values()):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s could not close %s: %s', self, s, ex)

        for t in self._cols:
            try:
                t.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s could not close inferior %s: %s', self, t, ex)

        self._cols.clear()
        assert not self._rows, 'All sessions should have been unregistered'

    def _validate_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        # Ensure all inferiors run on the same event loop.
        if self.loop is not transport.loop:
            raise InconsistentInferiorConfigurationError(
                f'The inferior operates on a different event loop {transport.loop}, expected {self.loop}'
            )

        # Prevent double-add.
        if transport in self._cols:
            raise ValueError(f'{transport} is already an inferior of {self}')

        # Just out of abundance of paranoia.
        if transport is self:
            raise ValueError(f'A redundant transport cannot be an inferior of itself')

        # If there are no other inferiors, no further checks are necessary.
        if self._cols:
            # Ensure all inferiors have the same node-ID.
            if self.local_node_id != transport.local_node_id:
                raise InconsistentInferiorConfigurationError(
                    f'The inferior has a different node-ID {transport.local_node_id}, expected {self.local_node_id}'
                )

            # Ensure all inferiors use the same transfer-ID overflow policy.
            if self._get_tid_modulo() is None:
                if transport.protocol_parameters.transfer_id_modulo < self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
                    raise InconsistentInferiorConfigurationError(
                        f'The new inferior shall use monotonic transfer-ID counters in order to match the '
                        f'other inferiors in the redundant transport group'
                    )
            else:
                tid_modulo = self.protocol_parameters.transfer_id_modulo
                if transport.protocol_parameters.transfer_id_modulo != tid_modulo:
                    raise InconsistentInferiorConfigurationError(
                        f'The transfer-ID modulo {transport.protocol_parameters.transfer_id_modulo} of the new '
                        f'inferior is not compatible with the other inferiors ({tid_modulo})'
                    )

    def _get_session(self,
                     specifier:       pyuavcan.transport.SessionSpecifier,
                     session_factory: typing.Callable[[typing.Callable[[], None]],
                                                      RedundantSession]) -> RedundantSession:
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
        try:
            # noinspection PyProtectedMember
            owner._add_inferior(inferior)
        except Exception:
            # The inferior MUST be closed manually because in the case of failure it is not registered
            # in the redundant session.
            inferior.close()
            # noinspection PyProtectedMember
            owner._close_inferior(inferior)
            raise

    def _get_tid_modulo(self) -> typing.Optional[int]:
        if self.protocol_parameters.transfer_id_modulo < self.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
            return self.protocol_parameters.transfer_id_modulo
        else:
            return None
