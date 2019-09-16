#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import pyuavcan.transport
from ._redundant_session import RedundantInputSession, RedundantOutputSession


_logger = logging.getLogger(__name__)


class InconsistentRedundantTransportConfigurationError(pyuavcan.transport.TransportError):
    pass


class NoInferiorTransportsError(pyuavcan.transport.InvalidTransportConfigurationError):
    pass


class RedundantTransport(pyuavcan.transport.Transport):
    """
    This is a stub, not yet implemented. Come back later.
    """

    def __init__(self) -> None:
        self._transports: typing.List[pyuavcan.transport.Transport] = []

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        raise NotImplementedError

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        self._raise_if_empty()
        return self._transports[0].loop

    @property
    def local_node_id(self) -> typing.Optional[int]:
        if not self._transports:
            return None

        nid_set = set(x.local_node_id for x in self._transports)
        if len(nid_set) == 1:
            out, = nid_set
            return out
        else:
            raise InconsistentRedundantTransportConfigurationError(
                f'Inconsistent local node ID: {[x.local_node_id for x in self._transports]}')

    def set_local_node_id(self, node_id: int) -> None:
        if self.local_node_id is None:
            nid_card = self.protocol_parameters.max_nodes
            if 0 <= node_id < nid_card:
                for t in self._transports:
                    t.set_local_node_id(node_id)
            else:
                raise ValueError(f'Node ID not in range, changes not applied: 0 <= {node_id} < {nid_card}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> RedundantInputSession:
        raise NotImplementedError

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> pyuavcan.transport.OutputSession:
        raise NotImplementedError

    @property
    def input_sessions(self) -> typing.Sequence[RedundantInputSession]:
        raise NotImplementedError

    @property
    def output_sessions(self) -> typing.Sequence[RedundantOutputSession]:
        raise NotImplementedError

    @property
    def descriptor(self) -> str:
        return '<redundant>' + ''.join(t.descriptor for t in self._transports) + '</redundant>'

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Transport]:
        return self._transports[:]  # Return copy to prevent mutation

    def add_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        for t in self._transports:
            if t.loop is not transport.loop:
                raise InconsistentRedundantTransportConfigurationError(
                    'Cannot add the specified transport because it operates on a different event loop')

        # TODO: node-ID consistency check

        raise NotImplementedError

    def remove_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    def close(self) -> None:
        for t in self._transports:
            try:
                t.close()
            except Exception as ex:
                _logger.exception('%s could not close inferior %s: %s', self, t, ex)
        self._transports.clear()

    def _raise_if_empty(self) -> None:
        if not self._transports:
            raise NoInferiorTransportsError('No inferior transports configured')
