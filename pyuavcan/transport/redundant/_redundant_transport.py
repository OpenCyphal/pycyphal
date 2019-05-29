#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.transport
from . import _redundant_session


class InconsistentRedundantTransportConfigurationError(pyuavcan.transport.TransportError):
    pass


class NoUnderlyingTransportsError(pyuavcan.transport.TransportError):
    pass


class RedundantTransport(pyuavcan.transport.Transport):
    def __init__(self) -> None:
        self._transports: typing.List[pyuavcan.transport.Transport] = []

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        if self._transports:
            tps = [x.protocol_parameters for x in self._transports]
            return pyuavcan.transport.ProtocolParameters(
                transfer_id_modulo=min(x.transfer_id_modulo for x in tps),
                node_id_set_cardinality=min(x.node_id_set_cardinality for x in tps),
                single_frame_transfer_payload_capacity_bytes=min(x.single_frame_transfer_payload_capacity_bytes
                                                                 for x in tps),
            )
        else:
            raise NoUnderlyingTransportsError

    @property
    def local_node_id(self) -> typing.Optional[int]:
        if self._transports:
            nid_set = set(x.local_node_id for x in self._transports)
            if len(nid_set) == 1:
                out, = nid_set
                return out
            else:
                raise InconsistentRedundantTransportConfigurationError(
                    f'Inconsistent local node ID: {[x.local_node_id for x in self._transports]}')
        else:
            raise NoUnderlyingTransportsError

    async def set_local_node_id(self, node_id: int) -> None:
        if self.local_node_id is None:
            nid_card = self.protocol_parameters.node_id_set_cardinality
            if 0 <= node_id < nid_card:
                for t in self._transports:
                    await t.set_local_node_id(node_id)
            else:
                raise ValueError(f'Node ID not in range, changes not applied: 0 <= {node_id} < {nid_card}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    @property
    def transports(self) -> typing.List[pyuavcan.transport.Transport]:
        return self._transports[:]  # Return copy to prevent mutation

    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def get_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    async def get_broadcast_output(self,
                                   data_specifier:   pyuavcan.transport.DataSpecifier,
                                   payload_metadata: pyuavcan.transport.PayloadMetadata) \
            -> _redundant_session.BroadcastOutputSession:
        raise NotImplementedError

    async def get_unicast_output(self,
                                 data_specifier:      pyuavcan.transport.DataSpecifier,
                                 payload_metadata:    pyuavcan.transport.PayloadMetadata,
                                 destination_node_id: int) -> _redundant_session.UnicastOutputSession:
        raise NotImplementedError

    async def get_promiscuous_input(self,
                                    data_specifier:   pyuavcan.transport.DataSpecifier,
                                    payload_metadata: pyuavcan.transport.PayloadMetadata) \
            -> _redundant_session.PromiscuousInputSession:
        raise NotImplementedError

    async def get_selective_input(self,
                                  data_specifier:   pyuavcan.transport.DataSpecifier,
                                  payload_metadata: pyuavcan.transport.PayloadMetadata,
                                  source_node_id:   int) -> _redundant_session.SelectiveInputSession:
        raise NotImplementedError

    def __str__(self) -> str:
        raise NotImplementedError
