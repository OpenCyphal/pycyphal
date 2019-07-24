#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio

import pyuavcan.transport
from ._input_session import LoopbackInputSession
from ._output_session import LoopbackOutputSession


class LoopbackTransport(pyuavcan.transport.Transport):
    """
    The loopback transport is intended for testing and API usage demonstrations.
    It works by short-circuiting input and output sessions together as if there was an underlying network.
    Service transfers are a special case: in order to allow usage of service transfers, the loopback
    transport flips the role from SERVER to CLIENT and back when routing the short-circuit data.
    """

    def __init__(self, loop: typing.Optional[asyncio.AbstractEventLoop] = None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        self._local_node_id: typing.Optional[int] = None

        self._input_sessions: typing.Dict[pyuavcan.transport.SessionSpecifier, LoopbackInputSession] = {}
        self._output_sessions: typing.Dict[pyuavcan.transport.SessionSpecifier, LoopbackOutputSession] = {}

        # Unlimited protocol capabilities by default.
        self._protocol_parameters = pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=2 ** 64,
            node_id_set_cardinality=2 ** 64,
            single_frame_transfer_payload_capacity_bytes=2 ** 64 - 1,
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return self._protocol_parameters

    @protocol_parameters.setter
    def protocol_parameters(self, value: pyuavcan.transport.ProtocolParameters) -> None:
        if isinstance(value, pyuavcan.transport.ProtocolParameters):
            self._protocol_parameters = value
        else:  # pragma: no cover
            raise ValueError(f'Unexpected value: {value}')

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    def set_local_node_id(self, node_id: int) -> None:
        if self._local_node_id is None:
            node_id = int(node_id)
            if 0 <= node_id < self._protocol_parameters.node_id_set_cardinality:
                self._local_node_id = node_id
            else:
                raise ValueError(f'Invalid node-ID value: {node_id}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node-ID is already assigned')

    def close(self) -> None:
        self._input_sessions.clear()
        self._output_sessions.clear()

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> LoopbackInputSession:
        def do_close() -> None:
            try:
                del self._input_sessions[specifier]
            except LookupError:
                pass

        try:
            sess = self._input_sessions[specifier]
        except KeyError:
            sess = LoopbackInputSession(specifier=specifier,
                                        payload_metadata=payload_metadata,
                                        loop=self.loop,
                                        closer=do_close)
            self._input_sessions[specifier] = sess
        return sess

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> LoopbackOutputSession:
        def do_close() -> None:
            try:
                del self._output_sessions[specifier]
            except LookupError:
                pass

        async def do_route(tr: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
            del monotonic_deadline      # Unused, all operations always successful and instantaneous.
            if specifier.remote_node_id not in {self.local_node_id, None}:
                return True  # Drop the transfer.

            tr_from = pyuavcan.transport.TransferFrom(
                timestamp=tr.timestamp,
                priority=tr.priority,
                transfer_id=tr.transfer_id % self.protocol_parameters.transfer_id_modulo,
                fragmented_payload=tr.fragmented_payload,
                source_node_id=self.local_node_id,
            )

            # Flip CLIENT/SERVER if this is a service session; do nothing otherwise.
            if isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier):
                role = pyuavcan.transport.ServiceDataSpecifier.Role
                flip_lookup = {
                    role.CLIENT: role.SERVER,
                    role.SERVER: role.CLIENT,
                }
                data_specifier: pyuavcan.transport.DataSpecifier = \
                    pyuavcan.transport.ServiceDataSpecifier(specifier.data_specifier.service_id,
                                                            flip_lookup[specifier.data_specifier.role])
            else:
                data_specifier = specifier.data_specifier

            for remote_node_id in {self.local_node_id, None}:  # Multicast to both: selective and promiscuous.
                try:
                    destination_session = self._input_sessions[pyuavcan.transport.SessionSpecifier(data_specifier,
                                                                                                   remote_node_id)]
                except LookupError:
                    pass
                else:
                    await destination_session.push(tr_from)

            return True

        try:
            sess = self._output_sessions[specifier]
        except KeyError:
            sess = LoopbackOutputSession(specifier=specifier,
                                         payload_metadata=payload_metadata,
                                         loop=self.loop,
                                         closer=do_close,
                                         router=do_route)
            self._output_sessions[specifier] = sess
        return sess

    @property
    def input_sessions(self) -> typing.Sequence[pyuavcan.transport.InputSession]:
        return list(self._input_sessions.values())

    @property
    def output_sessions(self) -> typing.Sequence[pyuavcan.transport.OutputSession]:
        return list(self._output_sessions.values())

    @property
    def descriptor(self) -> str:
        return '<loopback/>'
