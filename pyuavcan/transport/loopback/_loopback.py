#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import dataclasses

import pyuavcan.transport
from ._input_session import LoopbackInputSession
from ._output_session import LoopbackOutputSession


@dataclasses.dataclass
class LoopbackTransportStatistics(pyuavcan.transport.TransportStatistics):
    pass


class LoopbackTransport(pyuavcan.transport.Transport):
    """
    The loopback transport is intended for basic testing and API usage demonstrations.
    It works by short-circuiting input and output sessions together as if there was an underlying network.

    It is not possible to exchange data between different nodes using this transport.
    The only valid usage is sending and receiving same data on the same node.
    """

    def __init__(self,
                 local_node_id: typing.Optional[int],
                 loop:          typing.Optional[asyncio.AbstractEventLoop] = None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._local_node_id = int(local_node_id) if local_node_id is not None else None
        self._input_sessions: typing.Dict[pyuavcan.transport.InputSessionSpecifier, LoopbackInputSession] = {}
        self._output_sessions: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, LoopbackOutputSession] = {}
        # Unlimited protocol capabilities by default.
        self._protocol_parameters = pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=2 ** 64,
            max_nodes=2 ** 64,
            mtu=2 ** 64 - 1,
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

    def close(self) -> None:
        sessions = (*self._input_sessions.values(), *self._output_sessions.values())
        self._input_sessions.clear()
        self._output_sessions.clear()
        for s in sessions:
            s.close()

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
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
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> LoopbackOutputSession:
        def do_close() -> None:
            try:
                del self._output_sessions[specifier]
            except LookupError:
                pass

        async def do_route(tr: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
            del monotonic_deadline      # Unused, all operations always successful and instantaneous.
            if specifier.remote_node_id in {self.local_node_id, None}:  # Otherwise drop the transfer.
                tr_from = pyuavcan.transport.TransferFrom(
                    timestamp=tr.timestamp,
                    priority=tr.priority,
                    transfer_id=tr.transfer_id % self.protocol_parameters.transfer_id_modulo,
                    fragmented_payload=tr.fragmented_payload,
                    source_node_id=self.local_node_id,
                )
                for remote_node_id in {self.local_node_id, None}:  # Multicast to both: selective and promiscuous.
                    try:
                        destination_session = self._input_sessions[
                            pyuavcan.transport.InputSessionSpecifier(specifier.data_specifier, remote_node_id)
                        ]
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

    def sample_statistics(self) -> LoopbackTransportStatistics:
        return LoopbackTransportStatistics()

    @property
    def input_sessions(self) -> typing.Sequence[LoopbackInputSession]:
        return list(self._input_sessions.values())

    @property
    def output_sessions(self) -> typing.Sequence[LoopbackOutputSession]:
        return list(self._output_sessions.values())

    @property
    def descriptor(self) -> str:
        return '<loopback/>'
