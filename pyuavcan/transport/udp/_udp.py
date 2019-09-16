#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#
import asyncio
import ipaddress
import typing
import pyuavcan
from ._session import UDPInputSession, UDPOutputSession


class UDPTransport(pyuavcan.transport.Transport):
    def __init__(self):
        pass

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        pass

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        pass

    @property
    def local_node_id(self) -> typing.Optional[int]:
        pass

    def set_local_node_id(self, node_id: int) -> None:
        pass

    def close(self) -> None:
        pass

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPInputSession:
        pass

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPOutputSession:
        pass

    def sample_statistics(self) -> pyuavcan.transport.TransportStatistics:
        pass

    @property
    def input_sessions(self) -> typing.Sequence[UDPInputSession]:
        pass

    @property
    def output_sessions(self) -> typing.Sequence[UDPOutputSession]:
        pass

    @property
    def descriptor(self) -> str:
        pass
