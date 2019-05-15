#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import dataclasses
from . import _port


@dataclasses.dataclass
class Statistics:
    @dataclasses.dataclass
    class Directional:
        frames:   int
        bytes:    int
        errors:   int
        overruns: int

    outgoing: Directional
    incoming: Directional
    errors:   int


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    transfer_id_modulo:                           int   # 32 for CAN, 2**56 for UDP, etc.
    node_id_set_cardinality:                      int   # 128 for CAN, etc.
    single_frame_transfer_payload_capacity_bytes: int   # 7 for CAN 2.0, 63 for CAN FD, etc.


class Transport(abc.ABC):
    @property
    @abc.abstractmethod
    def protocol_parameters(self) -> ProtocolParameters:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    async def set_local_node_id(self, node_id: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_publisher(self, data_specifier: _port.MessagePort.DataSpecifier) -> _port.Publisher:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_subscriber(self, data_specifier: _port.MessagePort.DataSpecifier) -> _port.Subscriber:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_client(self, data_specifier: _port.ServicePort.DataSpecifier, server_node_id: int) -> _port.Client:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_server(self, data_specifier: _port.ServicePort.DataSpecifier) -> _port.Server:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_statistics(self) -> Statistics:
        raise NotImplementedError
