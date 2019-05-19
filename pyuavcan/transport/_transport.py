#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses
from ._session import PromiscuousInputSession, SelectiveInputSession, BroadcastOutputSession, UnicastOutputSession
from ._data_specifier import DataSpecifier


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    transfer_id_modulo:                           int   # 32 for CAN, 2**56 for UDP, etc.
    node_id_set_cardinality:                      int   # 128 for CAN, etc.
    single_frame_transfer_payload_capacity_bytes: int   # 7 for CAN 2.0, 63 for CAN FD, etc.


@dataclasses.dataclass
class Statistics:
    @dataclasses.dataclass
    class Directional:
        transfers: int
        frames:    int
        bytes:     int
        errors:    int
        overruns:  int

    output: Directional
    input:  Directional
    errors: int


class Transport(abc.ABC):
    @property
    @abc.abstractmethod
    def protocol_parameters(self) -> ProtocolParameters:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> typing.Optional[int]:
        raise NotImplementedError

    @abc.abstractmethod
    async def set_local_node_id(self, node_id: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_statistics(self) -> Statistics:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_broadcast_output(self, data_specifier: DataSpecifier) -> BroadcastOutputSession:
        """
        All transports must support this session type for messages.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_unicast_output(self, data_specifier: DataSpecifier, destination_node_id: int) -> UnicastOutputSession:
        """
        All transports must support this session type for services.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_promiscuous_input(self, data_specifier: DataSpecifier) -> PromiscuousInputSession:
        """
        All transports must support this session type for all kinds of transfers.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_selective_input(self, data_specifier: DataSpecifier, source_node_id: int) -> SelectiveInputSession:
        """
        All transports must support this session type for service transfers.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Should print the basic transport information: address, media configuration, etc.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__str__()
