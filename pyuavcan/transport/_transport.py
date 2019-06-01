#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses
from ._session import InputSession, OutputSession
from ._session import PromiscuousInput, SelectiveInput, BroadcastOutput, UnicastOutput
from ._data_specifier import DataSpecifier
from ._payload_metadata import PayloadMetadata


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    transfer_id_modulo:                           int   # 32 for CAN, 2**56 for UDP, etc.
    node_id_set_cardinality:                      int   # 128 for CAN, etc.
    single_frame_transfer_payload_capacity_bytes: int   # 7 for CAN 2.0, 63 for CAN FD, etc.


class Transport(abc.ABC):
    @property
    @abc.abstractmethod
    def protocol_parameters(self) -> ProtocolParameters:
        """
        Generally, the returned values are constant, as in, they never change for the current transport instance.
        This is not a hard guarantee, however. For example, the redundant transport aggregator may return a different
        set of parameters after the set of aggregated transports is changed (e.g., a transport is added or removed).
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> typing.Optional[int]:
        """
        By default, the local node ID is not assigned, meaning that the local node is in the anonymous mode.
        While in the anonymous mode, some transports may choose to operate in a particular mode to facilitate
        plug-and-play node ID allocation. For example, a CAN transport may disable automatic retransmission as
        dictated by the Specification.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def set_local_node_id(self, node_id: int) -> None:
        """
        This method can be invoked only if the local node ID is not assigned. Once a local node ID is assigned,
        this method shall not be invoked anymore. In other words, it can be invoked at most once.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        """
        After a transport is closed, none of its methods can be used. The behavior of methods invoked on a closed
        transport is undefined.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_promiscuous_input(self,
                                    data_specifier:   DataSpecifier,
                                    payload_metadata: PayloadMetadata) -> PromiscuousInput:
        """
        All transports must support this session type for all kinds of transfers.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_selective_input(self,
                                  data_specifier:   DataSpecifier,
                                  payload_metadata: PayloadMetadata,
                                  source_node_id:   int) -> SelectiveInput:
        """
        All transports must support this session type for services.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_broadcast_output(self,
                                   data_specifier:   DataSpecifier,
                                   payload_metadata: PayloadMetadata) -> BroadcastOutput:
        """
        All transports must support this session type for messages.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_unicast_output(self,
                                 data_specifier:      DataSpecifier,
                                 payload_metadata:    PayloadMetadata,
                                 destination_node_id: int) -> UnicastOutput:
        """
        All transports must support this session type for services.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def inputs(self) -> typing.Sequence[InputSession]:
        """
        All active input sessions.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def outputs(self) -> typing.Sequence[OutputSession]:
        """
        All active output sessions.
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
