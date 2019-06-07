#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses
from ._session import InputSession, OutputSession, SessionSpecifier
from ._payload_metadata import PayloadMetadata


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    transfer_id_modulo:                           int   # 32 for CAN, 2**56 for UDP, etc.
    node_id_set_cardinality:                      int   # 128 for CAN, etc.
    single_frame_transfer_payload_capacity_bytes: int   # 7 for CAN 2.0, <=63 for CAN FD, etc.


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
        Generally, the local node ID is not assigned by default, meaning that the local node is in the anonymous mode.
        While in the anonymous mode, some transports may choose to operate in a particular mode to facilitate
        plug-and-play node ID allocation. For example, a CAN transport may disable automatic retransmission as
        dictated by the Specification. Some transports, however, may initialize with a node ID already set if such is
        dictated by the media configuration (for example, a UDP transfer may initialize with the node ID derived
        from the address of the local host).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def set_local_node_id(self, node_id: int) -> None:
        """
        This method can be invoked only if the local node ID is not assigned. Once a local node ID is assigned,
        this method shall not be invoked anymore. In other words, it can be successfully invoked at most once.
        The transport implementation should raise an appropriate exception derived from TransportError when that
        is attempted.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        """
        After a transport is closed, none of its methods can be used. The behavior of methods invoked on a closed
        transport is undefined. Generally, when closed, the transport should also close its underlying resources
        such as media instances.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_input_session(self, specifier: SessionSpecifier, payload_metadata: PayloadMetadata) -> InputSession:
        """
        The transport will always return the same instance unless there is no session object with the requested
        specifier, in which case it will be created and stored internally until closed. The payload metadata parameter
        is used only when a new instance is created, ignored otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_output_session(self, specifier: SessionSpecifier, payload_metadata: PayloadMetadata) -> OutputSession:
        """
        The transport will always return the same instance unless there is no session object with the requested
        specifier, in which case it will be created and stored internally until closed. The payload metadata parameter
        is used only when a new instance is created, ignored otherwise.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def input_sessions(self) -> typing.Sequence[InputSession]:
        """
        All active input sessions.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def output_sessions(self) -> typing.Sequence[OutputSession]:
        """
        All active output sessions.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        """
        Prints the basic transport information. May be overridden if there is more relevant info to display.
        """
        # TODO: somehow obtain the media information and print it here. Add a basic media info property of type str?
        return f'{type(self).__name__}(' \
            f'protocol_parameters={self.protocol_parameters}, ' \
            f'local_node_id={self.local_node_id}, ' \
            f'input_sessions=[{", ".join(map(str, self.input_sessions))}], ' \
            f'output_sessions=[{", ".join(map(str, self.output_sessions))}])'

    def __repr__(self) -> str:
        return self.__str__()
