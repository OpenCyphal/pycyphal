#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import enum
import typing
import dataclasses


# The format of the memoryview object should be 'B'.
# We're using Sequence and not Iterable to permit sharing across multiple consumers.
FragmentedPayload = typing.Sequence[memoryview]


class Priority(enum.IntEnum):
    """
    We're using integers here in order to allow usage of static lookup tables for conversion into transport-specific
    priority values. The particular integer values used here are meaningless.
    """
    EXCEPTIONAL = 0
    IMMEDIATE   = 1
    FAST        = 2
    HIGH        = 3
    NOMINAL     = 4
    LOW         = 5
    SLOW        = 6
    OPTIONAL    = 7


@dataclasses.dataclass
class Timestamp:
    wall:      float    # Belongs to the domain of time.time()
    monotonic: float    # Belongs to the domain of time.monotonic()


@dataclasses.dataclass(frozen=True)
class DataSpecifier:
    compact_data_type_id:   int
    max_payload_size_bytes: int


@dataclasses.dataclass(frozen=True)
class MessageDataSpecifier(DataSpecifier):
    subject_id: int


@dataclasses.dataclass(frozen=True)
class ServiceDataSpecifier(DataSpecifier):
    class Role(enum.Enum):
        CLIENT = enum.auto()
        SERVER = enum.auto()

    service_id: int
    role:       Role


@dataclasses.dataclass
class Transfer:
    priority:           Priority
    transfer_id:        int                     # When transmitting, modulo will be computed by the transport
    fragmented_payload: FragmentedPayload
    loopback:           bool                    # Request in outgoing transfers, indicator in received transfers


@dataclasses.dataclass
class ReceivedTransfer(Transfer):
    timestamp:      Timestamp
    source_node_id: typing.Optional[int]        # Not set for anonymous transfers


@dataclasses.dataclass
class OutgoingTransfer(Transfer):
    destination_node_id: typing.Optional[int]   # Not set for broadcast transfers


class Port(abc.ABC):        # TODO: statistics
    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class InputPort(Port):
    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def receive(self) -> ReceivedTransfer:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[ReceivedTransfer]:
        raise NotImplementedError


class OutputPort(Port):
    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, transfer: OutgoingTransfer) -> None:
        raise NotImplementedError
