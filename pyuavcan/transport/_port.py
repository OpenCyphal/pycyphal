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
FragmentedPayload = typing.Iterable[memoryview]


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


class Port(abc.ABC):
    @dataclasses.dataclass(frozen=True)
    class DataSpecifierBase:
        compact_data_type_id: int

    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifierBase:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


class MessagePort(Port):
    @dataclasses.dataclass(frozen=True)
    class DataSpecifier(Port.DataSpecifierBase):
        subject_id:             int
        max_payload_size_bytes: int

    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError


class ServicePort(Port):
    @dataclasses.dataclass(frozen=True)
    class DataSpecifier(Port.DataSpecifierBase):
        MaxPayloadSize = typing.NamedTuple('MaxPayloadSize', [('request', int), ('response', int)])
        service_id:             int
        max_payload_size_bytes: MaxPayloadSize

    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError


class Publisher(MessagePort):
    @dataclasses.dataclass
    class Transfer:
        priority:           Priority
        transfer_id:        int
        fragmented_payload: FragmentedPayload
        loopback:           bool = False

    @abc.abstractmethod
    async def publish(self, transfer: Transfer) -> None:
        raise NotImplementedError


class Subscriber(MessagePort):
    @dataclasses.dataclass
    class Transfer:
        timestamp:          Timestamp
        transfer_id:        int
        publisher_node_id:  int
        fragmented_payload: FragmentedPayload
        loopback:           bool

    @abc.abstractmethod
    async def receive(self) -> Transfer:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, timeout: float) -> typing.Optional[Transfer]:
        raise NotImplementedError


class Client(ServicePort):
    @dataclasses.dataclass
    class Request:
        priority:           Priority
        transfer_id:        int
        fragmented_payload: FragmentedPayload

    @dataclasses.dataclass
    class Response:
        timestamp:          Timestamp
        fragmented_payload: FragmentedPayload

    @abc.abstractmethod
    async def try_request(self, request: Request, response_timeout: float) -> typing.Optional[Response]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def server_node_id(self) -> int:
        raise NotImplementedError


class Server(ServicePort):
    @dataclasses.dataclass(frozen=True)
    class TransactionMetadata:
        priority:           Priority
        transfer_id:        int
        client_node_id:     int
        fragmented_payload: FragmentedPayload

    @dataclasses.dataclass
    class Request:
        timestamp:            Timestamp
        transaction_metadata: Server.TransactionMetadata
        fragmented_payload:   FragmentedPayload

    @dataclasses.dataclass
    class Response:
        transaction_metadata: Server.TransactionMetadata
        fragmented_payload:   FragmentedPayload

    @abc.abstractmethod
    async def listen(self) -> Request:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_listen(self, timeout: float) -> typing.Optional[Request]:
        raise NotImplementedError

    @abc.abstractmethod
    async def respond(self, response: Response) -> None:
        raise NotImplementedError
