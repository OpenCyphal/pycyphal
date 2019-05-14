#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import enum
import dataclasses


class Publisher(abc.ABC):
    pass


class Subscriber(abc.ABC):
    pass


class Client(abc.ABC):
    pass


class Server(abc.ABC):
    pass


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


class TransferIDPolicy(enum.Enum):
    PROGRESSIVE = enum.auto()   # Like UDP or IEEE 802.15.4
    OVERFLOWING = enum.auto()   # Like CAN 2.0 or CAN FD


class Transport(abc.ABC):
    @property
    @abc.abstractmethod
    def transfer_id_policy(self) -> TransferIDPolicy:
        raise NotImplementedError

    @abc.abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_publisher(self, subject_id: int) -> Publisher:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_subscriber(self, subject_id: int) -> Subscriber:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_client(self, service_id: int) -> Client:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_server(self, service_id: int) -> Server:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_statistics(self) -> Statistics:
        raise NotImplementedError
