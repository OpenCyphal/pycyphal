#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
from ._transfer import Transfer, TransferFrom
from ._data_specifier import DataSpecifier


class Session(abc.ABC):        # TODO: statistics
    @property
    @abc.abstractmethod
    def data_specifier(self) -> DataSpecifier:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


# ------------------------------------- INPUT -------------------------------------

# noinspection PyAbstractClass
class InputSession(Session):
    @abc.abstractmethod
    async def receive(self) -> Transfer:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[Transfer]:
        raise NotImplementedError


# noinspection PyAbstractClass
class PromiscuousInputSession(InputSession):
    @abc.abstractmethod
    async def receive(self) -> TransferFrom:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[TransferFrom]:
        raise NotImplementedError


# noinspection PyAbstractClass
class SelectiveInputSession(InputSession):
    @property
    @abc.abstractmethod
    def source_node_id(self) -> int:
        raise NotImplementedError


# ------------------------------------- OUTPUT -------------------------------------

# noinspection PyAbstractClass
class OutputSession(Session):
    @abc.abstractmethod
    async def send(self, transfer: Transfer) -> None:
        raise NotImplementedError


# noinspection PyAbstractClass
class BroadcastOutputSession(OutputSession):
    pass


# noinspection PyAbstractClass
class UnicastOutputSession(OutputSession):
    @property
    @abc.abstractmethod
    def destination_node_id(self) -> int:
        raise NotImplementedError
