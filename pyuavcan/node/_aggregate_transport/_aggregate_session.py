#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import pyuavcan.transport


class AggregateSession(abc.ABC):
    @property
    @abc.abstractmethod
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    @abc.abstractmethod
    def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError


# ------------------------------------- INPUT -------------------------------------

# noinspection PyAbstractClass
class InputAggregateSession(AggregateSession):
    # We do not extend the Transfer type in order to permit return type covariance in derived classes.
    Output = typing.Tuple[pyuavcan.transport.Transfer, pyuavcan.transport.Transport]

    @abc.abstractmethod
    async def receive(self) -> Output:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[Output]:
        raise NotImplementedError


# noinspection PyAbstractClass
class PromiscuousInputAggregateSession(InputAggregateSession):
    Output = typing.Tuple[pyuavcan.transport.PromiscuousInputSession.TransferFrom, pyuavcan.transport.Transport]

    @abc.abstractmethod
    async def receive(self) -> Output:  # covariant on Transfer
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[Output]:  # covariant on Transfer
        raise NotImplementedError


# noinspection PyAbstractClass
class SelectiveInputAggregateSession(InputAggregateSession):
    @property
    @abc.abstractmethod
    def source_node_id(self) -> int:
        raise NotImplementedError


# ------------------------------------- OUTPUT -------------------------------------

# noinspection PyAbstractClass
class OutputAggregateSession(AggregateSession):
    @abc.abstractmethod
    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError


# noinspection PyAbstractClass
class BroadcastOutputAggregateSession(OutputAggregateSession):
    pass


# noinspection PyAbstractClass
class UnicastOutputAggregateSession(OutputAggregateSession):
    @property
    @abc.abstractmethod
    def destination_node_id(self) -> int:
        raise NotImplementedError
