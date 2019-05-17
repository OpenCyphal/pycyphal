#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import pyuavcan.transport


class AggregatePort(abc.ABC):
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


class AggregateInputPort(AggregatePort):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> pyuavcan.transport.ReceivedTransfer:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.ReceivedTransfer]:
        raise NotImplementedError


class AggregateOutputPort(AggregatePort):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.OutgoingTransfer) -> None:
        raise NotImplementedError
