#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import pyuavcan.transport


class TransportSpecificTransfer(abc.ABC):
    @property
    @abc.abstractmethod
    def transport(self) -> pyuavcan.transport.Transport:
        """The transport over which the transfer has been or to be transferred."""
        raise NotImplementedError


class RedundantSession:
    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError


# ------------------------------------- INPUT -------------------------------------

class PromiscuousInputRedundantSession(pyuavcan.transport.PromiscuousInputSession, RedundantSession):
    class RedundantTransferFrom(pyuavcan.transport.TransferFrom, TransportSpecificTransfer):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> RedundantTransferFrom:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        raise NotImplementedError


class SelectiveInputRedundantSession(pyuavcan.transport.SelectiveInputSession, RedundantSession):
    class RedundantTransfer(pyuavcan.transport.TransferFrom, TransportSpecificTransfer):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> RedundantTransfer:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransfer]:
        raise NotImplementedError

    @property
    def source_node_id(self) -> int:
        raise NotImplementedError


# ------------------------------------- OUTPUT -------------------------------------

class BroadcastOutputRedundantSession(pyuavcan.transport.BroadcastOutputSession, RedundantSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError


class UnicastOutputRedundantSession(pyuavcan.transport.UnicastOutputSession, RedundantSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @property
    def destination_node_id(self) -> int:
        raise NotImplementedError
