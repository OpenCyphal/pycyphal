#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan.transport


class PromiscuousInputSession(pyuavcan.transport.PromiscuousInputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> pyuavcan.transport.TransferFrom:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        raise NotImplementedError


class SelectiveInputSession(pyuavcan.transport.SelectiveInputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> pyuavcan.transport.Transfer:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.Transfer]:
        raise NotImplementedError

    @property
    def source_node_id(self) -> int:
        raise NotImplementedError


class BroadcastOutputSession(pyuavcan.transport.BroadcastOutputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError


class UnicastOutputSession(pyuavcan.transport.UnicastOutputSession):
    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    @property
    def destination_node_id(self) -> int:
        raise NotImplementedError
