#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.transport
import pyuavcan.dsdl


DataTypeClass = typing.TypeVar('DataTypeClass', bound=pyuavcan.dsdl.CompositeObject)


class Port(abc.ABC, typing.Generic[DataTypeClass]):
    @property
    @abc.abstractmethod
    def data_type_class(self) -> typing.Type[DataTypeClass]:
        raise NotImplementedError


class MessagePort(Port[DataTypeClass]):
    @property
    @abc.abstractmethod
    def subject_id(self) -> int:
        raise NotImplementedError


class Publisher(MessagePort[DataTypeClass]):
    def __init__(self,
                 cls:                  typing.Type[DataTypeClass],
                 transport_layer_port: pyuavcan.transport.Publisher,
                 priority:             pyuavcan.transport.Priority,
                 loopback:             bool):
        self._cls = cls
        self._tlp = transport_layer_port
        self._priority = priority
        self._loopback = loopback

    @property
    def data_type_class(self) -> typing.Type[DataTypeClass]:
        return self._cls

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return self._priority

    @priority.setter
    def priority(self, value: pyuavcan.transport.Priority) -> None:
        self._priority = pyuavcan.transport.Priority(value)

    @property
    def loopback(self) -> bool:
        return self._loopback

    @loopback.setter
    def loopback(self, value: bool) -> None:
        self._loopback = bool(value)

    @property
    def subject_id(self) -> int:
        return self._tlp.data_specifier.subject_id

    async def publish(self, message: DataTypeClass) -> None:
        fragmented_payload = pyuavcan.dsdl.serialize(message)
        # noinspection PyCallByClass
        transfer = pyuavcan.transport.Publisher.Transfer(priority=self._priority,
                                                         transfer_id=0,  # TODO
                                                         fragmented_payload=fragmented_payload,
                                                         loopback=self._loopback)
        await self._tlp.publish(transfer)
