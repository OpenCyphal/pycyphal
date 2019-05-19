#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport


@dataclasses.dataclass(frozen=True)
class SessionSpecifier:
    pass


@dataclasses.dataclass(frozen=True)
class MessageSessionSpecifier(SessionSpecifier):
    subject_id: int
    source_node_id: typing.Optional[int]  # None if anonymous


@dataclasses.dataclass(frozen=True)
class ServiceSessionSpecifier(SessionSpecifier):
    service_id:           int
    request_not_response: bool
    source_node_id:       int
    destination_node_id:  int


@dataclasses.dataclass
class Frame:
    identifier: int
    payload:    bytearray
    extended:   bool

    @staticmethod
    def new_message(priority: pyuavcan.transport.Priority) -> Frame:  # TODO
        raise NotImplementedError

    @staticmethod
    def new_service(priority: pyuavcan.transport.Priority) -> Frame:  # TODO
        raise NotImplementedError

    _NODE_ID_MASK = 127


@dataclasses.dataclass
class ReceivedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    @property
    def start_of_transfer(self) -> bool:
        return self._tail & (1 << 7) != 0

    @property
    def end_of_transfer(self) -> bool:
        return self._tail & (1 << 6) != 0

    @property
    def toggle_bit(self) -> bool:
        return self._tail & (1 << 5) != 0

    @property
    def transfer_id(self) -> int:
        return self._tail & 31

    @property
    def priority(self) -> pyuavcan.transport.Priority:
        return pyuavcan.transport.Priority((self.identifier >> 26) & 0b111)

    @property
    def session_specifier(self) -> SessionSpecifier:
        service_not_message = self.identifier & (1 << 25) != 0
        source_node_id = (self.identifier >> 1) & self._NODE_ID_MASK
        if service_not_message:
            return ServiceSessionSpecifier(service_id=(self.identifier >> 15) & (2 ** 9 - 1),
                                           request_not_response=self.identifier & (1 << 24) != 0,
                                           source_node_id=source_node_id,
                                           destination_node_id=(self.identifier >> 8) & self._NODE_ID_MASK)
        else:
            anonymous = self.identifier & (1 << 24) != 0
            return MessageSessionSpecifier(subject_id=(self.identifier >> 8) & (2 ** 15 - 1),
                                           source_node_id=None if anonymous else source_node_id)

    @property
    def _tail(self) -> int:
        return self.payload[-1]
