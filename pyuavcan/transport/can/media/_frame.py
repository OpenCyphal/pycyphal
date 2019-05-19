#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport


NODE_ID_MASK = 127
TRANSFER_ID_MODULO = 32

_SUBJECT_ID_MASK = 32767
_SERVICE_ID_MASK = 511
_PRIORITY_MASK = 7


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
    data:       bytearray
    extended:   bool

    @property
    def data_length_code(self) -> int:
        try:
            return _LENGTH_TO_DLC[len(self.data)]
        except LookupError:
            raise ValueError(f'{len(self.data)} bytes is not a valid data length; '
                             f'valid length values are: {list(_LENGTH_TO_DLC.keys())}') from None

    @staticmethod
    def parse_data_length_code(dlc: int) -> int:
        try:
            return _DLC_TO_LENGTH[dlc]
        except LookupError:
            raise ValueError(f'{dlc} is not a valid DLC') from None

    @staticmethod
    def new(priority:          pyuavcan.transport.Priority,
            session_specifier: SessionSpecifier,
            padded_payload:    memoryview,
            transfer_id:       int,
            start_of_transfer: bool,
            end_of_transfer:   bool,
            toggle_bit:        bool) -> Frame:
        # Tail byte computation
        if transfer_id < 0:
            raise ValueError(f'Invalid transfer ID: {transfer_id}')
        tail_byte = transfer_id % TRANSFER_ID_MODULO
        if start_of_transfer:
            tail_byte |= 1 << 7
        if end_of_transfer:
            tail_byte |= 1 << 6
        if toggle_bit:
            tail_byte |= 1 << 5

        # Frame data
        data = bytearray(padded_payload)
        data.append(tail_byte)
        if len(data) not in _LENGTH_TO_DLC:
            raise ValueError(f'Unsupported payload length: {len(padded_payload)}')

        # Identifier depending on the session specifier
        int_priority = int(priority)
        Frame._validate_unsigned_range(int_priority, _PRIORITY_MASK)
        identifier = int_priority << 26
        if isinstance(session_specifier, MessageSessionSpecifier):
            Frame._validate_unsigned_range(session_specifier.subject_id, _SUBJECT_ID_MASK)
            identifier |= session_specifier.subject_id << 8
            source_node_id = session_specifier.source_node_id
            if source_node_id is None:
                source_node_id = sum(data) & NODE_ID_MASK
                identifier |= (1 << 24)
        elif isinstance(session_specifier, ServiceSessionSpecifier):
            Frame._validate_unsigned_range(session_specifier.service_id, _SERVICE_ID_MASK)
            Frame._validate_unsigned_range(session_specifier.destination_node_id, NODE_ID_MASK)
            source_node_id = session_specifier.source_node_id
            identifier |= (1 << 25) | (session_specifier.service_id << 15) \
                | (session_specifier.destination_node_id << 8)
            if session_specifier.request_not_response:
                identifier |= 1 << 24
        else:
            raise ValueError(f'Unsupported session specifier: {type(session_specifier)}')

        Frame._validate_unsigned_range(source_node_id, NODE_ID_MASK)
        identifier |= source_node_id << 1

        assert 0 <= identifier < 2 ** 29
        out = Frame(identifier=identifier, data=data, extended=True)
        assert Frame.parse_data_length_code(out.data_length_code) == len(out.data)
        return out

    @staticmethod
    def _validate_unsigned_range(value: int, max_value: int) -> None:
        if value > max_value:
            raise ValueError(f'Value {value} exceeds the limit {max_value}')


@dataclasses.dataclass
class ReceivedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    @property
    def payload(self) -> memoryview:
        return memoryview(self.data)[:-1]

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
        return pyuavcan.transport.Priority((self.identifier >> 26) & _PRIORITY_MASK)

    @property
    def session_specifier(self) -> SessionSpecifier:
        service_not_message = self.identifier & (1 << 25) != 0
        source_node_id = (self.identifier >> 1) & NODE_ID_MASK
        if service_not_message:
            return ServiceSessionSpecifier(service_id=(self.identifier >> 15) & _SERVICE_ID_MASK,
                                           request_not_response=self.identifier & (1 << 24) != 0,
                                           source_node_id=source_node_id,
                                           destination_node_id=(self.identifier >> 8) & NODE_ID_MASK)
        else:
            anonymous = self.identifier & (1 << 24) != 0
            return MessageSessionSpecifier(subject_id=(self.identifier >> 8) & _SUBJECT_ID_MASK,
                                           source_node_id=None if anonymous else source_node_id)

    @property
    def _tail(self) -> int:
        return self.payload[-1]


_DLC_TO_LENGTH = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]
_LENGTH_TO_DLC: typing.Dict[int, int] = dict(zip(*list(zip(*enumerate(_DLC_TO_LENGTH)))[::-1]))  # type: ignore
assert len(_LENGTH_TO_DLC) == 16 == len(_DLC_TO_LENGTH)
for item in _DLC_TO_LENGTH:
    assert _DLC_TO_LENGTH[_LENGTH_TO_DLC[item]] == item, 'Invalid DLC tables'
