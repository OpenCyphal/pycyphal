#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class Frame:
    priority:            pyuavcan.transport.Priority
    source_node_id:      typing.Optional[int]
    destination_node_id: typing.Optional[int]
    data_specifier:      pyuavcan.transport.DataSpecifier
    data_type_hash:      int
    transfer_id:         int
    frame_index:         int
    end_of_transfer:     bool

    def compile(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class TimestampedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    @staticmethod
    def parse(source: bytes) -> typing.Optional[TimestampedFrame]:
        raise NotImplementedError
