#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import enum
import dataclasses


class Priority(enum.Enum):
    EXCEPTIONAL = enum.auto()
    IMMEDIATE   = enum.auto()
    FAST        = enum.auto()
    HIGH        = enum.auto()
    NOMINAL     = enum.auto()
    LOW         = enum.auto()
    SLOW        = enum.auto()
    OPTIONAL    = enum.auto()


@dataclasses.dataclass
class Transfer:
    priority: Priority
    transfer_id: int
    payload: bytes


@dataclasses.dataclass
class MessageTransfer(Transfer):
    subject_id: int


@dataclasses.dataclass
class ServiceTransfer(Transfer):
    service_id: int
    remote_node_id: int
