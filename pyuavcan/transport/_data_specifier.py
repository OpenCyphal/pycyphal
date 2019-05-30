#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import dataclasses


@dataclasses.dataclass(frozen=True)
class DataSpecifier:
    pass


@dataclasses.dataclass(frozen=True)
class MessageDataSpecifier(DataSpecifier):
    SUBJECT_ID_MASK = 32767

    subject_id: int


@dataclasses.dataclass(frozen=True)
class ServiceDataSpecifier(DataSpecifier):
    class Role(enum.Enum):
        CLIENT = enum.auto()
        SERVER = enum.auto()

    SERVICE_ID_MASK = 511

    service_id: int
    role:       Role
