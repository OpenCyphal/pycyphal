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

    def __post_init__(self) -> None:
        if not (0 <= self.subject_id <= self.SUBJECT_ID_MASK):
            raise ValueError(f'Invalid subject ID: {self.subject_id}')


@dataclasses.dataclass(frozen=True)
class ServiceDataSpecifier(DataSpecifier):
    class Role(enum.Enum):
        CLIENT = enum.auto()
        SERVER = enum.auto()

    SERVICE_ID_MASK = 511

    service_id: int
    role:       Role

    def __post_init__(self) -> None:
        assert self.role in self.Role
        if not (0 <= self.service_id <= self.SERVICE_ID_MASK):
            raise ValueError(f'Invalid service ID: {self.service_id}')
