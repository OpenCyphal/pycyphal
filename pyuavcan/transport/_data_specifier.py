# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import enum
import dataclasses


@dataclasses.dataclass(frozen=True)
class DataSpecifier:
    """
    The data specifier defines what category and type of data is exchanged over a transport session.
    See the abstract transport model for details.
    """


@dataclasses.dataclass(frozen=True)
class MessageDataSpecifier(DataSpecifier):
    SUBJECT_ID_MASK = 2 ** 13 - 1

    subject_id: int

    def __post_init__(self) -> None:
        if not (0 <= self.subject_id <= self.SUBJECT_ID_MASK):
            raise ValueError(f"Invalid subject ID: {self.subject_id}")


@dataclasses.dataclass(frozen=True)
class ServiceDataSpecifier(DataSpecifier):
    class Role(enum.Enum):
        REQUEST = enum.auto()
        """
        Request output role is for clients.
        Request input role is for servers.
        """
        RESPONSE = enum.auto()
        """
        Response output role is for servers.
        Response input role is for clients.
        """

    SERVICE_ID_MASK = 2 ** 9 - 1

    service_id: int
    role: Role

    def __post_init__(self) -> None:
        assert self.role in self.Role
        if not (0 <= self.service_id <= self.SERVICE_ID_MASK):
            raise ValueError(f"Invalid service ID: {self.service_id}")
