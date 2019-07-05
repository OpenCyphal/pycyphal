#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import dataclasses
from ._timestamp import Timestamp


# The format of the memoryview object should be 'B'.
# We're using Sequence and not Iterable to permit sharing across multiple consumers.
FragmentedPayload = typing.Sequence[memoryview]


class Priority(enum.IntEnum):
    """
    We use integers here in order to allow usage of static lookup tables for conversion into transport-specific
    priority values. The particular integer values used here are meaningless.
    """
    EXCEPTIONAL = 0
    IMMEDIATE   = 1
    FAST        = 2
    HIGH        = 3
    NOMINAL     = 4
    LOW         = 5
    SLOW        = 6
    OPTIONAL    = 7


@dataclasses.dataclass
class Transfer:
    timestamp:          Timestamp           # When transmitting, contains the creation timestamp
    priority:           Priority
    transfer_id:        int                 # When transmitting, modulo will be computed by the transport
    fragmented_payload: FragmentedPayload


@dataclasses.dataclass
class TransferFrom(Transfer):
    source_node_id: typing.Optional[int]    # Set to None to indicate anonymous transfers
