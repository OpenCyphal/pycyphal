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


#: Transfer payload is allowed to be segmented to facilitate zero-copy implementations.
#: The format of the memoryview object should be 'B'.
#: We're using Sequence and not Iterable to permit sharing across multiple consumers.
FragmentedPayload = typing.Sequence[memoryview]


class Priority(enum.IntEnum):
    """
    Transfer priority enumeration follows the recommended names provided in the UAVCAN specification.
    We use integers here in order to allow usage of static lookup tables for conversion into transport-specific
    priority values. The particular integer values used here may be meaningless for some transports.
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
    """
    UAVCAN transfer representation.
    """
    #: For output (tx) transfers this field contains the transfer creation timestamp.
    #: For input (rx) transfers this field contains the first frame reception timestamp.
    timestamp: Timestamp

    #: See :class:`Priority`.
    priority: Priority

    #: When transmitting, the appropriate modulus will be computed by the transport automatically.
    #: Higher layers shall use monotonically increasing transfer-ID counters.
    transfer_id: int

    #: See :class:`FragmentedPayload`. This is the serialized application-level payload.
    #: Fragmentation may be completely arbitrary.
    #: Received transfers usually have it fragmented such that one fragment corresponds to one received frame.
    #: Outgoing transfers usually fragment it according to the structure of the serialized data object.
    #: The purpose of fragmentation is to eliminate unnecessary data copying within the protocol stack.
    #: :func:`pyuavcan.util.refragment` is designed to facilitate regrouping when sending a transfer.
    fragmented_payload: FragmentedPayload


@dataclasses.dataclass
class TransferFrom(Transfer):
    """
    Specialization for received transfers.
    """
    #: None indicates anonymous transfers.
    source_node_id: typing.Optional[int]
