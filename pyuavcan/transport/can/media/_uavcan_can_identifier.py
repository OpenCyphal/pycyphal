#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import random
import typing
import dataclasses
import pyuavcan.transport
from . import _filter


@dataclasses.dataclass(frozen=True)
class CANIdentifier:
    PRIORITY_MASK = 7
    NODE_ID_MASK = 127

    priority: pyuavcan.transport.Priority

    def compile(self) -> int:
        raise NotImplementedError

    @staticmethod
    def parse(identifier: int) -> CANIdentifier:
        _validate_unsigned_range(identifier, 2 ** 29 - 1)
        priority = pyuavcan.transport.Priority(identifier >> 26)
        source_node_id = (identifier >> 1) & CANIdentifier.NODE_ID_MASK
        service_not_message = identifier & (1 << 25) != 0
        if service_not_message:
            spec: CANIdentifier = ServiceCANIdentifier(
                priority=priority,
                service_id=(identifier >> 15) & ServiceCANIdentifier.SERVICE_ID_MASK,
                request_not_response=identifier & (1 << 24) != 0,
                source_node_id=source_node_id,
                destination_node_id=(identifier >> 8) & CANIdentifier.NODE_ID_MASK
            )
        else:
            anonymous = identifier & (1 << 24) != 0
            spec = MessageCANIdentifier(
                priority=priority,
                subject_id=(identifier >> 8) & MessageCANIdentifier.SUBJECT_ID_MASK,
                source_node_id=None if anonymous else source_node_id
            )
        return spec


@dataclasses.dataclass(frozen=True)
class MessageCANIdentifier(CANIdentifier):
    SUBJECT_ID_MASK = 32767

    subject_id:     int
    source_node_id: typing.Optional[int]  # None if anonymous

    def compile(self) -> int:
        _validate_unsigned_range(int(self.priority), self.PRIORITY_MASK)
        _validate_unsigned_range(self.subject_id, self.SUBJECT_ID_MASK)

        identifier = (int(self.priority) << 26) | (self.subject_id << 8)

        source_node_id = self.source_node_id
        if source_node_id is None:  # Anonymous frame
            source_node_id = random.randint(0, self.NODE_ID_MASK)
            identifier |= (1 << 24)

        _validate_unsigned_range(source_node_id, self.NODE_ID_MASK)
        identifier |= source_node_id << 1

        assert 0 <= identifier < 2 ** 29
        return identifier


@dataclasses.dataclass(frozen=True)
class ServiceCANIdentifier(CANIdentifier):
    SERVICE_ID_MASK = 511

    service_id:           int
    request_not_response: bool
    source_node_id:       int
    destination_node_id:  int

    def compile(self) -> int:
        _validate_unsigned_range(int(self.priority), self.PRIORITY_MASK)
        _validate_unsigned_range(self.service_id, self.SERVICE_ID_MASK)
        _validate_unsigned_range(self.source_node_id, self.NODE_ID_MASK)
        _validate_unsigned_range(self.destination_node_id, self.NODE_ID_MASK)

        identifier = (int(self.priority) << 26) | (1 << 25) | (self.service_id << 15) | \
            (self.destination_node_id << 8) | (self.source_node_id << 1)

        if self.request_not_response:
            identifier |= 1 << 24

        assert 0 <= identifier < 2 ** 29
        return identifier


def _validate_unsigned_range(value: int, max_value: int) -> None:
    if not (0 <= value <= max_value):
        raise ValueError(f'Value {value} is not in the interval [0, {max_value}]')


def generate_filter_configurations(subject_id_list: typing.Iterable[int],
                                   local_node_id:   typing.Optional[int]) -> typing.List[_filter.FilterConfiguration]:
    from ._frame import Frame

    def ext(idn: int, msk: int) -> _filter.FilterConfiguration:
        assert idn < 2 ** 29 and msk < 2 ** 29
        return _filter.FilterConfiguration(identifier=idn, mask=msk, format=Frame.Format.EXTENDED)

    full: typing.List[_filter.FilterConfiguration] = []

    if local_node_id is not None:
        assert local_node_id < 2 ** 7
        # If the local node ID is set, we may receive service requests, so we need to allocate one filter for those.
        #                     prio s r service-id dest.  source  v
        full.append(ext(idn=0b_000_1_0_000000000_0000000_0000000_1 | (int(local_node_id) << 8),
                        msk=0b_000_1_0_000000000_1111111_0000000_1))
        # Also, we may need loopback frames for timestamping, so we add a filter for frames where the source node ID
        # equals ours.
        #                     prio m a    subject-id    source  v
        full.append(ext(idn=0b_000_0_0_0000000000000000_0000000_1 | (int(local_node_id) << 1),
                        msk=0b_000_1_0_0000000000000000_1111111_1))
    else:
        # If the local node ID is not set, we may need to receive loopback frames for sent anonymous transfers.
        # This essentially means that we need to allow ALL anonymous transfers.
        #                     prio m a    subject-id    source  v
        full.append(ext(idn=0b_000_0_1_0000000000000000_0000000_1,
                        msk=0b_000_1_1_0000000000000000_0000000_1))

    # One filter per unique subject ID. Sorted for testability.
    for sid in sorted(set(subject_id_list)):
        assert sid < 2 ** 16
        #                     prio m a    subject-id    source  v
        full.append(ext(idn=0b_000_0_0_0000000000000000_0000000_1 | (int(sid) << 8),
                        msk=0b_000_1_0_1111111111111111_0000000_1))

    return full


def _unittest_can_media_filter_configuration() -> None:
    from ._filter import FilterConfiguration
    from ._frame import Frame

    def ext(idn: int, msk: int) -> FilterConfiguration:
        assert idn < 2 ** 29 and msk < 2 ** 29
        return FilterConfiguration(identifier=idn, mask=msk, format=Frame.Format.EXTENDED)

    degenerate = FilterConfiguration.compact(generate_filter_configurations([], None), 999)
    assert degenerate == [ext(idn=0b_000_0_1_0000000000000000_0000000_1,    # Anonymous messages
                              msk=0b_000_1_1_0000000000000000_0000000_1)]

    no_subjects = FilterConfiguration.compact(generate_filter_configurations([], 0b1010101), 999)
    assert no_subjects == [
        ext(idn=0b_000_1_0_000000000_1010101_0000000_1,     # Services
            msk=0b_000_1_0_000000000_1111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000000000_1010101_1,      # Loopback messages
            msk=0b_000_1_0_0000000000000000_1111111_1),
    ]

    reference_subject_ids = [
        0b0000000000000000,
        0b0000000000000101,
        0b0000000000001010,
        0b0000000000010101,
        0b0000000000101010,
        0b0000000000101010,  # Duplicate
        0b0000000000101010,  # Triplicate
        0b0000000000101011,  # Similar, Hamming distance 1
    ]

    retained = FilterConfiguration.compact(generate_filter_configurations(reference_subject_ids, 0b1010101), 999)
    assert retained == [
        ext(idn=0b_000_1_0_000000000_1010101_0000000_1,
            msk=0b_000_1_0_000000000_1111111_0000000_1),    # Services

        ext(idn=0b_000_0_0_0000000000000000_1010101_1,      # Loopback messages
            msk=0b_000_1_0_0000000000000000_1111111_1),

        ext(idn=0b_000_0_0_0000000000000000_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000000101_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000001010_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000010101_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000101010_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),     # Duplicates removed

        ext(idn=0b_000_0_0_0000000000101011_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),
    ]

    reduced = FilterConfiguration.compact(generate_filter_configurations(reference_subject_ids, 0b1010101), 7)
    assert reduced == [
        ext(idn=0b_000_1_0_000000000_1010101_0000000_1,
            msk=0b_000_1_0_000000000_1111111_0000000_1),    # Services

        ext(idn=0b_000_0_0_0000000000000000_1010101_1,      # Loopback messages
            msk=0b_000_1_0_0000000000000000_1111111_1),

        ext(idn=0b_000_0_0_0000000000000000_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        ext(idn=0b_000_0_0_0000000000000101_0000000_1,
            msk=0b_000_1_0_1111111111101111_0000000_1),     # Merged with 6th

        ext(idn=0b_000_0_0_0000000000001010_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),

        # This one removed, merged with 4th

        ext(idn=0b_000_0_0_0000000000101010_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),     # Duplicates removed

        ext(idn=0b_000_0_0_0000000000101011_0000000_1,
            msk=0b_000_1_0_1111111111111111_0000000_1),
    ]
    print([str(r) for r in reduced])

    reduced = FilterConfiguration.compact(generate_filter_configurations(reference_subject_ids, 0b1010101), 3)
    assert reduced == [
        ext(idn=0b_000_1_0_000000000_1010101_0000000_1,
            msk=0b_000_1_0_000000000_1111111_0000000_1),    # Services

        ext(idn=0b_000_0_0_0000000000000000_1010101_1,      # Loopback messages
            msk=0b_000_1_0_0000000000000000_1111111_1),

        ext(idn=0b_000_0_0_0000000000000000_0000000_1,
            msk=0b_000_1_0_1111111111000000_0000000_1),
    ]
    print([str(r) for r in reduced])

    reduced = FilterConfiguration.compact(generate_filter_configurations(reference_subject_ids, 0b1010101), 1)
    assert reduced == [
        ext(idn=0b_000_0_0_000000000_0000000_0000000_1,
            msk=0b_000_0_0_000000000_0000000_0000000_1),    # Degenerates to checking only protocol version
    ]
    print([str(r) for r in reduced])
