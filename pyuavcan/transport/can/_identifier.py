# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport
import pyuavcan.transport.can


_CANID_EXT_MASK = 2 ** 29 - 1

_BIT_SRV_NOT_MSG = 1 << 25
_BIT_MSG_ANON = 1 << 24
_BIT_SRV_REQ = 1 << 24
_BIT_R23 = 1 << 23
_BIT_MSG_SET_IGNORE = 3 << 21
_BIT_MSG_R7 = 1 << 7


@dataclasses.dataclass(frozen=True)
class CANID:
    PRIORITY_MASK = 7
    NODE_ID_MASK = 127

    priority: pyuavcan.transport.Priority
    source_node_id: typing.Optional[int]  # None if anonymous; may be non-optional in derived classes

    def __post_init__(self) -> None:
        assert isinstance(self.priority, pyuavcan.transport.Priority)

    def compile(self, fragmented_transfer_payload: typing.Iterable[memoryview]) -> int:
        # You might be wondering, why the hell would a CAN ID abstraction depend on the payload of the transfer?
        # This is to accommodate the special case of anonymous message transfers. We need to know the payload to
        # compute the pseudo node ID when emitting anonymous messages. We could use just random numbers from the
        # standard library, but that would make the code hard to test.
        raise NotImplementedError

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        raise NotImplementedError

    def get_destination_node_id(self) -> typing.Optional[int]:
        """Hides the destination selection logic from users of the abstract type."""
        raise NotImplementedError

    @staticmethod
    def parse(identifier: int) -> typing.Optional[CANID]:
        """
        Attempts to parse the supplied CAN ID value.
        Returns None if the CAN ID is not valid for UAVCAN (different protocol or different version of UAVCAN).
        """
        _validate_unsigned_range(identifier, _CANID_EXT_MASK)
        priority = pyuavcan.transport.Priority(identifier >> 26)
        source_node_id = identifier & CANID.NODE_ID_MASK
        if identifier & _BIT_SRV_NOT_MSG:
            if identifier & _BIT_R23:
                return None  # Wrong protocol
            return ServiceCANID(
                priority=priority,
                service_id=(identifier >> 14) & pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK,
                request_not_response=identifier & _BIT_SRV_REQ != 0,
                source_node_id=source_node_id,
                destination_node_id=(identifier >> 7) & CANID.NODE_ID_MASK,
            )
        if identifier & (_BIT_R23 | _BIT_MSG_R7):
            return None  # Wrong protocol
        return MessageCANID(
            priority=priority,
            subject_id=(identifier >> 8) & pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK,
            source_node_id=None if identifier & _BIT_MSG_ANON else source_node_id,
        )


@dataclasses.dataclass(frozen=True)
class MessageCANID(CANID):
    subject_id: int

    def __post_init__(self) -> None:
        super().__post_init__()
        _validate_unsigned_range(int(self.priority), self.PRIORITY_MASK)
        _validate_unsigned_range(self.subject_id, pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK)
        if self.source_node_id is not None:
            _validate_unsigned_range(self.source_node_id, self.NODE_ID_MASK)

    def compile(self, fragmented_transfer_payload: typing.Iterable[memoryview]) -> int:
        identifier = (int(self.priority) << 26) | _BIT_MSG_SET_IGNORE | (self.subject_id << 8)

        source_node_id = self.source_node_id
        if source_node_id is None:  # Anonymous frame
            # Anonymous transfers cannot be multi-frame, but we have no way of enforcing this here since we don't
            # know what the MTU is. The caller must enforce this instead.
            source_node_id = int(sum(map(sum, fragmented_transfer_payload))) & self.NODE_ID_MASK
            identifier |= _BIT_MSG_ANON

        assert 0 <= source_node_id <= self.NODE_ID_MASK  # Should be valid here already
        identifier |= source_node_id

        assert 0 <= identifier <= _CANID_EXT_MASK
        assert identifier & self.NODE_ID_MASK == source_node_id
        assert (identifier >> 8) & pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK == self.subject_id
        assert identifier >> 26 == int(self.priority)
        return identifier

    @property
    def data_specifier(self) -> pyuavcan.transport.MessageDataSpecifier:
        return pyuavcan.transport.MessageDataSpecifier(self.subject_id)

    def get_destination_node_id(self) -> typing.Optional[int]:
        return None


@dataclasses.dataclass(frozen=True)
class ServiceCANID(CANID):
    source_node_id: int  # Overrides Optional[int] by covariance (property not writeable)
    destination_node_id: int
    service_id: int
    request_not_response: bool

    def __post_init__(self) -> None:
        super().__post_init__()
        _validate_unsigned_range(int(self.priority), self.PRIORITY_MASK)
        _validate_unsigned_range(self.service_id, pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK)
        _validate_unsigned_range(self.source_node_id, self.NODE_ID_MASK)
        _validate_unsigned_range(self.destination_node_id, self.NODE_ID_MASK)

        if self.source_node_id == self.destination_node_id:
            raise ValueError(f"Invalid service frame: source node ID == destination node ID == {self.source_node_id}")

    def compile(self, fragmented_transfer_payload: typing.Iterable[memoryview]) -> int:
        del fragmented_transfer_payload
        identifier = (
            (int(self.priority) << 26)
            | _BIT_SRV_NOT_MSG
            | (self.service_id << 14)
            | (self.destination_node_id << 7)
            | self.source_node_id
        )

        if self.request_not_response:
            identifier |= _BIT_SRV_REQ

        assert 0 <= identifier <= _CANID_EXT_MASK
        assert identifier & self.NODE_ID_MASK == self.source_node_id
        assert (identifier >> 14) & 1023 == self.service_id
        assert identifier >> 26 == int(self.priority)
        return identifier

    @property
    def data_specifier(self) -> pyuavcan.transport.ServiceDataSpecifier:
        role_enum = pyuavcan.transport.ServiceDataSpecifier.Role
        role = role_enum.REQUEST if self.request_not_response else role_enum.RESPONSE
        return pyuavcan.transport.ServiceDataSpecifier(self.service_id, role)

    def get_destination_node_id(self) -> typing.Optional[int]:
        return self.destination_node_id


def _validate_unsigned_range(value: int, max_value: int) -> None:
    if not isinstance(value, int) or not (0 <= value <= max_value):
        raise ValueError(f"Value {value} is not in the interval [0, {max_value}]")


def generate_filter_configurations(
    subject_id_list: typing.Iterable[int], local_node_id: typing.Optional[int]
) -> typing.Sequence[pyuavcan.transport.can.media.FilterConfiguration]:
    from .media import FrameFormat, FilterConfiguration

    def ext(idn: int, msk: int) -> FilterConfiguration:
        assert idn <= _CANID_EXT_MASK and msk <= _CANID_EXT_MASK
        return FilterConfiguration(identifier=idn, mask=msk, format=FrameFormat.EXTENDED)

    full: typing.List[FilterConfiguration] = []

    if local_node_id is not None:
        assert local_node_id <= CANID.NODE_ID_MASK
        # If the local node-ID is set, we may receive service requests, so we need to allocate one filter for those.
        full.append(
            ext(
                idn=_BIT_SRV_NOT_MSG | (int(local_node_id) << 7),
                msk=_BIT_SRV_NOT_MSG | _BIT_R23 | (CANID.NODE_ID_MASK << 7),
            )
        )
        # Also, we may need loopback frames for timestamping, so we add a filter for frames where the source node-ID
        # equals ours. Both messages and services!
        full.append(ext(idn=int(local_node_id), msk=_BIT_R23 | CANID.NODE_ID_MASK))
    else:
        # If the local node-ID is not set, we may need to receive loopback frames for sent anonymous transfers.
        # This essentially means that we need to allow ALL anonymous transfers. Those may be only messages, as there
        # is no such thing as anonymous service transfer.
        full.append(ext(idn=_BIT_MSG_ANON, msk=_BIT_SRV_NOT_MSG | _BIT_MSG_ANON | _BIT_R23 | _BIT_MSG_R7))

    # One filter per unique subject-ID. Sorted for testability.
    for sid in sorted(set(subject_id_list)):
        s_mask = pyuavcan.transport.MessageDataSpecifier.SUBJECT_ID_MASK
        assert sid <= s_mask
        full.append(ext(idn=int(sid) << 8, msk=_BIT_SRV_NOT_MSG | _BIT_R23 | (s_mask << 8) | _BIT_MSG_R7))

    return full


def _unittest_can_filter_configuration() -> None:
    from .media import FilterConfiguration, optimize_filter_configurations, FrameFormat

    def ext(idn: int, msk: int) -> FilterConfiguration:
        assert idn <= _CANID_EXT_MASK and msk <= _CANID_EXT_MASK
        return FilterConfiguration(identifier=idn, mask=msk, format=FrameFormat.EXTENDED)

    degenerate = optimize_filter_configurations(generate_filter_configurations([], None), 999)
    assert degenerate == [
        ext(
            idn=0b_000_0_1_0_000000000000000_0_0000000, msk=0b_000_1_1_1_000000000000000_1_0000000  # Anonymous messages
        )
    ]

    no_subjects = optimize_filter_configurations(generate_filter_configurations([], 0b1010101), 999)
    assert no_subjects == [
        ext(idn=0b_000_1_0_0_000000000_1010101_0000000, msk=0b_000_1_0_1_000000000_1111111_0000000),  # Services
        ext(
            idn=0b_000_0_0_0_0000000000000000_1010101,  # Loopback frames (both messages and services)
            msk=0b_000_0_0_1_0000000000000000_1111111,
        ),
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

    retained = optimize_filter_configurations(generate_filter_configurations(reference_subject_ids, 0b1010101), 999)
    assert retained == [
        ext(idn=0b_000_1_0_0_000000000_1010101_0000000, msk=0b_000_1_0_1_000000000_1111111_0000000),  # Services
        ext(
            idn=0b_000_0_0_0_0000000000000000_1010101,  # Loopback frames (both messages and services)
            msk=0b_000_0_0_1_0000000000000000_1111111,
        ),
        ext(idn=0b_000_0_0_0_000000000000000_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        ext(idn=0b_000_0_0_0_000000000000101_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        ext(idn=0b_000_0_0_0_000000000001010_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        ext(idn=0b_000_0_0_0_000000000010101_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        ext(
            idn=0b_000_0_0_0_000000000101010_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000
        ),  # Duplicates removed
        ext(idn=0b_000_0_0_0_000000000101011_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
    ]

    reduced = optimize_filter_configurations(generate_filter_configurations(reference_subject_ids, 0b1010101), 7)
    assert reduced == [
        ext(idn=0b_000_1_0_0_000000000_1010101_0000000, msk=0b_000_1_0_1_000000000_1111111_0000000),  # Services
        ext(
            idn=0b_000_0_0_0_0000000000000000_1010101,  # Loopback frames (both messages and services)
            msk=0b_000_0_0_1_0000000000000000_1111111,
        ),
        ext(idn=0b_000_0_0_0_000000000000000_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        ext(idn=0b_000_0_0_0_000000000000101_0_0000000, msk=0b_000_1_0_1_001111111101111_1_0000000),  # Merged with 6th
        ext(idn=0b_000_0_0_0_000000000001010_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
        # This one removed, merged with 4th
        ext(
            idn=0b_000_0_0_0_000000000101010_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000
        ),  # Duplicates removed
        ext(idn=0b_000_0_0_0_000000000101011_0_0000000, msk=0b_000_1_0_1_001111111111111_1_0000000),
    ]
    print([str(r) for r in reduced])

    reduced = optimize_filter_configurations(generate_filter_configurations(reference_subject_ids, 0b1010101), 3)
    assert reduced == [
        ext(idn=0b_000_1_0_0_000000000_1010101_0000000, msk=0b_000_1_0_1_000000000_1111111_0000000),  # Services
        ext(
            idn=0b_000_0_0_0_0000000000000000_1010101,  # Loopback frames (both messages and services)
            msk=0b_000_0_0_1_0000000000000000_1111111,
        ),
        ext(idn=0b_000_0_0_0_000000000000000_0_0000000, msk=0b_000_1_0_1_001111111000000_1_0000000),
    ]
    print([str(r) for r in reduced])

    reduced = optimize_filter_configurations(generate_filter_configurations(reference_subject_ids, 0b1010101), 1)
    assert reduced == [
        ext(
            idn=0b_000_0_0_0_000000000_0000000_0000000, msk=0b_000_0_0_1_000000000_0000000_0000000
        ),  # Degenerates to checking only the reserved bits
    ]
    print([str(r) for r in reduced])


def _unittest_can_identifier_parse() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, MessageDataSpecifier, ServiceDataSpecifier

    with raises(ValueError):
        CANID.parse(_CANID_EXT_MASK + 1)

    with raises(ValueError):
        MessageCANID(Priority.HIGH, None, 2 ** 15)

    with raises(ValueError):
        MessageCANID(Priority.HIGH, 128, 123)

    with raises(ValueError):
        MessageCANID(Priority.HIGH, 123, -1)

    with raises(ValueError):
        MessageCANID(Priority.HIGH, -1, 123)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, -1, 123, 123, True)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 128, 123, 123, True)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 123, -1, 123, True)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 123, 128, 123, True)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 123, 123, -1, True)

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 123, 123, 512, True)

    with raises(ValueError):
        # noinspection PyTypeChecker
        ServiceCANID(Priority.HIGH, None, 123, 512, True)  # type: ignore

    with raises(ValueError):
        ServiceCANID(Priority.HIGH, 123, 123, 42, True)  # Same source and destination

    assert CANID.parse(0b_010_0_0_0110100100101001_1_1111011) is None
    reference_message = MessageCANID(Priority.FAST, 123, 2345)
    assert CANID.parse(0b_010_0_0_0110100100101001_0_1111011) == reference_message
    assert CANID.parse(0b_010_0_0_0100100100101001_0_1111011) == reference_message
    assert CANID.parse(0b_010_0_0_0010100100101001_0_1111011) == reference_message
    assert CANID.parse(0b_010_0_0_0000100100101001_0_1111011) == reference_message
    assert 0b_010_0_0_0110100100101001_0_1111011 == reference_message.compile([])
    assert reference_message.data_specifier == MessageDataSpecifier(2345)

    assert CANID.parse(0b_010_0_1_0111000011100001_1_1111111) is None
    reference_message = MessageCANID(Priority.FAST, None, 4321)
    assert CANID.parse(0b_010_0_1_0111000011100001_0_1111111) == reference_message
    assert CANID.parse(0b_010_0_1_0101000011100001_0_1111111) == reference_message
    assert CANID.parse(0b_010_0_1_0011000011100001_0_1111111) == reference_message
    assert CANID.parse(0b_010_0_1_0001000011100001_0_1111111) == reference_message
    assert 0b_010_0_1_0111000011100001_0_1111111 == reference_message.compile([memoryview(bytes([100, 27]))])
    assert reference_message.data_specifier == MessageDataSpecifier(4321)

    reference_service = ServiceCANID(Priority.OPTIONAL, 123, 42, 300, True)
    reference_service_id = 0b_111_1_1_0100101100_0101010_1111011
    assert CANID.parse(reference_service_id) == reference_service
    assert reference_service_id == reference_service.compile([])
    assert reference_service.data_specifier == ServiceDataSpecifier(300, ServiceDataSpecifier.Role.REQUEST)

    reference_service = ServiceCANID(Priority.OPTIONAL, 42, 123, 255, False)
    reference_service_id = 0b_111_1_0_0011111111_1111011_0101010
    assert CANID.parse(reference_service_id) == reference_service
    assert reference_service_id == reference_service.compile([])
    assert reference_service.data_specifier == ServiceDataSpecifier(255, ServiceDataSpecifier.Role.RESPONSE)
