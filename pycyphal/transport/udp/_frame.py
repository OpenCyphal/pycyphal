# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import struct
import dataclasses
import pycyphal
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier


@dataclasses.dataclass(frozen=True, repr=False)
class UDPFrame(pycyphal.transport.commons.high_overhead_transport.Frame):
    """
    The header format is up to debate until it's frozen in Specification.

    An important thing to keep in mind is that the minimum size of an UDP/IPv4 payload when transferred over
    100M Ethernet is 18 bytes, due to the minimum Ethernet frame size limit. That is, if the application
    payload requires less space, the missing bytes will be padded out to the minimum size.

    The current header format enables encoding by trivial memory aliasing on any conventional little-endian platform::

        struct Header {
            uint4_t  version;               # <- 1
            uint4_t  _reserved_a;
            uint3_t  priority;              # Duplicates QoS for ease of access; 0 -- highest, 7 -- lowest.
            uint5_t  _reserved_b;
            uint16_t source_node_id;        # 0xFFFF == anonymous transfer
            uint16_t destination_node_id;   # 0xFFFF == broadcast
            uint15_t data_specifier;        # subject-ID | (service-ID + RNR (Request, Not Response))
            bool     snm;                   # SNM (Service, Not Message)
            uint64_t transfer_id;
            uint31_t frame_index;
            bool     frame_index_eot;       # End of transfer
            uint16_t user_data;             # Opaque application-specific data with user-defined semantics.
                                            # Generic implementations should ignore
            uint16_t header_crc;            # Checksum of the header, excluding the CRC field itself
        };
        static_assert(sizeof(struct Header) == 24, "Invalid layout");   # Fixed-size 24-byte header with
                                                                        # natural alignment for each field ensured.

    +---------------+---------------+---------------+-----------------+------------------+
    |**MAC header** | **IP header** |**UDP header** |**Cyphal header**|**Cyphal payload**|
    +---------------+---------------+---------------+-----------------+------------------+
    |                                               |    Layers modeled by this type     |
    +-----------------------------------------------+------------------------------------+
    """

    _HEADER_FORMAT_NO_CRC = struct.Struct(
        "<"  # little-endian
        "B"  # version, _reserved_a
        "B"  # priority, _reserved_b
        "H"  # source_node_id
        "H"  # destination_node_id
        "H"  # subject_id, snm (if Message); service_id, rnr, snm (if Service)
        "Q"  # transfer_id
        "I"  # frame_index, end_of_transfer
        "H"  # user_data
    )
    _HEADER_FORMAT_SIZE = _HEADER_FORMAT_NO_CRC.size + 2  # 2 bytes for CRC

    _VERSION = 1
    NODE_ID_MASK = 2**16 - 1
    SUBJECT_ID_MASK = 2**15 - 1
    SERVICE_ID_MASK = 2**14 - 1
    TRANSFER_ID_MASK = 2**64 - 1
    INDEX_MASK = 2**31 - 1

    source_node_id: int | None
    destination_node_id: int | None

    data_specifier: pycyphal.transport.DataSpecifier

    user_data: int

    def __post_init__(self) -> None:
        if not isinstance(self.priority, pycyphal.transport.Priority):
            raise TypeError(f"Invalid priority: {self.priority}")  # pragma: no cover

        if not (self.source_node_id is None or (0 <= self.source_node_id <= self.NODE_ID_MASK)):
            raise ValueError(f"Invalid source node id: {self.source_node_id}")

        if not (self.destination_node_id is None or (0 <= self.destination_node_id <= self.NODE_ID_MASK)):
            raise ValueError(f"Invalid destination node id: {self.destination_node_id}")

        if isinstance(self.data_specifier, pycyphal.transport.ServiceDataSpecifier) and self.source_node_id is None:
            raise ValueError(f"Anonymous nodes cannot use service transfers: {self.data_specifier}")

        if not isinstance(self.data_specifier, pycyphal.transport.DataSpecifier):
            raise TypeError(f"Invalid data specifier: {self.data_specifier}")

        if not (0 <= self.transfer_id <= self.TRANSFER_ID_MASK):
            raise ValueError(f"Invalid transfer-ID: {self.transfer_id}")

        if not (0 <= self.index <= self.INDEX_MASK):
            raise ValueError(f"Invalid frame index: {self.index}")

        if not isinstance(self.payload, memoryview):
            raise TypeError(f"Bad payload type: {type(self.payload).__name__}")  # pragma: no cover

    def compile_header_and_payload(self) -> typing.Tuple[memoryview, memoryview]:
        """
        Compiles the UDP frame header and returns it as a read-only memoryview along with the payload, separately.
        The caller is supposed to handle the header and the payload independently.
        The reason is to avoid unnecessary data copying in the user space,
        allowing the caller to rely on the vectorized IO API instead (sendmsg).
        """

        if isinstance(self.data_specifier, pycyphal.transport.ServiceDataSpecifier):
            snm = True
            subject_id = None
            service_id = self.data_specifier.service_id
            rnr = self.data_specifier.role == self.data_specifier.Role.REQUEST
            id_rnr = service_id | ((1 << 14) if rnr else 0)
        elif isinstance(self.data_specifier, pycyphal.transport.MessageDataSpecifier):
            snm = False
            subject_id = self.data_specifier.subject_id
            service_id = None
            rnr = None
            id_rnr = subject_id
        else:
            raise TypeError(f"Invalid data specifier: {self.data_specifier}")

        header_memory = self._HEADER_FORMAT_NO_CRC.pack(
            self._VERSION,
            int(self.priority),
            self.source_node_id if self.source_node_id is not None else 0xFFFF,
            self.destination_node_id if self.destination_node_id is not None else 0xFFFF,
            ((1 << 15) if snm else 0) | id_rnr,
            self.transfer_id,
            ((1 << 31) if self.end_of_transfer else 0) | self.index,
            0,  # user_data
        )

        header = header_memory + pycyphal.transport.commons.crc.CRC16CCITT.new(header_memory).value_as_bytes
        assert len(header) == self._HEADER_FORMAT_SIZE

        return memoryview(header), self.payload

    @staticmethod
    def parse(image: memoryview) -> typing.Optional[UDPFrame]:
        try:
            (
                version,
                int_priority,
                source_node_id,
                destination_node_id,
                data_specifier_snm,
                transfer_id,
                frame_index_eot,
                user_data,
            ) = UDPFrame._HEADER_FORMAT_NO_CRC.unpack_from(image)
        except struct.error:
            return None
        if version == UDPFrame._VERSION:
            # check the header CRC
            header = image[: UDPFrame._HEADER_FORMAT_SIZE]
            if not pycyphal.transport.commons.crc.CRC16CCITT.new(header).check_residue():
                return None

            # Service/Message specific
            snm = bool(data_specifier_snm & (1 << 15))
            data_specifier: pycyphal.transport.DataSpecifier
            if snm:
                ## Service
                service_id = data_specifier_snm & UDPFrame.SERVICE_ID_MASK
                rnr = bool(data_specifier_snm & (1 << 14))
                # check the service ID
                if not (0 <= service_id <= UDPFrame.SERVICE_ID_MASK):
                    return None
                # create the data specifier
                data_specifier = pycyphal.transport.ServiceDataSpecifier(
                    service_id=service_id,
                    role=pycyphal.transport.ServiceDataSpecifier.Role.REQUEST
                    if rnr
                    else pycyphal.transport.ServiceDataSpecifier.Role.RESPONSE,
                )
            else:
                ## Message
                subject_id = data_specifier_snm & UDPFrame.SUBJECT_ID_MASK
                rnr = None
                # check the subject ID
                if not (0 <= subject_id <= UDPFrame.SUBJECT_ID_MASK):
                    return None
                # create the data specifier
                data_specifier = pycyphal.transport.MessageDataSpecifier(subject_id=subject_id)

            return UDPFrame(
                priority=pycyphal.transport.Priority(int_priority),
                source_node_id=source_node_id,
                destination_node_id=destination_node_id,
                data_specifier=data_specifier,
                transfer_id=transfer_id,
                index=(frame_index_eot & UDPFrame.INDEX_MASK),
                end_of_transfer=bool(frame_index_eot & (UDPFrame.INDEX_MASK + 1)),
                user_data=user_data,
                payload=image[UDPFrame._HEADER_FORMAT_SIZE :],
            )
        return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_frame_compile() -> None:
    from pycyphal.transport import Priority
    from pytest import raises

    _ = UDPFrame(
        priority=Priority.LOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=0),
        transfer_id=0,
        index=0,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b""),
    )

    # Invalid source_node_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=2**16,
            destination_node_id=2,
            data_specifier=MessageDataSpecifier(subject_id=0),
            transfer_id=0,
            index=0,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Invalid destination_node_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=1,
            destination_node_id=2**16,
            data_specifier=MessageDataSpecifier(subject_id=0),
            transfer_id=0,
            index=0,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Invalid subject_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=1,
            destination_node_id=2,
            data_specifier=MessageDataSpecifier(subject_id=2**15),
            transfer_id=0,
            index=0,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Invalid service_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=1,
            destination_node_id=2,
            data_specifier=ServiceDataSpecifier(service_id=2**14, role=ServiceDataSpecifier.Role.RESPONSE),
            transfer_id=0,
            index=0,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Invalid transfer_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=1,
            destination_node_id=2,
            data_specifier=ServiceDataSpecifier(service_id=0, role=ServiceDataSpecifier.Role.RESPONSE),
            transfer_id=2**64,
            index=0,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Invalid frame index
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW,
            source_node_id=1,
            destination_node_id=2,
            data_specifier=ServiceDataSpecifier(service_id=0, role=ServiceDataSpecifier.Role.RESPONSE),
            transfer_id=0,
            index=2**31,
            end_of_transfer=False,
            user_data=0,
            payload=memoryview(b""),
        )

    # Multi-frame, not the end of the transfer. [subject]
    assert (
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\x00"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x00"  # index
            b"\x00\x00"  # user_data
            b"\xf2\xce"  # header_crc
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=3),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # Multi-frame, end of the transfer. [subject]
    assert (
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\x00"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xc9\x94"  # header_crc
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=3),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # test frame used in _input_session
    assert (
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\n\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\x00"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x01\x00\x00\x80"  # index
            b"\x00\x00"  # user_data
            b"\x8f\xc8"  # header_crc
        ),
        memoryview(b"Okay, I smashed your Corolla"),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=10,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=3),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x1,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"Okay, I smashed your Corolla"),
    ).compile_header_and_payload()

    # Multi-frame, not the end of the transfer. [service]
    assert (
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x00"  # index
            b"\x00\x00"  # user_data
            b"\x8c\xd5"  # header_crc
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=ServiceDataSpecifier(service_id=3, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # Multi-frame, end of the transfer. [service]
    assert (
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xb7\x8f"  # header_crc
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=ServiceDataSpecifier(service_id=3, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # From _output_session unit test
    assert (
        memoryview(b"\x01\x04\x05\x00\xff\xff\x8a\x0c40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00pr"),
        memoryview(b"onetwothree"),
    ) == UDPFrame(
        priority=Priority.NOMINAL,
        source_node_id=5,
        destination_node_id=None,
        data_specifier=MessageDataSpecifier(subject_id=3210),
        transfer_id=12340,
        index=0,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"onetwothree"),
    ).compile_header_and_payload()

    assert (
        memoryview(b"\x01\x07\x06\x00\xae\x08A\xc11\xd4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\n\xc6"),
        memoryview(b"onetwothre"),
    ) == UDPFrame(
        priority=Priority.OPTIONAL,
        source_node_id=6,
        destination_node_id=2222,
        data_specifier=ServiceDataSpecifier(service_id=321, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=54321,
        index=0,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b"onetwothre"),
    ).compile_header_and_payload()

    assert (
        memoryview(b"\x01\x07\x06\x00\xae\x08A\xc11\xd4\x00\x00\x00\x00\x00\x00\x01\x00\x00\x80\x00\x00t<"),
        memoryview(b"e"),
    ) == UDPFrame(
        priority=Priority.OPTIONAL,
        source_node_id=6,
        destination_node_id=2222,
        data_specifier=ServiceDataSpecifier(service_id=321, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=54321,
        index=1,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"e"),
    ).compile_header_and_payload()


def _unittest_udp_frame_parse() -> None:
    from pycyphal.transport import Priority

    for size in range(16):
        assert None is UDPFrame.parse(memoryview(bytes(range(size))))

    # Multi-frame, not the end of the transfer. [subject]
    assert UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=3),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\x00"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x00"  # index
            b"\x00\x00"  # user_data
            b"\xf2\xce"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )

    # Multi-frame, end of the transfer. [subject]
    assert UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(subject_id=3),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\x00"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xc9\x94"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )

    # Multi-frame, not the end of the transfer. [service]
    assert UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=ServiceDataSpecifier(service_id=3, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=False,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x00"  # index
            b"\x00\x00"  # user_data
            b"\x8c\xd5"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )

    # Multi-frame, end of the transfer. [service]
    assert UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        destination_node_id=2,
        data_specifier=ServiceDataSpecifier(service_id=3, role=ServiceDataSpecifier.Role.REQUEST),
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_DD_F00D,
        end_of_transfer=True,
        user_data=0,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xb7\x8f"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )

    # Wrong checksum. (same as Multiframe, end of the transfer. [service], but wrong checksum)
    assert None is UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xb8\x8f"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )

    # Too short.
    assert None is UDPFrame.parse(
        memoryview(
            b"\x01"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            # b"\xb8\x8f"  # header_crc
            # b"Well, I got here the same way the coin did."
        ),
    )

    # Bad version.
    assert None is UDPFrame.parse(
        memoryview(
            b"\x02"  # version
            b"\x06"  # priority
            b"\x01\x00"  # source_node_id
            b"\x02\x00"  # destination_node_id
            b"\x03\xc0"  # data_specifier_snm
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
            b"\x0d\xf0\xdd\x80"  # index
            b"\x00\x00"  # user_data
            b"\xb8\x8f"  # header_crc
            b"Well, I got here the same way the coin did."
        ),
    )
