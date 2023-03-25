# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import struct
import dataclasses
from cobs import cobs  # type: ignore
import pycyphal
from pycyphal.transport import Priority

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
assert _HEADER_FORMAT_NO_CRC.size == 22
_HEADER_FORMAT_SIZE = _HEADER_FORMAT_NO_CRC.size + 2

_ANONYMOUS_NODE_ID = 0xFFFF  # Same value represents broadcast node ID when transmitting.

_SUBJECT_ID_MASK = 2**15 - 1
_SERVICE_ID_MASK = 2**14 - 1


@dataclasses.dataclass(frozen=True, repr=False)
class SerialFrame(pycyphal.transport.commons.high_overhead_transport.Frame):

    VERSION = 1
    NODE_ID_MASK = 2**16 - 1
    TRANSFER_ID_MASK = 2**64 - 1
    INDEX_MASK = 2**31 - 1

    NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING = _HEADER_FORMAT_SIZE
    NODE_ID_RANGE = range(NODE_ID_MASK)
    FRAME_DELIMITER_BYTE = 0x00

    source_node_id: typing.Optional[int]
    destination_node_id: typing.Optional[int]

    data_specifier: pycyphal.transport.DataSpecifier

    user_data: int

    def __post_init__(self) -> None:
        if not isinstance(self.priority, pycyphal.transport.Priority):
            raise TypeError(f"Invalid priority: {self.priority}")  # pragma: no cover

        if self.source_node_id is not None and not (0 <= self.source_node_id <= self.NODE_ID_MASK):
            raise ValueError(f"Invalid source node ID: {self.source_node_id}")

        if self.destination_node_id is not None and not (0 <= self.destination_node_id <= self.NODE_ID_MASK):
            raise ValueError(f"Invalid destination node ID: {self.destination_node_id}")

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

    def compile_into(self, out_buffer: bytearray) -> memoryview:
        """
        Compiles the frame into the specified output buffer, escaping the data as necessary.
        The buffer must be large enough to accommodate the frame header with the payload and CRC,
        including escape sequences.
        :returns: View of the memory from the beginning of the buffer until the end of the compiled frame.
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

        header_memory = _HEADER_FORMAT_NO_CRC.pack(
            self.VERSION,
            int(self.priority),
            self.source_node_id if self.source_node_id is not None else _ANONYMOUS_NODE_ID,
            self.destination_node_id if self.destination_node_id is not None else _ANONYMOUS_NODE_ID,
            ((1 << 15) if snm else 0) | id_rnr,
            self.transfer_id,
            ((1 << 31) if self.end_of_transfer else 0) | self.index,
            0,  # user_data
        )

        header = header_memory + pycyphal.transport.commons.crc.CRC16CCITT.new(header_memory).value_as_bytes
        assert len(header) == _HEADER_FORMAT_SIZE

        out_buffer[0] = SerialFrame.FRAME_DELIMITER_BYTE
        next_byte_index = 1

        # noinspection PyTypeChecker
        packet_bytes = header + self.payload
        encoded_image = cobs.encode(packet_bytes)
        # place in the buffer and update next_byte_index:
        out_buffer[next_byte_index : next_byte_index + len(encoded_image)] = encoded_image
        next_byte_index += len(encoded_image)

        out_buffer[next_byte_index] = SerialFrame.FRAME_DELIMITER_BYTE
        next_byte_index += 1

        assert (next_byte_index - 2) >= (len(header) + len(self.payload))
        return memoryview(out_buffer)[:next_byte_index]

    @staticmethod
    def calc_cobs_size(payload_size_bytes: int) -> int:
        """
        :returns: worst case COBS-encoded message size for a given payload size.
        """
        # equivalent to int(math.ceil(payload_size_bytes * 255.0 / 254.0)
        return (payload_size_bytes * 255 + 253) // 254

    @staticmethod
    def parse_from_cobs_image(image: memoryview) -> typing.Optional[SerialFrame]:
        """
        Delimiters will be stripped if present but they are not required.
        :returns: Frame or None if the image is invalid.
        """
        try:
            while image[0] == SerialFrame.FRAME_DELIMITER_BYTE:
                image = image[1:]
            while image[-1] == SerialFrame.FRAME_DELIMITER_BYTE:
                image = image[:-1]
        except IndexError:
            return None
        try:
            unescaped_image = cobs.decode(bytearray(image))  # TODO: PERFORMANCE WARNING: AVOID THE COPY
        except cobs.DecodeError:
            return None
        return SerialFrame.parse_from_unescaped_image(memoryview(unescaped_image))

    @staticmethod
    def parse_from_unescaped_image(image: memoryview) -> typing.Optional[SerialFrame]:
        """
        :returns: Frame or None if the image is invalid.
        """
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
            ) = _HEADER_FORMAT_NO_CRC.unpack_from(image)
        except struct.error:
            return None

        try:
            if version == SerialFrame.VERSION:
                header = image[:_HEADER_FORMAT_SIZE]
                if not pycyphal.transport.commons.crc.CRC16CCITT.new(header).check_residue():
                    return None

                # Service/Message specific
                snm = bool(data_specifier_snm & (1 << 15))
                data_specifier: pycyphal.transport.DataSpecifier
                if snm:
                    ## Service
                    service_id = data_specifier_snm & _SERVICE_ID_MASK
                    rnr = bool(data_specifier_snm & (_SERVICE_ID_MASK + 1))
                    # check the service ID
                    if not (0 <= service_id <= _SERVICE_ID_MASK):
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
                    subject_id = data_specifier_snm & _SUBJECT_ID_MASK
                    rnr = None
                    # check the subject ID
                    if not (0 <= subject_id <= _SUBJECT_ID_MASK):
                        return None
                    # create the data specifier
                    data_specifier = pycyphal.transport.MessageDataSpecifier(subject_id=subject_id)

                source_node_id = None if source_node_id == _ANONYMOUS_NODE_ID else source_node_id
                destination_node_id = None if destination_node_id == _ANONYMOUS_NODE_ID else destination_node_id

                return SerialFrame(
                    priority=Priority(int_priority),
                    source_node_id=source_node_id,
                    destination_node_id=destination_node_id,
                    data_specifier=data_specifier,
                    transfer_id=transfer_id,
                    index=(frame_index_eot & SerialFrame.INDEX_MASK),
                    end_of_transfer=bool(frame_index_eot & (SerialFrame.INDEX_MASK + 1)),
                    user_data=user_data,
                    payload=image[_HEADER_FORMAT_SIZE:],
                )
            return None
        except ValueError:
            return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_serial_frame_compile_message() -> None:
    from pycyphal.transport import MessageDataSpecifier

    f = SerialFrame(
        priority=Priority.HIGH,
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=True,
        payload=memoryview(b"Who will survive in America?"),
        source_node_id=1,
        destination_node_id=2,
        data_specifier=MessageDataSpecifier(2345),
        user_data=0,
    )

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == SerialFrame.FRAME_DELIMITER_BYTE
    assert mv[-1] == SerialFrame.FRAME_DELIMITER_BYTE

    segment_cobs = bytes(mv[1:-1])
    assert SerialFrame.FRAME_DELIMITER_BYTE not in segment_cobs

    segment = cobs.decode(segment_cobs)

    # Header validation
    assert segment[0] == SerialFrame.VERSION  # version, _reserved_a
    assert segment[1] == int(Priority.HIGH)  # priority, _reserved_b
    assert (segment[2], segment[3]) == (1, 0)  # source_node_id
    assert (segment[4], segment[5]) == (2, 0)  # destination_node_id
    assert segment[6:8] == (2345).to_bytes(2, "little")  # subject_id, snm
    assert segment[8:16] == (1234567890123456789).to_bytes(8, "little")  # transfer_id
    assert segment[16:20] == (1234567 | (1 << 31)).to_bytes(4, "little")  # frame_index, end_of_transfer
    assert segment[20:22] == (0).to_bytes(2, "little")  # user_data
    # Header CRC here

    # Payload validation
    assert segment[24:] == b"Who will survive in America?"


def _unittest_serial_frame_compile_service() -> None:
    from pycyphal.transport import ServiceDataSpecifier

    f = SerialFrame(
        priority=Priority.FAST,
        transfer_id=1234567890123456789,
        index=123456,
        end_of_transfer=False,
        payload=memoryview(b"And America is now blood and tears instead of milk and honey"),
        source_node_id=1,
        destination_node_id=2,
        data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.RESPONSE),
        user_data=0,
    )

    buffer = bytearray(0 for _ in range(100))
    mv = f.compile_into(buffer)

    assert mv[0] == mv[-1] == SerialFrame.FRAME_DELIMITER_BYTE
    segment_cobs = bytes(mv[1:-1])
    assert SerialFrame.FRAME_DELIMITER_BYTE not in segment_cobs

    segment = cobs.decode(segment_cobs)

    # Header validation
    assert segment[0] == SerialFrame.VERSION  # version, _reserved_a
    assert segment[1] == int(Priority.FAST)  # priority, _reserved_b
    assert (segment[2], segment[3]) == (1, 0)  # source_node_id
    assert (segment[4], segment[5]) == (2, 0)  # destination_node_id
    assert segment[6:8] == ((1 << 15) | 123).to_bytes(2, "little")  # service_id, rnr, snm
    assert segment[8:16] == (1234567890123456789).to_bytes(8, "little")  # transfer_id
    assert segment[16:20] == (123456).to_bytes(4, "little")  # frame_index, end_of_transfer
    assert segment[20:22] == (0).to_bytes(2, "little")  # user_data
    # Header CRC here

    # Payload validation
    assert segment[24:] == b"And America is now blood and tears instead of milk and honey"


def _unittest_serial_frame_parse() -> None:
    from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier

    def get_crc(blocks: bytes) -> bytes:
        crc = pycyphal.transport.commons.crc.CRC16CCITT().new(blocks).value_as_bytes
        return crc

    # Valid message with payload
    header = bytes(
        [
            SerialFrame.VERSION,
            int(Priority.LOW),
            0x7B,
            0x00,  # Source NID        123
            0xC8,
            0x01,  # Destination NID   456
            0xE1,
            0x10,  # Data specifier    4321
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
            0x54,
            0xAB,  # Transfer ID       12345678901234567890
            0x31,
            0xD4,
            0x00,
            0x80,  # Frame index, EOT  54321 with EOT flag set
            0x00,
            0x00,  # User data
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    payload = b"They ain't do four years in college"
    f = SerialFrame.parse_from_unescaped_image(memoryview(header + payload))
    assert f == SerialFrame(
        priority=Priority.LOW,
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=True,
        payload=memoryview(payload),
        source_node_id=123,
        destination_node_id=456,
        data_specifier=MessageDataSpecifier(4321),
        user_data=0,
    )

    # Valid message with payload (Anonymous node ID's)
    header = bytes(
        [
            SerialFrame.VERSION,
            int(Priority.LOW),
            0xFF,
            0xFF,  # Source NID        Anonymous
            0xFF,
            0xFF,  # Destination NID   Anonymous
            0xE1,
            0x10,  # Data specifier    4321
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
            0x54,
            0xAB,  # Transfer ID       12345678901234567890
            0x31,
            0xD4,
            0x00,
            0x80,  # Frame index, EOT  54321 with EOT flag set
            0x00,
            0x00,  # User data
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    payload = b"But they'll do 25 to life"
    f = SerialFrame.parse_from_unescaped_image(memoryview(header + payload))
    assert f == SerialFrame(
        priority=Priority.LOW,
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=True,
        payload=memoryview(payload),
        source_node_id=None,
        destination_node_id=None,
        data_specifier=MessageDataSpecifier(4321),
        user_data=0,
    )

    # Valid service with no payload
    header = bytes(
        [
            SerialFrame.VERSION,
            int(Priority.LOW),
            0x01,
            0x00,  # Source NID        1
            0x00,
            0x00,  # Destination NID   0
            0x10,
            0xC0,  # Request, service ID 16
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
            0x54,
            0xAB,  # Transfer ID       12345678901234567890
            0x31,
            0xD4,
            0x00,
            0x00,  # Frame index, EOT  54321 with EOT flag not set
            0x00,
            0x00,  # User data
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    f = SerialFrame.parse_from_unescaped_image(memoryview(header))
    assert f == SerialFrame(
        priority=Priority.LOW,
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=False,
        payload=memoryview(b""),
        source_node_id=1,
        destination_node_id=0,
        data_specifier=ServiceDataSpecifier(16, ServiceDataSpecifier.Role.REQUEST),
        user_data=0,
    )

    # Valid service with no payload
    header = bytes(
        [
            SerialFrame.VERSION,
            int(Priority.LOW),
            0x01,
            0x00,  # Source NID        1
            0x00,
            0x00,  # Destination NID   0
            0x10,
            0x80,  # Response, service ID 16
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
            0x54,
            0xAB,  # Transfer ID       12345678901234567890
            0x31,
            0xD4,
            0x00,
            0x00,  # Frame index, EOT  54321 with EOT flag not set
            0x00,
            0x00,  # User data
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    f = SerialFrame.parse_from_unescaped_image(memoryview(header))
    assert f == SerialFrame(
        priority=Priority.LOW,
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=False,
        payload=memoryview(b""),
        source_node_id=1,
        destination_node_id=0,
        data_specifier=ServiceDataSpecifier(16, ServiceDataSpecifier.Role.RESPONSE),
        user_data=0,
    )

    # Too short
    assert SerialFrame.parse_from_unescaped_image(memoryview(header[1:])) is None

    # Bad version
    header = bytes(
        [
            SerialFrame.VERSION + 1,
            int(Priority.LOW),
            0x01,
            0x00,
            0x00,
            0x00,
            0x10,
            0x80,
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
            0x54,
            0xAB,
            0x31,
            0xD4,
            0x00,
            0x00,
            0x00,
            0x00,
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    assert SerialFrame.parse_from_unescaped_image(memoryview(header)) is None

    # Bad fields (Priority)
    header = bytes(
        [
            SerialFrame.VERSION,
            0x88,
            0xFF,
            0xFF,
            0x00,
            0xFF,
            0xE1,
            0x10,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xD2,
            0x0A,
            0x1F,
            0xEB,
            0x8C,
            0xA9,
        ]
    )
    header += get_crc(header)
    assert len(header) == 24
    assert SerialFrame.parse_from_unescaped_image(memoryview(header)) is None


def _unittest_serial_frame_check() -> None:
    from pytest import raises
    from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier

    _ = SerialFrame(
        priority=Priority.HIGH,
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=False,
        payload=memoryview(b"You might think you've peeped the scene"),
        source_node_id=123,
        destination_node_id=456,
        data_specifier=MessageDataSpecifier(2345),
        user_data=0,
    )

    # Invalid priority
    with raises(TypeError):
        SerialFrame(
            priority=-1,  # type: ignore
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"You haven't, the real one's far too mean"),
            source_node_id=123,
            destination_node_id=456,
            data_specifier=MessageDataSpecifier(2345),
            user_data=0,
        )

    # Invalid source node ID
    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"The watered down one, the one you know"),
            source_node_id=123456,
            destination_node_id=456,
            data_specifier=MessageDataSpecifier(2345),
            user_data=0,
        )

    # Invalid destination node ID
    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"Was made up centuries ago"),
            source_node_id=123,
            destination_node_id=123456,
            data_specifier=MessageDataSpecifier(2345),
            user_data=0,
        )

    # Anonymous nodes cannot use service transfers
    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"They made it sound all wack and corny"),
            source_node_id=None,
            destination_node_id=456,
            data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST),
            user_data=0,
        )

    # Invalid data specifier
    with raises(TypeError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"Yes, it's awful, blasted boring"),
            source_node_id=123,
            destination_node_id=456,
            data_specifier=-1,  # type: ignore
            user_data=0,
        )

    # Invalid transfer-ID
    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=-1,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"Twisted fictions, sick addiction"),
            source_node_id=None,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(2345),
            user_data=0,
        )

    # Invalid index
    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            transfer_id=0,
            index=-1,
            end_of_transfer=False,
            payload=memoryview(b"Well, gather 'round, children, zip it, listen"),
            source_node_id=None,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(2345),
            user_data=0,
        )
