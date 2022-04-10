# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import struct
import dataclasses
from cobs import cobs  # type: ignore
import pycyphal

_VERSION = 0

# Same value represents broadcast node ID when transmitting.
_ANONYMOUS_NODE_ID = 0xFFFF

_HEADER_WITHOUT_CRC_FORMAT = struct.Struct(
    "<"
    "BB"  # Version, priority
    "HHH"  # source NID, destination NID, data specifier
    "8x"  # reserved 64 bits
    "Q"  # transfer-ID
    "L"
)  # Frame index with end-of-transfer flag in the MSB
_CRC_SIZE_BYTES = len(pycyphal.transport.commons.high_overhead_transport.TransferCRC().value_as_bytes)
_HEADER_SIZE = _HEADER_WITHOUT_CRC_FORMAT.size + _CRC_SIZE_BYTES
assert _HEADER_SIZE == 32


@dataclasses.dataclass(frozen=True, repr=False)
class SerialFrame(pycyphal.transport.commons.high_overhead_transport.Frame):
    NODE_ID_MASK = 4095
    TRANSFER_ID_MASK = 2**64 - 1
    INDEX_MASK = 2**31 - 1

    NODE_ID_RANGE = range(NODE_ID_MASK + 1)

    FRAME_DELIMITER_BYTE = 0x00

    NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING = _HEADER_SIZE + _CRC_SIZE_BYTES

    source_node_id: typing.Optional[int]
    destination_node_id: typing.Optional[int]
    data_specifier: pycyphal.transport.DataSpecifier

    def __post_init__(self) -> None:
        if self.source_node_id is not None and not (0 <= self.source_node_id <= self.NODE_ID_MASK):
            raise ValueError(f"Invalid source node ID: {self.source_node_id}")

        if self.destination_node_id is not None and not (0 <= self.destination_node_id <= self.NODE_ID_MASK):
            raise ValueError(f"Invalid destination node ID: {self.destination_node_id}")

        if isinstance(self.data_specifier, pycyphal.transport.ServiceDataSpecifier) and self.source_node_id is None:
            raise ValueError(f"Anonymous nodes cannot use service transfers: {self.data_specifier}")

        if not isinstance(self.data_specifier, pycyphal.transport.DataSpecifier):
            raise TypeError(f"Invalid data specifier: {self.data_specifier}")  # pragma: no cover

        if not (0 <= self.transfer_id <= self.TRANSFER_ID_MASK):
            raise ValueError(f"Invalid transfer-ID: {self.transfer_id}")

        if not (0 <= self.index <= self.INDEX_MASK):
            raise ValueError(f"Invalid frame index: {self.index}")

    def compile_into(self, out_buffer: bytearray) -> memoryview:
        """
        Compiles the frame into the specified output buffer, escaping the data as necessary.
        The buffer must be large enough to accommodate the frame header with the payload and CRC,
        including escape sequences.
        :returns: View of the memory from the beginning of the buffer until the end of the compiled frame.
        """
        src_nid = _ANONYMOUS_NODE_ID if self.source_node_id is None else self.source_node_id
        dst_nid = _ANONYMOUS_NODE_ID if self.destination_node_id is None else self.destination_node_id

        if isinstance(self.data_specifier, pycyphal.transport.MessageDataSpecifier):
            data_spec = self.data_specifier.subject_id
        elif isinstance(self.data_specifier, pycyphal.transport.ServiceDataSpecifier):
            is_response = self.data_specifier.role == self.data_specifier.Role.RESPONSE
            data_spec = (1 << 15) | ((1 << 14) if is_response else 0) | self.data_specifier.service_id
        else:
            assert False

        index_eot = self.index | ((1 << 31) if self.end_of_transfer else 0)

        header = _HEADER_WITHOUT_CRC_FORMAT.pack(
            _VERSION, int(self.priority), src_nid, dst_nid, data_spec, self.transfer_id, index_eot
        )
        header += pycyphal.transport.commons.crc.CRC32C.new(header).value_as_bytes
        assert len(header) == _HEADER_SIZE

        payload_crc_bytes = pycyphal.transport.commons.crc.CRC32C.new(self.payload).value_as_bytes

        out_buffer[0] = self.FRAME_DELIMITER_BYTE
        next_byte_index = 1

        # noinspection PyTypeChecker
        packet_bytes = header + self.payload + payload_crc_bytes
        encoded_image = cobs.encode(packet_bytes)
        # place in the buffer and update next_byte_index:
        out_buffer[next_byte_index : next_byte_index + len(encoded_image)] = encoded_image
        next_byte_index += len(encoded_image)

        out_buffer[next_byte_index] = self.FRAME_DELIMITER_BYTE
        next_byte_index += 1

        assert (next_byte_index - 2) >= (len(header) + len(self.payload) + len(payload_crc_bytes))
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
    def parse_from_unescaped_image(header_payload_crc_image: memoryview) -> typing.Optional[SerialFrame]:
        """
        :returns: Frame or None if the image is invalid.
        """
        if len(header_payload_crc_image) < SerialFrame.NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING:
            return None

        header = header_payload_crc_image[:_HEADER_SIZE]
        if not pycyphal.transport.commons.crc.CRC32C.new(header).check_residue():
            return None

        payload_with_crc = header_payload_crc_image[_HEADER_SIZE:]
        if not pycyphal.transport.commons.crc.CRC32C.new(payload_with_crc).check_residue():
            return None
        payload = payload_with_crc[:-_CRC_SIZE_BYTES]

        # noinspection PyTypeChecker
        (
            version,
            int_priority,
            src_nid,
            dst_nid,
            int_data_spec,
            transfer_id,
            index_eot,
        ) = _HEADER_WITHOUT_CRC_FORMAT.unpack_from(header)
        if version != _VERSION:
            return None

        src_nid = None if src_nid == _ANONYMOUS_NODE_ID else src_nid
        dst_nid = None if dst_nid == _ANONYMOUS_NODE_ID else dst_nid

        try:  # https://github.com/OpenCyphal/pycyphal/issues/176
            data_specifier: pycyphal.transport.DataSpecifier
            if int_data_spec & (1 << 15) == 0:
                data_specifier = pycyphal.transport.MessageDataSpecifier(int_data_spec)
            else:
                if int_data_spec & (1 << 14):
                    role = pycyphal.transport.ServiceDataSpecifier.Role.RESPONSE
                else:
                    role = pycyphal.transport.ServiceDataSpecifier.Role.REQUEST
                service_id = int_data_spec & pycyphal.transport.ServiceDataSpecifier.SERVICE_ID_MASK
                data_specifier = pycyphal.transport.ServiceDataSpecifier(service_id, role)

            # noinspection PyArgumentList
            return SerialFrame(
                priority=pycyphal.transport.Priority(int_priority),
                source_node_id=src_nid,
                destination_node_id=dst_nid,
                data_specifier=data_specifier,
                transfer_id=transfer_id,
                index=index_eot & SerialFrame.INDEX_MASK,
                end_of_transfer=index_eot & (1 << 31) != 0,
                payload=payload,
            )
        except ValueError:
            return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_frame_compile_message() -> None:
    from pycyphal.transport import Priority, MessageDataSpecifier

    f = SerialFrame(
        priority=Priority.HIGH,
        source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        destination_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        data_specifier=MessageDataSpecifier(2345),
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=True,
        payload=memoryview(b"abcd\x00ef\x00"),
    )

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == SerialFrame.FRAME_DELIMITER_BYTE
    assert mv[-1] == SerialFrame.FRAME_DELIMITER_BYTE

    segment_cobs = bytes(mv[1:-1])
    assert SerialFrame.FRAME_DELIMITER_BYTE not in segment_cobs

    segment = cobs.decode(segment_cobs)

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.HIGH)
    assert (segment[2], segment[3]) == (SerialFrame.FRAME_DELIMITER_BYTE, 0)
    assert (segment[4], segment[5]) == (SerialFrame.FRAME_DELIMITER_BYTE, 0)
    assert segment[6:8] == (2345).to_bytes(2, "little")
    assert segment[8:16] == b"\x00" * 8
    assert segment[16:24] == (1234567890123456789).to_bytes(8, "little")
    assert segment[24:28] == (1234567 + 0x8000_0000).to_bytes(4, "little")
    # Header CRC here

    # Payload validation
    assert segment[32:40] == b"abcd\x00ef\x00"
    assert segment[40:] == pycyphal.transport.commons.crc.CRC32C.new(f.payload).value_as_bytes


def _unittest_frame_compile_service() -> None:
    from pycyphal.transport import Priority, ServiceDataSpecifier

    f = SerialFrame(
        priority=Priority.FAST,
        source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        destination_node_id=None,
        data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.RESPONSE),
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=False,
        payload=memoryview(b""),
    )

    buffer = bytearray(0 for _ in range(50))
    mv = f.compile_into(buffer)

    assert mv[0] == mv[-1] == SerialFrame.FRAME_DELIMITER_BYTE
    segment_cobs = bytes(mv[1:-1])
    assert SerialFrame.FRAME_DELIMITER_BYTE not in segment_cobs

    segment = cobs.decode(segment_cobs)

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.FAST)
    assert (segment[2], segment[3]) == (SerialFrame.FRAME_DELIMITER_BYTE, 0)
    assert (segment[4], segment[5]) == (0xFF, 0xFF)
    assert segment[6:8] == ((1 << 15) | (1 << 14) | 123).to_bytes(2, "little")
    assert segment[8:16] == b"\x00" * 8
    assert segment[16:24] == (1234567890123456789).to_bytes(8, "little")
    assert segment[24:28] == (1234567).to_bytes(4, "little")
    # Header CRC here

    # CRC validation
    assert segment[32:] == pycyphal.transport.commons.crc.CRC32C.new(f.payload).value_as_bytes


def _unittest_frame_parse() -> None:
    from pycyphal.transport import Priority, MessageDataSpecifier, ServiceDataSpecifier

    def get_crc(*blocks: typing.Union[bytes, memoryview]) -> bytes:
        return pycyphal.transport.commons.crc.CRC32C.new(*blocks).value_as_bytes

    # Valid message with payload
    header = bytes(
        [
            _VERSION,
            int(Priority.LOW),
            0x7B,
            0x00,  # Source NID        123
            0xC8,
            0x01,  # Destination NID   456
            0xE1,
            0x10,  # Data specifier    4321
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,  # Reserved
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
        ]
    )
    header += get_crc(header)
    assert len(header) == 32
    payload = b"Squeeze mayonnaise onto a hamster"
    f = SerialFrame.parse_from_unescaped_image(memoryview(header + payload + get_crc(payload)))
    assert f == SerialFrame(
        priority=Priority.LOW,
        source_node_id=123,
        destination_node_id=456,
        data_specifier=MessageDataSpecifier(4321),
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=True,
        payload=memoryview(payload),
    )

    # Valid service with no payload
    header = bytes(
        [
            _VERSION,
            int(Priority.LOW),
            0x01,
            0x00,
            0x00,
            0x00,
            0x10,
            0xC0,  # Response, service ID 16
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
            0x54,
            0xAB,
            0x31,
            0xD4,
            0x00,
            0x00,
        ]
    )
    header += get_crc(header)
    assert len(header) == 32
    f = SerialFrame.parse_from_unescaped_image(memoryview(header + get_crc(b"")))
    assert f == SerialFrame(
        priority=Priority.LOW,
        source_node_id=1,
        destination_node_id=0,
        data_specifier=ServiceDataSpecifier(16, ServiceDataSpecifier.Role.RESPONSE),
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=False,
        payload=memoryview(b""),
    )

    # Valid service with no payload
    header = bytes(
        [
            _VERSION,
            int(Priority.LOW),
            0x01,
            0x00,
            0x00,
            0x00,
            0x10,
            0x80,  # Request, service ID 16
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
            0x54,
            0xAB,
            0x31,
            0xD4,
            0x00,
            0x00,
        ]
    )
    header += get_crc(header)
    assert len(header) == 32
    f = SerialFrame.parse_from_unescaped_image(memoryview(header + get_crc(b"")))
    assert f == SerialFrame(
        priority=Priority.LOW,
        source_node_id=1,
        destination_node_id=0,
        data_specifier=ServiceDataSpecifier(16, ServiceDataSpecifier.Role.REQUEST),
        transfer_id=12345678901234567890,
        index=54321,
        end_of_transfer=False,
        payload=memoryview(b""),
    )

    # Too short
    assert SerialFrame.parse_from_unescaped_image(memoryview(header[1:] + get_crc(payload))) is None

    # Bad CRC
    assert SerialFrame.parse_from_unescaped_image(memoryview(header + payload + b"1234")) is None

    # Bad version
    header = bytes(
        [
            _VERSION + 1,
            int(Priority.LOW),
            0xFF,
            0xFF,
            0x00,
            0x00,
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
            0x54,
            0xAB,
            0x31,
            0xD4,
            0x00,
            0x00,
        ]
    )
    header += get_crc(header)
    assert len(header) == 32
    assert SerialFrame.parse_from_unescaped_image(memoryview(header + get_crc(b""))) is None

    # Bad fields
    header = bytes(
        [
            _VERSION,
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
            0x54,
            0xAB,
            0x31,
            0xD4,
            0x00,
            0x00,
        ]
    )
    header += get_crc(header)
    assert len(header) == 32
    assert SerialFrame.parse_from_unescaped_image(memoryview(header + get_crc(b""))) is None


def _unittest_frame_check() -> None:
    from pytest import raises
    from pycyphal.transport import Priority, MessageDataSpecifier, ServiceDataSpecifier

    _ = SerialFrame(
        priority=Priority.HIGH,
        source_node_id=123,
        destination_node_id=456,
        data_specifier=MessageDataSpecifier(2345),
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=False,
        payload=memoryview(b"abcdef"),
    )

    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            source_node_id=123456,
            destination_node_id=456,
            data_specifier=MessageDataSpecifier(2345),
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"abcdef"),
        )

    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            source_node_id=123,
            destination_node_id=123456,
            data_specifier=MessageDataSpecifier(2345),
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"abcdef"),
        )

    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            source_node_id=None,
            destination_node_id=456,
            data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST),
            transfer_id=1234567890123456789,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"abcdef"),
        )

    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            source_node_id=None,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(2345),
            transfer_id=-1,
            index=1234567,
            end_of_transfer=False,
            payload=memoryview(b"abcdef"),
        )

    with raises(ValueError):
        SerialFrame(
            priority=Priority.HIGH,
            source_node_id=None,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(2345),
            transfer_id=0,
            index=-1,
            end_of_transfer=False,
            payload=memoryview(b"abcdef"),
        )
