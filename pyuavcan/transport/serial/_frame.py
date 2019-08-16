#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import struct
import itertools
import dataclasses
import pyuavcan

_VERSION = 0

# Same value represents broadcast node ID when transmitting.
_ANONYMOUS_NODE_ID = 0xFFFF


@dataclasses.dataclass(frozen=True)
class Frame:
    NODE_ID_MASK     = 4095
    TRANSFER_ID_MASK = 2 ** 64 - 1
    FRAME_INDEX_MASK = 2 ** 31 - 1

    FRAME_DELIMITER_BYTE = 0x9E
    ESCAPE_PREFIX_BYTE   = 0x8E

    HEADER_STRUCT = struct.Struct('<'
                                  'BB'  # Version, priority
                                  'HHH'  # source NID, destination NID, data specifier
                                  'QQ'  # Data type hash, transfer-ID
                                  'L'  # Frame index with end-of-transfer flag in the MSB
                                  '4x')
    CRC_SIZE_BYTES = 4

    NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING = HEADER_STRUCT.size + CRC_SIZE_BYTES

    priority:            pyuavcan.transport.Priority
    source_node_id:      typing.Optional[int]
    destination_node_id: typing.Optional[int]
    data_specifier:      pyuavcan.transport.DataSpecifier
    data_type_hash:      int
    transfer_id:         int
    frame_index:         int
    end_of_transfer:     bool
    payload:             memoryview

    def __post_init__(self) -> None:
        if not isinstance(self.priority, pyuavcan.transport.Priority):
            raise ValueError(f'Invalid priority: {self.priority}')  # pragma: no cover

        if self.source_node_id is not None and not (0 <= self.source_node_id <= self.NODE_ID_MASK):
            raise ValueError(f'Invalid source node ID: {self.source_node_id}')

        if self.destination_node_id is not None and not (0 <= self.destination_node_id <= self.NODE_ID_MASK):
            raise ValueError(f'Invalid destination node ID: {self.destination_node_id}')

        if self.source_node_id is not None and self.source_node_id == self.destination_node_id:
            raise ValueError(f'Source and destination node IDs are equal: {self.source_node_id}')

        if isinstance(self.data_specifier, pyuavcan.transport.ServiceDataSpecifier) and self.source_node_id is None:
            raise ValueError(f'Anonymous nodes cannot use service transfers: {self.data_specifier}')

        if not isinstance(self.data_specifier, pyuavcan.transport.DataSpecifier):
            raise ValueError(f'Invalid data specifier: {self.data_specifier}')  # pragma: no cover

        if not (0 <= self.data_type_hash <= pyuavcan.transport.PayloadMetadata.DATA_TYPE_HASH_MASK):
            raise ValueError(f'Invalid data type hash: {self.data_type_hash}')

        if not (0 <= self.transfer_id <= self.TRANSFER_ID_MASK):
            raise ValueError(f'Invalid transfer-ID: {self.transfer_id}')

        if not (0 <= self.frame_index <= self.FRAME_INDEX_MASK):
            raise ValueError(f'Invalid frame index: {self.frame_index}')

        if not isinstance(self.payload, memoryview):
            raise ValueError(f'Bad payload type: {type(self.payload).__name__}')  # pragma: no cover

    def compile_into(self, out_buffer: bytearray) -> memoryview:
        """
        Compiles the frame into the specified output buffer, escaping the data as necessary.
        The buffer must be large enough to accommodate the frame header with the payload and CRC,
        including escape sequences.
        :returns: View of the memory from the beginning of the buffer until the end of the compiled frame.
        """
        src_nid = _ANONYMOUS_NODE_ID if self.source_node_id is None else self.source_node_id
        dst_nid = _ANONYMOUS_NODE_ID if self.destination_node_id is None else self.destination_node_id

        if isinstance(self.data_specifier, pyuavcan.transport.MessageDataSpecifier):
            data_spec = self.data_specifier.subject_id
        elif isinstance(self.data_specifier, pyuavcan.transport.ServiceDataSpecifier):
            # Servers send responses; clients send requests.
            is_response = self.data_specifier.role == self.data_specifier.Role.SERVER
            data_spec = (1 << 15) | ((1 << 14) if is_response else 0) | self.data_specifier.service_id
        else:
            assert False

        frame_index_eot = self.frame_index | ((1 << 31) if self.end_of_transfer else 0)

        header = self.HEADER_STRUCT.pack(_VERSION,
                                         int(self.priority),
                                         src_nid,
                                         dst_nid,
                                         data_spec,
                                         self.data_type_hash,
                                         self.transfer_id,
                                         frame_index_eot)
        assert len(header) == 32

        crc = pyuavcan.transport.commons.crc.CRC32C()
        crc.add(header)
        crc.add(self.payload)
        crc_bytes = crc.value_as_bytes

        escapees = self.FRAME_DELIMITER_BYTE, self.ESCAPE_PREFIX_BYTE
        out_buffer[0] = self.FRAME_DELIMITER_BYTE
        next_byte_index = 1
        for nb in itertools.chain(header, self.payload, crc_bytes):
            if nb in escapees:
                out_buffer[next_byte_index] = self.ESCAPE_PREFIX_BYTE
                next_byte_index += 1
                nb ^= 0xFF
            out_buffer[next_byte_index] = nb
            next_byte_index += 1

        out_buffer[next_byte_index] = self.FRAME_DELIMITER_BYTE
        next_byte_index += 1

        assert (next_byte_index - 2) >= (len(header) + len(self.payload) + len(crc_bytes))
        return memoryview(out_buffer)[:next_byte_index]


assert Frame.HEADER_STRUCT.size == 32


@dataclasses.dataclass(frozen=True)
class TimestampedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    @staticmethod
    def parse_from_unescaped_image(header_payload_crc_image: memoryview,
                                   timestamp: pyuavcan.transport.Timestamp) -> typing.Optional[TimestampedFrame]:
        """
        :returns: Frame or None if the image is invalid.
        """
        if len(header_payload_crc_image) < Frame.NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING:
            return None

        crc = pyuavcan.transport.commons.crc.CRC32C()
        crc.add(header_payload_crc_image)
        if not crc.check_residue():
            return None

        header = header_payload_crc_image[:Frame.HEADER_STRUCT.size]
        payload = header_payload_crc_image[Frame.HEADER_STRUCT.size:-Frame.CRC_SIZE_BYTES]

        # noinspection PyTypeChecker
        version, int_priority, src_nid, dst_nid, int_data_spec, dt_hash, transfer_id, frame_index_eot = \
            Frame.HEADER_STRUCT.unpack(header)  # type: ignore
        if version != _VERSION:
            return None

        src_nid = None if src_nid == _ANONYMOUS_NODE_ID else src_nid
        dst_nid = None if dst_nid == _ANONYMOUS_NODE_ID else dst_nid

        if int_data_spec & (1 << 15) == 0:
            data_specifier = pyuavcan.transport.MessageDataSpecifier(int_data_spec)
        else:
            is_response = int_data_spec & (1 << 14) != 0  # Servers receive requests; clients receive responses.
            role = \
                pyuavcan.transport.ServiceDataSpecifier.Role.CLIENT if is_response else \
                pyuavcan.transport.ServiceDataSpecifier.Role.SERVER
            service_id = int_data_spec & pyuavcan.transport.ServiceDataSpecifier.SERVICE_ID_MASK
            data_specifier = pyuavcan.transport.ServiceDataSpecifier(service_id, role)

        try:
            return TimestampedFrame(priority=pyuavcan.transport.Priority(int_priority),
                                    source_node_id=src_nid,
                                    destination_node_id=dst_nid,
                                    data_specifier=data_specifier,
                                    data_type_hash=dt_hash,
                                    transfer_id=transfer_id,
                                    frame_index=frame_index_eot & Frame.FRAME_INDEX_MASK,
                                    end_of_transfer=frame_index_eot & (1 << 31) != 0,
                                    payload=payload,
                                    timestamp=timestamp)
        except ValueError:
            return None


def _unittest_frame_compile_message() -> None:
    from pyuavcan.transport import Priority, MessageDataSpecifier

    f = Frame(priority=Priority.HIGH,
              source_node_id=Frame.FRAME_DELIMITER_BYTE,
              destination_node_id=Frame.ESCAPE_PREFIX_BYTE,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=True,
              payload=memoryview(b'abcd\x9Eef\x8E'))

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == Frame.FRAME_DELIMITER_BYTE
    assert mv[-1] == Frame.FRAME_DELIMITER_BYTE
    segment = bytes(mv[1:-1])
    assert Frame.FRAME_DELIMITER_BYTE not in segment

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.HIGH)
    assert segment[2] == Frame.ESCAPE_PREFIX_BYTE
    assert (segment[3], segment[4]) == (Frame.FRAME_DELIMITER_BYTE ^ 0xFF, 0)
    assert segment[5] == Frame.ESCAPE_PREFIX_BYTE
    assert (segment[6], segment[7]) == (Frame.ESCAPE_PREFIX_BYTE ^ 0xFF, 0)
    assert segment[8:10] == 12345 .to_bytes(2, 'little')
    assert segment[10:18] == 0xdead_beef_bad_c0ffe .to_bytes(8, 'little')
    assert segment[18:26] == 1234567890123456789 .to_bytes(8, 'little')
    assert segment[26:30] == (1234567 + 0x8000_0000).to_bytes(4, 'little')
    assert segment[30:34] == b'\x00' * 4

    # Payload validation
    assert segment[34:38] == b'abcd'
    assert segment[38] == Frame.ESCAPE_PREFIX_BYTE
    assert segment[39] == 0x9E ^ 0xFF
    assert segment[40:42] == b'ef'
    assert segment[42] == Frame.ESCAPE_PREFIX_BYTE
    assert segment[43] == 0x8E ^ 0xFF

    # CRC validation
    header = Frame.HEADER_STRUCT.pack(_VERSION,
                                      int(f.priority),
                                      f.source_node_id,
                                      f.destination_node_id,
                                      12345,
                                      f.data_type_hash,
                                      f.transfer_id,
                                      f.frame_index + 0x8000_0000)
    crc = pyuavcan.transport.commons.crc.CRC32C()
    crc.add(header)
    crc.add(f.payload)
    assert segment[44:] == crc.value_as_bytes


def _unittest_frame_compile_service() -> None:
    from pyuavcan.transport import Priority, ServiceDataSpecifier

    f = Frame(priority=Priority.HIGH,
              source_node_id=Frame.FRAME_DELIMITER_BYTE,
              destination_node_id=None,
              data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.SERVER),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b''))

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == Frame.FRAME_DELIMITER_BYTE
    assert mv[-1] == Frame.FRAME_DELIMITER_BYTE
    segment = bytes(mv[1:-1])
    assert Frame.FRAME_DELIMITER_BYTE not in segment

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.HIGH)
    assert segment[2] == Frame.ESCAPE_PREFIX_BYTE
    assert (segment[3], segment[4]) == (Frame.FRAME_DELIMITER_BYTE ^ 0xFF, 0)
    assert (segment[5], segment[6]) == (0xFF, 0xFF)
    assert segment[7:9] == ((1 << 15) | (1 << 14) | 123) .to_bytes(2, 'little')
    assert segment[9:17] == 0xdead_beef_bad_c0ffe .to_bytes(8, 'little')
    assert segment[17:25] == 1234567890123456789 .to_bytes(8, 'little')
    assert segment[25:29] == 1234567 .to_bytes(4, 'little')
    assert segment[29:33] == b'\x00' * 4

    # CRC validation
    header = Frame.HEADER_STRUCT.pack(_VERSION,
                                      int(f.priority),
                                      f.source_node_id,
                                      _ANONYMOUS_NODE_ID,
                                      (1 << 15) | (1 << 14) | 123,
                                      f.data_type_hash,
                                      f.transfer_id,
                                      f.frame_index)
    crc = pyuavcan.transport.commons.crc.CRC32C()
    crc.add(header)
    crc.add(f.payload)
    assert segment[33:] == crc.value_as_bytes


def _unittest_parse() -> None:
    from pyuavcan.transport import Priority, MessageDataSpecifier, ServiceDataSpecifier

    ts = pyuavcan.transport.Timestamp.now()

    def get_crc(*blocks: typing.Union[bytes, memoryview]) -> bytes:
        crc = pyuavcan.transport.commons.crc.CRC32C()
        for b in blocks:
            crc.add(b)
        return crc.value_as_bytes

    # Valid message with payload
    header = bytes([
        _VERSION,
        int(Priority.LOW),
        0x7B, 0x00,                                         # Source NID        123
        0xC8, 0x01,                                         # Destination NID   456
        0xE1, 0x10,                                         # Data specifier    4321
        0x0D, 0xF0, 0xDD, 0xE0, 0xFE, 0x0F, 0xDC, 0xBA,     # Data type hash    0xbad_c0ffee_0dd_f00d
        0xD2, 0x0A, 0x1F, 0xEB, 0x8C, 0xA9, 0x54, 0xAB,     # Transfer ID       12345678901234567890
        0x31, 0xD4, 0x00, 0x80,                             # Frame index, EOT  54321 with EOT flag set
        0x12, 0x34, 0x56, 0x78,                             # Padding ignored
    ])
    assert len(header) == 32
    payload = b'Squeeze mayonnaise onto a hamster'
    f = TimestampedFrame.parse_from_unescaped_image(memoryview(header + payload + get_crc(header, payload)), ts)
    assert f == TimestampedFrame(
        priority=Priority.LOW,
        source_node_id=123,
        destination_node_id=456,
        data_specifier=MessageDataSpecifier(4321),
        data_type_hash=0xbad_c0ffee_0dd_f00d,
        transfer_id=12345678901234567890,
        frame_index=54321,
        end_of_transfer=True,
        payload=memoryview(payload),
        timestamp=ts,
    )

    # Valid service with no payload
    header = bytes([
        _VERSION,
        int(Priority.LOW),
        0x01, 0x00,
        0x00, 0x00,
        0x10, 0xC0,                                         # Response, service ID 16
        0x0D, 0xF0, 0xDD, 0xE0, 0xFE, 0x0F, 0xDC, 0xBA,
        0xD2, 0x0A, 0x1F, 0xEB, 0x8C, 0xA9, 0x54, 0xAB,
        0x31, 0xD4, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ])
    f = TimestampedFrame.parse_from_unescaped_image(memoryview(header + get_crc(header)), ts)
    assert f == TimestampedFrame(
        priority=Priority.LOW,
        source_node_id=1,
        destination_node_id=0,
        data_specifier=ServiceDataSpecifier(16, ServiceDataSpecifier.Role.CLIENT),
        data_type_hash=0xbad_c0ffee_0dd_f00d,
        transfer_id=12345678901234567890,
        frame_index=54321,
        end_of_transfer=False,
        payload=memoryview(b''),
        timestamp=ts,
    )

    # Too short
    assert TimestampedFrame.parse_from_unescaped_image(memoryview(header[1:] + get_crc(header, payload)),
                                                       ts) is None

    # Bad CRC
    assert TimestampedFrame.parse_from_unescaped_image(memoryview(header + payload + b'1234'), ts) is None

    # Bad version
    header = bytes([
        _VERSION + 1,
        int(Priority.LOW),
        0xFF, 0xFF,
        0x00, 0x00,
        0xE1, 0x10,
        0x0D, 0xF0, 0xDD, 0xE0, 0xFE, 0x0F, 0xDC, 0xBA,
        0xD2, 0x0A, 0x1F, 0xEB, 0x8C, 0xA9, 0x54, 0xAB,
        0x31, 0xD4, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ])
    assert TimestampedFrame.parse_from_unescaped_image(memoryview(header + get_crc(header)), ts) is None

    # Bad fields
    header = bytes([
        _VERSION,
        0x88,
        0xFF, 0xFF,
        0x00, 0xFF,
        0xE1, 0x10,
        0x0D, 0xF0, 0xDD, 0xE0, 0xFE, 0x0F, 0xDC, 0xBA,
        0xD2, 0x0A, 0x1F, 0xEB, 0x8C, 0xA9, 0x54, 0xAB,
        0x31, 0xD4, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ])
    assert TimestampedFrame.parse_from_unescaped_image(memoryview(header + get_crc(header)), ts) is None


def _unittest_frame_check() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, MessageDataSpecifier, ServiceDataSpecifier

    f = Frame(priority=Priority.HIGH,
              source_node_id=123,
              destination_node_id=456,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))
    del f

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=123456,
              destination_node_id=456,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=123,
              destination_node_id=123456,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=123,
              destination_node_id=123,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=None,
              destination_node_id=456,
              data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.CLIENT),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=None,
              destination_node_id=None,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=2 ** 64,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=None,
              destination_node_id=None,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=-1,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))

    with raises(ValueError):
        Frame(priority=Priority.HIGH,
              source_node_id=None,
              destination_node_id=None,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=0,
              frame_index=-1,
              end_of_transfer=False,
              payload=memoryview(b'abcdef'))
