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

_FRAME_DELIMITER_BYTE = 0x9E
_ESCAPE_PREFIX_BYTE   = 0x8E

_HEADER_STRUCT = struct.Struct('<'
                               'BB'     # Version, priority
                               'HHH'    # source NID, destination NID, data specifier
                               'QQ'     # Data type hash, transfer-ID
                               'L'      # Frame index with end-of-transfer flag in the MSB
                               '4x')
assert _HEADER_STRUCT.size == 32


@dataclasses.dataclass(frozen=True)
class Frame:
    NODE_ID_MASK = 4095
    TRANSFER_ID_MASK = 2 ** 64 - 1
    FRAME_INDEX_MASK = 2 ** 31 - 1

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
        Compiles the frame into the specified output buffer.
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

        header = _HEADER_STRUCT.pack(_VERSION,
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
        crc_bytes = crc.value.to_bytes(4, 'little')

        escapees = _FRAME_DELIMITER_BYTE, _ESCAPE_PREFIX_BYTE
        out_buffer[0] = _FRAME_DELIMITER_BYTE
        next_byte_index = 1
        for nb in itertools.chain(header, self.payload, crc_bytes):
            if nb in escapees:
                out_buffer[next_byte_index] = _ESCAPE_PREFIX_BYTE
                next_byte_index += 1
                nb ^= 0xFF
            out_buffer[next_byte_index] = nb
            next_byte_index += 1

        out_buffer[next_byte_index] = _FRAME_DELIMITER_BYTE
        next_byte_index += 1

        assert (next_byte_index - 2) >= (len(header) + len(self.payload) + len(crc_bytes))
        return memoryview(out_buffer)[:next_byte_index]


@dataclasses.dataclass(frozen=True)
class TimestampedFrame(Frame):
    timestamp: pyuavcan.transport.Timestamp

    def __post_init__(self) -> None:
        if not (self.timestamp.monotonic_ns > 0 and self.timestamp.system_ns > 0):
            raise ValueError(f'Bad timestamp: {self.timestamp}')

    @staticmethod
    def parse(source: bytes) -> typing.Optional[TimestampedFrame]:
        raise NotImplementedError


def _unittest_frame_compile_message() -> None:
    from pyuavcan.transport import Priority, MessageDataSpecifier

    f = Frame(priority=Priority.HIGH,
              source_node_id=_FRAME_DELIMITER_BYTE,
              destination_node_id=_ESCAPE_PREFIX_BYTE,
              data_specifier=MessageDataSpecifier(12345),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=True,
              payload=memoryview(b'abcd\x9Eef\x8E'))

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == _FRAME_DELIMITER_BYTE
    assert mv[-1] == _FRAME_DELIMITER_BYTE
    segment = bytes(mv[1:-1])
    assert _FRAME_DELIMITER_BYTE not in segment

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.HIGH)
    assert segment[2] == _ESCAPE_PREFIX_BYTE
    assert (segment[3], segment[4]) == (_FRAME_DELIMITER_BYTE ^ 0xFF, 0)
    assert segment[5] == _ESCAPE_PREFIX_BYTE
    assert (segment[6], segment[7]) == (_ESCAPE_PREFIX_BYTE ^ 0xFF, 0)
    assert segment[8:10] == 12345 .to_bytes(2, 'little')
    assert segment[10:18] == 0xdead_beef_bad_c0ffe .to_bytes(8, 'little')
    assert segment[18:26] == 1234567890123456789 .to_bytes(8, 'little')
    assert segment[26:30] == (1234567 + 0x8000_0000).to_bytes(4, 'little')
    assert segment[30:34] == b'\x00' * 4

    # Payload validation
    assert segment[34:38] == b'abcd'
    assert segment[38] == _ESCAPE_PREFIX_BYTE
    assert segment[39] == 0x9E ^ 0xFF
    assert segment[40:42] == b'ef'
    assert segment[42] == _ESCAPE_PREFIX_BYTE
    assert segment[43] == 0x8E ^ 0xFF

    # CRC validation
    header = _HEADER_STRUCT.pack(_VERSION,
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
    assert segment[44:] == crc.value.to_bytes(4, 'little')


def _unittest_frame_compile_service() -> None:
    from pyuavcan.transport import Priority, ServiceDataSpecifier

    f = Frame(priority=Priority.HIGH,
              source_node_id=_FRAME_DELIMITER_BYTE,
              destination_node_id=None,
              data_specifier=ServiceDataSpecifier(123, ServiceDataSpecifier.Role.SERVER),
              data_type_hash=0xdead_beef_bad_c0ffe,
              transfer_id=1234567890123456789,
              frame_index=1234567,
              end_of_transfer=False,
              payload=memoryview(b''))

    buffer = bytearray(0 for _ in range(1000))
    mv = f.compile_into(buffer)

    assert mv[0] == _FRAME_DELIMITER_BYTE
    assert mv[-1] == _FRAME_DELIMITER_BYTE
    segment = bytes(mv[1:-1])
    assert _FRAME_DELIMITER_BYTE not in segment

    # Header validation
    assert segment[0] == _VERSION
    assert segment[1] == int(Priority.HIGH)
    assert segment[2] == _ESCAPE_PREFIX_BYTE
    assert (segment[3], segment[4]) == (_FRAME_DELIMITER_BYTE ^ 0xFF, 0)
    assert (segment[5], segment[6]) == (0xFF, 0xFF)
    assert segment[7:9] == ((1 << 15) | (1 << 14) | 123) .to_bytes(2, 'little')
    assert segment[9:17] == 0xdead_beef_bad_c0ffe .to_bytes(8, 'little')
    assert segment[17:25] == 1234567890123456789 .to_bytes(8, 'little')
    assert segment[25:29] == 1234567 .to_bytes(4, 'little')
    assert segment[29:33] == b'\x00' * 4

    # CRC validation
    header = _HEADER_STRUCT.pack(_VERSION,
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
    assert segment[33:] == crc.value.to_bytes(4, 'little')


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
