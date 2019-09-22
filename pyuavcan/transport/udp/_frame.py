#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class UDPFrame(pyuavcan.transport.commons.high_overhead_transport.Frame):
    """
    The header format is up to debate until it's frozen in Specification.

    We want the payload of single-frame transfers to be aligned at 128 bits (64 bit is the absolute minimum).
    Alignment is important for memory-aliased data structures. The two available header sizes that match
    the alignment requirement are 16 and 32 bytes; 24 bytes are less desirable but for many applications could
    be acceptable alignment-wise. Considering the typical transfer payload size, a 24/32-byte header is
    likely to cause bandwidth utilization concerns in high-bandwidth systems, so we are focusing on 16-byte
    formats first.

    An important thing to keep in mind is that the minimum size of an UDP/IPv4 payload when transferred over
    100M Ethernet is 18 bytes, due to the minimum Ethernet frame size limit. That is, if the application
    payload requires less space, the missing bytes will be padded out to the minimum size.

    It is preferred to allow header encoding by naive memory-aliasing; hence, the data should be natively
    representable using conventional memory layouts. We have two major large fields that shall always be
    present in the header: the 56-bit transfer-ID value and the 64-bit data type hash. The smaller fields
    are the priority field (3 bits) and the multi-frame flag (1 bit).
    A possible layout is::

        uint56 transfer_id
        uint8  flags             # priority bits 7..5, multi-frame flag bit 4, version bits 3..0
        uint64 data_type_hash

    The priority fields are shifted up to enable simple priority comparison by direct comparison of the flags.
    There is a side effect that single-frame transfers will take precedence over multi-frame transfers if they
    share the same 3-bit priority code, but this is found to be acceptable.
    The format can be trivially represented as a native structure::

        struct {
            uint64_t flags_and_transfer_id;
            const uint64_t data_type_hash;
            // In simple applications, this may be followed directly by the aliased payload.
        };

    The transfer-ID field can be updated between transfers by direct incrementation,
    because the flags are stored in the most significant byte.
    This is neat; however, the disadvantages are:

    - Variable header format: multi-frame transfers require us to append additional info to the header
      (namely, the frame index value and the end-of-transfer flag). This extends the state space: more
      branching, more testing, more chances for an error to creep in.
    - The version bits are thrown all the way to the seventh byte because of the little-endian byte order.

    We could move the flags to the least significant bit, but that would complicate handling
    because the transfer-ID would have to be incremented not by one but by 256 (0x100) to retain the flags.

    Another approach is to use a fixed 24/32-byte header, shared for single-frame and multi-frame transfers.
    This approach allows us to implement simpler handling (less branching) and arrange the fields sensibly::

        uint8 version
        uint8 priority
        void16
        uint32 frame_index_eot
        uint64 data_type_hash
        uint64 transfer_id

    The above is 24-bytes large; an extra 64-bit padding field can be added to ensure 32-byte alignment.
    Neither seems practical.

    If you have any feedback concerning the frame format, please bring it to
    https://forum.uavcan.org/t/alternative-transport-protocols/324.
    """
    TRANSFER_ID_MASK = 2 ** 56 - 1
    INDEX_MASK       = 2 ** 31 - 1

    SINGLE_FRAME_TRANSFER_HEADER_SIZE_BYTES = 16

    data_type_hash: int

    def __post_init__(self) -> None:
        if not isinstance(self.priority, pyuavcan.transport.Priority):
            raise TypeError(f'Invalid priority: {self.priority}')  # pragma: no cover

        if not (0 <= self.data_type_hash <= pyuavcan.transport.PayloadMetadata.DATA_TYPE_HASH_MASK):
            raise ValueError(f'Invalid data type hash: {self.data_type_hash}')

        if not (0 <= self.transfer_id <= self.TRANSFER_ID_MASK):
            raise ValueError(f'Invalid transfer-ID: {self.transfer_id}')

        if not (0 <= self.index <= self.INDEX_MASK):
            raise ValueError(f'Invalid frame index: {self.index}')

        if not isinstance(self.payload, memoryview):
            raise TypeError(f'Bad payload type: {type(self.payload).__name__}')  # pragma: no cover

    def compile_header_and_payload(self) -> typing.Tuple[memoryview, memoryview]:
        """
        Compiles the UDP frame header and returns it as a read-only memoryview along with the payload, separately.
        The caller is supposed to handle the header and the payload independently.
        The reason is to avoid unnecessary data copying in the user space,
        allowing the caller to rely on the vectorized IO API instead (sendmsg).
        """
        header = self.transfer_id | (int(self.priority) << 61) | (self.data_type_hash << 64)
        if self.single_frame_transfer:
            #   0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
            # +--------------------+--+-----------------------+
            # |     Transfer ID    |Fl|    Data type hash     |
            # +--------------------+--+-----------------------+
            # Flags ("Fl"): bits 7..5 - priority, bit 4 - multiframe transfer (cleared).
            return memoryview(header.to_bytes(16, _BYTE_ORDER)), self.payload
        else:
            #   0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19
            # +--------------------+--+-----------------------+-----------+
            # |     Transfer ID    |Fl|    Data type hash     |Fr.idx. EOT|
            # +--------------------+--+-----------------------+-----------+
            # Flags ("Fl"): bits 7..5 - priority, bit 4 - multiframe transfer (set).
            # Frame index with end-of-transfer: MSB set in the last frame, cleared otherwise.
            header |= 1 << 60  # Multiframe transfer flag
            idx_eot = self.index | ((1 if self.end_of_transfer else 0) << 31)
            header |= idx_eot << 128
            return memoryview(header.to_bytes(20, _BYTE_ORDER)), self.payload

    @staticmethod
    def parse(image: memoryview, timestamp: pyuavcan.transport.Timestamp) -> typing.Optional[UDPFrame]:
        if len(image) < 16:
            return None     # Insufficient length

        flags: int = image[7]
        version = flags & 0x0F
        if version != 0:
            return None     # Bad version

        transfer_id = int.from_bytes(image[0:8], _BYTE_ORDER) & UDPFrame.TRANSFER_ID_MASK  # type: ignore
        data_type_hash = int.from_bytes(image[8:16], _BYTE_ORDER)  # type: ignore
        single_frame_transfer = flags & 16 == 0
        priority = pyuavcan.transport.Priority(flags >> 5)

        if single_frame_transfer:
            index = 0
            end_of_transfer = True
            payload = image[16:]
        else:
            if len(image) < 20:
                return None     # Insufficient length

            idx_eot = int.from_bytes(image[16:20], _BYTE_ORDER)  # type: ignore
            index = idx_eot & UDPFrame.INDEX_MASK
            end_of_transfer = idx_eot > UDPFrame.INDEX_MASK
            payload = image[20:]

        return UDPFrame(timestamp=timestamp,
                        priority=priority,
                        transfer_id=transfer_id,
                        index=index,
                        end_of_transfer=end_of_transfer,
                        payload=payload,
                        data_type_hash=data_type_hash)


_BYTE_ORDER = 'little'


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------

def _unittest_udp_frame_compile() -> None:
    from pyuavcan.transport import Priority, Timestamp
    from pytest import raises

    ts = Timestamp.now()

    _ = UDPFrame(timestamp=ts,
                 priority=Priority.LOW,
                 transfer_id=0,
                 index=0,
                 end_of_transfer=False,
                 payload=memoryview(b''),
                 data_type_hash=0)

    with raises(ValueError):
        _ = UDPFrame(timestamp=ts,
                     priority=Priority.LOW,
                     transfer_id=2 ** 56,
                     index=0,
                     end_of_transfer=False,
                     payload=memoryview(b''),
                     data_type_hash=0)

    with raises(ValueError):
        _ = UDPFrame(timestamp=ts,
                     priority=Priority.LOW,
                     transfer_id=0,
                     index=2 ** 31,
                     end_of_transfer=False,
                     payload=memoryview(b''),
                     data_type_hash=0)

    with raises(ValueError):
        _ = UDPFrame(timestamp=ts,
                     priority=Priority.LOW,
                     transfer_id=0,
                     index=0,
                     end_of_transfer=False,
                     payload=memoryview(b''),
                     data_type_hash=2 ** 64)

    # Multi-frame, not the end of the transfer.
    assert (
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xD0',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             0x_0dd_f00d.to_bytes(4, 'little')])),
        memoryview(b'Well, I got here the same way the coin did.'),
    ) == UDPFrame(
        timestamp=ts,
        priority=Priority.SLOW,
        transfer_id=0x_dead_beef_c0ffee,
        index=0x_0dd_f00d,
        end_of_transfer=False,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ).compile_header_and_payload()

    # Multi-frame, end of the transfer.
    assert (
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xF0',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             (0x_0dd_f00d | 2 ** 31).to_bytes(4, 'little')])),
        memoryview(b'Well, I got here the same way the coin did.'),
    ) == UDPFrame(
        timestamp=ts,
        priority=Priority.OPTIONAL,
        transfer_id=0x_dead_beef_c0ffee,
        index=0x_0dd_f00d,
        end_of_transfer=True,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ).compile_header_and_payload()

    # Single-frame.
    assert (
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\x00',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little')])),
        memoryview(b'Well, I got here the same way the coin did.'),
    ) == UDPFrame(
        timestamp=ts,
        priority=Priority.EXCEPTIONAL,
        transfer_id=0x_dead_beef_c0ffee,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ).compile_header_and_payload()


def _unittest_udp_frame_parse() -> None:
    from pyuavcan.transport import Priority, Timestamp

    ts = Timestamp.now()

    for size in range(16):
        assert None is UDPFrame.parse(memoryview(bytes(range(size))), ts)

    # Multi-frame, not the end of the transfer.
    assert UDPFrame(
        timestamp=ts,
        priority=Priority.SLOW,
        transfer_id=0x_dead_beef_c0ffee,
        index=0x_0dd_f00d,
        end_of_transfer=False,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ) == UDPFrame.parse(
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xD0',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             0x_0dd_f00d.to_bytes(4, 'little'),
                             b'Well, I got here the same way the coin did.'])),
        ts,
    )

    # Multi-frame, end of the transfer.
    assert UDPFrame(
        timestamp=ts,
        priority=Priority.OPTIONAL,
        transfer_id=0x_dead_beef_c0ffee,
        index=0x_0dd_f00d,
        end_of_transfer=True,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ) == UDPFrame.parse(
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xF0',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             (0x_0dd_f00d | 2 ** 31).to_bytes(4, 'little'),
                             b'Well, I got here the same way the coin did.'])),
        ts,
    )

    # Single-frame.
    assert UDPFrame(
        timestamp=ts,
        priority=Priority.EXCEPTIONAL,
        transfer_id=0x_dead_beef_c0ffee,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b'Well, I got here the same way the coin did.'),
        data_type_hash=0x_0dd_c0ffee_bad_f00d,
    ) == UDPFrame.parse(
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\x00',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             b'Well, I got here the same way the coin did.'])),
        ts,
    )

    # Bad formats.
    assert None is UDPFrame.parse(
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xD0',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             0x_0dd_f00d.to_bytes(4, 'little')]))[:-1],
        ts,
    )
    assert None is UDPFrame.parse(
        memoryview(b''.join([0x_dead_beef_c0ffee.to_bytes(7, 'little'),
                             b'\xDF',
                             0x_0dd_c0ffee_bad_f00d.to_bytes(8, 'little'),
                             0x_0dd_f00d.to_bytes(4, 'little')])),
        ts,
    )
