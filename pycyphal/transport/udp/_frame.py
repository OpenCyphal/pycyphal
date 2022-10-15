# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import struct
import dataclasses
import pycyphal


@dataclasses.dataclass(frozen=True, repr=False)
class UDPFrame(pycyphal.transport.commons.high_overhead_transport.Frame):
    """
    The header format is up to debate until it's frozen in Specification.

    An important thing to keep in mind is that the minimum size of an UDP/IPv4 payload when transferred over
    100M Ethernet is 18 bytes, due to the minimum Ethernet frame size limit. That is, if the application
    payload requires less space, the missing bytes will be padded out to the minimum size.

    The current header format enables encoding by trivial memory aliasing on any conventional little-endian platform::

        struct Header {
            uint8_t  version;
            uint8_t  priority;
            uint16_t source_node_id;   
            uint32_t frame_index_eot;
            uint64_t transfer_id;
            uint64_t _reserved_b;   // Set to zero when encoding, ignore when decoding.
        };
        static_assert(sizeof(struct Header) == 24, "Invalid layout");

    If you have any feedback concerning the frame format, please bring it to
    https://forum.opencyphal.org/t/alternative-transport-protocols/324.

    +---------------+---------------+---------------+-----------------+------------------+
    |**MAC header** | **IP header** |**UDP header** |**Cyphal header**|**Cyphal payload**|
    +---------------+---------------+---------------+-----------------+------------------+
    |                                               |    Layers modeled by this type     |
    +-----------------------------------------------+------------------------------------+
    """

    _HEADER_FORMAT = struct.Struct(
        "<"  # little-endian
        "B"  # version
        "B"  # priority
        "H"  # source_node_id
        "I"  # frame_index_eot
        "Q"  # transfer_id
        "8x" # reserved 64 bits
    )
    _VERSION = 1

    TRANSFER_ID_MASK = 2**64 - 1
    SOURCE_NODE_ID_MASK = 2**16 - 1
    INDEX_MASK = 2**31 - 1

    # No anonymous nodes for Cyphal/UDP
    source_node_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.priority, pycyphal.transport.Priority):
            raise TypeError(f"Invalid priority: {self.priority}")  # pragma: no cover

        if not (0 <= self.transfer_id <= self.TRANSFER_ID_MASK):
            raise ValueError(f"Invalid transfer-ID: {self.transfer_id}")

        if not (0 <= self.index <= self.INDEX_MASK):
            raise ValueError(f"Invalid frame index: {self.index}")

        if not (0 <= self.source_node_id <= self.SOURCE_NODE_ID_MASK):
            raise ValueError(f"Invalid source node id: {self.source_node_id}")

        if not isinstance(self.payload, memoryview):
            raise TypeError(f"Bad payload type: {type(self.payload).__name__}")  # pragma: no cover

    def compile_header_and_payload(self) -> typing.Tuple[memoryview, memoryview]:
        """
        Compiles the UDP frame header and returns it as a read-only memoryview along with the payload, separately.
        The caller is supposed to handle the header and the payload independently.
        The reason is to avoid unnecessary data copying in the user space,
        allowing the caller to rely on the vectorized IO API instead (sendmsg).
        """
        header = self._HEADER_FORMAT.pack(
            self._VERSION, int(self.priority), self.source_node_id, self.index | ((1 << 31) if self.end_of_transfer else 0), self.transfer_id
        )
        return memoryview(header), self.payload

    @staticmethod
    def parse(image: memoryview) -> typing.Optional[UDPFrame]:
        try:
            version, int_priority, source_node_id, frame_index_eot, transfer_id = UDPFrame._HEADER_FORMAT.unpack_from(image)
        except struct.error:
            return None
        if version == UDPFrame._VERSION:
            # noinspection PyArgumentList
            return UDPFrame(
                priority=pycyphal.transport.Priority(int_priority),
                source_node_id = source_node_id,
                transfer_id=transfer_id,
                index=(frame_index_eot & UDPFrame.INDEX_MASK),
                end_of_transfer=bool(frame_index_eot & (UDPFrame.INDEX_MASK + 1)),
                payload=image[UDPFrame._HEADER_FORMAT.size :],
            )
        return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_frame_compile() -> None:
    from pycyphal.transport import Priority
    from pytest import raises

    _ = UDPFrame(priority=Priority.LOW, source_node_id=1, transfer_id=0, index=0, end_of_transfer=False, payload=memoryview(b""))

    # Invalid transfer_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW, source_node_id=1, transfer_id=2**64, index=0, end_of_transfer=False, payload=memoryview(b"")
        )

    # Invalid frame index
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW, source_node_id=1, transfer_id=0, index=2**31, end_of_transfer=False, payload=memoryview(b"")
        )
    
    # Invalid source_node_id
    with raises(ValueError):
        _ = UDPFrame(
            priority=Priority.LOW, source_node_id=2**16, transfer_id=0, index=0, end_of_transfer=False, payload=memoryview(b"")
        )

    # Multi-frame, not the end of the transfer.
    assert (
        memoryview(
            b"\x01\x06\x01\x00"
            b"\r\xf0\xdd\x00"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.SLOW,
        source_node_id=1,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_0DD_F00D,
        end_of_transfer=False,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # Multi-frame, end of the transfer.
    assert (
        memoryview(
            b"\x01\x07\x02\x00"
            b"\r\xf0\xdd\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.OPTIONAL,
        source_node_id=2,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_0DD_F00D,
        end_of_transfer=True,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()

    # Single-frame.
    assert (
        memoryview(
            b"\x01\x00\xff\xff"
            b"\x00\x00\x00\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        ),
        memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame(
        priority=Priority.EXCEPTIONAL,
        source_node_id=2**16-1,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ).compile_header_and_payload()


def _unittest_udp_frame_parse() -> None:
    from pycyphal.transport import Priority

    for size in range(16):
        assert None is UDPFrame.parse(memoryview(bytes(range(size))))

    # Multi-frame, not the end of the transfer.
    assert UDPFrame(
        priority=Priority.SLOW,
        source_node_id=4,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_0DD_F00D,
        end_of_transfer=False,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01\x06\x04\x00"
            b"\r\xf0\xdd\x00"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
            b"Well, I got here the same way the coin did."
        ),
    )

    # Multi-frame, end of the transfer.
    assert UDPFrame(
        priority=Priority.OPTIONAL,
        source_node_id=5,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0x_0DD_F00D,
        end_of_transfer=True,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01\x07\x05\x00"
            b"\r\xf0\xdd\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
            b"Well, I got here the same way the coin did."
        ),
    )

    # Single-frame.
    assert UDPFrame(
        priority=Priority.EXCEPTIONAL,
        source_node_id=6,
        transfer_id=0x_DEAD_BEEF_C0FFEE,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b"Well, I got here the same way the coin did."),
    ) == UDPFrame.parse(
        memoryview(
            b"\x01\x00\x06\x00"
            b"\x00\x00\x00\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
            b"Well, I got here the same way the coin did."
        ),
    )

    # Too short.
    assert None is UDPFrame.parse(
        memoryview(
            b"\x01\x07\x00\x00"
            b"\r\xf0\xdd\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        )[:-1],
    )
    # Bad version.
    assert None is UDPFrame.parse(
        memoryview(
            b"\x02\x07\x00\x00"
            b"\r\xf0\xdd\x80"
            b"\xee\xff\xc0\xef\xbe\xad\xde\x00"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        ),
    )
