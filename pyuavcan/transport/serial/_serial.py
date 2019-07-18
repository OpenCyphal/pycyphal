#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio

import serial

import pyuavcan.transport


# Same value represents broadcast node ID when transmitting.
_ANONYMOUS_NODE_ID = 0xFFFF

_FRAME_DELIMITER_BYTE = 0x9E
_ESCAPE_PREFIX_BYTE = 0x8E


class SerialTransport(pyuavcan.transport.Transport):
    """
    The serial transport is experimental and is not yet part of the UAVCAN specification.
    Future revisions may break wire compatibility until the transport is formally specified.
    Context: https://forum.uavcan.org/t/alternative-transport-protocols/324, also see the discussion at
    https://forum.uavcan.org/t/yukon-design-megathread/390/115?u=pavel.kirienko.

    This transport is not yet implemented. Please come back later.

    The serial transport is designed for basic raw byte-level low-speed serial links:

    - UART, RS-232/485/422 (the recommended rates are [baud]: 115200, 921600, 3'000'000).
    - USB CDC ACM.

    It is also suitable for raw transport log storage, because one-dimensional flat binary files are structurally
    similar to serial byte-level links.

    The packet header is defined as follows (byte and bit ordering follow the DSDL specification (least
    significant byte first, most significant bit first))::

        uint8   version                 # Always zero. Discard the frame if not.
        uint8   priority                # Like IEEE 802.15.4, three most significant bits: 0 = highest, 7 = lowest.
        uint16  source node ID          # 0xFFFF = anonymous.
        uint16  destination node ID     # 0xFFFF = broadcast.
        uint16  data specifier          # Like IEEE 802.15.4.

        uint64  data type hash
        uint64  transfer ID

        uint32  frame index EOT         # Like IEEE 802.15.4; MSB set if last frame of the transfer.
        void32

    Total header size: 32 bytes (256 bits).

    The header is prepended before the frame payload; the resulting structure is
    encoded into its serialized form using the following packet format (influenced by HDLC, SLIP, POPCOP):

    +------------------------+-----------------------+-----------------------+------------------------+
    |Frame delimiter **0x9E**|Escaped header+payload |CRC32C (Castagnoli)    |Frame delimiter **0x9E**|
    +========================+=======================+=======================+========================+
    |Single-byte frame       |The following bytes are|Four bytes long,       |Same frame delimiter as |
    |delimiter **0x9E**.     |escaped: **0x9E**      |little-endian byte     |at the start.           |
    |Begins a new frame and  |(frame delimiter);     |order; bytes 0x9E      |Terminates the current  |
    |possibly terminates the |**0x8E** (escape       |(frame delimiter) and  |frame and possibly      |
    |previous frame.         |character). An escaped |0x8E (escape character)|begins the next frame.  |
    |                        |byte is bitwise        |are escaped like in    |                        |
    |                        |inverted and prepended |the payload.           |                        |
    |                        |with the escape        |The CRC is computed    |                        |
    |                        |character 0x8E. For    |over the unescaped     |                        |
    |                        |example: byte 0x9E is  |(i.e., original form)  |                        |
    |                        |transformed into 0x8E  |payload, not including |                        |
    |                        |followed by 0x71.      |the start delimiter.   |                        |
    +------------------------+-----------------------+-----------------------+------------------------+

    There are no magic bytes in this format because the strong CRC and the compact-data-type-ID field render the
    format sufficiently recognizable. The worst case overhead exceeds 100% if every byte of the payload and the CRC
    is either 0x9E or 0x8E. Despite the overhead, this format is still considered superior to the alternatives
    since it is robust and guarantees a constant recovery time. Consistent-overhead byte stuffing (COBS) is sometimes
    employed for similar tasks, but it should be understood that while it offers a substantially lower overhead,
    it undermines the synchronization recovery properties of the protocol. There is a somewhat relevant discussion
    at https://github.com/vedderb/bldc/issues/79.

    The format can share the same serial medium with ASCII text exchanges such as command-line interfaces or
    real-time logging. The special byte values employed by the format do not belong to the ASCII character set.

    The last four bytes of a multi-frame transfer payload contain the CRC32C (Castagnoli) hash of the transfer
    payload in little-endian byte order.
    """

    DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES = 512

    def __init__(
        self,
        serial_port:                                  serial.SerialBase,
        single_frame_transfer_payload_capacity_bytes: int = DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES,
        loop:                                         typing.Optional[asyncio.AbstractEventLoop] = None
    ):
        """
        :param serial_port: The serial port to communicate over. The caller may configure the transmit timeout as
            necessary. On timeout, :class:`pyuavcan.transport.SendTimeoutError` will be raised.

        :param single_frame_transfer_payload_capacity_bytes: Use single-frame transfers for all outgoing transfers
            containing not more than than this many bytes of payload. Otherwise, use multi-frame transfers.
            This setting does not affect transfer reception (any payload size is always accepted). Defaults to
            :attr:`DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES`.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._port = serial_port
        self._sft_payload_capacity_bytes = int(single_frame_transfer_payload_capacity_bytes)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=2 ** 64,
            node_id_set_cardinality=4096,
            single_frame_transfer_payload_capacity_bytes=self._sft_payload_capacity_bytes
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        raise NotImplementedError

    def set_local_node_id(self, node_id: int) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def get_input_session(self,
                          specifier:        pyuavcan.transport.SessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> pyuavcan.transport.InputSession:
        raise NotImplementedError

    def get_output_session(self,
                           specifier:        pyuavcan.transport.SessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> pyuavcan.transport.OutputSession:
        raise NotImplementedError

    @property
    def input_sessions(self) -> typing.Sequence[pyuavcan.transport.InputSession]:
        raise NotImplementedError

    @property
    def output_sessions(self) -> typing.Sequence[pyuavcan.transport.OutputSession]:
        raise NotImplementedError
