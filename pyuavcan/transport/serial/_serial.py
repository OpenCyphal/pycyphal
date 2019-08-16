#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import threading
import concurrent.futures

import serial

import pyuavcan.transport
from ._frame import Frame, TimestampedFrame
from ._stream_parser import StreamParser


_SERIAL_PORT_READ_TIMEOUT = 1.0
_MAX_RECEIVE_PAYLOAD_SIZE_BYTES = 1024 * 100


_logger = logging.getLogger(__name__)


class SerialTransport(pyuavcan.transport.Transport):
    """
    The serial transport is experimental and is not yet part of the UAVCAN specification.
    Future revisions may break wire compatibility until the transport is formally specified.
    Context: https://forum.uavcan.org/t/alternative-transport-protocols/324, also see the discussion at
    https://forum.uavcan.org/t/yukon-design-megathread/390/115?u=pavel.kirienko.

    This transport is not yet implemented. Please come back later.

    The serial transport is designed for basic raw byte-level low-speed serial links:

    - UART, RS-232/485/422 (the recommended baud rates are: 115200, 921600, 3'000'000).
    - USB CDC ACM.

    It is also suitable for raw transport log storage, because one-dimensional flat binary files are structurally
    similar to serial byte-level links.

    The packet header is defined as follows (byte and bit ordering follow the DSDL specification:
    least significant byte first, most significant bit first)::

        uint8   version                 # Always zero. Discard the frame if not.
        uint8   priority                # Like IEEE 802.15.4, three most significant bits: 0 = highest, 7 = lowest.
        uint16  source node ID          # 0xFFFF = anonymous.
        uint16  destination node ID     # 0xFFFF = broadcast.
        uint16  data specifier          # Like IEEE 802.15.4.

        uint64  data type hash
        uint64  transfer ID

        uint32  frame index EOT         # Like IEEE 802.15.4; MSB set if last frame of the transfer.
        void32                          # Set to zero when sending, ignore when receiving.

    For message frames, the data specifier field contains the subject-ID value,
    so that the most significant bit is always cleared.
    For service frames, the most significant bit (15th) is always set,
    and the second-to-most-significant bit (14th) is set for response transfers only;
    the remaining 14 least significant bits contain the service-ID value.

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

    There are no magic bytes in this format because the strong CRC and the data type hash field render the
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
        serial_port:                                  typing.Union[str, serial.SerialBase],
        single_frame_transfer_payload_capacity_bytes: int = DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES,
        service_transfer_multiplier:                  int = 1,
        loop:                                         typing.Optional[asyncio.AbstractEventLoop] = None
    ):
        """
        :param serial_port: The serial port instance to communicate over, or its name.
            In the latter case, the port will be constructed via :func:`serial.serial_for_url`.
            The new instance takes ownership of the port; when the instance is closed, its port will also be closed.

        :param single_frame_transfer_payload_capacity_bytes: Use single-frame transfers for all outgoing transfers
            containing not more than than this many bytes of payload. Otherwise, use multi-frame transfers.
            This setting does not affect transfer reception (any payload size is always accepted). Defaults to
            :attr:`DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES`.

        :param service_transfer_multiplier: Specifies the number of times each outgoing service transfer will be
            repeated. The duplicates are emitted subsequently immediately following the original. This feature
            can be used to reduce the likelihood of service transfer loss over unreliable links. Assuming that
            the probability of transfer loss ``P`` is time-invariant, the influence of the multiplier ``M`` can
            be approximately modeled as ``P' = P^M``. For example, given a link that successfully delivers 90%
            of transfers, and the probabilities of adjacent transfer loss are uncorrelated, the multiplication
            factor of 2 can increase the link reliability up to ``100% - (100% - 90%)^2 = 99%``. Removal of
            duplicate transfers at the opposite end of the link is natively guaranteed by the UAVCAN protocol;
            no special activities are needed there (read the UAVCAN Specification for background). This setting
            does not affect message transfers.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._sft_payload_capacity_bytes = int(single_frame_transfer_payload_capacity_bytes)
        self._service_transfer_multiplier = int(service_transfer_multiplier)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        if self._service_transfer_multiplier < 1:
            raise ValueError(f'Invalid service transfer multiplier: {self._service_transfer_multiplier}')

        self._port_lock = asyncio.Lock(loop=loop)
        self._local_node_id: typing.Optional[int] = None

        # The serialization buffer is pre-allocated for performance reasons;
        # it is needed to store frame contents before they are emitted into the serial port.
        self._serialization_buffer = bytearray(0 for _ in range(self._sft_payload_capacity_bytes * 3))

        if not isinstance(serial_port, serial.SerialBase):
            serial_port = serial.serial_for_url(serial_port)
        assert isinstance(serial_port, serial.SerialBase)
        if not serial_port.is_open:
            raise pyuavcan.transport.InvalidMediaConfigurationError('The serial port instance is not open')
        serial_port.timeout = _SERIAL_PORT_READ_TIMEOUT
        self._serial_port = serial_port

        self._background_executor = concurrent.futures.ThreadPoolExecutor()

        self._reader_thread = threading.Thread(target=self._reader_thread_func, daemon=True)
        self._reader_thread.start()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=Frame.TRANSFER_ID_MASK + 1,
            node_id_set_cardinality=Frame.NODE_ID_MASK,  # The last one is reserved for anonymous, so 4095
            single_frame_transfer_payload_capacity_bytes=self._sft_payload_capacity_bytes
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    def set_local_node_id(self, node_id: int) -> None:
        if self._local_node_id is None:
            if 0 <= node_id < self.protocol_parameters.node_id_set_cardinality:
                self._local_node_id = int(node_id)
            else:
                raise ValueError(f'Invalid node ID for serial: {node_id}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    def close(self) -> None:
        # TODO: close sessions
        if self._serial_port.is_open:
            self._serial_port.close()

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

    @property
    def descriptor(self) -> str:
        return \
            f'<serial baudrate="{self._serial_port.baudrate}" sft_capacity="{self._sft_payload_capacity_bytes}" ' \
            f'srv_mult="{self._service_transfer_multiplier}">{self._serial_port.name}</serial>'

    @property
    def serial_port(self) -> serial.SerialBase:
        assert isinstance(self._serial_port, serial.SerialBase)
        return self._serial_port

    def _handle_received_frame(self, frame: TimestampedFrame) -> None:
        pass

    @staticmethod
    def _handle_received_unparsed_data(data: memoryview) -> None:
        printable: typing.Union[str, bytes] = bytes(data)
        try:
            printable = printable.decode('utf8')
        except ValueError:
            pass
        _logger.warning('Unparsed data: %s', printable)

    async def _send_transfer(self, frames: typing.Iterable[Frame], monotonic_deadline: float) \
            -> typing.Optional[pyuavcan.transport.Timestamp]:
        """
        Emits the frames belonging to the same transfer, returns the first frame transmission timestamp.
        The returned timestamp can be used for transfer feedback implementation.
        Aborts if the frames cannot be emitted before the deadline or if a write call fails.
        :returns: The first frame transmission timestamp if all frames are sent successfully.
            None on timeout or on write failure.
        """
        tx_ts: typing.Optional[pyuavcan.transport.Timestamp] = None
        for fr in frames:
            compiled = fr.compile_into(self._serialization_buffer)
            with self._port_lock:       # TODO: the lock acquisition should be prioritized by frame priority!
                timeout = monotonic_deadline - self._loop.time()
                if timeout <= 0:
                    return None    # Timed out
                self._serial_port.write_timeout = timeout
                num_written = await self._loop.run_in_executor(self._background_executor,
                                                               self._serial_port.write,
                                                               compiled)
                tx_ts = tx_ts or pyuavcan.transport.Timestamp.now()

            num_written = len(compiled) if num_written is None else num_written
            if num_written < len(compiled):
                return None    # Write failed

        assert tx_ts is not None
        return tx_ts

    def _reader_thread_func(self) -> None:
        def callback(item: typing.Union[TimestampedFrame, memoryview]) -> None:
            if isinstance(item, TimestampedFrame):
                handler = self._handle_received_frame
            elif isinstance(item, memoryview):
                handler = self._handle_received_unparsed_data
            else:
                assert False
            self._loop.call_soon_threadsafe(handler, item)

        try:
            parser = StreamParser(callback, _MAX_RECEIVE_PAYLOAD_SIZE_BYTES)
            assert abs(self._serial_port.timeout - _SERIAL_PORT_READ_TIMEOUT) < 0.1
            while self._serial_port.is_open:
                chunk = self._serial_port.read(max(1, self._serial_port.inWaiting()))
                timestamp = pyuavcan.transport.Timestamp.now()
                parser.process_next_chunk(chunk, timestamp)
        except Exception as ex:
            _logger.exception('Reader thread has failed, the instance will be terminated: %s', ex)
            self._serial_port.close()
