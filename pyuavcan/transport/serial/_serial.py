#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import copy
import typing
import asyncio
import logging
import threading
import dataclasses
import concurrent.futures

import serial

import pyuavcan.transport
from ._frame import SerialFrame
from ._stream_parser import StreamParser
from ._session import SerialOutputSession, SerialInputSession


_SERIAL_PORT_READ_TIMEOUT = 1.0


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SerialTransportStatistics(pyuavcan.transport.TransportStatistics):
    in_bytes:             int = 0
    in_frames:            int = 0
    in_out_of_band_bytes: int = 0

    out_bytes:      int = 0
    out_frames:     int = 0
    out_transfers:  int = 0
    out_incomplete: int = 0


class SerialTransport(pyuavcan.transport.Transport):
    """
    The serial transport is experimental and is not yet part of the UAVCAN specification.
    Future revisions may break wire compatibility until the transport is formally specified.
    Context: https://forum.uavcan.org/t/alternative-transport-protocols/324, also see the discussion at
    https://forum.uavcan.org/t/yukon-design-megathread/390/115?u=pavel.kirienko.

    The serial transport is designed for basic raw byte-level low-speed serial links:

    - UART, RS-232/485/422 (the recommended baud rates are: 115200, 921600, 3'000'000).
    - USB CDC ACM.

    It is also suitable for raw transport log storage, because one-dimensional flat binary files are structurally
    similar to serial byte-level links.

    The packet header is defined as follows (byte and bit ordering follow the DSDL specification:
    least significant byte first, most significant bit first)::

        uint8   version                 # Always zero. Discard the frame if not.
        uint8   priority                # 0 = highest, 7 = lowest; the rest are unused.
        uint16  source node ID          # 0xFFFF = anonymous.
        uint16  destination node ID     # 0xFFFF = broadcast.
        uint16  data specifier

        uint64  data type hash
        uint64  transfer ID

        uint32  frame index EOT         # MSB set if last frame of the transfer.
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

    The serial transport supports all transfer categories:

    +--------------------+--------------------------+---------------------------+
    | Supported transfers| Unicast                  | Broadcast                 |
    +====================+==========================+===========================+
    |**Message**         | Yes                      | Yes                       |
    +-----------+--------+--------------------------+---------------------------+
    |           |Request | Yes                      | Yes                       |
    |**Service**+--------+--------------------------+---------------------------+
    |           |Response| Yes                      | Banned by Specification   |
    +-----------+--------+--------------------------+---------------------------+
    """

    DEFAULT_SERVICE_TRANSFER_MULTIPLIER = 2
    DEFAULT_MTU = 1024

    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)
    VALID_MTU_RANGE = (1024, 1024 * 10)

    def __init__(self,
                 serial_port:                 typing.Union[str, serial.SerialBase],
                 service_transfer_multiplier: int = DEFAULT_SERVICE_TRANSFER_MULTIPLIER,
                 mtu:                         int = DEFAULT_MTU,
                 loop:                        typing.Optional[asyncio.AbstractEventLoop] = None):
        """
        :param serial_port: The serial port instance to communicate over, or its name.
            In the latter case, the port will be constructed via :func:`serial.serial_for_url`
            (refer to the PySerial docs for the background).
            The new instance takes ownership of the port; when the instance is closed, its port will also be closed.

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

        :param mtu: Use single-frame transfers for all outgoing transfers containing not more than than
            this many bytes of payload. Otherwise, use multi-frame transfers.
            This setting does not affect transfer reception; the RX MTU is hard-coded as ``max(VALID_MTU_RANGE)``.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._service_transfer_multiplier = int(service_transfer_multiplier)
        self._mtu = int(mtu)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._service_transfer_multiplier <= high):
            raise ValueError(f'Invalid service transfer multiplier: {self._service_transfer_multiplier}')

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f'Invalid MTU: {self._mtu} bytes')

        self._local_node_id: typing.Optional[int] = None

        # At first I tried using serial.is_open, but unfortunately that doesn't work reliably because the close()
        # method on most serial port classes is non-atomic, which causes all sorts of weird race conditions
        # and spurious errors in the reader thread (at least). A simple explicit flag is reliable.
        self._closed = False

        # For serial port write serialization. Read operations are performed concurrently (no sync) in separate thread.
        self._port_lock = asyncio.Lock(loop=loop)

        # The serialization buffer is pre-allocated for performance reasons;
        # it is needed to store frame contents before they are emitted into the serial port.
        # Access must be protected with the port lock!
        self._serialization_buffer = bytearray(0 for _ in range(self._mtu * 3))

        self._input_registry: typing.Dict[pyuavcan.transport.InputSessionSpecifier, SerialInputSession] = {}
        self._output_registry: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, SerialOutputSession] = {}

        self._statistics = SerialTransportStatistics()

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
            transfer_id_modulo=SerialFrame.TRANSFER_ID_MASK + 1,
            max_nodes=len(SerialFrame.NODE_ID_RANGE),
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    def set_local_node_id(self, node_id: int) -> None:
        if self._local_node_id is None:
            if 0 <= node_id < self.protocol_parameters.max_nodes:
                self._ensure_not_closed()
                self._local_node_id = int(node_id)
            else:
                raise ValueError(f'Invalid node ID for serial: {node_id}')
        else:
            raise pyuavcan.transport.InvalidTransportConfigurationError('Node ID can be assigned only once')

    def close(self) -> None:
        self._closed = True
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s: Failed to close session %r: %s', self, s, ex)

        if self._serial_port.is_open:  # Double-close is not an error.
            self._serial_port.close()

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> SerialInputSession:
        def finalizer() -> None:
            del self._input_registry[specifier]

        self._ensure_not_closed()
        try:
            out = self._input_registry[specifier]
        except LookupError:
            out = SerialInputSession(specifier=specifier,
                                     payload_metadata=payload_metadata,
                                     loop=self._loop,
                                     finalizer=finalizer)
            self._input_registry[specifier] = out

        assert isinstance(out, SerialInputSession)
        assert specifier in self._input_registry
        assert out.specifier == specifier
        return out

    def get_output_session(self,
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> SerialOutputSession:
        self._ensure_not_closed()
        if specifier not in self._output_registry:
            def finalizer() -> None:
                del self._output_registry[specifier]

            if isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier) \
                    and self._service_transfer_multiplier > 1:
                async def send_transfer(frames: typing.Iterable[SerialFrame],
                                        monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.Timestamp]:
                    frames = list(frames)
                    first_tx_ts: typing.Optional[pyuavcan.transport.Timestamp] = None
                    for _ in range(self._service_transfer_multiplier):  # pragma: no branch
                        ts = await self._send_transfer(frames, monotonic_deadline)
                        first_tx_ts = first_tx_ts or ts
                    return first_tx_ts
            else:
                send_transfer = self._send_transfer

            self._output_registry[specifier] = SerialOutputSession(
                specifier=specifier,
                payload_metadata=payload_metadata,
                mtu=self._mtu,
                local_node_id_accessor=lambda: self._local_node_id,
                send_handler=send_transfer,
                finalizer=finalizer
            )

        out = self._output_registry[specifier]
        assert isinstance(out, SerialOutputSession)
        assert out.specifier == specifier
        return out

    @property
    def input_sessions(self) -> typing.Sequence[SerialInputSession]:
        return list(self._input_registry.values())

    @property
    def output_sessions(self) -> typing.Sequence[SerialOutputSession]:
        return list(self._output_registry.values())

    @property
    def descriptor(self) -> str:
        return \
            f'<serial baudrate="{self._serial_port.baudrate}" mtu="{self._mtu}" ' \
            f'srv_mult="{self._service_transfer_multiplier}">{self._serial_port.name}</serial>'

    @property
    def serial_port(self) -> serial.SerialBase:
        assert isinstance(self._serial_port, serial.SerialBase)
        return self._serial_port

    def sample_statistics(self) -> SerialTransportStatistics:
        return copy.copy(self._statistics)

    def _handle_received_frame(self, frame: SerialFrame) -> None:
        self._statistics.in_frames += 1
        if frame.destination_node_id in (self._local_node_id, None):
            for source_node_id in {None, frame.source_node_id}:
                ss = pyuavcan.transport.InputSessionSpecifier(frame.data_specifier, source_node_id)
                try:
                    session = self._input_registry[ss]
                except LookupError:
                    pass
                else:
                    # noinspection PyProtectedMember
                    session._process_frame(frame)

    def _handle_received_out_of_band_data(self, data: memoryview) -> None:
        self._statistics.in_out_of_band_bytes += len(data)
        printable: typing.Union[str, bytes] = bytes(data)
        try:
            assert isinstance(printable, bytes)
            printable = printable.decode('utf8')
        except ValueError:
            pass
        _logger.warning('%s: Out-of-band: %s', self._serial_port.name, printable)

    def _handle_received_item_and_update_stats(self,
                                               item:           typing.Union[SerialFrame, memoryview],
                                               in_bytes_count: int) -> None:
        if isinstance(item, SerialFrame):
            self._handle_received_frame(item)
        elif isinstance(item, memoryview):
            self._handle_received_out_of_band_data(item)
        else:
            assert False

        assert self._statistics.in_bytes <= in_bytes_count
        self._statistics.in_bytes = int(in_bytes_count)

    async def _send_transfer(self, frames: typing.Iterable[SerialFrame], monotonic_deadline: float) \
            -> typing.Optional[pyuavcan.transport.Timestamp]:
        """
        Emits the frames belonging to the same transfer, returns the first frame transmission timestamp.
        The returned timestamp can be used for transfer feedback implementation.
        Aborts if the frames cannot be emitted before the deadline or if a write call fails.
        :returns: The first frame transmission timestamp if all frames are sent successfully.
            None on timeout or on write failure.
        """
        tx_ts: typing.Optional[pyuavcan.transport.Timestamp] = None
        self._ensure_not_closed()
        try:  # Jeez this is getting complex
            for fr in frames:
                async with self._port_lock:       # TODO: the lock acquisition should be prioritized by frame priority!
                    compiled = fr.compile_into(self._serialization_buffer)
                    timeout = monotonic_deadline - self._loop.time()
                    if timeout > 0:
                        self._serial_port.write_timeout = timeout
                        try:
                            num_written = await self._loop.run_in_executor(self._background_executor,
                                                                           self._serial_port.write,
                                                                           compiled)
                            tx_ts = tx_ts or pyuavcan.transport.Timestamp.now()
                        except serial.SerialTimeoutException:
                            num_written = 0
                            _logger.info('%s: Port write timed out in %.3fs on frame %r', self, timeout, fr)
                        self._statistics.out_bytes += num_written or 0
                    else:
                        tx_ts = None  # Timed out
                        break

                num_written = len(compiled) if num_written is None else num_written
                if num_written < len(compiled):
                    tx_ts = None  # Write failed
                    break

                self._statistics.out_frames += 1
        except Exception as ex:
            if self._closed:
                raise pyuavcan.transport.ResourceClosedError(f'{self} is closed, transmission aborted.') from ex
            else:
                raise
        else:
            if tx_ts is not None:
                self._statistics.out_transfers += 1
            else:
                self._statistics.out_incomplete += 1
            return tx_ts

    def _reader_thread_func(self) -> None:
        in_bytes_count = 0

        def callback(item: typing.Union[SerialFrame, memoryview]) -> None:
            self._loop.call_soon_threadsafe(self._handle_received_item_and_update_stats, item, in_bytes_count)

        try:
            parser = StreamParser(callback, max(self.VALID_MTU_RANGE))
            assert abs(self._serial_port.timeout - _SERIAL_PORT_READ_TIMEOUT) < 0.1

            while not self._closed and self._serial_port.is_open:
                chunk = self._serial_port.read(max(1, self._serial_port.inWaiting()))
                timestamp = pyuavcan.transport.Timestamp.now()
                in_bytes_count += len(chunk)
                parser.process_next_chunk(chunk, timestamp)

        except Exception as ex:  # pragma: no cover
            if self._closed or not self._serial_port.is_open:
                _logger.debug('%s: The serial port is closed, exception ignored: %r', self, ex)
            else:
                _logger.exception('%s: Reader thread has failed, the instance with port %s will be terminated: %s',
                                  self, self._serial_port, ex)
            self._closed = True
            self._serial_port.close()

        finally:
            _logger.debug('%s: Reader thread is exiting. Head aega.', self)

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')
