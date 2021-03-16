# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import copy
import typing
import asyncio
import logging
import threading
import dataclasses
import concurrent.futures
import serial
import pyuavcan.transport
from pyuavcan.transport import Timestamp
from ._frame import SerialFrame
from ._stream_parser import StreamParser
from ._session import SerialOutputSession, SerialInputSession
from ._tracer import SerialCapture, SerialTracer


_SERIAL_PORT_READ_TIMEOUT = 1.0


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SerialTransportStatistics(pyuavcan.transport.TransportStatistics):
    in_bytes: int = 0
    in_frames: int = 0
    in_out_of_band_bytes: int = 0

    out_bytes: int = 0
    out_frames: int = 0
    out_transfers: int = 0
    out_incomplete: int = 0


class SerialTransport(pyuavcan.transport.Transport):
    """
    The UAVCAN/Serial transport is designed for OSI L1 byte-level serial links and tunnels,
    such as UART, RS-422/485/232 (duplex), USB CDC ACM, TCP/IP, etc.
    Please read the module documentation for details.
    """

    TRANSFER_ID_MODULO = SerialFrame.TRANSFER_ID_MASK + 1

    VALID_MTU_RANGE = (1024, 1024 ** 3)
    """
    The maximum MTU is practically unlimited, and it is also the default MTU.
    This is by design to ensure that all frames are single-frame transfers.
    Compliant implementations of the serial transport do not have to support multi-frame transfers,
    which removes the greatest chunk of complexity from the protocol.
    """

    DEFAULT_SERVICE_TRANSFER_MULTIPLIER = 2
    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)

    def __init__(
        self,
        serial_port: typing.Union[str, serial.SerialBase],
        local_node_id: typing.Optional[int],
        *,
        mtu: int = max(VALID_MTU_RANGE),
        service_transfer_multiplier: int = DEFAULT_SERVICE_TRANSFER_MULTIPLIER,
        baudrate: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        """
        :param serial_port: The serial port instance to communicate over, or its name.
            In the latter case, the port will be constructed via :func:`serial.serial_for_url`
            (refer to the PySerial docs for the background).
            The new instance takes ownership of the port; when the instance is closed, its port will also be closed.
            Examples:

            - ``/dev/ttyACM0`` -- a regular serial port on GNU/Linux (USB CDC ACM in this example).
            - ``COM9`` -- likewise, on Windows.
            - ``/dev/serial/by-id/usb-Black_Sphere_Technologies_Black_Magic_Probe_B5DCABF5-if02`` -- a regular
              USB CDC ACM port referenced by the device name and ID (GNU/Linux).
            - ``hwgrep:///dev/serial/by-id/*Black_Magic_Probe*-if02`` -- glob instead of exact name.
            - ``socket://127.0.0.1:50905`` -- a TCP/IP tunnel instead of a physical port.
            - ``spy://COM3?file=dump.txt`` -- open a regular port and dump all data exchange into a text file.

            Read the PySerial docs for more info.

        :param local_node_id: The node-ID to use. Can't be changed after initialization.
            None means that the transport will operate in the anonymous mode.

        :param mtu: Use single-frame transfers for all outgoing transfers containing not more than than
            this many bytes of payload. Otherwise, use multi-frame transfers.

            By default, the MTU is virtually unlimited (to be precise, it is set to a very large number that
            is unattainable in practice), meaning that all transfers will be single-frame transfers.
            Such behavior is optimal for the serial transport because it does not have native framing
            and as such it supports frames of arbitrary sizes. Implementations may omit the support for
            multi-frame transfers completely, which removes the greatest chunk of complexity from the protocol.

            This setting does not affect transfer reception -- the RX MTU is always set to the maximum valid MTU
            (i.e., practically unlimited).

        :param service_transfer_multiplier: Deterministic data loss mitigation for service transfers.
            This parameter specifies the number of times each outgoing service transfer will be repeated.
            This setting does not affect message transfers.

        :param baudrate: If not None, the specified baud rate will be configured on the serial port.
            Otherwise, the baudrate will be left unchanged.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        self._service_transfer_multiplier = int(service_transfer_multiplier)
        self._mtu = int(mtu)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._service_transfer_multiplier <= high):
            raise ValueError(f"Invalid service transfer multiplier: {self._service_transfer_multiplier}")

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f"Invalid MTU: {self._mtu} bytes")

        self._local_node_id = int(local_node_id) if local_node_id is not None else None
        if self._local_node_id is not None and not (0 <= self._local_node_id < self.protocol_parameters.max_nodes):
            raise ValueError(f"Invalid node ID for serial: {self._local_node_id}")

        # At first I tried using serial.is_open, but unfortunately that doesn't work reliably because the close()
        # method on most serial port classes is non-atomic, which causes all sorts of weird race conditions
        # and spurious errors in the reader thread (at least). A simple explicit flag is reliable.
        self._closed = False

        # For serial port write serialization. Read operations are performed concurrently (no sync) in separate thread.
        self._port_lock = asyncio.Lock()

        # The serialization buffer is re-used for performance reasons; it is needed to store frame contents before
        # they are emitted into the serial port. It may grow as necessary at runtime; the initial size is a guess.
        # Access must be protected with the port lock!
        self._serialization_buffer = bytearray(b"\x00" * (1024 * 1024))

        self._input_registry: typing.Dict[pyuavcan.transport.InputSessionSpecifier, SerialInputSession] = {}
        self._output_registry: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, SerialOutputSession] = {}

        self._capture_handlers: typing.List[pyuavcan.transport.CaptureCallback] = []

        self._statistics = SerialTransportStatistics()

        if not isinstance(serial_port, serial.SerialBase):
            serial_port = serial.serial_for_url(serial_port)
        assert isinstance(serial_port, serial.SerialBase)
        if not serial_port.is_open:
            raise pyuavcan.transport.InvalidMediaConfigurationError("The serial port instance is not open")
        serial_port.timeout = _SERIAL_PORT_READ_TIMEOUT
        self._serial_port = serial_port
        if baudrate is not None:
            self._serial_port.baudrate = int(baudrate)

        self._background_executor = concurrent.futures.ThreadPoolExecutor()

        self._reader_thread = threading.Thread(target=self._reader_thread_func, daemon=True)
        self._reader_thread.start()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=self.TRANSFER_ID_MODULO,
            max_nodes=len(SerialFrame.NODE_ID_RANGE),
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    def close(self) -> None:
        self._closed = True
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception("%s: Failed to close session %r: %s", self, s, ex)

        if self._serial_port.is_open:  # Double-close is not an error.
            self._serial_port.close()

    def get_input_session(
        self, specifier: pyuavcan.transport.InputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> SerialInputSession:
        def finalizer() -> None:
            del self._input_registry[specifier]

        self._ensure_not_closed()
        try:
            out = self._input_registry[specifier]
        except LookupError:
            out = SerialInputSession(
                specifier=specifier, payload_metadata=payload_metadata, loop=self._loop, finalizer=finalizer
            )
            self._input_registry[specifier] = out

        assert isinstance(out, SerialInputSession)
        assert specifier in self._input_registry
        assert out.specifier == specifier
        return out

    def get_output_session(
        self, specifier: pyuavcan.transport.OutputSessionSpecifier, payload_metadata: pyuavcan.transport.PayloadMetadata
    ) -> SerialOutputSession:
        self._ensure_not_closed()
        if specifier not in self._output_registry:

            def finalizer() -> None:
                del self._output_registry[specifier]

            if (
                isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier)
                and self._service_transfer_multiplier > 1
            ):

                async def send_transfer(
                    frames: typing.List[SerialFrame], monotonic_deadline: float
                ) -> typing.Optional[Timestamp]:
                    first_tx_ts: typing.Optional[Timestamp] = None
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
                local_node_id=self._local_node_id,
                send_handler=send_transfer,
                finalizer=finalizer,
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
    def serial_port(self) -> serial.SerialBase:
        assert isinstance(self._serial_port, serial.SerialBase)
        return self._serial_port

    def sample_statistics(self) -> SerialTransportStatistics:
        return copy.copy(self._statistics)

    def begin_capture(self, handler: pyuavcan.transport.CaptureCallback) -> None:
        """
        The reported events are of type :class:`SerialCapture`, please read its documentation for details.
        The events may be reported from a different thread (use locks).
        """
        self._capture_handlers.append(handler)

    @property
    def capture_active(self) -> bool:
        return len(self._capture_handlers) > 0

    @staticmethod
    def make_tracer() -> SerialTracer:
        """
        See :class:`SerialTracer`.
        """
        return SerialTracer()

    async def spoof(self, transfer: pyuavcan.transport.AlienTransfer, monotonic_deadline: float) -> bool:
        """
        Spoofing over the serial transport is trivial and it does not involve reconfiguration of the media layer.
        It can be invoked at no cost at any time (unlike, say, UAVCAN/UDP).
        See the overridden method :meth:`pyuavcan.transport.Transport.spoof` for details.

        Notice that if the transport operates over the virtual loopback port ``loop://`` with capture enabled,
        every spoofed frame will be captured twice: one TX, one RX. Same goes for regular transfers.
        """

        ss = transfer.metadata.session_specifier
        src, dst = ss.source_node_id, ss.destination_node_id
        if isinstance(ss.data_specifier, pyuavcan.transport.ServiceDataSpecifier) and (src is None or dst is None):
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                f"Anonymous nodes cannot participate in service calls. Spoof metadata: {transfer.metadata}"
            )

        def construct_frame(index: int, end_of_transfer: bool, payload: memoryview) -> SerialFrame:
            if not end_of_transfer and src is None:
                raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                    f"Anonymous nodes cannot emit multi-frame transfers. Spoof metadata: {transfer.metadata}"
                )
            return SerialFrame(
                priority=transfer.metadata.priority,
                transfer_id=transfer.metadata.transfer_id,
                index=index,
                end_of_transfer=end_of_transfer,
                payload=payload,
                source_node_id=src,
                destination_node_id=dst,
                data_specifier=ss.data_specifier,
            )

        frames = list(
            pyuavcan.transport.commons.high_overhead_transport.serialize_transfer(
                transfer.fragmented_payload, self._mtu, construct_frame
            )
        )
        _logger.debug("%s: Spoofing %s", self, frames)
        return await self._send_transfer(frames, monotonic_deadline) is not None

    def _handle_received_frame(self, timestamp: Timestamp, frame: SerialFrame) -> None:
        self._statistics.in_frames += 1
        if frame.destination_node_id in (self._local_node_id, None):
            for source_node_id in {None, frame.source_node_id}:
                ss = pyuavcan.transport.InputSessionSpecifier(frame.data_specifier, source_node_id)
                try:
                    session = self._input_registry[ss]
                except LookupError:
                    pass
                else:
                    session._process_frame(timestamp, frame)  # pylint: disable=protected-access

    def _handle_received_out_of_band_data(self, timestamp: Timestamp, data: memoryview) -> None:
        self._statistics.in_out_of_band_bytes += len(data)
        printable: typing.Union[str, bytes] = bytes(data)
        try:
            assert isinstance(printable, bytes)
            printable = printable.decode("utf8")
        except ValueError:
            pass
        _logger.warning("%s: Out-of-band received at %s: %r", self._serial_port.name, timestamp, printable)

    def _handle_received_item_and_update_stats(
        self, timestamp: Timestamp, item: typing.Union[SerialFrame, memoryview], in_bytes_count: int
    ) -> None:
        if isinstance(item, SerialFrame):
            self._handle_received_frame(timestamp, item)
        elif isinstance(item, memoryview):
            self._handle_received_out_of_band_data(timestamp, item)
        else:
            assert False

        assert self._statistics.in_bytes <= in_bytes_count
        self._statistics.in_bytes = int(in_bytes_count)

    async def _send_transfer(
        self, frames: typing.List[SerialFrame], monotonic_deadline: float
    ) -> typing.Optional[Timestamp]:
        """
        Emits the frames belonging to the same transfer, returns the first frame transmission timestamp.
        The returned timestamp can be used for transfer feedback implementation.
        Aborts if the frames cannot be emitted before the deadline or if a write call fails.
        :returns: The first frame transmission timestamp if all frames are sent successfully.
            None on timeout or on write failure.
        """
        tx_ts: typing.Optional[Timestamp] = None
        self._ensure_not_closed()
        try:  # Jeez this is getting complex
            num_sent = 0
            for fr in frames:
                async with self._port_lock:  # TODO: the lock acquisition should be prioritized by frame priority!
                    min_buffer_size = len(fr.payload) * 3
                    if len(self._serialization_buffer) < min_buffer_size:
                        _logger.debug(
                            "%s: The serialization buffer is being enlarged from %d to %d bytes",
                            self,
                            len(self._serialization_buffer),
                            min_buffer_size,
                        )
                        self._serialization_buffer = bytearray(0 for _ in range(min_buffer_size))
                    compiled = fr.compile_into(self._serialization_buffer)
                    timeout = monotonic_deadline - self._loop.time()
                    if timeout > 0:
                        self._serial_port.write_timeout = timeout
                        try:
                            num_written = await self._loop.run_in_executor(
                                self._background_executor, self._serial_port.write, compiled
                            )
                            tx_ts = tx_ts or Timestamp.now()
                        except serial.SerialTimeoutException:
                            num_written = 0
                            _logger.info("%s: Port write timed out in %.3fs on frame %r", self, timeout, fr)
                        else:
                            if self._capture_handlers:  # Create a copy to decouple data from the serialization buffer!
                                cap = SerialCapture(tx_ts, memoryview(bytes(compiled)), own=True)
                                pyuavcan.util.broadcast(self._capture_handlers)(cap)
                        self._statistics.out_bytes += num_written or 0
                    else:
                        tx_ts = None  # Timed out
                        break

                num_written = len(compiled) if num_written is None else num_written
                if num_written < len(compiled):
                    tx_ts = None  # Write failed
                    break
                num_sent += 1

            self._statistics.out_frames += num_sent
        except Exception as ex:
            if self._closed:
                raise pyuavcan.transport.ResourceClosedError(f"{self} is closed, transmission aborted.") from ex
            raise
        else:
            if tx_ts is not None:
                self._statistics.out_transfers += 1
            else:
                self._statistics.out_incomplete += 1
            return tx_ts

    def _reader_thread_func(self) -> None:
        in_bytes_count = 0

        def callback(ts: Timestamp, buf: memoryview, frame: typing.Optional[SerialFrame]) -> None:
            item = buf if frame is None else frame
            self._loop.call_soon_threadsafe(self._handle_received_item_and_update_stats, ts, item, in_bytes_count)
            if self._capture_handlers:
                pyuavcan.util.broadcast(self._capture_handlers)(SerialCapture(ts, buf, own=False))

        try:
            parser = StreamParser(callback, max(self.VALID_MTU_RANGE))
            assert abs(self._serial_port.timeout - _SERIAL_PORT_READ_TIMEOUT) < 0.1

            while not self._closed and self._serial_port.is_open:
                chunk = self._serial_port.read(max(1, self._serial_port.inWaiting()))
                chunk_ts = Timestamp.now()
                in_bytes_count += len(chunk)
                parser.process_next_chunk(chunk, chunk_ts)

        except Exception as ex:  # pragma: no cover
            if self._closed or not self._serial_port.is_open:
                _logger.debug("%s: The serial port is closed, exception ignored: %r", self, ex)
            else:
                _logger.exception(
                    "%s: Reader thread has failed, the instance with port %s will be terminated: %s",
                    self,
                    self._serial_port,
                    ex,
                )
            self._closed = True
            self._serial_port.close()

        finally:
            _logger.debug("%s: Reader thread is exiting. Head aega.", self)

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f"{self} is closed")

    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        kwargs = {
            "local_node_id": self.local_node_id,
            "service_transfer_multiplier": self._service_transfer_multiplier,
            "baudrate": self._serial_port.baudrate,
        }
        if self._mtu < max(SerialTransport.VALID_MTU_RANGE):
            kwargs["mtu"] = self._mtu
        return [repr(self._serial_port.name)], kwargs
