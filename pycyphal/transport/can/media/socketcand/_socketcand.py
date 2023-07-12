# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Alex Kiselev <a.kiselev@volz-servos.com>, Pavel Kirienko <pavel@opencyphal.org>

"""
Note: This Media interface functions almost indentically to PythonCANMedia, the only reason
    for a completely different Media type is due to extra two variables, host and port, that none
    of the other PythonCAN compatible interfaces take. In the future, hopefully this Media type
    will be implimented without the use of PythonCAN at all.

"""

from __future__ import annotations
import queue
import time
import typing
import asyncio
import logging
import threading
from functools import partial
import dataclasses
import warnings

import can
from pycyphal.transport import Timestamp, ResourceClosedError, InvalidMediaConfigurationError
from pycyphal.transport.can.media import Media, FilterConfiguration, Envelope, FrameFormat, DataFrame


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class _TxItem:
    msg: can.Message
    timeout: float
    future: asyncio.Future[None]
    loop: asyncio.AbstractEventLoop


@dataclasses.dataclass(frozen=True)
class PythonCANBusOptions:
    hardware_loopback: bool = False
    """
    Hardware loopback support.
        If True, loopback is handled by the supported hardware.
        If False, loopback is emulated with software.
    """
    hardware_timestamp: bool = False
    """
    Hardware timestamp support.
        If True, timestamp returned by the hardware is used.
        If False, approximate timestamp is captured by software.
    """


class SocketcandMedia(Media):
    # pylint: disable=line-too-long
    """
    Media interface adapter for `Socketcand <https://github.com/linux-can/socketcand/tree/master>` using the
    built in interface compatibility from `Python-CAN <https://python-can.readthedocs.io/>`.
    Please refer to the Socketcand documentation and the Python-CAN documentation for information about
    supported hardware, configuration, and installation instructions.

    This media interface supports both Classic CAN and CAN FD. The selection logic is documented below.

    Here is a basic usage example based on the Yakut CLI tool.
    Suppose you have two computers:
    One connected to a CAN capable device and that computer is able to connect and recieve CAN data from the
    CAN device. Using socketcand with a command such as `socketcand -v -i can0 -l 123.123.1.123`
    on this first computer will bind it too a socket (default port for socketcand is 29536, so it is also default here).

    On your second computer:

        export UAVCAN__CAN__IFACE="socketcand:can0:123.123.1.123"
        export UAVCAN__CAN__BITRATE='500000'
        export UAVCAN__CAN__MTU=8
        yakut sub 33:uavcan.si.unit.voltage.scalar

    This will allow you to wirelessly recieve CAN data on computer two through the wired connection on computer 1.

    """

    _MAXIMAL_TIMEOUT_SEC = 0.1

    def __init__(
        self,
        iface_name: str,
        bitrate: typing.Union[int, typing.Tuple[int, int]],
        mtu: typing.Optional[int] = None,
        *,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """
        :param iface_name: Name of CAN interface that remote computer is connected through via the socketcand library:
            This holds the can channel, host ip addr, and port of the socket.
            It must be in the form: "socketcand:[CAN_CHANNEL]:[HOST_IP]:[PORT]"
            or without port: "socketcand:[CAN_CHANNEL]:[HOST_IP]" (default port is 29536).

        :param bitrate: Bit rate value in bauds; either a single integer or a tuple:

            - A single integer selects Classic CAN.
            - A tuple of two selects CAN FD, where the first integer defines the arbitration (nominal) bit rate
              and the second one defines the data phase bit rate.
            - If MTU (see below) is given and is greater than 8 bytes, CAN FD is used regardless of the above.
            - An MTU of 8 bytes and a tuple of two identical bit rates selects Classic CAN.

        :param mtu: The maximum CAN data field size in bytes.
            If provided, this value must belong to :attr:`Media.VALID_MTU_SET`.
            If not provided, the default is determined as follows:

            - If `bitrate` is a single integer: classic CAN is assumed, MTU defaults to 8 bytes.
            - If `bitrate` is two integers: CAN FD is assumed, MTU defaults to 64 bytes.

        :param loop: Deprecated.

        :raises: :class:`InvalidMediaConfigurationError` if the specified media instance
            could not be constructed, the interface name is unknown,
            or if the underlying library raised a :class:`can.CanError`.
        """

        self._conn_name = str(iface_name).split(":")

        if (len(self._conn_name) == 3) or (len(self._conn_name) == 4):
            self._iface = self._conn_name[0]
            self._can_channel = self._conn_name[1]
            self._host = self._conn_name[2]
            if len(self._conn_name) == 4:
                self._port = self._conn_name[3]
            else:
                self._port = 29536
        else:
            raise InvalidMediaConfigurationError(
                f"Interface name {iface_name!r} does not match the format 'interface:channel:host:port' or 'interface:channel:host'"
            )

        if loop:
            warnings.warn("The loop argument is deprecated", DeprecationWarning)

        single_bitrate = isinstance(bitrate, (int, float))
        bitrate = (int(bitrate), int(bitrate)) if single_bitrate else (int(bitrate[0]), int(bitrate[1]))  # type: ignore

        default_mtu = min(self.VALID_MTU_SET) if single_bitrate else 64
        self._mtu = int(mtu) if mtu is not None else default_mtu
        if self._mtu not in self.VALID_MTU_SET:
            raise InvalidMediaConfigurationError(f"Wrong MTU value: {mtu}")

        self._is_fd = (self._mtu > min(self.VALID_MTU_SET) or not single_bitrate) and not (
            self._mtu == min(self.VALID_MTU_SET) and bitrate[0] == bitrate[1]
        )

        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._rx_handler: typing.Optional[Media.ReceivedFramesHandler] = None
        # This is for communication with a thread that handles the call to _bus.send
        self._tx_queue: queue.Queue[_TxItem | None] = queue.Queue()
        self._tx_thread = threading.Thread(target=self.transmit_thread_worker, daemon=True)

        try:
            bus = can.ThreadSafeBus(
                interface="socketcand",
                host=self._host,
                port=self._port,
                channel=self._can_channel,
                bitrate=bitrate,
                fd=self.is_fd,
                data_bitrate=bitrate[1],
            )

            self._bus: can.ThreadSafeBus = bus
        except can.CanError as ex:
            raise InvalidMediaConfigurationError(f"Could not initialize PythonCAN: {ex}") from ex
        super().__init__()

    @property
    def interface_name(self) -> str:
        return self._can_channel

    @property
    def host_name(self) -> str:
        return self._host

    @property
    def port_name(self) -> int:
        return self._port

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def number_of_acceptance_filters(self) -> int:
        """
        The value is currently fixed at 1 for all interfaces.
        TODO: obtain the number of acceptance filters from Python-CAN.
        """
        return 1

    @property
    def is_fd(self) -> bool:
        """
        Introspection helper. The value is True if the underlying interface operates in CAN FD mode.
        """
        return self._is_fd

    def start(self, handler: Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        self._tx_thread.start()
        if self._maybe_thread is None:
            self._rx_handler = handler
            self._maybe_thread = threading.Thread(
                target=self._thread_function, args=(asyncio.get_event_loop(),), name=str(self), daemon=True
            )
            self._maybe_thread.start()
            if no_automatic_retransmission:
                _logger.info("%s non-automatic retransmission is not supported", self)
        else:
            raise RuntimeError("The RX frame handler is already set up")

    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        if self._closed:
            raise ResourceClosedError(repr(self))
        filters = []
        for f in configuration:
            d = {"can_id": f.identifier, "can_mask": f.mask}
            if f.format is not None:  # Per Python-CAN docs, if "extended" is not set, both base/ext will be accepted.
                d["extended"] = f.format == FrameFormat.EXTENDED
            filters.append(d)
        _logger.debug("%s: Acceptance filters activated: %s", self, ", ".join(map(str, configuration)))
        self._bus.set_filters(filters)

    def transmit_thread_worker(self) -> None:
        try:
            while not self._closed:
                tx = self._tx_queue.get(block=True)
                if self._closed or tx is None:
                    break
                try:
                    self._bus.send(tx.msg, tx.timeout)
                    tx.loop.call_soon_threadsafe(partial(tx.future.set_result, None))
                except Exception as ex:
                    tx.loop.call_soon_threadsafe(partial(tx.future.set_exception, ex))
        except Exception as ex:
            _logger.critical(
                "Unhandled exception in transmit thread, transmission thread stopped and transmission is no longer possible: %s",
                ex,
                exc_info=True,
            )

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        num_sent = 0
        loopback: typing.List[typing.Tuple[Timestamp, Envelope]] = []
        loop = asyncio.get_running_loop()
        for f in frames:
            if self._closed:
                raise ResourceClosedError(repr(self))
            message = can.Message(
                arbitration_id=f.frame.identifier,
                is_extended_id=(f.frame.format == FrameFormat.EXTENDED),
                data=f.frame.data,
                is_fd=self._is_fd,
            )
            try:
                desired_timeout = monotonic_deadline - loop.time()
                received_future: asyncio.Future[None] = asyncio.Future()
                self._tx_queue.put_nowait(
                    _TxItem(
                        message,
                        max(desired_timeout, 0),
                        received_future,
                        asyncio.get_running_loop(),
                    )
                )
                await received_future
            except (asyncio.TimeoutError, can.CanError):  # CanError is also used to report timeouts (weird).
                break
            else:
                num_sent += 1
                if f.loopback:
                    loopback.append((Timestamp.now(), f))
        # Fake received frames if hardware does not support loopback
        if loopback:
            loop.call_soon(self._invoke_rx_handler, loopback)
        return num_sent

    def close(self) -> None:
        self._closed = True
        try:
            self._tx_queue.put(None)
            self._tx_thread.join(timeout=self._MAXIMAL_TIMEOUT_SEC * 10)
            if self._maybe_thread is not None:
                self._maybe_thread.join(timeout=self._MAXIMAL_TIMEOUT_SEC * 10)
                self._maybe_thread = None
        finally:
            try:
                self._bus.shutdown()
            except Exception as ex:
                _logger.exception("%s: Bus closing error: %s", self, ex)

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        """
        Returns an empty list. TODO: provide minimally functional implementation.
        """
        return []

    def _invoke_rx_handler(self, frs: typing.List[typing.Tuple[Timestamp, Envelope]]) -> None:
        try:
            # Don't call after closure to prevent race conditions and use-after-close.
            if not self._closed and self._rx_handler is not None:
                self._rx_handler(frs)
        except Exception as exc:
            _logger.exception("%s unhandled exception in the receive handler: %s; lost frames: %s", self, exc, frs)

    def _thread_function(self, loop: asyncio.AbstractEventLoop) -> None:
        while not self._closed:
            try:
                batch = self._read_batch()
                if batch:
                    try:
                        loop.call_soon_threadsafe(self._invoke_rx_handler, batch)
                    except RuntimeError as ex:
                        _logger.debug("%s: Event loop is closed, exiting: %r", self, ex)
                        break
            except OSError as ex:
                if not self._closed:
                    _logger.exception("%s thread input/output error; stopping: %s", self, ex)
                break
            except Exception as ex:
                _logger.exception("%s thread failure: %s", self, ex)
                if not self._closed:
                    time.sleep(1)  # Is this an adequate failure management strategy?

        self._closed = True
        _logger.info("%s thread is about to exit", self)

    def _read_batch(self) -> typing.List[typing.Tuple[Timestamp, Envelope]]:
        batch: typing.List[typing.Tuple[Timestamp, Envelope]] = []
        while not self._closed:
            msg = self._bus.recv(0.0 if batch else self._MAXIMAL_TIMEOUT_SEC)
            if msg is None:
                break

            timestamp = Timestamp(system_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())

            frame = self._parse_native_frame(msg)
            if frame is not None:
                batch.append((timestamp, Envelope(frame, False)))
        return batch

    @staticmethod
    def _parse_native_frame(msg: can.Message) -> typing.Optional[DataFrame]:
        if msg.is_error_frame:  # error frame, ignore silently
            _logger.debug("Error frame dropped: id_raw=%08x", msg.arbitration_id)
            return None
        frame_format = FrameFormat.EXTENDED if msg.is_extended_id else FrameFormat.BASE
        data = msg.data
        return DataFrame(frame_format, msg.arbitration_id, data)
