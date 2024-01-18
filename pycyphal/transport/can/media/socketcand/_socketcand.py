# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Alex Kiselev <a.kiselev@volz-servos.com>, Pavel Kirienko <pavel@opencyphal.org>
# pylint: disable=duplicate-code

from __future__ import annotations
import queue
import time
import typing
import asyncio
import logging
import threading
from functools import partial
import dataclasses

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


class SocketcandMedia(Media):
    """
    Media interface adapter for `Socketcand <https://github.com/linux-can/socketcand/tree/master>`_ using the
    built-in interface from `Python-CAN <https://python-can.readthedocs.io/>`_.
    Please refer to the Socketcand documentation for information about supported hardware,
    configuration, and installation instructions.

    This media interface supports only Classic CAN.

    Here is a basic usage example based on the Yakut CLI tool.
    Suppose you have two computers:
    One connected to a CAN-capable device and that computer is able to connect and receive CAN data from the
    CAN device. Using socketcand with a command such as ``socketcand -v -i can0 -l 123.123.1.123``
    on this first computer will bind it too a socket (default port for socketcand is 29536, so it is also default here).

    Then, on your second computer::

        export UAVCAN__CAN__IFACE="socketcand:can0:123.123.1.123"
        yakut sub 33:uavcan.si.unit.voltage.scalar

    This will allow you to remotely receive CAN data on computer two through the wired connection on computer 1.
    """

    _MAXIMAL_TIMEOUT_SEC = 0.1

    def __init__(self, channel: str, host: str, port: int = 29536) -> None:
        """
        :param channel: Name of the CAN channel/interface that your remote computer is connected to;
            often ``can0`` or ``vcan0``.
            Comes after the ``-i`` in the socketcand command.

        :param host: Name of the remote IP address of the computer running socketcand;
            should be in the format ``123.123.1.123``.
            In the socketcand command, this is the IP address after ``-l``.

        :param port: Name of the port the socket is bound too.
            As per socketcand's default value, here, the default is also 29536.
        """

        self._iface = "socketcand"
        self._host = host
        self._port = port
        self._can_channel = channel

        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._rx_handler: typing.Optional[Media.ReceivedFramesHandler] = None
        # This is for communication with a thread that handles the call to _bus.send
        self._tx_queue: queue.Queue[_TxItem | None] = queue.Queue()
        self._tx_thread = threading.Thread(target=self._transmit_thread_worker, daemon=True)

        try:
            self._bus = can.ThreadSafeBus(
                interface=self._iface,
                host=self._host,
                port=self._port,
                channel=self._can_channel,
            )
        except can.CanError as ex:
            raise InvalidMediaConfigurationError(f"Could not initialize PythonCAN: {ex}") from ex
        super().__init__()

    @property
    def interface_name(self) -> str:
        return f"{self._iface}:{self._can_channel}:{self._host}:{self._port}"

    @property
    def channel_name(self) -> str:
        return self._can_channel

    @property
    def host_name(self) -> str:
        return self._host

    @property
    def port_name(self) -> int:
        return self._port

    # Python-CAN's wrapper for socketcand does not support FD frames, so mtu will always be 8 for now
    @property
    def mtu(self) -> int:
        return 8

    @property
    def number_of_acceptance_filters(self) -> int:
        """
        The value is currently fixed at 1 for all interfaces.
        TODO: obtain the number of acceptance filters from Python-CAN.
        """
        return 1

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
        self._bus.set_filters(filters)
        _logger.debug("%s: Acceptance filters activated: %s", self, ", ".join(map(str, configuration)))

    def _transmit_thread_worker(self) -> None:
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
                "Unhandled exception in transmit thread, "
                "transmission thread stopped and transmission is no longer possible: %s",
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
            try:
                self._tx_thread.join(timeout=self._MAXIMAL_TIMEOUT_SEC * 10)
            except RuntimeError:
                pass
            if self._maybe_thread is not None:
                try:
                    self._maybe_thread.join(timeout=self._MAXIMAL_TIMEOUT_SEC * 10)
                except RuntimeError:
                    pass
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
        while not self._closed and not loop.is_closed():
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
