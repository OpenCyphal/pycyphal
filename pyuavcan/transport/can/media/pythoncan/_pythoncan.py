# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Alex Kiselev <a.kiselev@volz-servos.com>, Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import time
import typing
import asyncio
import logging
import threading
import functools
import dataclasses
import collections
import concurrent.futures
import can  # type: ignore
from pyuavcan.transport import Timestamp, ResourceClosedError, InvalidMediaConfigurationError
from pyuavcan.transport.can.media import Media, FilterConfiguration, Envelope, FrameFormat, DataFrame


_logger = logging.getLogger(__name__)


class PythonCANMedia(Media):
    """
    Media interface adapter for `Python-CAN <https://python-can.readthedocs.io/>`_.
    It is designed to be usable with all host platforms supported by Python-CAN (GNU/Linux, Windows, macOS).
    Please refer to the Python-CAN documentation for information about supported CAN hardware, its configuration,
    and how to install the dependencies properly.

    This media interface supports both Classic CAN and CAN FD. The selection logic is documented below.

    This implementation emulates loopback instead of requesting it from the underlying driver due to the limitations
    of Python-CAN. Same goes for timestamping.
    If accurate timestamping is desired, consider using the SocketCAN media driver instead.

    Here is a basic usage example based on the Yakut CLI tool.
    Suppose that there are two interconnected CAN bus adapters connected to the host computer:
    one SLCAN-based, the other is PCAN USB.
    Launch Yakut to listen for messages using the SLCAN adapter::

        export YAKUT_TRANSPORT='CAN(can.media.pythoncan.PythonCANMedia("slcan:/dev/ttyACM0", 1_000_000), None)'
        yakut sub 33.uavcan.si.unit.voltage.Scalar.1.0

    While the subscriber is running, publish a message to this subject::

        export YAKUT_TRANSPORT='CAN(can.media.pythoncan.PythonCANMedia("pcan:PCAN_USBBUS1", 1_000_000), 42)'
        yakut pub 33.uavcan/si/unit/voltage/Scalar_1_0 '{volt: 12}'
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
        :param iface_name: Interface name consisting of Python-CAN interface module name and its channel,
            separated with a colon. Supported interfaces are documented below.
            The semantics of the channel name are described in the documentation for Python-CAN.

            - Interface ``socketcan`` is implemented by :class:`can.interfaces.socketcan.SocketcanBus`.
              The bit rate values are only used to select Classic/FD mode.
              It is not possible to configure the actual CAN bit rate using this API.
              Example: ``socketcan:vcan0``

            - Interface ``kvaser`` is implemented by :class:`can.interfaces.kvaser.canlib.KvaserBus`.
              Example: ``kvaser:0``

            - Interface ``slcan`` is implemented by :class:`can.interfaces.slcan.slcanBus`.
              Only Classic CAN is supported.
              The serial port settings are fixed at 115200-8N1.
              Example: ``slcan:COM12``

            - Interface ``pcan`` is implemented by :class:`can.interfaces.pcan.PcanBus`.
              Ensure that `PCAN-Basic <https://www.peak-system.com/PCAN-Basic.239.0.html>`_ is installed.
              Example: ``pcan:PCAN_USBBUS1``

            - Interface ``virtual`` is described in https://python-can.readthedocs.io/en/master/interfaces/virtual.html.
              The channel name should be empty.
              Example: ``virtual:``

            - Interface ``usb2can`` is described in https://python-can.readthedocs.io/en/stable/interfaces/usb2can.html.
              Example: ``usb2can:ED000100``

        :param bitrate: Bit rate value in bauds; either a single integer or a tuple:

            - A single integer selects Classic CAN.
            - A tuple of two selects CAN FD, where the first integer defines the arbitration (nominal) bit rate
              and the second one defines the data phase bit rate.
            - If MTU (see below) is given and is greater than 8 bytes, CAN FD is used regardless of the above.

        :param mtu: The maximum CAN data field size in bytes.
            If provided, this value must belong to :attr:`Media.VALID_MTU_SET`.
            If not provided, the default is determined as follows:

            - If `bitrate` is a single integer: classic CAN is assumed, MTU defaults to 8 bytes.
            - If `bitrate` is two integers: CAN FD is assumed, MTU defaults to 64 bytes.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.

        :raises: :class:`InvalidMediaConfigurationError` if the specified media instance
            could not be constructed, the interface name is unknown,
            or if the underlying library raised a :class:`can.CanError`.

        Use virtual bus with various bit rate and FD configurations:

        >>> media = PythonCANMedia('virtual:', 500_000)
        >>> media.is_fd, media.mtu
        (False, 8)
        >>> media = PythonCANMedia('virtual:', (500_000, 2_000_000))
        >>> media.is_fd, media.mtu
        (True, 64)
        >>> media = PythonCANMedia('virtual:', 1_000_000, 16)
        >>> media.is_fd, media.mtu
        (True, 16)

        Use PCAN-USB channel 1 in FD mode with nominal bitrate 500 kbit/s, data bitrate 2 Mbit/s, MTU 64 bytes::

            PythonCANMedia('pcan:PCAN_USBBUS1', (500_000, 2_000_000))

        Use Kvaser channel 0 in classic mode with bitrate 500k::

            PythonCANMedia('kvaser:0', 500_000)
        """
        self._conn_name = str(iface_name).split(":")
        if len(self._conn_name) != 2:
            raise InvalidMediaConfigurationError(
                f"Interface name {iface_name!r} does not match the format 'interface:channel'"
            )

        single_bitrate = isinstance(bitrate, (int, float))
        bitrate = (int(bitrate), int(bitrate)) if single_bitrate else (int(bitrate[0]), int(bitrate[1]))  # type: ignore

        default_mtu = min(self.VALID_MTU_SET) if single_bitrate else 64
        self._mtu = int(mtu) if mtu is not None else default_mtu
        if self._mtu not in self.VALID_MTU_SET:
            raise InvalidMediaConfigurationError(f"Wrong MTU value: {mtu}")

        self._is_fd = self._mtu > min(self.VALID_MTU_SET) or not single_bitrate

        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._rx_handler: typing.Optional[Media.ReceivedFramesHandler] = None
        self._background_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        params: typing.Union[_FDInterfaceParameters, _ClassicInterfaceParameters]
        if self._is_fd:
            params = _FDInterfaceParameters(
                interface_name=self._conn_name[0], channel_name=self._conn_name[1], bitrate=bitrate
            )
        else:
            params = _ClassicInterfaceParameters(
                interface_name=self._conn_name[0], channel_name=self._conn_name[1], bitrate=bitrate[0]
            )
        try:
            self._bus = _CONSTRUCTORS[self._conn_name[0]](params)
        except can.CanError as ex:
            raise InvalidMediaConfigurationError(f"Could not initialize PythonCAN: {ex}") from ex
        super().__init__()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def interface_name(self) -> str:
        return ":".join(self._conn_name)

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
        if self._maybe_thread is None:
            self._rx_handler = handler
            self._maybe_thread = threading.Thread(target=self._thread_function, name=str(self), daemon=True)
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

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        num_sent = 0
        loopback: typing.List[typing.Tuple[Timestamp, Envelope]] = []
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
                await self._loop.run_in_executor(
                    self._background_executor,
                    functools.partial(self._bus.send, message, timeout=monotonic_deadline - self._loop.time()),
                )
            except (asyncio.TimeoutError, can.CanError):  # CanError is also used to report timeouts (weird).
                break
            else:
                num_sent += 1
                if f.loopback:
                    loopback.append((Timestamp.now(), f))
        if loopback:
            self.loop.call_soon(self._invoke_rx_handler, loopback)
        return num_sent

    def close(self) -> None:
        self._closed = True
        try:
            self._bus.shutdown()
            if self._maybe_thread is not None:
                self._maybe_thread.join()
        except Exception as ex:
            _logger.exception("%s: Bus closing error: %s", self, ex)
        self._maybe_thread = None

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

    def _thread_function(self) -> None:
        while not self._closed:
            try:
                batch = self._read_batch()
                if batch:
                    self._loop.call_soon_threadsafe(self._invoke_rx_handler, batch)
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
            timestamp = Timestamp.now()  # TODO: use accurate timestamping
            loopback = False  # TODO: no possibility to get real loopback yet
            frame = self._parse_native_frame(msg)
            if frame is not None:
                batch.append((timestamp, Envelope(frame, loopback)))
        return batch

    @staticmethod
    def _parse_native_frame(msg: can.Message) -> typing.Optional[DataFrame]:
        if msg.is_error_frame:  # error frame, ignore silently
            _logger.debug("Error frame dropped: id_raw=%08x", msg.arbitration_id)
            return None
        frame_format = FrameFormat.EXTENDED if msg.is_extended_id else FrameFormat.BASE
        data = msg.data
        return DataFrame(frame_format, msg.arbitration_id, data)


@dataclasses.dataclass(frozen=True)
class _InterfaceParameters:
    interface_name: str
    channel_name: str


@dataclasses.dataclass(frozen=True)
class _ClassicInterfaceParameters(_InterfaceParameters):
    bitrate: int


@dataclasses.dataclass(frozen=True)
class _FDInterfaceParameters(_InterfaceParameters):
    bitrate: typing.Tuple[int, int]


def _construct_socketcan(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name, channel=parameters.channel_name, fd=False)
    if isinstance(parameters, _FDInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name, channel=parameters.channel_name, fd=True)
    assert False, "Internal error"


def _construct_kvaser(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate,
            fd=False,
        )
    if isinstance(parameters, _FDInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate[0],
            fd=True,
            data_bitrate=parameters.bitrate[1],
        )
    assert False, "Internal error"


def _construct_slcan(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate,
        )
    if isinstance(parameters, _FDInterfaceParameters):
        raise InvalidMediaConfigurationError(f"Interface does not support CAN FD: {parameters.interface_name}")
    assert False, "Internal error"


def _construct_pcan(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate,
        )
    if isinstance(parameters, _FDInterfaceParameters):
        # These magic numbers come from the settings of PCAN adapter.
        # They don't allow any direct baudrate settings, you have to set all lengths and value of the main frequency.
        # Bit lengths below are very universal and can be applied for almost every popular baudrate.
        # There is probably a better solution here, but it needs significantly more time to implement it.
        f_clock = 40000000
        nom_tseg1, nom_tseg2, nom_sjw = 3, 1, 1
        data_tseg1, data_tseg2, data_sjw = 3, 1, 1

        nom_br = int(f_clock / parameters.bitrate[0] / (nom_tseg1 + nom_tseg2 + nom_sjw))
        data_br = int(f_clock / parameters.bitrate[1] / (data_tseg1 + data_tseg2 + data_sjw))
        # TODO: validate the result and see if it is within an acceptable range

        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            f_clock=f_clock,
            nom_brp=nom_br,
            data_brp=data_br,
            nom_tseg1=nom_tseg1,
            nom_tseg2=nom_tseg2,
            nom_sjw=nom_sjw,
            data_tseg1=data_tseg1,
            data_tseg2=data_tseg2,
            data_sjw=data_sjw,
            fd=True,
        )

    assert False, "Internal error"


def _construct_virtual(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name, bitrate=parameters.bitrate)
    if isinstance(parameters, _FDInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name)
    assert False, "Internal error"


def _construct_usb2can(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate,
        )
    if isinstance(parameters, _FDInterfaceParameters):
        raise InvalidMediaConfigurationError(f"Interface does not support CAN FD: {parameters.interface_name}")
    assert False, "Internal error"


def _construct_any(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    raise InvalidMediaConfigurationError(f"Interface not supported yet: {parameters.interface_name}")


_CONSTRUCTORS: typing.DefaultDict[
    str, typing.Callable[[_InterfaceParameters], can.ThreadSafeBus]
] = collections.defaultdict(
    lambda: _construct_any,
    {
        "socketcan": _construct_socketcan,
        "kvaser": _construct_kvaser,
        "slcan": _construct_slcan,
        "pcan": _construct_pcan,
        "virtual": _construct_virtual,
        "usb2can": _construct_usb2can,
    },
)
