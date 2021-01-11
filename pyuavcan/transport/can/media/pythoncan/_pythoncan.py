# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Alex Kiselev <a.kiselev@volz-servos.com>, Pavel Kirienko <pavel@uavcan.org>

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
    raise TypeError(f"Invalid parameters: {parameters}")


def _construct_kvaser(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name, channel=parameters.channel_name, bitrate=parameters.bitrate, fd=False
        )
    if isinstance(parameters, _FDInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name,
            channel=parameters.channel_name,
            bitrate=parameters.bitrate[0],
            fd=True,
            data_bitrate=parameters.bitrate[1],
        )
    raise TypeError(f"Invalid parameters: {parameters}")


def _construct_slcan(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        # only default ttyBaudrate is possible (115200)
        return can.ThreadSafeBus(
            interface=parameters.interface_name, channel=parameters.channel_name, bitrate=parameters.bitrate
        )
    if isinstance(parameters, _FDInterfaceParameters):
        raise TypeError(f"Interface does not support CAN FD: {parameters.interface_name}")
    raise TypeError(f"Invalid parameters: {parameters}")


def _construct_pcan(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(
            interface=parameters.interface_name, channel=parameters.channel_name, bitrate=parameters.bitrate
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

    raise TypeError(f"Invalid parameters: {parameters}")


def _construct_virtual(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    if isinstance(parameters, _ClassicInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name, bitrate=parameters.bitrate)
    if isinstance(parameters, _FDInterfaceParameters):
        return can.ThreadSafeBus(interface=parameters.interface_name)
    raise TypeError(f"Invalid parameters: {parameters}")


def _construct_any(parameters: _InterfaceParameters) -> can.ThreadSafeBus:
    raise TypeError(f"Interface not supported yet: {parameters.interface_name}")


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
    },
)


class PythonCANMedia(Media):
    """
    A media interface adapter for `python-can <https://github.com/hardbyte/python-can>`_.

    - Usage example for PCAN-USB channel 1 (bitrate = 500k, mtu = 8, Node-ID = 10)::

        CAN(can.media.pythoncan.PythonCANMedia('pcan:PCAN_USBBUS1',5000000,8),10)
    - Usage example for PCAN-USB channel 1 (nom.bitrate = 500k, data.bitrate = 2M, mtu = 8, Node-ID = 10)::

        CAN(can.media.pythoncan.PythonCANMedia('pcan:PCAN_USBBUS1',[5000000,2000000],8),10)
    - Usage example for Kvaser channel 0 (bitrate = 500k, mtu = 8, Node-ID = 10)::

        CAN(can.media.pythoncan.PythonCANMedia('kvaser:0',5000000,8),10)
    """

    MAXIMAL_TIMEOUT_SEC = 0.001

    def __init__(self, iface_name: str, bitrate: typing.Union[int, typing.Tuple[int, int]], mtu: int) -> None:
        """
        CAN Classic/FD are possible. CAN FD is used if MTU value > 8 or two bit rates are used (nom and data).

        :param iface_name: Interface name consisting of interface and channel separated with a colon.
            E.g., ``kvaser:0``.

        :param bitrate: bitrate value in bauds.
            Single integer for CAN Classic, two values for CAN FD.

        :param mtu: The maximum data field size in bytes.
            This value must belong to Media.VALID_MTU_SET.

        """
        self._conn_name = str(iface_name).split(":")
        if len(self._conn_name) != 2:
            raise InvalidMediaConfigurationError(
                "Interface name %r does not match the format 'interface:channel'" % str(iface_name)
            )
        if mtu not in self.VALID_MTU_SET:
            raise RuntimeError(f"Wrong MTU value: {mtu}")
        self._mtu = int(mtu)
        self._loop = asyncio.get_event_loop()
        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._background_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        bitrate = (int(bitrate), int(bitrate)) if isinstance(bitrate, int) else (int(bitrate[0]), int(bitrate[1]))
        self._is_fd = self._mtu > min(self.VALID_MTU_SET) or len(set(bitrate)) > 1
        params: typing.Union[_FDInterfaceParameters, _ClassicInterfaceParameters]
        if self._is_fd:
            params = _FDInterfaceParameters(
                interface_name=self._conn_name[0], channel_name=self._conn_name[1], bitrate=bitrate
            )
        else:
            params = _ClassicInterfaceParameters(
                interface_name=self._conn_name[0], channel_name=self._conn_name[1], bitrate=bitrate[0]
            )
        self._bus = _CONSTRUCTORS[self._conn_name[0]](params)
        self._loopback_lock = threading.Lock()
        self._loop_frames: typing.List[DataFrame] = []
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
        # just a placeholder to avoid error
        return 1

    def start(self, handler: Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        if self._maybe_thread is None:
            self._maybe_thread = threading.Thread(
                target=self._thread_function, name=str(self), args=(handler,), daemon=True
            )
            self._maybe_thread.start()
            if no_automatic_retransmission:
                _logger.info("%s non-automatic retransmission is not supported", self)
        else:
            raise RuntimeError("The RX frame handler is already set up")

    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        if self._closed:
            raise ResourceClosedError(repr(self))
        filters = [
            {
                "can_id": f.identifier,
                "can_mask": f.mask,
                "extended": f.format == FrameFormat.EXTENDED,
            }
            for f in configuration
        ]
        _logger.debug("%s: Acceptance filters activated: %s", self, ", ".join(map(str, configuration)))
        self._bus.set_filters(filters)

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        num_sent = 0
        for f in frames:
            if self._closed:
                raise ResourceClosedError(repr(self))
            message = can.Message(
                arbitration_id=f.frame.identifier,
                is_extended_id=(f.frame.format == FrameFormat.EXTENDED),
                data=f.frame.data,
                is_fd=self._is_fd,
            )
            if f.loopback:
                with self._loopback_lock:
                    self._loop_frames.append(f.frame)
            try:
                await self._loop.run_in_executor(
                    self._background_executor,
                    functools.partial(self._bus.send, message, timeout=monotonic_deadline - self._loop.time()),
                )
            except asyncio.TimeoutError:
                break
            else:
                num_sent += 1
        return num_sent

    def close(self) -> None:
        self._closed = True
        try:
            self._bus.shutdown()
        except Exception as ex:
            _logger.exception("Bus closing error: %s", ex)

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        return []  # No support is possible now

    def _thread_function(self, handler: Media.ReceivedFramesHandler) -> None:
        def handler_wrapper(frs: typing.List[typing.Tuple[Timestamp, Envelope]]) -> None:
            try:
                if not self._closed:  # Don't call after closure to prevent race conditions and use-after-close.
                    handler(frs)
            except Exception as exc:
                _logger.exception("%s unhandled exception in the receive handler: %s; lost frames: %s", self, exc, frs)

        while not self._closed:
            try:
                frames: typing.List[typing.Tuple[Timestamp, Envelope]] = []
                item = self._read_frame()
                if item is not None:
                    frames.append(item)
                if len(self._loop_frames) > 0:
                    loop_ts = Timestamp.now()
                    with self._loopback_lock:
                        for frame in self._loop_frames:
                            frames.append(
                                (
                                    loop_ts,
                                    Envelope(DataFrame(frame.format, frame.identifier, frame.data), loopback=True),
                                )
                            )
                        self._loop_frames.clear()
                if len(frames) > 0:
                    self._loop.call_soon_threadsafe(handler_wrapper, frames)
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

    def _read_frame(self) -> typing.Optional[typing.Tuple[Timestamp, Envelope]]:
        msg = self._bus.recv(self.MAXIMAL_TIMEOUT_SEC)
        if msg is not None:
            timestamp = Timestamp.now()
            loopback = False  # no possibility to get real loopback yet
            frame = self._parse_native_frame(msg)
            if frame is not None:
                return timestamp, Envelope(frame, loopback=loopback)
        return None

    @staticmethod
    def _parse_native_frame(msg: can.Message) -> typing.Optional[DataFrame]:
        if msg.is_error_frame:  # error frame, ignore silently
            _logger.debug("Frame dropped: id_raw=%08x", msg.arbitration_id)
            return None
        frame_format = FrameFormat.EXTENDED if msg.is_extended_id else FrameFormat.BASE
        data = msg.data
        return DataFrame(frame_format, msg.arbitration_id, data)
