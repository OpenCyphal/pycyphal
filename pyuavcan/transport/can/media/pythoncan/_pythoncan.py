#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import enum
import time
import errno
import typing
import struct
import asyncio
import concurrent.futures
import logging
import threading
import can  # type: ignore
import pyuavcan.transport
import pyuavcan.transport.can.media as _media


_logger = logging.getLogger(__name__)


class PythonCANMedia(_media.Media):
    """
    A media interface adapter for `python-can <https://github.com/hardbyte/python-can>`_.

    - Usage example for PCAN-USB channel 1 (bitrate = 500k, mtu = 8, Node-ID = 10)::
    
        CAN(can.media.pythoncan.PythonCANMedia('pcan:PCAN_USBBUS1',5000000,8),10)
    - Usage example for PCAN-USB channel 1 (nom.bitrate = 500k, data.bitrate = 2M, mtu = 8, Node-ID = 10)::
    
        CAN(can.media.pythoncan.PythonCANMedia('pcan:PCAN_USBBUS1',[5000000,2000000],8),10)
    - Usage example for Kvaser channel 0 (bitrate = 500k, mtu = 8, Node-ID = 10)::
    
        CAN(can.media.pythoncan.PythonCANMedia('kvaser:0',5000000,8),10)
    """

    VALID_FD_ADAPTER_SET = {'pcan', 'kvaser'}

    def __init__(self, iface_name: str, bitrate: typing.Union[int, typing.Tuple[int, int]], mtu: int) -> None:
        self._iface_name = str(iface_name)
        self._conn_name = self._iface_name.split(':')
        if len(self._conn_name) != 2:
            raise pyuavcan.transport.InvalidMediaConfigurationError(
                "Interface name %r does not match the format 'interface:channel'" % self._iface_name
            )
        if mtu not in self.VALID_MTU_SET:
            raise RuntimeError('Wrong MTU value: {}'.format(mtu))
        self._mtu = int(mtu)
        self._is_fd = self._mtu > min(self.VALID_MTU_SET) or type(bitrate) != int
        self._loop = asyncio.get_event_loop()
        self._closed = False
        self._maybe_thread: typing.Optional[threading.Thread] = None
        self._background_executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        self._loopback_enabled = False
        if not isinstance(bitrate, int):
            nom_br = int(bitrate[0])
            data_br = int(bitrate[1])
        else:
            nom_br = int(bitrate)
            data_br = int(bitrate)
        if not self._is_fd:
            self._bus = can.ThreadSafeBus(interface=self._conn_name[0], channel=self._conn_name[1], bitrate=nom_br)
        elif self._conn_name[0] == 'pcan':
            f_clock = 40000000
            nom_tseg1, nom_tseg2, nom_sjw = 3, 1, 1
            data_tseg1, data_tseg2, data_sjw = 3, 1, 1
            nom_br = int(f_clock / nom_br / (nom_tseg1 + nom_tseg2 + nom_sjw))
            data_br = int(f_clock / data_br / (data_tseg1 + data_tseg2 + data_sjw))
            self._bus = can.ThreadSafeBus(interface=self._conn_name[0],
                                          channel=self._conn_name[1],
                                          f_clock=f_clock,
                                          nom_brp=nom_br,
                                          data_brp=data_br,
                                          nom_tseg1=nom_tseg1,
                                          nom_tseg2=nom_tseg2,
                                          nom_sjw=nom_sjw,
                                          data_tseg1=data_tseg1,
                                          data_tseg2=data_tseg2,
                                          data_sjw=data_sjw,
                                          fd=True)
        elif self._conn_name[0] == 'kvaser':
            self._bus = can.ThreadSafeBus(interface=self._conn_name[0],
                                          channel=self._conn_name[1],
                                          bitrate=nom_br,
                                          data_bitrate=data_br)
        else:
            #have to define other adapters here, not every adapter supports CAN FD!
            raise RuntimeError('Interface doesn\'t support CAN FD: {}'.format(self._conn_name[0]))
        self._loopback_lock = threading.RLock()
        self._loop_frames: typing.List[_media.DataFrame] = []
        super(PythonCANMedia, self).__init__()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def interface_name(self) -> str:
        return self._iface_name

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def number_of_acceptance_filters(self) -> int:
        # TODO: obtain the correct value from the adapter.
        return 1

    def start(self, handler: _media.Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        if self._maybe_thread is None:
            self._maybe_thread = threading.Thread(target=self._thread_function,
                                                  name=str(self),
                                                  args=(handler,),
                                                  daemon=True)
            self._maybe_thread.start()
            if no_automatic_retransmission:
                _logger.info('%s non-automatic retransmission is not supported', self)
        else:
            raise RuntimeError('The RX frame handler is already set up')

    def configure_acceptance_filters(self, configuration: typing.Sequence[_media.FilterConfiguration]) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(repr(self))
        filters = []
        for f in configuration:
            f_dict = {}
            f_dict['can_id'] = f.identifier
            f_dict['can_mask'] = f.mask
            f_dict['extended'] = f.format == _media.FrameFormat.EXTENDED
            filters.append(f_dict)
        _logger.info('Acceptance filters activated: %s', ', '.join(map(str, configuration)))
        self._bus.set_filters(filters)

    async def send_until(self, frames: typing.Iterable[_media.DataFrame], monotonic_deadline: float) -> int:
        num_sent = 0
        for f in frames:
            if self._closed:
                raise pyuavcan.transport.ResourceClosedError(repr(self))
            self._set_loopback_enabled(f.loopback)
            message = can.Message(arbitration_id=f.identifier, is_extended_id=True, data=f.data, is_fd=self._is_fd)
            if f.loopback == True:
                with self._loopback_lock:
                    self._loop_frames.append(f)
            try:
                await self._loop.run_in_executor(
                    self._background_executor,
                    lambda: self._bus.send(message, timeout=monotonic_deadline - self._loop.time()),
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
        except Exception:
            _logger.exception('Bus closing error')

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        return []  # No support is possible now

    def _thread_function(self, handler: _media.Media.ReceivedFramesHandler) -> None:
        def handler_wrapper(frs: typing.Sequence[_media.TimestampedDataFrame]) -> None:
            try:
                if not self._closed:  # Don't call after closure to prevent race conditions and use-after-close.
                    handler(frs)
            except Exception as exc:
                _logger.exception('%s unhandled exception in the receive handler: %s; lost frames: %s', self, exc, frs)

        while not self._closed:
            try:
                frames: typing.List[_media.TimestampedDataFrame] = []
                try:
                    frames.append(self._read_frame())
                except OSError as ex:
                    raise
                if len(self._loop_frames) > 0:
                    with self._loopback_lock:
                        for frame in self._loop_frames:
                            loop_ts = pyuavcan.transport.Timestamp(system_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
                            frames.append(_media.TimestampedDataFrame(identifier=frame.identifier,
                                                                      data=frame.data,
                                                                      format=frame.format,
                                                                      loopback=frame.loopback,
                                                                      timestamp=loop_ts))
                        self._loop_frames.clear()
                if len(frames) > 0:
                    self._loop.call_soon_threadsafe(handler_wrapper, frames)
            except OSError as ex:
                if not self._closed:
                    _logger.exception('%s thread input/output error; stopping: %s', self, ex)
                break
            except Exception as ex:
                _logger.exception('%s thread failure: %s', self, ex)
                if not self._closed:
                    time.sleep(1)       # Is this an adequate failure management strategy?

        self._closed = True
        _logger.info('%s thread is about to exit', self)

    def _read_frame(self) -> _media.TimestampedDataFrame:
        while True:
            msg = self._bus.recv()
            if msg is not None:
                ts_system_ns = time.time_ns()
                ts_mono_ns = time.monotonic_ns()
                timestamp = pyuavcan.transport.Timestamp(system_ns=ts_system_ns, monotonic_ns=ts_mono_ns)
                loopback = False      # no possibility to get real loopback yet
                out = self._parse_native_frame(msg, loopback=loopback, timestamp=timestamp)
                if out is not None:
                    return out

    @staticmethod
    def _parse_native_frame(msg: can.Message,
                            loopback: bool,
                            timestamp: pyuavcan.transport.Timestamp) \
            -> typing.Optional[_media.TimestampedDataFrame]:
        if msg.is_error_frame:  # error frame, ignore silently
            _logger.debug('Frame dropped: id_raw=%08x', msg.arbitration_id)
            return None
        frame_format = _media.FrameFormat.EXTENDED if msg.is_extended_id else _media.FrameFormat.BASE
        data = msg.data
        return _media.TimestampedDataFrame(identifier=msg.arbitration_id,
                                           data=data,
                                           format=frame_format,
                                           loopback=loopback,
                                           timestamp=timestamp)

    def _set_loopback_enabled(self, enable: bool) -> None:
        # it do nothing at the moment
        if enable != self._loopback_enabled:
            self._loopback_enabled = enable
