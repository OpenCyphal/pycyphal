#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import ctypes
import logging
import threading
import dataclasses
import pyuavcan
from pyuavcan.transport import TransportError, Timestamp


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class LinkLayerPacket:
    """
    The addresses are represented here in the link-native byte order.
    """
    source:      memoryview
    destination: memoryview
    payload:     memoryview

    def __repr__(self) -> str:
        """
        The repr displays only the first 100 bytes of the payload.
        If the payload is longer, its string representation is appended with an ellipsis.
        """
        limit = 100
        if len(self.payload) <= limit:
            pld = bytes(self.payload).hex()
        else:
            pld = bytes(self.payload[:limit]).hex() + '...'
        return pyuavcan.util.repr_attributes(self,
                                             source=bytes(self.source).hex(),
                                             destination=bytes(self.destination).hex(),
                                             payload=pld)

    Encoder = typing.Callable[['LinkLayerPacket'], memoryview]
    Decoder = typing.Callable[[memoryview], typing.Optional['LinkLayerPacket']]

    @staticmethod
    def get_enc_dec(data_link_type: int) -> typing.Optional[typing.Tuple[Encoder, Decoder]]:
        """
        A factory of paired encode/decode functions that are used for building and parsing link-layer packets.
        If the supplied link layer type code (from libpcap ``DLT_*``) is not supported, returns None.
        See https://www.tcpdump.org/linktypes.html.
        """
        import libpcap as pcap  # type: ignore
        if data_link_type == pcap.DLT_EN10MB:
            def enc(p: LinkLayerPacket) -> memoryview:
                return memoryview(b''.join((
                    bytes(p.source).rjust(6, b'\x00')[:6],
                    bytes(p.destination).rjust(6, b'\x00')[:6],
                    len(p.payload).to_bytes(2, 'big'),
                    p.payload,
                )))

            def dec(p: memoryview) -> typing.Optional[LinkLayerPacket]:
                if len(p) < 14:
                    return None
                src = p[0:6]
                dst = p[6:12]
                ln = int.from_bytes(p[12:14], 'big')
                if len(p) - 14 < ln:
                    return None
                return LinkLayerPacket(source=src, destination=dst, payload=p[14:14 + ln])

            return enc, dec
        return None


@dataclasses.dataclass(frozen=True)
class LinkLayerSniff:
    timestamp:   Timestamp
    packet:      LinkLayerPacket
    device_name: str
    # Do we also need to report the link layer type here?


class LinkLayerSniffer:
    """
    This wrapper is intended to insulate the rest of the transport implementation from the specifics of the
    libpcap wrapper implementation (there are dozens of different wrappers out there).
    Observe anything libpcap-related shall not be imported outside of these methods because we only require
    this dependency if protocol sniffing capability is needed.
    Regular use should be possible without libpcap installed.

    - https://www.tcpdump.org/manpages/pcap.3pcap.html
    - https://github.com/karpierz/libpcap/blob/master/tests/capturetest.py
    """

    def __init__(self,
                 address_families:  typing.Iterable[int],
                 filter_expression: str,
                 callback:          typing.Callable[[LinkLayerSniff], None]) -> None:
        """
        Once a new instance is constructed, it is launched immediately.
        Execution is carried out in a background thread pool.
        It is required to call :meth:`close` when done.

        If a new network device is added or re-initialized while the sniffer is running, it will not be recognized.
        Removal or a re-configuration of a device while the sniffer is running may cause it to fail.

        :param address_families: Collection of ``socket.AF_*`` constants defining which types of network to use.
            This duplicates the filter expression somewhat but it allows the sniffer to automatically determine
            which network interface devices to use and which ones to ignore.

        :param filter_expression: The standard pcap filter expression;
            see https://www.tcpdump.org/manpages/pcap-filter.7.html.

        :param callback: This callback will be invoked once whenever a packet is captured with a single argument
            of type :class:`LinkLayerSniff`.
            Notice an important detail: the sniffer takes care of managing the link layer packets.
            The user does not need to care which type of data link layer encapsulation is used:
            it could be Ethernet, IEEE 802.15.4, or whatever.
            The application always gets a high-level view of the data with the link-layer specifics abstracted away.
            This function may be invoked directly from a worker thread, so be sure to apply synchronization.
        """
        import libpcap as pcap

        self._address_families = list(address_families)
        self._filter_expr = str(filter_expression)
        self._callback = callback
        self._keep_going = True

        # Find devices that we can work with.
        dev_names = _filter_devices(self._address_families)
        _logger.debug('Capturable network devices that support address families %s: %s',
                      self._address_families, dev_names)

        # Begin capture on all devices (that support the specified address family) in promiscuous mode.
        # We can't use "any" because libpcap does not support promiscuous mode with it:
        # https://github.com/the-tcpdump-group/libpcap/blob/bcca74d2713dc9c0a27992102c469f77bdd8dd1f/pcap-linux.c#L2522
        # It shouldn't be a problem because we have our filter expression that is expected to be highly efficient.
        # Devices whose ifaces are down will be filtered out here.
        self._caps: typing.List[typing.Tuple[str, object, LinkLayerPacket.Decoder]] = []
        try:
            for name in dev_names:
                pd = _try_begin_capture(name, self._filter_expr, pcap.DLT_EN10MB)
                if pd is not None:
                    data_link_type = pcap.datalink(pd)
                    enc_dec = LinkLayerPacket.get_enc_dec(data_link_type)
                    if enc_dec:
                        self._caps.append((name, pd, enc_dec[1]))
                    else:
                        pcap.close(pd)
                        _logger.critical(
                            f'Device {name!r} cannot be used for packet capture because its link layer type '
                            f'{data_link_type} is not yet supported by this library. A pull request would be welcome!'
                        )
        except Exception:
            for c in self._caps:
                pcap.close(c)
            raise
        if not self._caps:
            raise TransportError(
                f'There are no devices available for capture at the moment. Evaluated candidates: {dev_names}'
            )
        _logger.info('Capture sessions have been set up on: %s', list(n for n, _, _ in self._caps))

        self._workers = [
            threading.Thread(target=self._thread_worker,
                             name=f'pcap_worker_{name}',
                             args=(name, pd, decoder),
                             daemon=True)
            for name, pd, decoder in self._caps
        ]
        for w in self._workers:
            w.start()

    def close(self) -> None:
        import libpcap as pcap
        self._keep_going = False
        # TODO: this is not thread-safe.
        for _, pd, _ in self._caps:
            pcap.close(pd)

    def _thread_worker(self, name: str, pd: object, decoder: LinkLayerPacket.Decoder) -> None:
        import libpcap as pcap
        assert isinstance(pd, ctypes.POINTER(pcap.pcap_t))
        _logger.debug('Worker thread for %r is started', name)

        # noinspection PyTypeChecker
        @pcap.pcap_handler  # type: ignore
        def proxy(_: object, header: ctypes.Structure, packet: typing.Any) -> None:
            # Parse the header, extract the timestamp and the packet length.
            header = header.contents
            ts_ns = (header.ts.tv_sec * 1_000_000 + header.ts.tv_usec) * 1000
            ts = Timestamp(ts_ns, monotonic_ns=time.monotonic_ns())
            length, real_length = header.caplen, header.len
            _logger.debug('Captured: ts=%s dev=%r len=%d bytes', ts, name, length)
            if real_length != length:
                # This should never occur because we use a huge capture buffer.
                _logger.critical(f'Length mismatch in a packet captured from {name!r}: '
                                 f'real {real_length} bytes, captured {length} bytes')
            # Create a copy of the payload. This is required per the libpcap API contract -- it says that the
            # memory is invalidated upon return from the callback.
            packet = memoryview(ctypes.cast(packet,
                                            ctypes.POINTER(ctypes.c_ubyte * length))[0]).tobytes()
            llp = decoder(memoryview(packet))
            if llp is None:
                _logger.info('Link-layer packet of %d bytes captured from %r could not be parsed', len(packet), name)
            else:
                self._callback(LinkLayerSniff(timestamp=ts, packet=llp, device_name=name))

        try:
            while self._keep_going:
                err = pcap.dispatch(pd, -1, proxy, ctypes.POINTER(ctypes.c_ubyte)())
                if err != 0:
                    if self._keep_going:
                        _logger.critical(f'Worker thread for %r has failed with error %s; %s',
                                         name, pcap.statustostr(err), pcap.geterr(pd).decode())
                    else:
                        _logger.debug('Failure in worker thread for %r ignored because it is commanded to stop: %s',
                                      name, pcap.statustostr(err))
                    break
        except Exception as ex:
            _logger.exception('Unhandled exception in worker thread for %r; stopping: %r', name, ex)
        finally:
            pcap.close(pd)
        _logger.debug('Worker thread for %r is being terminated', name)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self,
                                             address_families=self._address_families,
                                             filter_expression=self._filter_expr)


def _filter_devices(address_families: typing.Sequence[int]) -> typing.List[str]:
    """
    Returns a list of local network devices that have at least one address from the specified list of address
    families. This is needed so that we won't attempt capturing Ethernet frames on a CAN device, for instance.
    Such filtering automatically excludes devices whose interfaces are down, since they don't have any address.
    """
    import libpcap as pcap
    err_buf = ctypes.create_string_buffer(pcap.PCAP_ERRBUF_SIZE)
    devices = ctypes.POINTER(pcap.pcap_if_t)()
    if pcap.findalldevs(ctypes.byref(devices), err_buf) != 0:
        raise TransportError(f"Could not list network devices: {err_buf.value.decode()}")
    dev_names: typing.List[str] = []
    d = typing.cast(ctypes.Structure, devices)
    while d:
        d = d.contents
        # noinspection PyUnresolvedReferences
        a = d.addresses
        while a:
            a = a.contents
            if a.addr and a.addr.contents.sa_family in address_families:
                # noinspection PyUnresolvedReferences
                dev_names.append(d.name.decode())
                break
            a = a.next
        d = d.next
    pcap.freealldevs(devices)
    return dev_names


def _try_begin_capture(device:            str,
                       filter_expression: str,
                       data_link_hint:    int) -> typing.Optional[object]:
    """
    Returns None if the interface managed by this device is not up.

    If the device does not support the specified data link type, a log message is emitted but no error is raised.
    The available link types are listed in https://www.tcpdump.org/linktypes.html.
    """
    import libpcap as pcap

    err_buf = ctypes.create_string_buffer(pcap.PCAP_ERRBUF_SIZE)
    pd = pcap.create(device.encode(), err_buf)
    if pd is None:
        raise TransportError(f"Could not instantiate pcap_t for {device!r}: {err_buf.value.decode()}")
    try:
        err = pcap.set_snaplen(pd, _SNAPSHOT_LENGTH)
        if err != 0:
            raise TransportError(f"Could not set snapshot length for {device!r}: {pcap.statustostr(err).decode()}")

        err = pcap.set_timeout(pd, int(_BUFFER_TIMEOUT * 1e3))
        if err != 0:
            raise TransportError(f"Could not set timeout for {device!r}: {pcap.statustostr(err).decode()}")

        err = pcap.set_promisc(pd, 1)
        if err != 0:
            raise TransportError(f"Could not enable promiscuous mode for {device!r}: {pcap.statustostr(err).decode()}")

        err = pcap.activate(pd)
        if err == pcap.PCAP_ERROR_IFACE_NOT_UP:
            _logger.info('Skipping device %r because the iface is not up. %s',
                         device, pcap.statustostr(err).decode())
            pcap.close(pd)
            return None
        if err in (pcap.PCAP_ERROR_PERM_DENIED, pcap.PCAP_ERROR_PROMISC_PERM_DENIED):
            raise PermissionError(f"Capture is not permitted on {device!r}: {pcap.statustostr(err).decode()}")
        if err < 0:
            raise TransportError(f"Could not activate capture on {device!r}: {pcap.statustostr(err).decode()}; "
                                 f"{pcap.geterr(pd).decode()}")
        if err > 0:
            _logger.warning("Capture on %r started successfully, but libpcap reported a warning: %s",
                            device, pcap.statustostr(err).decode())

        # https://www.tcpdump.org/manpages/pcap_set_datalink.3pcap.html
        err = pcap.set_datalink(pd, data_link_hint)
        if err != 0:
            _logger.info("Device %r does not appear to support data link type %r: %s",
                         device, data_link_hint, pcap.geterr(pd).decode())

        # https://www.tcpdump.org/manpages/pcap_compile.3pcap.html
        code = pcap.bpf_program()  # This memory needs to be freed when closed. Fix it later.
        err = pcap.compile(pd, ctypes.byref(code), filter_expression.encode(), 1, pcap.PCAP_NETMASK_UNKNOWN)
        if err != 0:
            raise TransportError(
                f"Could not compile filter expression {filter_expression!r}: {pcap.statustostr(err).decode()}; "
                f"{pcap.geterr(pd).decode()}"
            )

        err = pcap.setfilter(pd, ctypes.byref(code))
        if err != 0:
            raise TransportError(f"Could not install filter: {pcap.statustostr(err).decode()}; "
                                 f"{pcap.geterr(pd).decode()}")
    except Exception:
        pcap.close(pd)
        raise
    return typing.cast(object, pd)


_SNAPSHOT_LENGTH = 65_536
"""
The doc says: "A snapshot length of 65535 should be sufficient, on most if not all networks,
to capture all the data available from the packet."
"""

_BUFFER_TIMEOUT = 0.005
"""
See "packet buffer timeout" in https://www.tcpdump.org/manpages/pcap.3pcap.html.
This value should be sensible for any kind of real-time monitoring application.
"""
