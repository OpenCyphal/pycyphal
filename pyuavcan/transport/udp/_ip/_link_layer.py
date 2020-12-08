#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import sys
import time
import typing
import ctypes
import socket
from socket import AddressFamily
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
    protocol: AddressFamily
    """
    The protocol encapsulated inside the link-layer packet; e.g., IPv6.
    """

    source:      memoryview
    destination: memoryview
    """
    Link-layer addresses, if applicable. If not supported by the link layer, they are to be empty.
    """

    payload: memoryview
    """
    The packet of the specified protocol.
    """

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
                                             protocol=str(self.protocol),
                                             source=bytes(self.source).hex(),
                                             destination=bytes(self.destination).hex(),
                                             payload=pld)

    Encoder = typing.Callable[['LinkLayerPacket'], typing.Optional[memoryview]]
    Decoder = typing.Callable[[memoryview], typing.Optional['LinkLayerPacket']]

    @staticmethod
    def get_encoder_decoder(data_link_type: int) -> typing.Optional[typing.Tuple[Encoder, Decoder]]:
        """
        A factory of paired encode/decode functions that are used for building and parsing link-layer packets.
        If the supplied link layer type code (from libpcap ``DLT_*``) is not supported, returns None.
        See https://www.tcpdump.org/linktypes.html.

        The encoder returns None if the encapsulated protocol is not supported by the selected link layer.
        The decoder returns None if the packet is not valid or the encapsulated protocol is not supported.
        """
        import libpcap as pcap  # type: ignore
        if data_link_type == pcap.DLT_EN10MB:
            # https://en.wikipedia.org/wiki/EtherType
            af_to_ethertype = {
                AddressFamily.AF_INET: 0x0800,
                AddressFamily.AF_INET6: 0x86DD,
            }
            ethertype_to_af = {v: k for k, v in af_to_ethertype.items()}

            def encode(p: LinkLayerPacket) -> typing.Optional[memoryview]:
                try:
                    return memoryview(b''.join((
                        bytes(p.source).rjust(6, b'\x00')[:6],
                        bytes(p.destination).rjust(6, b'\x00')[:6],
                        af_to_ethertype[p.protocol].to_bytes(2, 'big'),
                        p.payload,
                    )))
                except LookupError:
                    return None

            def decode(p: memoryview) -> typing.Optional[LinkLayerPacket]:
                if len(p) < 14:
                    return None
                src = p[0:6]
                dst = p[6:12]
                ethertype = int.from_bytes(p[12:14], 'big')
                try:
                    protocol = ethertype_to_af[ethertype]
                except LookupError:
                    return None
                return LinkLayerPacket(protocol=protocol, source=src, destination=dst, payload=p[14:])

            return encode, decode
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

    Once a new instance is constructed, it is launched immediately.
    Execution is carried out in a background thread pool.
    It is required to call :meth:`close` when done.

    If a new network device is added or re-initialized while the sniffer is running, it will not be recognized.
    Removal or a re-configuration of a device while the sniffer is running may cause it to fail.

    Should a worker thread encounter an error (e.g., if the device becomes unavailable), its capture context
    is closed automatically and then the thread is terminated.
    Such occurrences are logged at the CRITICAL severity level.

    - https://www.tcpdump.org/manpages/pcap.3pcap.html
    - https://github.com/karpierz/libpcap/blob/master/tests/capturetest.py
    """

    def __init__(self,
                 address_families:  typing.Iterable[AddressFamily],
                 filter_expression: str,
                 callback:          typing.Callable[[LinkLayerSniff], None]) -> None:
        """
        :param address_families: Collection of ``socket.AF_*`` constants defining which types of network to use.
            This duplicates the filter expression somewhat but it allows the sniffer to automatically determine
            which network interface devices to use and which ones to ignore.

        :param filter_expression: The standard pcap filter expression;
            see https://www.tcpdump.org/manpages/pcap-filter.7.html.
            Use Wireshark for testing filter expressions.

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
        caps: typing.List[typing.Tuple[str, object, LinkLayerPacket.Decoder]] = []
        try:
            for name in dev_names:
                pd = _try_begin_capture(name, self._filter_expr, pcap.DLT_EN10MB)
                if pd is not None:
                    data_link_type = pcap.datalink(pd)
                    enc_dec = LinkLayerPacket.get_encoder_decoder(data_link_type)
                    if enc_dec:
                        caps.append((name, pd, enc_dec[1]))
                    else:
                        pcap.close(pd)
                        _logger.critical(
                            f'Device {name!r} cannot be used for packet capture because its data link layer type='
                            f'{data_link_type} is not yet supported by this library. A pull request would be welcome!'
                        )
        except Exception:
            for c in caps:
                pcap.close(c)
            raise
        if not caps:
            raise TransportError(
                f'There are no devices available for capture at the moment. Evaluated candidates: {dev_names}'
            )
        _logger.info('Capture sessions have been set up on: %s', list(n for n, _, _ in caps))

        self._workers = [
            threading.Thread(target=self._thread_worker,
                             name=f'pcap_worker_{name}',
                             args=(name, pd, decoder),
                             daemon=True)
            for name, pd, decoder in caps
        ]
        for w in self._workers:
            w.start()

    @property
    def is_stable(self) -> bool:
        """
        True if all devices detected during the initial configuration are still being captured from.
        If at least one of them failed (e.g., due to a system reconfiguration), this value would be false.
        """
        return all(x.is_alive() for x in self._workers)

    def close(self) -> None:
        """
        After closing the callback reference is immediately destroyed to prevent the receiver from being kept alive
        by the not-yet-terminated worker threads and to prevent residual packets from generating spurious events.
        """
        self._keep_going = False
        self._callback = lambda *_: None
        # This is not a great solution, honestly. Consider improving it later.
        # Currently we just unbind the callback from the user-supplied destination and mark that the threads should
        # terminate. The sniffer is then left in a locked-in state, where it may keep performing some no-longer-useful
        # activities in the background, but they remain invisible to the outside world. Eventually, the instance will
        # be disposed after the last worker is terminated, but we should make it more deterministic.

    def _thread_worker(self, name: str, pd: object, decoder: LinkLayerPacket.Decoder) -> None:
        import libpcap as pcap
        assert isinstance(pd, ctypes.POINTER(pcap.pcap_t))
        try:
            _logger.debug('Worker thread for %r is started: %s', name, threading.current_thread())

            # noinspection PyTypeChecker
            @pcap.pcap_handler  # type: ignore
            def proxy(_: object, header: ctypes.Structure, packet: typing.Any) -> None:
                # Parse the header, extract the timestamp and the packet length.
                header = header.contents
                ts_ns = (header.ts.tv_sec * 1_000_000 + header.ts.tv_usec) * 1000
                ts = Timestamp(system_ns=ts_ns, monotonic_ns=time.monotonic_ns())
                length, real_length = header.caplen, header.len
                _logger.debug('CAPTURED PACKET ts=%s dev=%r len=%d bytes', ts, name, length)
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
                    if _logger.isEnabledFor(logging.INFO):
                        _logger.info('Link-layer packet of %d bytes captured from %r at %s could not be parsed. '
                                     'The header is: %s',
                                     len(packet), name, ts, packet[:32].hex())
                else:
                    self._callback(LinkLayerSniff(timestamp=ts, packet=llp, device_name=name))

            packets_per_batch = 100
            while self._keep_going:
                err = pcap.dispatch(pd, packets_per_batch, proxy, ctypes.POINTER(ctypes.c_ubyte)())
                if err < 0:  # Negative values represent errors, otherwise it's the number of packets processed.
                    if self._keep_going:
                        _logger.critical(f'Worker thread for %r has failed with error %s; %s',
                                         name, pcap.statustostr(err), pcap.geterr(pd).decode())
                    else:
                        _logger.debug('Error in worker thread for %r ignored because it is commanded to stop: %s',
                                      name, pcap.statustostr(err))
                    break
        except Exception as ex:
            _logger.exception('Unhandled exception in worker thread for %r; stopping: %r', name, ex)
        finally:
            # BEWARE: pcap_close() is not idempotent! Second close causes a heap corruption. *sigh*
            pcap.close(pd)
        _logger.debug('Worker thread for %r is being terminated', name)

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self,
                                             address_families=self._address_families,
                                             filter_expression=repr(self._filter_expr))


def _filter_devices(address_families: typing.Sequence[AddressFamily]) -> typing.List[str]:
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
    if not devices:
        # This may seem odd, but libpcap returns an empty list if the user is not allowed to perform capture.
        # This is documented in the API docs as follows:
        #   Note that there may be network devices that cannot be opened by the process calling pcap_findalldevs(),
        #   because, for example, that process does not have sufficient privileges to open them for capturing;
        #   if so, those devices will not appear on the list.
        raise PermissionError("No capturable devices have been found. Do you have the required privileges?")
    dev_names: typing.List[str] = []
    d = typing.cast(ctypes.Structure, devices)
    while d:
        d = d.contents
        name = d.name.decode()
        if name == 'any':
            _logger.debug('Synthetic device %r does not support promiscuous mode, skipping', name)
        else:
            a = d.addresses
            while a:
                a = a.contents
                if a.addr and a.addr.contents.sa_family in address_families:
                    dev_names.append(name)
                    break
                a = a.next
            else:
                _logger.debug('Device %r is incompatible with requested address families %s, skipping',
                              name, address_families)
        d = d.next
    pcap.freealldevs(devices)
    return dev_names


def _try_begin_capture(device: str, filter_expression: str, data_link_hint: int) -> typing.Optional[object]:
    """
    Returns None if the interface managed by this device is not up.

    If the device does not support the specified data link type, a log message is emitted but no error is raised.
    The available link types are listed in https://www.tcpdump.org/linktypes.html.
    """
    import libpcap as pcap

    # This is helpful: https://github.com/karpierz/libpcap/blob/master/tests/capturetest.py
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


_SNAPSHOT_LENGTH = 2 ** 16
"""
The doc says: "A snapshot length of 65535 should be sufficient, on most if not all networks,
to capture all the data available from the packet."
"""

_BUFFER_TIMEOUT = 0.005
"""
See "packet buffer timeout" in https://www.tcpdump.org/manpages/pcap.3pcap.html.
This value should be sensible for any kind of real-time monitoring application.
"""


def _apply_windows_workarounds() -> None:  # pragma: no cover
    import os
    import pathlib
    import importlib.util
    # This is a Windows Server-specific workaround for this libpcap issue: https://github.com/karpierz/libpcap/issues/7
    # tl;dr: It works on desktop Windows 8/10, but Windows Server 2019 is unable to find "wpcap.dll" unless the
    # DLL search path is specified manually via PATH. The workaround is valid per libpcap==1.10.0b15.
    # Later versions of libpcap may not require it, so please consider removing it in the future.
    spec = importlib.util.find_spec('libpcap')
    if spec:
        is_64_bit = sys.maxsize.bit_length() > 32
        libpcap_dir = pathlib.Path(spec.origin).parent
        dll_path = libpcap_dir / '_platform' / '_windows' / ('x64' if is_64_bit else 'x86') / 'wpcap'
        os.environ['PATH'] += os.pathsep + str(dll_path)


if sys.platform.startswith('win'):  # pragma: no cover
    _apply_windows_workarounds()


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_encode_decode_ethernet() -> None:
    import libpcap as pcap

    mv = memoryview

    enc_dec = LinkLayerPacket.get_encoder_decoder(pcap.DLT_EN10MB)
    assert enc_dec
    enc, dec = enc_dec
    llp = dec(mv(b'\x11\x22\x33\x44\x55\x66' + b'\xAA\xBB\xCC\xDD\xEE\xFF' + b'\x08\x00' + b'abcd'))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b'\x11\x22\x33\x44\x55\x66'
    assert llp.destination == b'\xAA\xBB\xCC\xDD\xEE\xFF'
    assert llp.payload == b'abcd'
    assert str(llp) == (
        "LinkLayerPacket(protocol=AddressFamily.AF_INET, "
        + "source=112233445566, destination=aabbccddeeff, payload=61626364)"
    )

    llp = dec(mv(b'\x11\x22\x33\x44\x55\x66' + b'\xAA\xBB\xCC\xDD\xEE\xFF' + b'\x08\x00'))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.source == b'\x11\x22\x33\x44\x55\x66'
    assert llp.destination == b'\xAA\xBB\xCC\xDD\xEE\xFF'
    assert llp.payload == b''

    assert enc(LinkLayerPacket(
        protocol=AddressFamily.AF_INET6,
        source=mv(b'\x11\x22'),
        destination=mv(b'\xAA\xBB\xCC'),
        payload=mv(b'abcd'),
    )) == b'\x00\x00\x00\x00\x11\x22' + b'\x00\x00\x00\xAA\xBB\xCC' + b'\x86\xDD' + b'abcd'

    assert enc(LinkLayerPacket(
        protocol=AddressFamily.AF_IRDA,  # Unsupported encapsulation
        source=mv(b'\x11\x22'),
        destination=mv(b'\xAA\xBB\xCC'),
        payload=mv(b'abcd'),
    )) is None

    assert dec(mv(b'')) is None
    assert dec(mv(b'\x11\x22\x33\x44\x55\x66' + b'\xAA\xBB\xCC\xDD\xEE\xFF' + b'\xAA\xAA' + b'abcdef')) is None
    # Bad ethertype/length
    assert dec(mv(b'\x11\x22\x33\x44\x55\x66' + b'\xAA\xBB\xCC\xDD\xEE\xFF' + b'\x00\xFF' + b'abcdef')) is None


def _unittest_filter_devices() -> None:
    import sys

    assert not _filter_devices([])  # No address families -- no devices.

    devices = _filter_devices([socket.AF_INET, socket.AF_INET6])
    print('IPv4/6:', devices)
    assert len(devices) >= 2  # One loopback, at least one external
    if sys.platform.startswith('linux'):
        assert 'lo' in devices


def _unittest_sniff() -> None:
    ts_last = Timestamp.now()
    sniffs: typing.List[LinkLayerPacket] = []

    def callback(lls: LinkLayerSniff) -> None:
        nonlocal ts_last
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= lls.timestamp.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= lls.timestamp.system_ns <= now.system_ns
        ts_last = lls.timestamp
        sniffs.append(lls.packet)

    filter_expression = 'udp and src net 127.66.0.0/16'
    sn = LinkLayerSniffer([socket.AF_INET], filter_expression, callback)
    assert sn.is_stable

    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        b.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton('127.42.0.123'))
        b.bind(('127.42.0.123', 0))  # Some random noise on an adjacent subnet.
        for i in range(10):
            b.sendto(f'{i:04x}'.encode(), ('127.66.1.200', 3333))  # Ignored unicast
            b.sendto(f'{i:04x}'.encode(), ('239.66.1.200', 4444))  # Ignored multicast
            time.sleep(0.1)

        time.sleep(1)
        assert sniffs == []  # Make sure we are not picking up any noise.

        a.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton('127.66.33.44'))
        a.bind(('127.66.33.44', 0))  # This one is on our local subnet, it should be heard.
        a.sendto(b'\xAA\xAA\xAA\xAA', ('127.66.1.200', 1234))  # Accepted unicast inside subnet
        a.sendto(b'\xBB\xBB\xBB\xBB', ('127.33.1.200', 5678))  # Accepted unicast outside subnet
        a.sendto(b'\xCC\xCC\xCC\xCC', ('239.66.1.200', 9012))  # Accepted multicast

        b.sendto(b'x', ('127.66.1.200', 5555))  # Ignored unicast
        b.sendto(b'y', ('239.66.1.200', 6666))  # Ignored multicast

        time.sleep(3)

        # Validate the received callbacks.
        print(sniffs[0])
        print(sniffs[1])
        print(sniffs[2])
        assert len(sniffs) == 3
        # Assume the packets are not reordered (why would they be?)
        assert b'\xAA\xAA\xAA\xAA' in bytes(sniffs[0].payload)
        assert b'\xBB\xBB\xBB\xBB' in bytes(sniffs[1].payload)
        assert b'\xCC\xCC\xCC\xCC' in bytes(sniffs[2].payload)

        sniffs.clear()
        sn.close()

        time.sleep(1)
        a.sendto(b'd', ('127.66.1.100', 4321))
        time.sleep(1)
        assert sniffs == []  # Should be terminated.
    finally:
        sn.close()
        a.close()
        b.close()


def _unittest_sniff_errors() -> None:
    from pytest import raises

    with raises(TransportError, match=r'.*no devices.*'):
        LinkLayerSniffer([9999], '', lambda x: None)  # type: ignore

    with raises(TransportError, match=r'.*filter expression.*'):
        LinkLayerSniffer([AddressFamily.AF_INET6], 'invalid filter expression', lambda x: None)
