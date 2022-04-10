# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import sys
import time
from typing import Callable, Any, Optional, cast, Sequence
import ctypes
import socket
import logging
import threading
import dataclasses
import pycyphal
from pycyphal.transport import Timestamp


_logger = logging.getLogger(__name__)


class LinkLayerError(pycyphal.transport.TransportError):
    pass


class LinkLayerCaptureError(LinkLayerError):
    pass


@dataclasses.dataclass(frozen=True)
class LinkLayerPacket:
    """
    OSI L2 packet representation.
    The addresses are represented here in the link-native byte order (big endian for Ethernet).
    """

    protocol: socket.AddressFamily
    """
    The protocol encapsulated inside this link-layer packet; e.g., IPv6.
    """

    source: memoryview
    destination: memoryview
    """
    Link-layer addresses, if applicable. Empty if not supported by the link layer.
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
            pld = bytes(self.payload[:limit]).hex() + "..."
        return pycyphal.util.repr_attributes(
            self,
            protocol=str(self.protocol),
            source=bytes(self.source).hex(),
            destination=bytes(self.destination).hex(),
            payload=pld,
        )


@dataclasses.dataclass(frozen=True)
class LinkLayerCapture:
    timestamp: Timestamp
    packet: LinkLayerPacket
    device_name: str
    # Do we also need to report the link layer type here?


class LinkLayerSniffer:
    """
    This wrapper is intended to insulate the rest of the transport implementation from the specifics of the
    libpcap wrapper implementation (there are dozens of different wrappers out there).
    Observe that anything libpcap-related shall not be imported outside of these methods because we only require
    this dependency if protocol sniffing capability is needed.
    Regular use of the library should be possible without libpcap installed.

    Once a new instance is constructed, it is launched immediately.
    Execution is carried out in a background daemon thread pool.
    It is required to call :meth:`close` when done, which will hint the worker threads to terminate soon.

    If a new network device is added or re-initialized while the sniffer is running, it will not be recognized.
    Removal or a re-configuration of a device while the sniffer is running may cause it to fail,
    which will be logged from the worker threads.

    Should a worker thread encounter an error (e.g., if the device becomes unavailable), its capture context
    is closed automatically and then the thread is terminated.
    Such occurrences are logged at the CRITICAL severity level.

    - https://www.tcpdump.org/manpages/pcap.3pcap.html
    - https://github.com/karpierz/libpcap/blob/master/tests/capturetest.py
    """

    def __init__(self, filter_expression: str, callback: Callable[[LinkLayerCapture], None]) -> None:
        """
        :param filter_expression: The standard pcap filter expression;
            see https://www.tcpdump.org/manpages/pcap-filter.7.html.
            Use Wireshark for testing filter expressions.

        :param callback: This callback will be invoked once whenever a packet is captured with a single argument
            of type :class:`LinkLayerCapture`.
            Notice an important detail: the sniffer takes care of managing the link layer packets.
            The user does not need to care which type of data link layer encapsulation is used:
            it could be Ethernet, IEEE 802.15.4, or whatever.
            The application always gets a high-level view of the data with the link-layer specifics abstracted away.
            This function may be invoked directly from a worker thread, so be sure to apply synchronization.
        """
        self._filter_expr = str(filter_expression)
        self._callback = callback
        self._keep_going = True
        self._workers: list[threading.Thread] = []
        try:
            dev_names = _find_devices()
            _logger.debug("Capturable network devices: %s", dev_names)
            caps = _capture_all(dev_names, filter_expression)
        except PermissionError:
            if sys.platform.startswith("linux"):
                suggestion = f'Run this:\nsudo setcap cap_net_raw+eip "$(readlink -f {sys.executable})"'
            elif sys.platform.startswith("win"):
                suggestion = "Make sure you have Npcap installed and configured properly: https://nmap.org/npcap"
            else:
                suggestion = ""
            raise PermissionError(
                f"You need special privileges to perform low-level network packet capture (sniffing). {suggestion}"
            ) from None
        if not caps:
            raise LinkLayerCaptureError(
                f"There are no devices available for packet capture at the moment. Evaluated candidates: {dev_names}"
            )
        self._workers = [
            threading.Thread(target=self._thread_worker, name=f"pcap_{name}", args=(name, pd, decoder), daemon=True)
            for name, pd, decoder in caps
        ]
        for w in self._workers:
            w.start()
        assert len(self._workers) > 0

    @property
    def is_stable(self) -> bool:
        """
        True if all devices detected during the initial configuration are still being captured from.
        If at least one of them failed (e.g., due to a system reconfiguration), this value would be false.
        """
        assert len(self._workers) > 0
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

    def _thread_worker(self, name: str, pd: object, decoder: PacketDecoder) -> None:
        import libpcap as pcap  # type: ignore

        assert isinstance(pd, ctypes.POINTER(pcap.pcap_t))
        try:
            _logger.debug("%r: Worker thread for %r is started: %s", self, name, threading.current_thread())

            # noinspection PyTypeChecker
            @pcap.pcap_handler  # type: ignore
            def proxy(_: object, header: ctypes.Structure, packet: Any) -> None:
                # Parse the header, extract the timestamp and the packet length.
                header = header.contents
                ts_ns = (header.ts.tv_sec * 1_000_000 + header.ts.tv_usec) * 1000
                ts = Timestamp(system_ns=ts_ns, monotonic_ns=time.monotonic_ns())
                length, real_length = header.caplen, header.len
                _logger.debug("%r: CAPTURED PACKET ts=%s dev=%r len=%d bytes", self, ts, name, length)
                if real_length != length:
                    # In theory, this should never occur because we use a huge capture buffer.
                    # On Windows, however, when using Npcap v0.96, the captured length is (always?) reported to be
                    # 32 bytes shorter than the real length, despite the fact that the packet is not truncated.
                    _logger.debug(
                        "%r: Length mismatch in a packet captured from %r: real %r bytes, captured %r bytes",
                        self,
                        name,
                        real_length,
                        length,
                    )
                # Create a copy of the payload. This is required per the libpcap API contract -- it says that the
                # memory is invalidated upon return from the callback.
                packet = memoryview(ctypes.cast(packet, ctypes.POINTER(ctypes.c_ubyte * length))[0]).tobytes()
                llp = decoder(memoryview(packet))
                if llp is None:
                    if _logger.isEnabledFor(logging.INFO):
                        _logger.info(
                            "%r: Link-layer packet of %d bytes captured from %r at %s could not be parsed. "
                            "The header is: %s",
                            self,
                            len(packet),
                            name,
                            ts,
                            packet[:32].hex(),
                        )
                else:
                    self._callback(LinkLayerCapture(timestamp=ts, packet=llp, device_name=name))

            packets_per_batch = 100
            while self._keep_going:
                err = pcap.dispatch(pd, packets_per_batch, proxy, ctypes.POINTER(ctypes.c_ubyte)())
                if err < 0:  # Negative values represent errors, otherwise it's the number of packets processed.
                    if self._keep_going:
                        _logger.critical(
                            "%r: Worker thread for %r has failed with error %s; %s",
                            self,
                            name,
                            err,
                            pcap.geterr(pd).decode(),
                        )
                    else:
                        _logger.debug(
                            "%r: Error %r in worker thread for %r ignored because it is commanded to stop",
                            self,
                            err,
                            name,
                        )
                    break
        except Exception as ex:
            _logger.exception("%r: Unhandled exception in worker thread for %r; stopping: %r", self, name, ex)
        finally:
            # BEWARE: pcap_close() is not idempotent! Second close causes a heap corruption. *sigh*
            pcap.close(pd)
        _logger.debug("%r: Worker thread for %r is being terminated", self, name)

    def __repr__(self) -> str:
        return pycyphal.util.repr_attributes(
            self,
            filter_expression=repr(self._filter_expr),
            num_devices=len(self._workers),
            num_devices_active=len(list(x.is_alive() for x in self._workers)),
        )


PacketEncoder = Callable[["LinkLayerPacket"], Optional[memoryview]]
PacketDecoder = Callable[[memoryview], Optional["LinkLayerPacket"]]


def _get_codecs() -> dict[int, tuple[PacketEncoder, PacketDecoder]]:
    """
    A factory of paired encode/decode functions that are used for building and parsing link-layer packets.
    The pairs are organized into a dict where the key is the data link type code from libpcap;
    see https://www.tcpdump.org/linktypes.html.
    The dict is ordered such that the recommended data link types come first.
    This is useful when setting up packet capture if the adapter supports multiple link layer formats.

    The encoder returns None if the encapsulated protocol is not supported by the selected link layer.
    The decoder returns None if the packet is not valid or the encapsulated protocol is not supported.
    """
    import libpcap as pcap
    from socket import AddressFamily

    def get_ethernet() -> tuple[PacketEncoder, PacketDecoder]:
        # https://en.wikipedia.org/wiki/EtherType
        af_to_ethertype = {
            AddressFamily.AF_INET: 0x0800,
            AddressFamily.AF_INET6: 0x86DD,
        }
        ethertype_to_af = {v: k for k, v in af_to_ethertype.items()}

        def enc(p: LinkLayerPacket) -> Optional[memoryview]:
            try:
                return memoryview(
                    b"".join(
                        (
                            bytes(p.source).rjust(6, b"\x00")[:6],
                            bytes(p.destination).rjust(6, b"\x00")[:6],
                            af_to_ethertype[p.protocol].to_bytes(2, "big"),
                            p.payload,
                        )
                    )
                )
            except LookupError:
                return None

        def dec(p: memoryview) -> Optional[LinkLayerPacket]:
            if len(p) < 14:
                return None
            src = p[0:6]
            dst = p[6:12]
            ethertype = int.from_bytes(p[12:14], "big")
            try:
                protocol = ethertype_to_af[ethertype]
            except LookupError:
                return None
            return LinkLayerPacket(protocol=protocol, source=src, destination=dst, payload=p[14:])

        return enc, dec

    def get_loopback(byte_order: str) -> tuple[PacketEncoder, PacketDecoder]:
        # DLT_NULL is used by the Windows loopback interface. Info: https://wiki.wireshark.org/NullLoopback
        # The source and destination addresses are not representable in this data link layer.
        def enc(p: LinkLayerPacket) -> Optional[memoryview]:
            return memoryview(b"".join((p.protocol.to_bytes(4, byte_order), p.payload)))  # type: ignore

        def dec(p: memoryview) -> Optional[LinkLayerPacket]:
            if len(p) < 4:
                return None
            try:
                protocol = AddressFamily(int.from_bytes(p[0:4], byte_order))  # type: ignore
            except ValueError:
                return None
            empty = memoryview(b"")
            return LinkLayerPacket(protocol=protocol, source=empty, destination=empty, payload=p[4:])

        return enc, dec

    # The output is ORDERED, best option first.
    return {
        pcap.DLT_EN10MB: get_ethernet(),
        pcap.DLT_LOOP: get_loopback("big"),
        pcap.DLT_NULL: get_loopback(sys.byteorder),
    }


def _find_devices() -> list[str]:
    """
    Returns a list of local network devices that can be captured from.
    Raises a PermissionError if the user is suspected to lack the privileges necessary for capture.

    We used to filter the devices by address family, but it turned out to be a dysfunctional solution because
    a device does not necessarily have to have an address in a particular family to be able to capture packets
    of that kind. For instance, on Windows, a virtual network adapter may have no addresses while still being
    able to capture packets.
    """
    import libpcap as pcap

    err_buf = ctypes.create_string_buffer(pcap.PCAP_ERRBUF_SIZE)
    devices = ctypes.POINTER(pcap.pcap_if_t)()
    if pcap.findalldevs(ctypes.byref(devices), err_buf) != 0:
        raise LinkLayerError(f"Could not list network devices: {err_buf.value.decode()}")
    if not devices:
        # This may seem odd, but libpcap returns an empty list if the user is not allowed to perform capture.
        # This is documented in the API docs as follows:
        #   Note that there may be network devices that cannot be opened by the process calling pcap_findalldevs(),
        #   because, for example, that process does not have sufficient privileges to open them for capturing;
        #   if so, those devices will not appear on the list.
        raise PermissionError("No capturable devices have been found. Do you have the required privileges?")
    dev_names: list[str] = []
    d = cast(ctypes.Structure, devices)
    while d:
        d = d.contents
        name = d.name.decode()
        if name != "any":
            dev_names.append(name)
        else:
            _logger.debug("Synthetic device %r does not support promiscuous mode, skipping", name)
        d = d.next
    pcap.freealldevs(devices)
    return dev_names


def _capture_all(device_names: list[str], filter_expression: str) -> list[tuple[str, object, PacketDecoder]]:
    """
    Begin capture on all devices in promiscuous mode.
    We can't use "any" because libpcap does not support promiscuous mode with it, as stated in the docs and here:
    https://github.com/the-tcpdump-group/libpcap/blob/bcca74d2713dc9c0a27992102c469f77bdd8dd1f/pcap-linux.c#L2522.
    It shouldn't be a problem because we have our filter expression that is expected to be highly efficient.
    Devices whose ifaces are down or that are not usable for other valid reasons will be silently filtered out here.
    """
    import libpcap as pcap

    codecs = _get_codecs()
    caps: list[tuple[str, object, PacketDecoder]] = []
    try:
        for name in device_names:
            pd = _capture_single_device(name, filter_expression, list(codecs.keys()))
            if pd is None:
                _logger.info("Could not set up capture on %r", name)
                continue
            data_link_type = pcap.datalink(pd)
            try:
                _, dec = codecs[data_link_type]
            except LookupError:
                # This is where we filter out devices that certainly have no relevance, like CAN adapters.
                pcap.close(pd)
                _logger.info(
                    "Device %r will not be used for packet capture because its data link layer type=%r "
                    "is not supported by this library. Either the device is irrelevant, "
                    "or the library needs to be extended to support this link layer protocol.",
                    name,
                    data_link_type,
                )
            else:
                caps.append((name, pd, dec))
    except Exception:
        for _, c, _ in caps:
            pcap.close(c)
        raise
    _logger.info(
        "Capture sessions with filter %r have been set up on: %s", filter_expression, list(n for n, _, _ in caps)
    )
    return caps


def _capture_single_device(device: str, filter_expression: str, data_link_hints: Sequence[int]) -> Optional[object]:
    """
    Returns None if the interface managed by this device is not up or if it cannot be captured from for other reasons.
    On GNU/Linux, some virtual devices (like netfilter devices) can only be accessed by a superuser.

    The function will configure libpcap to use the first supported data link type from the list.
    If none of the specified data link types are supported, a log message is emitted but no error is raised.
    The available link types are listed in https://www.tcpdump.org/linktypes.html.
    """
    import libpcap as pcap

    def status_to_str(error_code: int) -> str:
        """
        Some libpcap-compatible libraries (e.g., WinPCap) do not have this function, so we have to define a fallback.
        """
        try:
            return str(pcap.statustostr(error_code).decode())
        except AttributeError:  # pragma: no cover
            return f"[error {error_code}]"

    # This is helpful: https://github.com/karpierz/libpcap/blob/master/tests/capturetest.py
    err_buf = ctypes.create_string_buffer(pcap.PCAP_ERRBUF_SIZE)
    pd = pcap.create(device.encode(), err_buf)
    if pd is None:
        raise LinkLayerCaptureError(f"Could not instantiate pcap_t for {device!r}: {err_buf.value.decode()}")
    try:
        # Non-fatal errors are intentionally logged at a low severity level to not disturb the user unnecessarily.
        err = pcap.set_snaplen(pd, _SNAPSHOT_LENGTH)
        if err != 0:
            _logger.info("Could not set snapshot length for %r: %r", device, status_to_str(err))

        err = pcap.set_timeout(pd, int(_BUFFER_TIMEOUT * 1e3))
        if err != 0:
            _logger.info("Could not set timeout for %r: %r", device, status_to_str(err))

        err = pcap.set_promisc(pd, 1)
        if err != 0:
            _logger.info("Could not enable promiscuous mode for %r: %r", device, status_to_str(err))

        err = pcap.activate(pd)
        if err in (pcap.PCAP_ERROR_PERM_DENIED, pcap.PCAP_ERROR_PROMISC_PERM_DENIED):
            raise PermissionError(f"Capture is not permitted on {device!r}: {status_to_str(err)}")
        if err == pcap.PCAP_ERROR_IFACE_NOT_UP:
            _logger.debug("Device %r is not capturable because the iface is not up. %s", device, status_to_str(err))
            pcap.close(pd)
            return None
        if err < 0:
            _logger.info(
                "Could not activate capture on %r: %s; %s", device, status_to_str(err), pcap.geterr(pd).decode()
            )
            pcap.close(pd)
            return None
        if err > 0:
            _logger.info(
                "Capture on %r started successfully, but libpcap reported a warning: %s", device, status_to_str(err)
            )

        # https://www.tcpdump.org/manpages/pcap_set_datalink.3pcap.html
        for dlt in data_link_hints:
            err = pcap.set_datalink(pd, dlt)
            if err == 0:
                _logger.debug("Device %r is configured to use the data link type %r", device, dlt)
                break
        else:
            _logger.debug(
                "Device %r supports none of the following data link types: %r. Last error was: %s",
                device,
                list(data_link_hints),
                pcap.geterr(pd).decode(),
            )
            return None

        # https://www.tcpdump.org/manpages/pcap_compile.3pcap.html
        code = pcap.bpf_program()  # This memory needs to be freed when closed. Fix it later.
        err = pcap.compile(pd, ctypes.byref(code), filter_expression.encode(), 1, pcap.PCAP_NETMASK_UNKNOWN)
        if err != 0:
            raise LinkLayerCaptureError(
                f"Could not compile filter expression {filter_expression!r}: {status_to_str(err)}; "
                f"{pcap.geterr(pd).decode()}"
            )
        err = pcap.setfilter(pd, ctypes.byref(code))
        if err != 0:
            raise LinkLayerCaptureError(f"Could not install filter: {status_to_str(err)}; {pcap.geterr(pd).decode()}")
    except Exception:
        pcap.close(pd)
        raise
    return cast(object, pd)


_SNAPSHOT_LENGTH = 65535
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
    spec = importlib.util.find_spec("libpcap")
    if spec and spec.origin:
        is_64_bit = sys.maxsize.bit_length() > 32
        libpcap_dir = pathlib.Path(spec.origin).parent
        dll_path = libpcap_dir / "_platform" / "_windows" / ("x64" if is_64_bit else "x86") / "wpcap"
        os.environ["PATH"] += os.pathsep + str(dll_path)


if sys.platform.startswith("win"):  # pragma: no cover
    _apply_windows_workarounds()
