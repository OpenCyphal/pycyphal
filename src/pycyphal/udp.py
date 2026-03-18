"""Cyphal/UDP transport implementation."""
from __future__ import annotations

import asyncio
import logging
import os
import platform
import socket
import struct
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from ipaddress import IPv4Address

import ifaddr

from pycyphal._common import Closable, Instant, Priority, SendError
from pycyphal._hash import (
    CRC32C_INITIAL,
    CRC32C_OUTPUT_XOR,
    CRC32C_RESIDUE,
    crc32c_add,
    crc32c_full,
    rapidhash,
)
from pycyphal._transport import (
    SUBJECT_ID_MODULUS_23bit,
    SubjectWriter,
    Transport,
    TransportArrival,
)

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

_logger = logging.getLogger(__name__)

UDP_PORT = 9382
HEADER_SIZE = 32
HEADER_VERSION = 2
IPv4_MCAST_PREFIX = 0xEF000000
IPv4_SUBJECT_ID_MAX = 0x7FFFFF
TRANSFER_ID_MASK = (1 << 48) - 1
_MULTICAST_TTL = 16
_SIOCGIFMTU = 0x8921
_CYPHAL_OVERHEAD_MAX = 100
_CYPHAL_MTU_LINK_MIN = 576
_MAX_PENDING_TRANSFERS = 256
_DEDUP_MAX = 1024


# =====================================================================================================================
# Header Serialization / Deserialization
# =====================================================================================================================


@dataclass(frozen=True)
class _FrameHeader:
    priority: int
    transfer_id: int
    sender_uid: int
    frame_payload_offset: int
    transfer_payload_size: int
    prefix_crc: int


def _header_serialize(
    priority: int,
    transfer_id: int,
    sender_uid: int,
    frame_payload_offset: int,
    transfer_payload_size: int,
    prefix_crc: int,
) -> bytes:
    """Serialize a 32-byte Cyphal/UDP frame header."""
    buf = bytearray(HEADER_SIZE)
    buf[0] = HEADER_VERSION | ((priority & 0x07) << 5)
    buf[1] = 0  # incompatibility | reserved
    for i in range(6):
        buf[2 + i] = (transfer_id >> (i * 8)) & 0xFF
    struct.pack_into("<Q", buf, 8, sender_uid)
    struct.pack_into("<I", buf, 16, frame_payload_offset)
    struct.pack_into("<I", buf, 20, transfer_payload_size)
    struct.pack_into("<I", buf, 24, prefix_crc)
    struct.pack_into("<I", buf, 28, crc32c_full(memoryview(buf[:28])))
    return bytes(buf)


def _header_deserialize(data: bytes | memoryview) -> _FrameHeader | None:
    """Deserialize a 32-byte frame header. Returns None on validation failure."""
    if len(data) < HEADER_SIZE:
        return None
    # Validate header CRC (CRC of all 32 bytes must equal the residue constant)
    if crc32c_full(memoryview(data[:HEADER_SIZE])) != CRC32C_RESIDUE:
        return None
    head = data[0]
    if (head & 0x1F) != HEADER_VERSION:
        return None
    if (data[1] >> 5) != 0:  # incompatibility bits
        return None
    priority = (head >> 5) & 0x07
    transfer_id = 0
    for i in range(6):
        transfer_id |= data[2 + i] << (i * 8)
    sender_uid = struct.unpack_from("<Q", data, 8)[0]
    frame_payload_offset = struct.unpack_from("<I", data, 16)[0]
    transfer_payload_size = struct.unpack_from("<I", data, 20)[0]
    prefix_crc = struct.unpack_from("<I", data, 24)[0]
    # Validate frame bounds
    return _FrameHeader(priority, transfer_id, sender_uid, frame_payload_offset, transfer_payload_size, prefix_crc)


# =====================================================================================================================
# TX Segmentation
# =====================================================================================================================


def _segment_transfer(
    priority: int, transfer_id: int, sender_uid: int, payload: bytes | memoryview, mtu: int
) -> list[bytes]:
    """Segment a transfer payload into wire-format frames (header + chunk each).

    The ``mtu`` parameter is the max Cyphal frame payload size per frame (mtu_cyphal).
    """
    payload = bytes(payload)
    size = len(payload)
    frames: list[bytes] = []
    offset = 0
    running_crc = CRC32C_INITIAL
    while True:
        progress = min(size - offset, mtu)
        chunk = payload[offset : offset + progress]
        running_crc = crc32c_add(running_crc, chunk)
        header = _header_serialize(priority, transfer_id, sender_uid, offset, size, running_crc ^ CRC32C_OUTPUT_XOR)
        frames.append(header + chunk)
        offset += progress
        if offset >= size:
            break
    return frames


# =====================================================================================================================
# RX Reassembly
# =====================================================================================================================


class _TransferSlot:
    """Accumulates fragments for a single transfer."""

    __slots__ = ("transfer_payload_size", "fragments", "covered")

    def __init__(self, transfer_payload_size: int) -> None:
        self.transfer_payload_size = transfer_payload_size
        self.fragments: list[tuple[int, bytes, int]] = []  # (offset, data, prefix_crc)
        self.covered = 0  # contiguous bytes from offset 0

    def add_fragment(self, offset: int, data: bytes, prefix_crc: int) -> None:
        self.fragments.append((offset, data, prefix_crc))
        self.fragments.sort(key=lambda f: f[0])
        covered = 0
        for off, d, _ in self.fragments:
            if off <= covered:
                covered = max(covered, off + len(d))
        self.covered = covered

    def is_complete(self) -> bool:
        return self.covered >= self.transfer_payload_size

    def assemble(self) -> bytes:
        if self.transfer_payload_size == 0:
            return b""
        buf = bytearray(self.transfer_payload_size)
        for off, d, _ in sorted(self.fragments, key=lambda f: f[0]):
            end = min(off + len(d), self.transfer_payload_size)
            buf[off:end] = d[: end - off]
        return bytes(buf)

    def final_prefix_crc(self) -> int:
        """Get prefix_crc from the frame covering the last byte of the transfer."""
        for off, d, crc in self.fragments:
            if off + len(d) >= self.transfer_payload_size:
                return crc
        return 0


class _RxReassembler:
    """Multi-frame transfer reassembly with per-sender dedup."""

    def __init__(self) -> None:
        self._slots: dict[tuple[int, int], _TransferSlot] = {}  # (sender_uid, transfer_id) -> slot
        self._completed: dict[int, set[int]] = {}  # sender_uid -> set of completed transfer_ids

    def accept(self, header: _FrameHeader, payload_chunk: bytes) -> tuple[int, int, bytes] | None:
        """Accept a frame. Returns (sender_uid, priority, message) on transfer completion, None otherwise."""
        key = (header.sender_uid, header.transfer_id)

        # Dedup: skip already-completed transfers
        if header.transfer_id in self._completed.get(header.sender_uid, set()):
            return None

        # Validate first-frame CRC
        if header.frame_payload_offset == 0:
            if crc32c_full(payload_chunk) != header.prefix_crc:
                return None

        # Validate frame bounds
        if header.frame_payload_offset + len(payload_chunk) > header.transfer_payload_size:
            return None

        # Get or create slot
        slot = self._slots.get(key)
        if slot is None:
            # Evict oldest slot if at capacity
            if len(self._slots) >= _MAX_PENDING_TRANSFERS:
                oldest_key = next(iter(self._slots))
                del self._slots[oldest_key]
            slot = _TransferSlot(header.transfer_payload_size)
            self._slots[key] = slot
        elif slot.transfer_payload_size != header.transfer_payload_size:
            return None  # Conflicting transfer_payload_size

        slot.add_fragment(header.frame_payload_offset, payload_chunk, header.prefix_crc)

        if not slot.is_complete():
            return None

        payload = slot.assemble()
        expected_crc = slot.final_prefix_crc()
        del self._slots[key]
        if crc32c_full(payload) != expected_crc:
            return None

        # Record for dedup
        completed_set = self._completed.setdefault(header.sender_uid, set())
        completed_set.add(header.transfer_id)
        if len(completed_set) > _DEDUP_MAX:
            completed_set.clear()
            completed_set.add(header.transfer_id)

        return (header.sender_uid, header.priority, payload)


# =====================================================================================================================
# Utilities
# =====================================================================================================================


def make_subject_endpoint(subject_id: int) -> tuple[str, int]:
    """Return (multicast_ip, port) for a given subject_id."""
    ip_int = IPv4_MCAST_PREFIX | (subject_id & IPv4_SUBJECT_ID_MAX)
    return (str(IPv4Address(ip_int)), UDP_PORT)


def generate_uid() -> int:
    """Generate a semi-random EUI-64 identifier following eui64_semirandom() from cy_udp_posix."""
    host_20 = 0
    if sys.platform == "linux":
        try:
            with open("/etc/machine-id", "rb") as f:
                data = f.read(32)
            host_20 = rapidhash(data) & 0xFFFFF
        except OSError:
            host_20 = rapidhash(platform.node().encode()) & 0xFFFFF
    else:
        host_20 = rapidhash(platform.node().encode()) & 0xFFFFF
    rand_44 = int.from_bytes(os.urandom(8), "little") & ((1 << 44) - 1)
    out = (host_20 << 44) | rand_44
    out &= ~(1 << 56)  # clear I/G bit (unicast)
    out |= 1 << 57  # set U/L bit (locally administered)
    return out


def _get_iface_mtu(ifname: str) -> int:
    """Get link MTU via ioctl on Linux, default 1500 otherwise."""
    if sys.platform == "linux" and fcntl is not None:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                ifreq = struct.pack("256s", ifname.encode()[:15])
                result = fcntl.ioctl(s.fileno(), _SIOCGIFMTU, ifreq)
                return struct.unpack_from("i", result, 16)[0]
            finally:
                s.close()
        except OSError:
            pass
    return 1500


def _get_default_iface_ip() -> IPv4Address | None:
    """Determine the default interface IP via the connect-to-1.1.1.1 trick."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("1.1.1.1", 80))
            return IPv4Address(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        return None


# =====================================================================================================================
# Interface
# =====================================================================================================================


@dataclass(frozen=True)
class Interface:
    address: IPv4Address
    mtu_link: int
    """Link-layer MTU. E.g., 1500 for Ethernet, ~64K for loopback."""

    @property
    def mtu_cyphal(self) -> int:
        """Max Cyphal frame payload: mtu_link - 60 (IPv4 max) - 8 (UDP) - 32 (Cyphal header)."""
        assert self.mtu_link >= _CYPHAL_MTU_LINK_MIN
        return self.mtu_link - _CYPHAL_OVERHEAD_MAX


# =====================================================================================================================
# Subject Writer / Listener
# =====================================================================================================================


class _UDPSubjectWriter(SubjectWriter):
    def __init__(self, transport: UDPTransport, subject_id: int) -> None:
        self._transport = transport
        self._subject_id = subject_id
        self._transfer_id = int.from_bytes(os.urandom(6), "little")
        self._closed = False

    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        if self._closed:
            raise SendError("Writer closed")
        if self._transport._closed:
            raise SendError("Transport closed")

        mcast_ip, port = make_subject_endpoint(self._subject_id)
        transfer_id = self._transfer_id & TRANSFER_ID_MASK
        self._transfer_id += 1

        errors: list[Exception] = []
        success_count = 0
        for i, iface in enumerate(self._transport._interfaces):
            mtu = iface.mtu_cyphal
            frames = _segment_transfer(priority, transfer_id, self._transport._uid, message, mtu)
            try:
                for frame in frames:
                    await self._transport._async_sendto(self._transport._tx_socks[i], frame, (mcast_ip, port), deadline)
                success_count += 1
            except (OSError, SendError) as e:
                errors.append(e)

        if errors:
            eg = ExceptionGroup("send failed on some interfaces", errors)
            if success_count == 0:
                _logger.error("Send failed on all interfaces for subject %d", self._subject_id)
                raise SendError("send failed on all interfaces") from eg
            _logger.warning("Send failed on %d/%d interfaces for subject %d",
                            len(errors), len(errors) + success_count, self._subject_id)
            raise eg

        _logger.debug("Sent %d frames on subject %d, transfer_id=%d",
                       len(frames) if self._transport._interfaces else 0, self._subject_id, transfer_id)

    def close(self) -> None:
        self._closed = True
        _logger.debug("Subject writer closed for subject %d", self._subject_id)


class _UDPSubjectListener(Closable):
    def __init__(
        self, transport: UDPTransport, subject_id: int, handler: Callable[[TransportArrival], None]
    ) -> None:
        self._transport = transport
        self._subject_id = subject_id
        self._handler = handler
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _logger.info("Subject listener closed for subject %d", self._subject_id)
        handlers = self._transport._subject_handlers.get(self._subject_id, [])
        if self._handler in handlers:
            handlers.remove(self._handler)
        if not handlers:
            # No more listeners for this subject -- clean up sockets
            self._transport._subject_handlers.pop(self._subject_id, None)
            self._transport._reassemblers.pop(self._subject_id, None)
            for i in range(len(self._transport._interfaces)):
                key = (self._subject_id, i)
                sock = self._transport._mcast_socks.pop(key, None)
                if sock is not None:
                    try:
                        self._transport._loop.remove_reader(sock.fileno())
                    except Exception:
                        pass
                    sock.close()


# =====================================================================================================================
# UDPTransport
# =====================================================================================================================


class UDPTransport(Transport):
    @staticmethod
    def list_interfaces() -> list[Interface]:
        """List usable IPv4 network interfaces. Default interface first, loopback last."""
        default_ip = _get_default_iface_ip()
        result: list[Interface] = []
        for adapter in ifaddr.get_adapters():
            for ip in adapter.ips:
                if not isinstance(ip.ip, str):
                    _logger.info("Skipping non-string IP on %s: %r", adapter.name, ip.ip)
                    continue
                try:
                    addr = IPv4Address(ip.ip)
                except ValueError:
                    _logger.info("Skipping non-IPv4 address on %s: %s", adapter.name, ip.ip)
                    continue
                mtu = _get_iface_mtu(adapter.name)
                if mtu < _CYPHAL_MTU_LINK_MIN:
                    _logger.info("Skipping %s (%s): MTU %d < %d", adapter.name, addr, mtu, _CYPHAL_MTU_LINK_MIN)
                    continue
                _logger.info("Found interface %s: %s, MTU=%d", adapter.name, addr, mtu)
                result.append(Interface(address=addr, mtu_link=mtu))

        def sort_key(iface: Interface) -> tuple[int, str]:
            if default_ip is not None and iface.address == default_ip:
                return (0, str(iface.address))
            if iface.address.is_loopback:
                return (2, str(iface.address))
            return (1, str(iface.address))

        result.sort(key=sort_key)
        return result

    def __init__(
        self,
        interfaces: Iterable[Interface] | None = None,
        uid: int | None = None,
        *,
        subject_id_modulus: int = SUBJECT_ID_MODULUS_23bit,
    ) -> None:
        if uid is None:
            uid = generate_uid()
        self._uid = uid
        self._subject_id_modulus_val = subject_id_modulus
        self._loop = asyncio.get_running_loop()
        self._closed = False

        # Resolve interfaces
        if interfaces is None:
            ifaces = self.list_interfaces()
            if not ifaces:
                raise RuntimeError("No suitable network interfaces found")
            interfaces = [ifaces[0]]
        self._interfaces: list[Interface] = list(interfaces)
        if not self._interfaces:
            _logger.error("Empty interfaces list provided")
            raise ValueError("At least one network interface is required")

        # Per-interface TX/unicast sockets
        self._tx_socks: list[socket.socket] = []
        self._self_endpoints: set[tuple[str, int]] = set()
        for iface in self._interfaces:
            sock = self._create_tx_socket(iface)
            self._tx_socks.append(sock)
            self._self_endpoints.add(sock.getsockname()[:2])

        # Subject state
        self._subject_handlers: dict[int, list[Callable[[TransportArrival], None]]] = {}
        self._mcast_socks: dict[tuple[int, int], socket.socket] = {}
        self._reassemblers: dict[int, _RxReassembler] = {}

        # Unicast state
        self._unicast_handler: Callable[[TransportArrival], None] | None = None
        self._unicast_reassembler = _RxReassembler()
        self._remote_endpoints: dict[tuple[int, int], tuple[str, int]] = {}
        self._next_unicast_transfer_id = int.from_bytes(os.urandom(6), "little")

        # Register unicast RX readers on TX sockets
        for i, sock in enumerate(self._tx_socks):
            self._loop.add_reader(sock.fileno(), self._on_unicast_data, i)

        _logger.info("UDPTransport initialized: uid=0x%016x, interfaces=%s, modulus=%d",
                      self._uid, [str(i.address) for i in self._interfaces], self._subject_id_modulus_val)

    @staticmethod
    def _create_tx_socket(iface: Interface) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setblocking(False)
        sock.bind((str(iface.address), 0))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, _MULTICAST_TTL)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(str(iface.address)))
        _logger.info("TX socket created on %s, bound to port %d", iface.address, sock.getsockname()[1])
        return sock

    @staticmethod
    def _create_mcast_socket(subject_id: int, iface: Interface) -> socket.socket:
        mcast_ip, port = make_subject_endpoint(subject_id)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # Bind to multicast group address on Linux; INADDR_ANY on Windows
        if sys.platform == "win32":
            sock.bind(("", port))
        else:
            sock.bind((mcast_ip, port))
        # Join multicast group on the specific interface
        mreq = socket.inet_aton(mcast_ip) + socket.inet_aton(str(iface.address))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        _logger.info("Multicast socket for subject %d on %s (%s:%d)", subject_id, iface.address, mcast_ip, port)
        return sock

    # -- Async sendto helper --

    async def _async_sendto(self, sock: socket.socket, data: bytes, addr: tuple[str, int], deadline: Instant) -> None:
        """Send a UDP datagram, suspending until writable or deadline exceeded."""
        loop = self._loop
        while True:
            remaining_ns = deadline.ns - Instant.now().ns
            if remaining_ns <= 0:
                raise SendError("Deadline exceeded")
            try:
                sock.sendto(data, addr)
                return
            except BlockingIOError:
                # Socket buffer full -- wait for writability or deadline
                fut: asyncio.Future[None] = loop.create_future()
                fd = sock.fileno()

                def _ready() -> None:
                    loop.remove_writer(fd)
                    if not fut.done():
                        fut.set_result(None)

                loop.add_writer(fd, _ready)
                try:
                    await asyncio.wait_for(fut, timeout=remaining_ns * 1e-9)
                except asyncio.TimeoutError:
                    loop.remove_writer(fd)
                    raise SendError("Deadline exceeded waiting for socket writability")

    # -- Transport ABC --

    @property
    def subject_id_modulus(self) -> int:
        return self._subject_id_modulus_val

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if subject_id not in self._subject_handlers:
            _logger.info("Subscribing to subject %d", subject_id)
            self._subject_handlers[subject_id] = []
            for i, iface in enumerate(self._interfaces):
                key = (subject_id, i)
                sock = self._create_mcast_socket(subject_id, iface)
                self._mcast_socks[key] = sock
                self._loop.add_reader(sock.fileno(), self._on_mcast_data, subject_id, i)
        self._subject_handlers[subject_id].append(handler)
        return _UDPSubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> SubjectWriter:
        _logger.info("Advertising subject %d", subject_id)
        return _UDPSubjectWriter(self, subject_id)

    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        self._unicast_handler = handler

    async def unicast(
        self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview
    ) -> None:
        if self._closed:
            raise SendError("Transport closed")
        transfer_id = self._next_unicast_transfer_id & TRANSFER_ID_MASK
        self._next_unicast_transfer_id += 1

        errors: list[Exception] = []
        success_count = 0
        for i, iface in enumerate(self._interfaces):
            ep = self._remote_endpoints.get((remote_id, i))
            if ep is None:
                continue
            frames = _segment_transfer(priority, transfer_id, self._uid, message, iface.mtu_cyphal)
            try:
                for frame in frames:
                    await self._async_sendto(self._tx_socks[i], frame, ep, deadline)
                success_count += 1
            except (OSError, SendError) as e:
                errors.append(e)

        if success_count == 0:
            if errors:
                raise SendError("Unicast failed on all interfaces") from errors[0]
            _logger.warning("No endpoint known for remote_id=0x%016x", remote_id)
            raise SendError("No endpoint known for remote_id")
        if errors:
            raise ExceptionGroup("unicast send failed on some interfaces", errors)
        _logger.debug("Unicast sent %d frames to remote_id=0x%016x", len(frames), remote_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _logger.info("Closing UDPTransport uid=0x%016x", self._uid)
        for sock in self._tx_socks:
            try:
                self._loop.remove_reader(sock.fileno())
            except Exception:
                pass
            sock.close()
        for sock in self._mcast_socks.values():
            try:
                self._loop.remove_reader(sock.fileno())
            except Exception:
                pass
            sock.close()
        self._mcast_socks.clear()
        self._tx_socks.clear()
        self._subject_handlers.clear()
        self._reassemblers.clear()

    # -- Internal RX callbacks --

    def _on_mcast_data(self, subject_id: int, iface_idx: int) -> None:
        sock = self._mcast_socks.get((subject_id, iface_idx))
        if sock is None:
            return
        try:
            data, addr = sock.recvfrom(65536)
        except OSError as e:
            _logger.debug("Multicast recv error on subject %d iface %d: %s", subject_id, iface_idx, e)
            return
        src_ip, src_port = addr[0], addr[1]
        if (src_ip, src_port) in self._self_endpoints:
            return  # Self-send filter
        self._process_subject_datagram(data, src_ip, src_port, subject_id, iface_idx)

    def _on_unicast_data(self, iface_idx: int) -> None:
        sock = self._tx_socks[iface_idx]
        try:
            data, addr = sock.recvfrom(65536)
        except OSError as e:
            _logger.debug("Unicast recv error on iface %d: %s", iface_idx, e)
            return
        src_ip, src_port = addr[0], addr[1]
        if len(data) < HEADER_SIZE:
            return
        header = _header_deserialize(data[:HEADER_SIZE])
        if header is None:
            return
        # Record remote endpoint for unicast discovery
        self._remote_endpoints[(header.sender_uid, iface_idx)] = (src_ip, src_port)
        payload_chunk = data[HEADER_SIZE:]
        result = self._unicast_reassembler.accept(header, payload_chunk)
        if result is not None:
            sender_uid, priority, message = result
            _logger.debug("Unicast transfer complete from sender_uid=0x%016x", sender_uid)
            if self._unicast_handler is not None:
                self._unicast_handler(
                    TransportArrival(
                        timestamp=Instant.now(), priority=Priority(priority), remote_id=sender_uid, message=message
                    )
                )

    def _process_subject_datagram(
        self, data: bytes, src_ip: str, src_port: int, subject_id: int, iface_idx: int
    ) -> None:
        if len(data) < HEADER_SIZE:
            return
        header = _header_deserialize(data[:HEADER_SIZE])
        if header is None:
            return
        # Record remote endpoint for unicast discovery
        self._remote_endpoints[(header.sender_uid, iface_idx)] = (src_ip, src_port)
        payload_chunk = data[HEADER_SIZE:]
        reassembler = self._reassemblers.get(subject_id)
        if reassembler is None:
            reassembler = _RxReassembler()
            self._reassemblers[subject_id] = reassembler
        result = reassembler.accept(header, payload_chunk)
        if result is not None:
            sender_uid, priority, message = result
            _logger.debug("Subject %d transfer complete from sender_uid=0x%016x", subject_id, sender_uid)
            arrival = TransportArrival(
                timestamp=Instant.now(), priority=Priority(priority), remote_id=sender_uid, message=message
            )
            for handler in self._subject_handlers.get(subject_id, []):
                handler(arrival)
