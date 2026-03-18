"""Cyphal/UDP transport implementation based on libudpard."""
from __future__ import annotations

import asyncio
import os
import platform
import socket
import struct
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from ipaddress import IPv4Address

import ifaddr
from pycyphal._common import rapidhash

from pycyphal._common import Closable, Instant, Priority, SendError
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

# ExceptionGroup compatibility for Python < 3.11
if sys.version_info < (3, 11):

    class ExceptionGroup(Exception):  # type: ignore[no-redef]
        def __init__(self, message: str, exceptions: list[Exception]) -> None:
            super().__init__(message)
            self.exceptions = list(exceptions)


# =====================================================================================================================
# Wire Protocol Constants
# =====================================================================================================================

UDP_PORT = 9382
HEADER_SIZE = 32
HEADER_VERSION = 2
IPv4_MCAST_PREFIX = 0xEF000000
IPv4_SUBJECT_ID_MAX = 0x7FFFFF
TRANSFER_ID_MASK = (1 << 48) - 1
CRC_INITIAL = 0xFFFFFFFF
CRC_OUTPUT_XOR = 0xFFFFFFFF
CRC_RESIDUE = 0x48674BC7
_MULTICAST_TTL = 16
_SIOCGIFMTU = 0x8921
_CYPHAL_OVERHEAD_MAX = 100
_CYPHAL_MTU_LINK_MIN = 576
_MAX_PENDING_TRANSFERS = 256
_DEDUP_MAX = 1024

# CRC-32C lookup table (Castagnoli, polynomial 0x1EDC6F41), copied from libudpard/udpard.c
_CRC_TABLE = [
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
    0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
    0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
    0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
    0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
    0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
    0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
    0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
    0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
    0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
    0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
    0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
    0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
    0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
    0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
    0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
    0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
    0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
    0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
    0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
    0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
    0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
    0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
]


# =====================================================================================================================
# CRC-32C
# =====================================================================================================================


def _crc_add(crc: int, data: bytes | memoryview) -> int:
    """Update CRC-32C state (without output XOR)."""
    for b in data:
        crc = (crc >> 8) ^ _CRC_TABLE[b ^ (crc & 0xFF)]
    return crc


def crc32c(data: bytes | memoryview) -> int:
    """Compute CRC-32C of data."""
    return _crc_add(CRC_INITIAL, data) ^ CRC_OUTPUT_XOR


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
    struct.pack_into("<I", buf, 28, crc32c(bytes(buf[:28])))
    return bytes(buf)


def _header_deserialize(data: bytes | memoryview) -> _FrameHeader | None:
    """Deserialize a 32-byte frame header. Returns None on validation failure."""
    if len(data) < HEADER_SIZE:
        return None
    # Validate header CRC (CRC of all 32 bytes must equal the residue constant)
    if crc32c(bytes(data[:HEADER_SIZE])) != CRC_RESIDUE:
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
    running_crc = CRC_INITIAL
    while True:
        progress = min(size - offset, mtu)
        chunk = payload[offset : offset + progress]
        running_crc = _crc_add(running_crc, chunk)
        header = _header_serialize(priority, transfer_id, sender_uid, offset, size, running_crc ^ CRC_OUTPUT_XOR)
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
            if crc32c(payload_chunk) != header.prefix_crc:
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
        if crc32c(payload) != expected_crc:
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


def crc32c_residue_check(data: bytes, expected_crc: int) -> bool:
    """Check if CRC of data matches expected. Equivalent to checking CRC residue."""
    return crc32c(data) == expected_crc


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
                    if Instant.now().ns > deadline.ns:
                        raise SendError("Deadline exceeded")
                    self._transport._tx_socks[i].sendto(frame, (mcast_ip, port))
                success_count += 1
            except OSError as e:
                errors.append(e)

        if errors:
            eg = ExceptionGroup("send failed on some interfaces", errors)
            if success_count == 0:
                raise SendError("send failed on all interfaces") from eg
            raise eg

    def close(self) -> None:
        self._closed = True


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
                    continue
                try:
                    addr = IPv4Address(ip.ip)
                except ValueError:
                    continue
                mtu = _get_iface_mtu(adapter.name)
                if mtu < _CYPHAL_MTU_LINK_MIN:
                    continue
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
        self._loop = asyncio.get_event_loop()
        self._closed = False

        # Resolve interfaces
        if interfaces is None:
            ifaces = self.list_interfaces()
            if not ifaces:
                raise RuntimeError("No suitable network interfaces found")
            interfaces = [ifaces[0]]
        self._interfaces: list[Interface] = list(interfaces)

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

    @staticmethod
    def _create_tx_socket(iface: Interface) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setblocking(False)
        sock.bind((str(iface.address), 0))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, _MULTICAST_TTL)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(str(iface.address)))
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
        return sock

    # -- Transport ABC --

    @property
    def subject_id_modulus(self) -> int:
        return self._subject_id_modulus_val

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if subject_id not in self._subject_handlers:
            self._subject_handlers[subject_id] = []
            for i, iface in enumerate(self._interfaces):
                key = (subject_id, i)
                sock = self._create_mcast_socket(subject_id, iface)
                self._mcast_socks[key] = sock
                self._loop.add_reader(sock.fileno(), self._on_mcast_data, subject_id, i)
        self._subject_handlers[subject_id].append(handler)
        return _UDPSubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> SubjectWriter:
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
                    if Instant.now().ns > deadline.ns:
                        raise SendError("Deadline exceeded")
                    self._tx_socks[i].sendto(frame, ep)
                success_count += 1
            except OSError as e:
                errors.append(e)

        if success_count == 0:
            if errors:
                raise SendError("Unicast failed on all interfaces") from errors[0]
            raise SendError("No endpoint known for remote_id")
        if errors:
            raise ExceptionGroup("unicast send failed on some interfaces", errors)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
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
        except OSError:
            return
        src_ip, src_port = addr[0], addr[1]
        if (src_ip, src_port) in self._self_endpoints:
            return  # Self-send filter
        self._process_subject_datagram(data, src_ip, src_port, subject_id, iface_idx)

    def _on_unicast_data(self, iface_idx: int) -> None:
        sock = self._tx_socks[iface_idx]
        try:
            data, addr = sock.recvfrom(65536)
        except OSError:
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
            arrival = TransportArrival(
                timestamp=Instant.now(), priority=Priority(priority), remote_id=sender_uid, message=message
            )
            for handler in self._subject_handlers.get(subject_id, []):
                handler(arrival)
