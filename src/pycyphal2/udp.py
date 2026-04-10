"""
Cyphal/UDP transport — zero-config reliable pub/sub over IPv4 multicast.

```python
from pycyphal2.udp import UDPTransport

transport = UDPTransport.new()  # auto-detects network interfaces to use
```

Pass the transport to `pycyphal2.Node.new()` to start a node.

`UDPTransport.new()` discovers usable IPv4 interfaces automatically and generates a random node identity.
For machine-local networking, use `UDPTransport.new_loopback()`.

Requires third-party dependencies — install with `pip install pycyphal2[udp]`.
"""

# This module is directly importable by the application (hence no underscore prefix), so its API must be spotless!

from __future__ import annotations

import asyncio
import logging
import os
import socket
import struct
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from ipaddress import IPv4Address

import ifaddr

from . import Closable, ClosedError, Instant, Priority, SendError, eui64
from ._api import SUBJECT_ID_PINNED_MAX
from ._hash import CRC32C_INITIAL, CRC32C_OUTPUT_XOR, CRC32C_RESIDUE, crc32c_add, crc32c_full
from ._transport import SUBJECT_ID_MODULUS_23bit, SubjectWriter, Transport, TransportArrival

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
_RX_SESSION_LIFETIME_NS = round(30.0 * 1e9)
_RX_SLOT_COUNT = 8
_RX_TRANSFER_HISTORY_COUNT = 32
_SUBJECT_ID_MODULUS_MAX = IPv4_SUBJECT_ID_MAX - SUBJECT_ID_PINNED_MAX


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
    # Wire data is untrusted: malformed headers are dropped here, never surfaced as exceptions.
    if len(data) < HEADER_SIZE:
        _logger.debug("UDP hdr drop short len=%d", len(data))
        return None
    # Validate header CRC (CRC of all 32 bytes must equal the residue constant)
    if crc32c_full(memoryview(data[:HEADER_SIZE])) != CRC32C_RESIDUE:
        _logger.debug("UDP hdr drop crc")
        return None
    head = data[0]
    if (head & 0x1F) != HEADER_VERSION:
        _logger.debug("UDP hdr drop version=%d", head & 0x1F)
        return None
    if (data[1] >> 5) != 0:  # incompatibility bits
        _logger.debug("UDP hdr drop incompatibility=%d", data[1] >> 5)
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


def _frame_is_valid(header: _FrameHeader, payload_chunk: bytes | memoryview) -> bool:
    # This validator is part of the RX policy boundary: bad wire frames are rejected with False, not exceptions.
    if header.frame_payload_offset == 0 and crc32c_full(payload_chunk) != header.prefix_crc:
        return False
    return (header.frame_payload_offset + len(payload_chunk)) <= header.transfer_payload_size


@dataclass(frozen=True)
class _Fragment:
    offset: int
    data: bytes
    crc: int

    @property
    def end(self) -> int:
        return self.offset + len(self.data)


@dataclass(frozen=True)
class _RxTransfer:
    sender_uid: int
    priority: int
    payload: bytes
    timestamp_ns: int


@dataclass
class _TransferSlot:
    transfer_id: int
    total_size: int
    priority: int
    ts_min_ns: int
    ts_max_ns: int
    covered_prefix: int = 0
    crc_end: int = 0
    crc: int = CRC32C_INITIAL
    fragments: list[_Fragment] = field(default_factory=list)

    @classmethod
    def create(cls, header: _FrameHeader, timestamp_ns: int) -> _TransferSlot:
        return cls(
            transfer_id=header.transfer_id,
            total_size=header.transfer_payload_size,
            priority=header.priority,
            ts_min_ns=timestamp_ns,
            ts_max_ns=timestamp_ns,
        )

    def update(self, timestamp_ns: int, header: _FrameHeader, payload_chunk: bytes) -> bytes | None:
        if self._accept_fragment(header.frame_payload_offset, payload_chunk, header.prefix_crc):
            self.ts_max_ns = max(self.ts_max_ns, timestamp_ns)
            self.ts_min_ns = min(self.ts_min_ns, timestamp_ns)
            crc_end = header.frame_payload_offset + len(payload_chunk)
            if crc_end >= self.crc_end:
                self.crc_end = crc_end
                self.crc = header.prefix_crc
        if self.covered_prefix < self.total_size:
            return None
        return self._finalize_payload()

    def _accept_fragment(self, offset: int, data: bytes, crc: int) -> bool:
        left = offset
        right = offset + len(data)
        for frag in self.fragments:
            if frag.offset <= left and frag.end >= right:
                return False

        left_neighbor = self._find_left_neighbor(left)
        right_neighbor = self._find_right_neighbor(right)
        left_size = len(left_neighbor.data) if left_neighbor is not None else 0
        right_size = len(right_neighbor.data) if right_neighbor is not None else 0
        accept = (
            left_neighbor is None
            or right_neighbor is None
            or left_neighbor.end < right_neighbor.offset
            or len(data) > min(left_size, right_size)
        )
        if not accept:
            return False

        v_left = min(left, left_neighbor.offset + 1) if left_neighbor is not None else left
        v_right = max(right, max(right_neighbor.end, 1) - 1) if right_neighbor is not None else right
        self.fragments = [frag for frag in self.fragments if not (frag.offset >= v_left and frag.end <= v_right)]
        self.fragments.append(_Fragment(offset=offset, data=data, crc=crc))
        self.fragments.sort(key=lambda frag: frag.offset)
        self.covered_prefix = self._compute_covered_prefix()
        return True

    def _find_left_neighbor(self, left: int) -> _Fragment | None:
        for frag in self.fragments:
            if frag.end >= left:
                return None if frag.offset >= left else frag
        return None

    def _find_right_neighbor(self, right: int) -> _Fragment | None:
        candidate: _Fragment | None = None
        for frag in self.fragments:
            if frag.offset < right:
                candidate = frag
            else:
                break
        if candidate is not None and candidate.end <= right:
            return None
        return candidate

    def _compute_covered_prefix(self) -> int:
        covered = 0
        for frag in self.fragments:
            if frag.offset > covered:
                break
            covered = max(covered, frag.end)
        return covered

    def _finalize_payload(self) -> bytes | None:
        offset = 0
        parts: list[bytes] = []
        for frag in self.fragments:
            if frag.offset > offset:
                return None
            trim = offset - frag.offset
            if trim >= len(frag.data):
                continue
            view = frag.data[trim:]
            parts.append(view)
            offset += len(view)
        payload = b"".join(parts)
        if len(payload) != self.total_size:
            return None
        if crc32c_full(payload) != self.crc:
            return None
        return payload


@dataclass
class _RxSession:
    last_animated_ns: int
    history: list[int] = field(default_factory=lambda: [0] * _RX_TRANSFER_HISTORY_COUNT)
    history_current: int = 0
    initialized: bool = False
    slots: list[_TransferSlot | None] = field(default_factory=lambda: [None] * _RX_SLOT_COUNT)

    def is_transfer_ejected(self, transfer_id: int) -> bool:
        return transfer_id in self.history

    def initialize_history(self, transfer_id: int) -> None:
        value = (transfer_id - 1) & TRANSFER_ID_MASK
        self.history = [value] * _RX_TRANSFER_HISTORY_COUNT
        self.history_current = 0
        self.initialized = True

    def record_transfer_ejected(self, transfer_id: int) -> None:
        self.history_current = (self.history_current + 1) % _RX_TRANSFER_HISTORY_COUNT
        self.history[self.history_current] = transfer_id

    def get_slot(self, timestamp_ns: int, header: _FrameHeader) -> tuple[int, _TransferSlot]:
        for index, slot in enumerate(self.slots):
            if slot is not None and slot.transfer_id == header.transfer_id:
                return index, slot
        for index, slot in enumerate(self.slots):
            if slot is not None and timestamp_ns >= (slot.ts_max_ns + _RX_SESSION_LIFETIME_NS):
                self.slots[index] = None
        for index, slot in enumerate(self.slots):
            if slot is None:
                created = _TransferSlot.create(header, timestamp_ns)
                self.slots[index] = created
                return index, created
        oldest_index = 0
        oldest_slot: _TransferSlot | None = None
        for index, slot in enumerate(self.slots):
            if slot is None:
                continue
            if (oldest_slot is None) or (slot.ts_max_ns < oldest_slot.ts_max_ns):
                oldest_index = index
                oldest_slot = slot
        if oldest_slot is None:
            _logger.debug("UDP reasm slot fallback uid=%016x tid=%d", header.sender_uid, header.transfer_id)
        created = _TransferSlot.create(header, timestamp_ns)
        self.slots[oldest_index] = created
        return oldest_index, created


class _RxReassembler:
    """Multi-frame transfer reassembly with per-sender session state."""

    def __init__(self) -> None:
        self._sessions: OrderedDict[int, _RxSession] = OrderedDict()

    def accept(
        self,
        header: _FrameHeader,
        payload_chunk: bytes,
        *,
        timestamp_ns: int | None = None,
        frame_validated: bool = False,
    ) -> _RxTransfer | None:
        timestamp_ns = Instant.now().ns if timestamp_ns is None else timestamp_ns
        if not frame_validated and not _frame_is_valid(header, payload_chunk):
            _logger.debug("UDP reasm drop invalid uid=%016x tid=%d", header.sender_uid, header.transfer_id)
            return None
        session: _RxSession | None = None
        slot_index: int | None = None
        try:
            self._retire_one_stale_session(timestamp_ns)
            session = self._sessions.get(header.sender_uid)
            if session is None:
                session = _RxSession(last_animated_ns=timestamp_ns)
                self._sessions[header.sender_uid] = session
            session.last_animated_ns = timestamp_ns
            self._sessions.move_to_end(header.sender_uid, last=False)
            if not session.initialized:
                session.initialize_history(header.transfer_id)
            if session.is_transfer_ejected(header.transfer_id):
                _logger.debug("UDP reasm dup uid=%016x tid=%d", header.sender_uid, header.transfer_id)
                return None
            slot_index, slot = session.get_slot(timestamp_ns, header)
            if (slot.total_size != header.transfer_payload_size) or (slot.priority != header.priority):
                # Per RX policy, inconsistent per-transfer metadata is malformed wire input, not an exception path.
                session.slots[slot_index] = None
                _logger.debug("UDP reasm drop uid=%016x tid=%d reason=metadata", header.sender_uid, header.transfer_id)
                return None
            payload = slot.update(timestamp_ns, header, payload_chunk)
        except Exception as ex:
            if (session is not None) and (slot_index is not None):
                session.slots[slot_index] = None
            # RX state is driven by untrusted wire data; any malformed-input fault is downgraded to drop+debug.
            _logger.debug(
                "UDP reasm fault uid=%016x tid=%d %s", header.sender_uid, header.transfer_id, ex, exc_info=True
            )
            return None
        if payload is None:
            if (session is not None) and (slot_index is not None):
                slot_state = session.slots[slot_index]
                if (slot_state is not None) and (slot_state.covered_prefix >= slot_state.total_size):
                    # A fully covered but non-finalizable transfer is malformed on the wire, so we drop its slot here.
                    session.slots[slot_index] = None
                    _logger.debug(
                        "UDP reasm drop uid=%016x tid=%d reason=finalize", header.sender_uid, header.transfer_id
                    )
            return None
        if (session is None) or (slot_index is None):
            _logger.debug("UDP reasm completion fallback uid=%016x tid=%d", header.sender_uid, header.transfer_id)
            return None
        session.record_transfer_ejected(header.transfer_id)
        session.slots[slot_index] = None
        _logger.debug("UDP reasm done uid=%016x tid=%d n=%d", header.sender_uid, header.transfer_id, len(payload))
        return _RxTransfer(
            sender_uid=header.sender_uid,
            priority=slot.priority,
            payload=payload,
            timestamp_ns=slot.ts_min_ns,
        )

    def _retire_one_stale_session(self, timestamp_ns: int) -> None:
        if not self._sessions:
            return
        oldest_uid = next(reversed(self._sessions))
        oldest = self._sessions[oldest_uid]
        if timestamp_ns >= (oldest.last_animated_ns + _RX_SESSION_LIFETIME_NS):
            self._sessions.pop(oldest_uid)
            _logger.debug("UDP reasm retire uid=%016x", oldest_uid)


# =====================================================================================================================
# Utilities
# =====================================================================================================================


def _make_subject_endpoint(subject_id: int) -> tuple[str, int]:
    """Return (multicast_ip, port) for a given subject_id."""
    ip_int = IPv4_MCAST_PREFIX | (subject_id & IPv4_SUBJECT_ID_MAX)
    return (str(IPv4Address(ip_int)), UDP_PORT)


def _get_iface_mtu(ifname: str) -> int:
    """Get link MTU via ioctl on Linux, default 1500 otherwise."""
    if sys.platform == "linux" and fcntl is not None:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                ifreq = struct.pack("256s", ifname.encode()[:15])
                result = fcntl.ioctl(s.fileno(), _SIOCGIFMTU, ifreq)
                return int(struct.unpack_from("i", result, 16)[0])
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
    def __init__(self, transport: _UDPTransportImpl, subject_id: int) -> None:
        self._transport = transport
        self._subject_id = subject_id
        self._transfer_id = int.from_bytes(os.urandom(6), "little")
        self._closed = False

    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        if self._closed:
            raise ClosedError("Writer closed")
        if self._transport.closed:
            raise ClosedError("Transport closed")

        mcast_ip, port = _make_subject_endpoint(self._subject_id)
        transfer_id = self._transfer_id & TRANSFER_ID_MASK
        self._transfer_id += 1
        _logger.debug("Subject tx start sid=%d tid=%d bytes=%d", self._subject_id, transfer_id, len(message))

        errors: list[Exception] = []
        success_count = 0
        for i, iface in enumerate(self._transport.interfaces):
            mtu = iface.mtu_cyphal
            frames = _segment_transfer(priority, transfer_id, self._transport.uid, message, mtu)
            try:
                for frame in frames:
                    await self._transport.async_sendto(self._transport.tx_socks[i], frame, (mcast_ip, port), deadline)
                success_count += 1
            except (OSError, SendError) as e:
                errors.append(e)

        if errors:
            eg = ExceptionGroup("send failed on some interfaces", errors)
            if success_count == 0:
                _logger.error("Send failed on all interfaces for subject %d", self._subject_id)
                raise SendError("send failed on all interfaces") from eg
            _logger.warning(
                "Send failed on %d/%d interfaces for subject %d",
                len(errors),
                len(errors) + success_count,
                self._subject_id,
            )
            raise eg

        _logger.debug("Subject tx done sid=%d tid=%d", self._subject_id, transfer_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._transport.remove_subject_writer(self._subject_id, self)
        _logger.debug("Subject writer closed for subject %d", self._subject_id)


class _UDPSubjectListener(Closable):
    def __init__(
        self, transport: _UDPTransportImpl, subject_id: int, handler: Callable[[TransportArrival], None]
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
        self._transport.remove_subject_listener(self._subject_id, self._handler)


# =====================================================================================================================
# UDPTransport
# =====================================================================================================================


class UDPTransport(Transport, ABC):
    """
    The public API of the Cyphal/UDP transport.
    """

    @property
    @abstractmethod
    def uid(self) -> int:
        """The 64-bit globally unique ID of the local node."""
        raise NotImplementedError

    @property
    @abstractmethod
    def interfaces(self) -> list[Interface]:
        """List of (redundant) interfaces that the transport is operating over. Never empty."""
        raise NotImplementedError

    @staticmethod
    def new(
        interfaces: Iterable[Interface] | None = None,
        uid: int | None = None,
        *,
        subject_id_modulus: int = SUBJECT_ID_MODULUS_23bit,
    ) -> UDPTransport:
        """
        Constructs a new Cyphal/UDP transport instance that will operate over the specified local network interfaces.

        If no interfaces are given (empty list or None, which is default), suitable interfaces will be automatically
        detected. You can also use ``UDPTransport.list_interfaces()`` for a semi-automatic approach.

        The UID is a globally unique 64-bit identifier of the local node. If not given, one will be generated randomly.
        """
        # Resolve interfaces.
        if not interfaces:
            ifaces = UDPTransport.list_interfaces()
            if not ifaces:
                raise RuntimeError("No suitable network interfaces found")
            interfaces = [ifaces[0]]
        else:
            interfaces = list(interfaces)
        if not isinstance(interfaces, list) or not all(isinstance(i, Interface) for i in interfaces):
            raise ValueError("interfaces must be an iterable of Interface instances")

        # Resolve UID.
        uid = uid or eui64()
        if not isinstance(uid, int) or not (0 < uid < 2**64):
            raise ValueError("uid must be a positive 64-bit integer")

        return _UDPTransportImpl(interfaces=interfaces, uid=uid, subject_id_modulus=subject_id_modulus)

    @staticmethod
    def new_loopback() -> UDPTransport:
        """A simple wrapper that uses the local loopback interface."""
        return UDPTransport.new([Interface(IPv4Address("127.0.0.1"), mtu_link=1500)])

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
                return 0, str(iface.address)
            if iface.address.is_loopback:
                return 2, str(iface.address)
            return 1, str(iface.address)

        result.sort(key=sort_key)
        return result


class _UDPTransportImpl(UDPTransport):
    def __init__(self, interfaces: Iterable[Interface], uid: int, subject_id_modulus: int) -> None:
        if not (1 <= subject_id_modulus <= _SUBJECT_ID_MODULUS_MAX):
            raise ValueError(f"subject_id_modulus must be in [1, {_SUBJECT_ID_MODULUS_MAX}] for Cyphal/UDP")
        self._uid = uid
        self._subject_id_modulus_val = subject_id_modulus
        self._loop = asyncio.get_running_loop()
        self._closed = False

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
        self._subject_handlers: dict[int, Callable[[TransportArrival], None]] = {}
        self._subject_writers: dict[int, _UDPSubjectWriter] = {}
        self._mcast_socks: dict[tuple[int, int], socket.socket] = {}
        self._reassemblers: dict[int, _RxReassembler] = {}

        # Unicast state
        self._unicast_handler: Callable[[TransportArrival], None] | None = None
        self._unicast_reassembler = _RxReassembler()
        self._remote_endpoints: dict[tuple[int, int], tuple[str, int]] = {}
        self._next_unicast_transfer_id = int.from_bytes(os.urandom(6), "little")

        # Async RX tasks (platform-agnostic, replaces add_reader)
        self._unicast_rx_tasks: list[asyncio.Task[None]] = []
        self._mcast_rx_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}

        # Start unicast RX tasks on TX sockets
        for i, sock in enumerate(self._tx_socks):
            task = self._loop.create_task(self._unicast_rx_loop(sock, i))
            self._unicast_rx_tasks.append(task)

        _logger.info(
            "UDPTransport initialized: uid=0x%016x, interfaces=%s, modulus=%d",
            self._uid,
            [str(i.address) for i in self._interfaces],
            self._subject_id_modulus_val,
        )

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
        mcast_ip, port = _make_subject_endpoint(subject_id)
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

    # -- Public accessors for internal classes --

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def uid(self) -> int:
        assert self._uid is not None
        return self._uid

    @property
    def interfaces(self) -> list[Interface]:
        return self._interfaces

    @property
    def tx_socks(self) -> list[socket.socket]:
        return self._tx_socks

    def __repr__(self) -> str:
        addrs = ", ".join(str(i.address) for i in self._interfaces)
        return f"UDPTransport(uid=0x{self._uid:016x}, interfaces=[{addrs}], modulus={self._subject_id_modulus_val})"

    def remove_subject_listener(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> None:
        """
        Remove the handler for a subject; clean up sockets/tasks if none remains. Internal use only.
        """
        if self._subject_handlers.get(subject_id) is not handler:
            return
        self._subject_handlers.pop(subject_id, None)
        self._reassemblers.pop(subject_id, None)
        for i in range(len(self._interfaces)):
            key = (subject_id, i)
            task = self._mcast_rx_tasks.pop(key, None)
            if task is not None:
                task.cancel()
            sock = self._mcast_socks.pop(key, None)
            if sock is not None:
                sock.close()

    def remove_subject_writer(self, subject_id: int, writer: _UDPSubjectWriter) -> None:
        if self._subject_writers.get(subject_id) is writer:
            self._subject_writers.pop(subject_id, None)

    # -- Async sendto helper --

    async def async_sendto(self, sock: socket.socket, data: bytes, addr: tuple[str, int], deadline: Instant) -> None:
        """Send a UDP datagram, suspending until writable or deadline exceeded."""
        remaining_ns = deadline.ns - Instant.now().ns
        if remaining_ns <= 0:
            raise SendError("Deadline exceeded")
        try:
            await asyncio.wait_for(self._loop.sock_sendto(sock, data, addr), timeout=remaining_ns * 1e-9)
        except asyncio.TimeoutError:
            raise SendError("Deadline exceeded waiting for socket writability")

    # -- Transport ABC --

    @property
    def subject_id_modulus(self) -> int:
        return self._subject_id_modulus_val

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if subject_id in self._subject_handlers:
            raise ValueError(f"Subject {subject_id} already has an active listener")
        _logger.info("Subscribing to subject %d", subject_id)
        self._subject_handlers[subject_id] = handler
        for i, iface in enumerate(self._interfaces):
            key = (subject_id, i)
            sock = self._create_mcast_socket(subject_id, iface)
            self._mcast_socks[key] = sock
            task = self._loop.create_task(self._mcast_rx_loop(sock, subject_id, i))
            self._mcast_rx_tasks[key] = task
        return _UDPSubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> SubjectWriter:
        if subject_id in self._subject_writers:
            raise ValueError(f"Subject {subject_id} already has an active writer")
        _logger.info("Advertising subject %d", subject_id)
        writer = _UDPSubjectWriter(self, subject_id)
        self._subject_writers[subject_id] = writer
        return writer

    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        self._unicast_handler = handler
        _logger.info("Unicast listener set")

    async def unicast(self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview) -> None:
        if self._closed:
            raise ClosedError("Transport closed")
        transfer_id = self._next_unicast_transfer_id & TRANSFER_ID_MASK
        self._next_unicast_transfer_id += 1
        _logger.debug("Unicast tx start rid=%016x tid=%d bytes=%d", remote_id, transfer_id, len(message))

        errors: list[Exception] = []
        success_count = 0
        for i, iface in enumerate(self._interfaces):
            ep = self._remote_endpoints.get((remote_id, i))
            if ep is None:
                _logger.debug("Unicast tx skip rid=%016x iface=%d reason=no-endpoint", remote_id, i)
                continue
            frames = _segment_transfer(priority, transfer_id, self._uid, message, iface.mtu_cyphal)
            try:
                for frame in frames:
                    await self.async_sendto(self._tx_socks[i], frame, ep, deadline)
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
        for task in self._unicast_rx_tasks:
            task.cancel()
        self._unicast_rx_tasks.clear()
        for task in self._mcast_rx_tasks.values():
            task.cancel()
        self._mcast_rx_tasks.clear()
        for sock in self._tx_socks:
            sock.close()
        for sock in self._mcast_socks.values():
            sock.close()
        self._mcast_socks.clear()
        self._tx_socks.clear()
        self._subject_handlers.clear()
        self._subject_writers.clear()
        self._reassemblers.clear()

    # -- Internal async RX loops --

    async def _mcast_rx_loop(self, sock: socket.socket, subject_id: int, iface_idx: int) -> None:
        """Async receive loop for a multicast socket. Runs until cancelled or transport is closed."""
        try:
            while not self._closed:
                try:
                    data, addr = await self._loop.sock_recvfrom(sock, 65536)
                except OSError:
                    if self._closed:
                        break
                    _logger.debug("Multicast recv error on subject %d iface %d", subject_id, iface_idx)
                    await asyncio.sleep(0.1)
                    continue
                src_ip, src_port = addr[0], addr[1]
                if (src_ip, src_port) in self._self_endpoints:
                    _logger.debug("Multicast drop self sid=%d iface=%d", subject_id, iface_idx)
                    continue  # Self-send filter
                self._process_subject_datagram(data, src_ip, src_port, subject_id, iface_idx, Instant.now())
        except asyncio.CancelledError:
            _logger.debug("Multicast rx cancelled sid=%d iface=%d", subject_id, iface_idx)

    async def _unicast_rx_loop(self, sock: socket.socket, iface_idx: int) -> None:
        """Async receive loop for a unicast socket. Runs until cancelled or transport is closed."""
        try:
            while not self._closed:
                try:
                    data, addr = await self._loop.sock_recvfrom(sock, 65536)
                except OSError:
                    if self._closed:
                        break
                    _logger.debug("Unicast recv error on iface %d", iface_idx)
                    await asyncio.sleep(0.1)
                    continue
                src_ip, src_port = addr[0], addr[1]
                self._process_unicast_datagram(data, src_ip, src_port, iface_idx, Instant.now())
        except asyncio.CancelledError:
            _logger.debug("Unicast rx cancelled iface=%d", iface_idx)

    def _learn_remote_endpoint(self, remote_id: int, iface_idx: int, src_ip: str, src_port: int) -> None:
        existing = self._remote_endpoints.get((remote_id, iface_idx))
        self._remote_endpoints[(remote_id, iface_idx)] = (src_ip, src_port)
        if existing != (src_ip, src_port):
            _logger.info("Remote endpoint rid=%016x iface=%d ep=%s:%d", remote_id, iface_idx, src_ip, src_port)

    def _process_unicast_datagram(
        self, data: bytes, src_ip: str, src_port: int, iface_idx: int, timestamp: Instant | None = None
    ) -> None:
        try:
            if len(data) < HEADER_SIZE:
                # Malformed wire inputs are dropped in-place to keep the receive path exception-free.
                _logger.debug("Unicast rx drop short iface=%d len=%d", iface_idx, len(data))
                return
            header = _header_deserialize(data[:HEADER_SIZE])
            if header is None:
                _logger.debug("Unicast rx drop bad-header iface=%d len=%d", iface_idx, len(data))
                return
            payload_chunk = data[HEADER_SIZE:]
            if not _frame_is_valid(header, payload_chunk):
                _logger.debug("Unicast rx drop bad-frame iface=%d rid=%016x", iface_idx, header.sender_uid)
                return
            timestamp = Instant.now() if timestamp is None else timestamp
            self._learn_remote_endpoint(header.sender_uid, iface_idx, src_ip, src_port)
            # Keep a local fault boundary here so future wire-triggered bugs still degrade to drop+debug.
            result = self._unicast_reassembler.accept(
                header, payload_chunk, timestamp_ns=timestamp.ns, frame_validated=True
            )
            arrival = None
            if result is not None:
                arrival = TransportArrival(
                    timestamp=Instant(ns=result.timestamp_ns),
                    priority=Priority(result.priority),
                    remote_id=result.sender_uid,
                    message=result.payload,
                )
        except Exception as ex:
            _logger.debug("Unicast rx fault iface=%d %s", iface_idx, ex, exc_info=True)
            return
        if arrival is not None and self._unicast_handler is not None:
            _logger.debug("Unicast transfer complete from sender_uid=0x%016x", arrival.remote_id)
            self._unicast_handler(arrival)

    def _process_subject_datagram(
        self,
        data: bytes,
        src_ip: str,
        src_port: int,
        subject_id: int,
        iface_idx: int,
        timestamp: Instant | None = None,
    ) -> None:
        try:
            if len(data) < HEADER_SIZE:
                # Malformed wire inputs are dropped in-place to keep the receive path exception-free.
                _logger.debug("Subject rx drop short sid=%d iface=%d len=%d", subject_id, iface_idx, len(data))
                return
            header = _header_deserialize(data[:HEADER_SIZE])
            if header is None:
                _logger.debug("Subject rx drop bad-header sid=%d iface=%d len=%d", subject_id, iface_idx, len(data))
                return
            payload_chunk = data[HEADER_SIZE:]
            if not _frame_is_valid(header, payload_chunk):
                _logger.debug(
                    "Subject rx drop bad-frame sid=%d iface=%d rid=%016x", subject_id, iface_idx, header.sender_uid
                )
                return
            timestamp = Instant.now() if timestamp is None else timestamp
            self._learn_remote_endpoint(header.sender_uid, iface_idx, src_ip, src_port)
            reassembler = self._reassemblers.get(subject_id)
            if reassembler is None:
                reassembler = _RxReassembler()
                self._reassemblers[subject_id] = reassembler
                _logger.debug("Subject reasm create sid=%d", subject_id)
            # Keep a local fault boundary here so future wire-triggered bugs still degrade to drop+debug.
            result = reassembler.accept(header, payload_chunk, timestamp_ns=timestamp.ns, frame_validated=True)
            handler = self._subject_handlers.get(subject_id)
            arrival = None
            if result is not None:
                arrival = TransportArrival(
                    timestamp=Instant(ns=result.timestamp_ns),
                    priority=Priority(result.priority),
                    remote_id=result.sender_uid,
                    message=result.payload,
                )
        except Exception as ex:
            _logger.debug("Subject rx fault sid=%d iface=%d %s", subject_id, iface_idx, ex, exc_info=True)
            return
        if arrival is not None:
            _logger.debug("Subject %d transfer complete from sender_uid=0x%016x", subject_id, arrival.remote_id)
            if handler is not None:
                handler(arrival)
