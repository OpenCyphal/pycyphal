from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass
import logging
import os
import random

from .._api import ClosedError, Closable, Instant, Priority, SUBJECT_ID_PINNED_MAX, SendError
from .._hash import rapidhash
from .._header import HEADER_SIZE
from .._transport import SUBJECT_ID_MODULUS_16bit, SubjectWriter, Transport, TransportArrival
from ._interface import Filter, Interface, TimestampedFrame
from ._reassembly import Endpoint, Reassembler
from ._wire import (
    MTU_CAN_CLASSIC,
    MTU_CAN_FD,
    NODE_ID_ANONYMOUS,
    NODE_ID_CAPACITY,
    NODE_ID_MAX,
    SUBJECT_ID_MAX_16,
    TRANSFER_ID_MODULO,
    ParsedFrame,
    TransferKind,
    UNICAST_SERVICE_ID,
    ensure_forced_filters,
    make_filter,
    pack_u32_le,
    pack_u64_le,
    parse_frames,
    serialize_transfer,
)

_logger = logging.getLogger(__name__)


@dataclass
class _PinnedSubjectState:
    subject_id: int
    header_prefix: bytes
    next_tag: int = 0

    @staticmethod
    def new(subject_id: int) -> _PinnedSubjectState:
        buf = bytearray(HEADER_SIZE)
        buf[3] = 0xFF
        buf[4:8] = pack_u32_le(0xFFFFFFFF - subject_id)
        buf[8:16] = pack_u64_le(rapidhash(str(subject_id)))
        return _PinnedSubjectState(subject_id=subject_id, header_prefix=bytes(buf[:16]))

    def wrap(self, payload: bytes) -> bytes:
        self.next_tag += 1
        return self.header_prefix + pack_u64_le(self.next_tag) + payload


class CANTransport(Transport, ABC):
    @property
    @abstractmethod
    def id(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def interfaces(self) -> list[Interface]:
        raise NotImplementedError

    @property
    @abstractmethod
    def closed(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def collision_count(self) -> int:
        raise NotImplementedError

    @staticmethod
    def new(interfaces: Iterable[Interface] | Interface) -> CANTransport:
        if isinstance(interfaces, Interface):
            items = [interfaces]
        else:
            items = list(interfaces)
        if not items or not all(isinstance(itf, Interface) for itf in items):
            raise ValueError("interfaces must contain at least one Interface instance")
        return _CANTransportImpl(items)


class _SubjectWriter(SubjectWriter):
    def __init__(self, transport: _CANTransportImpl, subject_id: int) -> None:
        self._transport = transport
        self._subject_id = subject_id
        self._closed = False
        self._next_tid_13 = 0
        self._next_tid_16 = 0

    async def __call__(self, deadline: Instant, priority: Priority, message: bytes | memoryview) -> None:
        if self._closed:
            raise ClosedError("CAN subject writer closed")
        if self._transport.closed:
            raise ClosedError("CAN transport closed")
        data = bytes(message)
        pinned = self._subject_id <= SUBJECT_ID_PINNED_MAX
        best_effort = len(data) >= HEADER_SIZE and data[0] == 0
        use_13b = pinned and best_effort
        if use_13b:
            transfer_id = self._next_tid_13
            self._next_tid_13 = (transfer_id + 1) % TRANSFER_ID_MODULO
            payload = data[HEADER_SIZE:]
            kind = TransferKind.MESSAGE_13
        else:
            transfer_id = self._next_tid_16
            self._next_tid_16 = (transfer_id + 1) % TRANSFER_ID_MODULO
            payload = data
            kind = TransferKind.MESSAGE_16
        await self._transport.send_transfer(
            deadline=deadline,
            priority=priority,
            kind=kind,
            port_id=self._subject_id,
            payload=payload,
            transfer_id=transfer_id,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._transport.remove_subject_writer(self._subject_id, self)


class _SubjectListener(Closable):
    def __init__(
        self, transport: _CANTransportImpl, subject_id: int, handler: Callable[[TransportArrival], None]
    ) -> None:
        self._transport = transport
        self._subject_id = subject_id
        self._handler = handler
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._transport.remove_subject_listener(self._subject_id, self._handler)


class _CANTransportImpl(CANTransport):
    def __init__(self, interfaces: Iterable[Interface]) -> None:
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._interfaces = list(interfaces)
        if not self._interfaces:
            raise ValueError("At least one CAN interface is required")
        if len({itf.fd for itf in self._interfaces}) > 1:
            raise ValueError("Mixed Classic-CAN and CAN FD interface sets are not supported")

        self._fd = self._interfaces[0].fd
        self._interface_index = {id(itf): i for i, itf in enumerate(self._interfaces)}
        self._reader_tasks: dict[int, asyncio.Task[None]] = {}
        self._filter_dirty: set[Interface] = set(self._interfaces)
        self._filter_retry_event = asyncio.Event()
        self._filter_failures: dict[Interface, int] = {}
        self._rng = random.Random(int.from_bytes(os.urandom(8), "little"))
        self._node_id_occupancy = 1
        self._local_node_id = self._rng.randrange(1, NODE_ID_CAPACITY)
        self._collision_count = 0
        self._subject_handlers: dict[int, Callable[[TransportArrival], None]] = {}
        self._subject_writers: dict[int, _SubjectWriter] = {}
        self._pinned_subjects: dict[int, _PinnedSubjectState] = {}
        self._endpoints: dict[tuple[TransferKind, int], Endpoint] = {}
        self._unicast_handler: Callable[[TransportArrival], None] | None = None
        self._unicast_tid = [0] * NODE_ID_CAPACITY
        self._filter_retry_task = self._loop.create_task(self._filter_retry_loop())
        self._cleanup_task = self._loop.create_task(self._cleanup_loop())

        self._install_unicast_endpoint()
        for itf in self._interfaces:
            self._reader_tasks[id(itf)] = self._loop.create_task(self._reader_loop(itf))
        self._refresh_filters()
        _logger.info(
            "CAN transport init ifaces=%s fd=%s nid=%d", [itf.name for itf in self._interfaces], self._fd, self.id
        )

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def id(self) -> int:
        return self._local_node_id

    @property
    def interfaces(self) -> list[Interface]:
        return list(self._interfaces)

    @property
    def collision_count(self) -> int:
        return self._collision_count

    @property
    def subject_id_modulus(self) -> int:
        return SUBJECT_ID_MODULUS_16bit

    def __repr__(self) -> str:
        return f"CANTransport(id={self.id}, fd={self._fd}, interfaces={[itf.name for itf in self._interfaces]!r})"

    def subject_listen(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> Closable:
        if not (0 <= subject_id <= SUBJECT_ID_MAX_16):
            raise ValueError(f"Invalid subject-ID: {subject_id}")
        if subject_id in self._subject_handlers:
            raise ValueError(f"Subject {subject_id} already has an active listener")
        self._subject_handlers[subject_id] = handler

        def on_transfer_16(timestamp: Instant, remote_id: int, priority: Priority, payload: bytes) -> None:
            handler(TransportArrival(timestamp, priority, remote_id, payload))

        self._endpoints[(TransferKind.MESSAGE_16, subject_id)] = Endpoint(
            kind=TransferKind.MESSAGE_16,
            port_id=subject_id,
            on_transfer=on_transfer_16,
        )
        if subject_id <= SUBJECT_ID_PINNED_MAX:
            pinned = self._pinned_subjects.setdefault(subject_id, _PinnedSubjectState.new(subject_id))

            def on_transfer_13(timestamp: Instant, remote_id: int, priority: Priority, payload: bytes) -> None:
                handler(TransportArrival(timestamp, priority, remote_id, pinned.wrap(payload)))

            self._endpoints[(TransferKind.MESSAGE_13, subject_id)] = Endpoint(
                kind=TransferKind.MESSAGE_13,
                port_id=subject_id,
                on_transfer=on_transfer_13,
            )
        self._refresh_filters()
        return _SubjectListener(self, subject_id, handler)

    def subject_advertise(self, subject_id: int) -> SubjectWriter:
        if not (0 <= subject_id <= SUBJECT_ID_MAX_16):
            raise ValueError(f"Invalid subject-ID: {subject_id}")
        if subject_id in self._subject_writers:
            raise ValueError(f"Subject {subject_id} already has an active writer")
        writer = _SubjectWriter(self, subject_id)
        self._subject_writers[subject_id] = writer
        return writer

    def unicast_listen(self, handler: Callable[[TransportArrival], None]) -> None:
        self._unicast_handler = handler

    async def unicast(self, deadline: Instant, priority: Priority, remote_id: int, message: bytes | memoryview) -> None:
        if self._closed:
            raise ClosedError("CAN transport closed")
        if not (1 <= remote_id <= NODE_ID_MAX):
            raise ValueError(f"Invalid remote node-ID: {remote_id}")
        transfer_id = self._unicast_tid[remote_id]
        self._unicast_tid[remote_id] = (transfer_id + 1) % TRANSFER_ID_MODULO
        await self.send_transfer(
            deadline=deadline,
            priority=priority,
            kind=TransferKind.REQUEST,
            port_id=UNICAST_SERVICE_ID,
            payload=bytes(message),
            transfer_id=transfer_id,
            destination_id=remote_id,
        )

    async def send_transfer(
        self,
        *,
        deadline: Instant,
        priority: Priority,
        kind: TransferKind,
        port_id: int,
        payload: bytes | memoryview,
        transfer_id: int,
        destination_id: int | None = None,
    ) -> None:
        if self._closed:
            raise ClosedError("CAN transport closed")
        if Instant.now().ns >= deadline.ns:
            raise SendError("Deadline exceeded")
        identifier, frames = serialize_transfer(
            kind=kind,
            priority=int(priority),
            port_id=port_id,
            source_id=self._local_node_id,
            destination_id=destination_id,
            payload=payload,
            transfer_id=transfer_id,
            fd=self._fd,
        )
        views = tuple(memoryview(frm) for frm in frames)
        accepted = 0
        errors: list[BaseException] = []
        for itf in tuple(self._interfaces):
            try:
                itf.enqueue(identifier, views, deadline)
            except ClosedError as ex:
                errors.append(ex)
                self._drop_interface(itf, ex)
            except Exception as ex:  # pragma: no cover - exercised via tests with injected failures
                errors.append(ex)
                _logger.debug("CAN iface %s tx rejected: %s", itf.name, ex)
            else:
                accepted += 1
        if accepted > 0:
            return
        first_error = errors[0] if errors else None
        if self._closed:
            raise ClosedError("CAN transport closed") from first_error
        raise SendError("CAN transfer rejected by all interfaces") from first_error

    def remove_subject_listener(self, subject_id: int, handler: Callable[[TransportArrival], None]) -> None:
        if self._subject_handlers.get(subject_id) is not handler:
            return
        self._subject_handlers.pop(subject_id, None)
        self._endpoints.pop((TransferKind.MESSAGE_16, subject_id), None)
        self._endpoints.pop((TransferKind.MESSAGE_13, subject_id), None)
        self._pinned_subjects.pop(subject_id, None)
        self._refresh_filters()

    def remove_subject_writer(self, subject_id: int, writer: _SubjectWriter) -> None:
        if self._subject_writers.get(subject_id) is writer:
            self._subject_writers.pop(subject_id, None)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._filter_retry_task.cancel()
        self._cleanup_task.cancel()
        for task in self._reader_tasks.values():
            task.cancel()
        self._reader_tasks.clear()
        for itf in self._interfaces:
            itf.close()
        self._interfaces.clear()
        self._filter_dirty.clear()
        self._filter_failures.clear()
        self._subject_handlers.clear()
        self._subject_writers.clear()
        self._pinned_subjects.clear()
        self._endpoints.clear()
        self._unicast_handler = None

    async def _reader_loop(self, itf: Interface) -> None:
        while not self._closed:
            try:
                frame = await itf.receive()
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                if not self._closed:
                    self._drop_interface(itf, ex)
                return
            iface_index = self._interface_index.get(id(itf))
            if iface_index is None:
                return
            self._ingest_frame(iface_index, frame)

    def _drop_interface(self, itf: Interface, ex: BaseException) -> None:
        if itf not in self._interfaces:
            return
        _logger.error("CAN iface %s failed and is being removed: %s", itf.name, ex)
        self._interfaces.remove(itf)
        self._interface_index.pop(id(itf), None)
        self._filter_dirty.discard(itf)
        self._filter_failures.pop(itf, None)
        task = self._reader_tasks.pop(id(itf), None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        try:
            itf.close()
        except Exception:  # pragma: no cover - defensive
            _logger.exception("CAN iface %s close failed", itf.name)
        if not self._interfaces:
            _logger.critical("CAN transport closed because no interfaces remain")
            self.close()

    def _install_unicast_endpoint(self) -> None:
        self._endpoints[(TransferKind.REQUEST, UNICAST_SERVICE_ID)] = Endpoint(
            kind=TransferKind.REQUEST,
            port_id=UNICAST_SERVICE_ID,
            on_transfer=self._on_unicast_transfer,
        )

    def _on_unicast_transfer(self, timestamp: Instant, remote_id: int, priority: Priority, payload: bytes) -> None:
        handler = self._unicast_handler
        if handler is not None:
            handler(TransportArrival(timestamp, priority, remote_id, payload))

    def _current_filters(self) -> list[Filter]:
        filters = [make_filter(TransferKind.REQUEST, UNICAST_SERVICE_ID, self._local_node_id)]
        for subject_id in self._subject_handlers:
            filters.append(make_filter(TransferKind.MESSAGE_16, subject_id, self._local_node_id))
            if subject_id <= SUBJECT_ID_PINNED_MAX:
                filters.append(make_filter(TransferKind.MESSAGE_13, subject_id, self._local_node_id))
        return ensure_forced_filters(filters, self._local_node_id)

    def _mark_filters_dirty(self, interfaces: Iterable[Interface] | None = None) -> None:
        if interfaces is None:
            self._filter_dirty.update(self._interfaces)
        else:
            self._filter_dirty.update(itf for itf in interfaces if itf in self._interfaces)

    def _refresh_filters(self) -> None:
        self._mark_filters_dirty()
        self._apply_dirty_filters()
        if self._filter_dirty:
            self._filter_retry_event.set()

    def _apply_dirty_filters(self) -> None:
        if self._closed:
            return
        filters = self._current_filters()
        for itf in tuple(self._filter_dirty):
            if itf not in self._interfaces:
                self._filter_dirty.discard(itf)
                self._filter_failures.pop(itf, None)
                continue
            try:
                itf.filter(filters)
            except Exception as ex:
                failures = self._filter_failures.get(itf, 0) + 1
                self._filter_failures[itf] = failures
                if failures == 1:
                    _logger.critical("CAN iface %s filter apply failed: %s", itf.name, ex)
                else:
                    _logger.debug("CAN iface %s filter retry failed #%d: %s", itf.name, failures, ex)
            else:
                if self._filter_failures.pop(itf, None) is not None:
                    _logger.info("CAN iface %s filter apply recovered", itf.name)
                self._filter_dirty.discard(itf)

    async def _filter_retry_loop(self) -> None:
        try:
            while not self._closed:
                if not self._filter_dirty:
                    self._filter_retry_event.clear()
                    await self._filter_retry_event.wait()
                    continue
                self._apply_dirty_filters()
                if not self._filter_dirty:
                    continue
                attempts = max(self._filter_failures.get(itf, 1) for itf in self._filter_dirty)
                delay = min(1.0, 0.05 * (2 ** min(attempts - 1, 4)))
                self._filter_retry_event.clear()
                try:
                    await asyncio.wait_for(self._filter_retry_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise

    async def _cleanup_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(1.0)
                Reassembler.cleanup_sessions(self._endpoints.values(), Instant.now().ns)
        except asyncio.CancelledError:
            raise

    def _ingest_frame(self, iface_index: int, frame: TimestampedFrame) -> None:
        parsed_items = parse_frames(frame.id, frame.data, mtu=MTU_CAN_FD if self._fd else MTU_CAN_CLASSIC)
        if not parsed_items:
            _logger.debug("CAN drop malformed id=%08x len=%d", frame.id, len(frame.data))
            return
        for parsed in parsed_items:
            if parsed.start_of_transfer:
                self._node_id_occupancy_update(parsed.source_id)
            endpoint = self._route_endpoint(parsed)
            if endpoint is not None:
                Reassembler.ingest(endpoint, iface_index, frame.timestamp, parsed)

    def _route_endpoint(self, parsed: ParsedFrame) -> Endpoint | None:
        if parsed.kind is TransferKind.MESSAGE_16:
            return self._endpoints.get((TransferKind.MESSAGE_16, parsed.port_id))
        if parsed.kind is TransferKind.MESSAGE_13:
            return self._endpoints.get((TransferKind.MESSAGE_13, parsed.port_id))
        if (
            parsed.kind is TransferKind.REQUEST
            and parsed.port_id == UNICAST_SERVICE_ID
            and parsed.destination_id == self._local_node_id
        ):
            return self._endpoints.get((TransferKind.REQUEST, UNICAST_SERVICE_ID))
        return None

    def _purge_interfaces(self) -> None:
        # REFERENCE PARITY: Because TX queues are backend-owned in this design,
        # a node-ID collision drops each backend queue wholesale instead of preserving unstarted transfers.
        for itf in tuple(self._interfaces):
            try:
                itf.purge()
            except Exception as ex:  # pragma: no cover - defensive
                _logger.error("CAN iface %s purge failed: %s", itf.name, ex)

    def _node_id_occupancy_update(self, source_id: int) -> None:
        if source_id == NODE_ID_ANONYMOUS:
            return
        mask = 1 << source_id
        if (self._node_id_occupancy & mask) and (self._local_node_id != source_id):
            return
        self._node_id_occupancy |= mask
        population = self._node_id_occupancy.bit_count()
        free_count = NODE_ID_CAPACITY - population
        purge = free_count > 0 and population > (NODE_ID_CAPACITY // 2) and (self._rng.randrange(free_count) == 0)
        if self._local_node_id == source_id:
            if free_count > 0:
                free_index = self._rng.randrange(free_count)
                new_node_id = 0
                while True:
                    if (self._node_id_occupancy & (1 << new_node_id)) == 0:
                        if free_index == 0:
                            break
                        free_index -= 1
                    new_node_id += 1
                self._local_node_id = new_node_id
                self._collision_count += 1
                self._purge_interfaces()
                self._refresh_filters()
                _logger.warning("CAN node-ID collision detected, switched to %d", self._local_node_id)
            else:
                _logger.warning("CAN node-ID collision detected on %d but no free slot remains", source_id)
        if purge:
            self._node_id_occupancy = 1 | mask
