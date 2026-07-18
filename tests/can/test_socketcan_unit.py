from __future__ import annotations

import asyncio
import errno
from pathlib import Path
import sys
import types
from typing import Any, Awaitable, cast

import pytest

from pycyphal2 import ClosedError, Instant, SendError
from pycyphal2.can import Filter, TimestampedFrame
from pycyphal2.can._tx import TxEntry, TxJob, TxQueue, new_tx_job

_SOURCE = Path(__file__).resolve().parents[2] / "src/pycyphal2/can/socketcan.py"


class _FakeRawSocket:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def setblocking(self, enabled: bool) -> None:
        self.calls.append(("setblocking", enabled))

    def setsockopt(self, level: int, option: int, value: object) -> None:
        self.calls.append(("setsockopt", level, option, value))

    def bind(self, address: tuple[str]) -> None:
        self.calls.append(("bind", address))

    def close(self) -> None:
        self.calls.append(("close",))


class _TaskStub:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True

    def add_done_callback(self, _cb: object) -> None:
        pass


class _FakeLoop:
    def __init__(self, *, recv: list[object] | None = None, send: list[object] | None = None) -> None:
        self.recv = list(recv or [])
        self.send = list(send or [])
        self.sent_frames: list[bytes] = []
        self.created_tasks: list[object] = []

    def create_task(self, coro: object) -> _TaskStub:
        if hasattr(coro, "close"):
            coro.close()  # type: ignore[call-arg]
        task = _TaskStub()
        self.created_tasks.append(task)
        return task

    def create_future(self) -> asyncio.Future[Any]:
        # These tests patch asyncio.get_running_loop, so the TX machinery reaches this fake loop when it
        # builds a completion future. asyncio.events holds the unpatched function.
        return asyncio.events.get_running_loop().create_future()

    async def sock_recv(self, _sock: object, _size: int) -> bytes:
        item = self.recv.pop(0)
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, bytes)
        return item

    async def sock_sendall(self, _sock: object, frame: bytes) -> None:
        self.sent_frames.append(frame)
        if self.send:
            item = self.send.pop(0)
            if isinstance(item, BaseException):
                raise item


def _entry(id: int, seq: int, deadline_ns: int, payload: bytes) -> TxEntry:
    return TxEntry(id, seq, payload, new_tx_job(1, Instant(ns=deadline_ns)))


class _TxScript:
    """Stands in for TxQueue, feeding _tx_loop a scripted sequence of entries."""

    def __init__(self, iface: object, items: list[object]) -> None:
        self._iface = iface
        self._items = list(items)
        self.requeued: list[TxEntry] = []
        self.popped = 0
        self.inflight: TxJob | None = None

    async def pop(self) -> TxEntry:
        if self._items:
            item = self._items.pop(0)
            entry = item() if callable(item) else item
            assert isinstance(entry, TxEntry)
            self.inflight = entry.job
            self.popped += 1
            return entry
        self._iface._closed = True  # type: ignore[attr-defined]
        return _entry(0, 0, 0, b"")

    def requeue(self, entry: TxEntry) -> None:
        self.requeued.append(entry)

    def abort_all(self, make_error: object) -> int:
        dropped = 0
        for item in self._items:
            if isinstance(item, TxEntry):
                item.job.abort(cast(Any, make_error)())
                dropped += 1
        self._items.clear()
        if self.inflight is not None:
            self.inflight.abort(cast(Any, make_error)())
        return dropped


def _make_socket_module() -> tuple[types.SimpleNamespace, list[_FakeRawSocket]]:
    created: list[_FakeRawSocket] = []

    def socket_ctor(*_args: object) -> _FakeRawSocket:
        sock = _FakeRawSocket()
        created.append(sock)
        return sock

    module = types.SimpleNamespace(
        AF_CAN=29,
        PF_CAN=29,
        SOCK_RAW=3,
        CAN_RAW=1,
        SOL_CAN_RAW=101,
        CAN_RAW_LOOPBACK=3,
        CAN_RAW_FD_FRAMES=5,
        CAN_RAW_FILTER=7,
        CAN_EFF_FLAG=0x80000000,
        CAN_EFF_MASK=0x1FFFFFFF,
        CAN_RTR_FLAG=0x40000000,
        CAN_ERR_FLAG=0x20000000,
        CANFD_FDF=0x04,
        socket=socket_ctor,
    )
    return module, created


def _load_socketcan_module(
    monkeypatch: pytest.MonkeyPatch, *, platform: str = "linux", socket_module: object | None = None
) -> types.ModuleType:
    module = types.ModuleType(f"pycyphal2.can._socketcan_unit_{platform}_{id(socket_module)}")
    module.__file__ = str(_SOURCE)
    module.__package__ = "pycyphal2.can"
    monkeypatch.setattr(sys, "platform", platform)
    if socket_module is not None:
        monkeypatch.setitem(sys.modules, "socket", socket_module)
    exec(compile(_SOURCE.read_text(), str(_SOURCE), "exec"), module.__dict__)
    return module


def _make_iface(
    module: types.ModuleType, *, fd: bool = False, closed: bool = False, failure: BaseException | None = None
) -> Any:
    iface = object.__new__(module.SocketCANInterface)
    iface._name = "vcan0"
    iface._sock = _FakeRawSocket()
    iface._fd = fd
    iface._closed = closed
    iface._failure = failure
    iface._tx = TxQueue()
    iface._tx_task = None
    iface._rx_queue = asyncio.Queue()
    iface._rx_task = None
    return iface


def test_socketcan_import_guard_rejects_non_linux(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ImportError, match="SocketCAN is available only on Linux"):
        _load_socketcan_module(monkeypatch, platform="darwin")


def test_socketcan_init_fd_and_classic_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, created = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)

    monkeypatch.setattr(module.SocketCANInterface, "_read_iface_mtu", lambda self: module._FD_FRAME_SIZE)
    fd_iface = module.SocketCANInterface("vcan0")
    fd_sock = created[-1]
    assert fd_iface.name == "vcan0"
    assert fd_iface.fd is True
    assert ("setsockopt", fake_socket.SOL_CAN_RAW, fake_socket.CAN_RAW_FD_FRAMES, 1) in fd_sock.calls
    assert "vcan0" in repr(fd_iface)

    monkeypatch.setattr(module.SocketCANInterface, "_read_iface_mtu", lambda self: module._CLASSIC_FRAME_SIZE)
    classic_iface = module.SocketCANInterface("vcan1")
    classic_sock = created[-1]
    assert classic_iface.fd is False
    assert ("bind", ("vcan1",)) in classic_sock.calls

    fd_iface.close()
    classic_iface.close()


def test_filter_coalesces_and_respects_closed_state(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)

    iface.filter([Filter(id=1, mask=fake_socket.CAN_EFF_MASK)])
    iface.filter(Filter(id=i, mask=fake_socket.CAN_EFF_MASK) for i in range(module._CAN_FILTER_CAPACITY + 1))
    packed = [
        call
        for call in iface._sock.calls
        if call[:3] == ("setsockopt", fake_socket.SOL_CAN_RAW, fake_socket.CAN_RAW_FILTER)
    ]
    assert packed
    assert len(packed[-1][3]) == module._CAN_FILTER_STRUCT.size * module._CAN_FILTER_CAPACITY

    iface._closed = True
    with pytest.raises(ClosedError, match="closed"):
        iface.filter([])


async def test_enqueue_purge_and_close_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)

    deadline = Instant(ns=10)
    purged_a = iface.enqueue(123, [memoryview(b"a")], deadline)
    purged_b = iface.enqueue(123, [memoryview(b"b")], deadline)
    assert len(loop.created_tasks) == 1
    assert iface._tx.qsize() == 2

    iface.purge()
    assert iface._tx.qsize() == 0
    for fut in (purged_a, purged_b):
        with pytest.raises(SendError):
            await fut
    iface.purge()

    closed_fut = iface.enqueue(123, [memoryview(b"c")], deadline)
    task = iface._tx_task
    assert isinstance(task, _TaskStub)
    iface.close()
    iface.close()
    assert task.cancelled is True
    with pytest.raises(ClosedError):
        await closed_fut

    closed = _make_iface(module, closed=True)
    closed.purge()


async def test_rx_loop_decodes_skips_and_drops_cleanly_on_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    good = module._CAN_FRAME_STRUCT.pack(fake_socket.CAN_EFF_FLAG | 0x123, 2, b"ab".ljust(8, b"\x00"))
    loop = _FakeLoop(recv=[b"\x00", good, asyncio.CancelledError()])  # Undecodable, good, then cancelled.
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)

    with pytest.raises(asyncio.CancelledError):
        await iface._rx_loop()
    frame = iface._rx_queue.get_nowait()  # The good frame was queued; the undecodable one was dropped.
    assert isinstance(frame, TimestampedFrame)
    assert frame.id == 0x123
    assert frame.data == b"ab"
    assert iface._rx_queue.empty()


async def test_rx_loop_failure_marks_interface_and_installs_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    err = OSError("rx failed")
    loop = _FakeLoop(recv=[err])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)

    await iface._rx_loop()  # Fails the interface via _fail(), then returns.
    assert iface._closed is True
    assert iface._failure is err
    sentinel = iface._rx_queue.get_nowait()
    assert isinstance(sentinel, ClosedError)


async def test_receive_raises_clean_close_without_recording_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit close is surfaced to a parked receive() as a plain ClosedError and is not recorded as
    an interface failure (only a receive-side error is)."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    iface._rx_task = _TaskStub()  # Already spawned, so receive() only drains the queue.
    iface._rx_queue.put_nowait(ClosedError("SocketCAN interface vcan0 closed"))
    with pytest.raises(ClosedError):
        await iface.receive()
    assert iface._failure is None


async def test_close_wakes_parked_receiver(monkeypatch: pytest.MonkeyPatch) -> None:
    """close() must wake a reader parked on the RX queue with a ClosedError sentinel, so interface loss
    propagates instead of leaving the transport's reader hung (review finding #10)."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    iface._rx_task = asyncio.create_task(asyncio.sleep(100))  # Stand-in for the RX loop task.

    recv_task = asyncio.create_task(iface.receive())
    await asyncio.sleep(0)
    assert not recv_task.done()  # Parked on the empty queue.

    iface.close()
    with pytest.raises(ClosedError):
        await asyncio.wait_for(recv_task, timeout=1.0)
    assert iface._rx_task is None
    assert iface._failure is None  # A clean close is not an interface failure.


async def test_fail_wakes_parked_receiver(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-transient TX failure (_fail -> close) must likewise unblock a parked reader."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    iface._rx_task = asyncio.create_task(asyncio.sleep(100))

    recv_task = asyncio.create_task(iface.receive())
    await asyncio.sleep(0)

    iface._fail(OSError("ENETDOWN"))
    with pytest.raises(ClosedError):
        await asyncio.wait_for(recv_task, timeout=1.0)
    assert isinstance(iface._failure, OSError)


def test_raise_if_closed_and_transient_error_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)

    closed = _make_iface(module, closed=True)
    with pytest.raises(ClosedError, match="closed"):
        closed._raise_if_closed()

    failed = _make_iface(module, closed=True, failure=OSError("boom"))
    with pytest.raises(ClosedError, match="failed"):
        failed._raise_if_closed()

    assert module.SocketCANInterface._is_transient_tx_error(OSError(errno.EAGAIN, "again")) is True
    assert module.SocketCANInterface._is_transient_tx_error(OSError(errno.EINVAL, "bad")) is False


def test_encode_and_decode_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=123)))

    classic = _make_iface(module, fd=False)
    with pytest.raises(ValueError, match="Classic CAN"):
        classic._encode(123, b"012345678")

    encoded_classic = classic._encode(123, b"abc")
    assert len(encoded_classic) == module._CLASSIC_FRAME_SIZE

    fd_iface = _make_iface(module, fd=True)
    encoded_fd = fd_iface._encode(456, b"012345678")
    assert len(encoded_fd) == module._FD_FRAME_SIZE

    # The frame format follows the interface mode, not the payload length: a short payload on an FD
    # interface is still emitted as an FD frame, as in the reference.
    encoded_fd_short = fd_iface._encode(456, b"abc")
    assert len(encoded_fd_short) == module._FD_FRAME_SIZE

    assert module.SocketCANInterface._decode(b"\x00") is None

    non_extended = module._CAN_FRAME_STRUCT.pack(0x123, 1, b"x".ljust(8, b"\x00"))
    assert module.SocketCANInterface._decode(non_extended) is None

    bad_flags = module._CAN_FRAME_STRUCT.pack(
        fake_socket.CAN_EFF_FLAG | fake_socket.CAN_RTR_FLAG | 0x123,
        1,
        b"x".ljust(8, b"\x00"),
    )
    assert module.SocketCANInterface._decode(bad_flags) is None

    good_classic = module._CAN_FRAME_STRUCT.pack(fake_socket.CAN_EFF_FLAG | 0x123, 3, b"abc".ljust(8, b"\x00"))
    assert module.SocketCANInterface._decode(good_classic) == TimestampedFrame(
        id=0x123, data=b"abc", timestamp=Instant(ns=123)
    )

    good_fd = module._CANFD_FRAME_STRUCT.pack(
        fake_socket.CAN_EFF_FLAG | 0x456,
        70,
        fake_socket.CANFD_FDF,
        0,
        0,
        bytes(range(64)),
    )
    assert module.SocketCANInterface._decode(good_fd) == TimestampedFrame(
        id=0x456,
        data=bytes(range(64)),
        timestamp=Instant(ns=123),
    )


def test_read_iface_mtu_and_list_interfaces_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)

    class _Leaf:
        def __init__(self, text: str | None = None, exc: BaseException | None = None) -> None:
            self._text = text
            self._exc = exc

        def read_text(self) -> str:
            if self._exc is not None:
                raise self._exc
            assert self._text is not None
            return self._text

    class _Node:
        def __init__(self, name: str, type_file: _Leaf) -> None:
            self.name = name
            self._type_file = type_file

        def __lt__(self, other: object) -> bool:
            assert isinstance(other, _Node)
            return self.name < other.name

        def __truediv__(self, item: str) -> _Leaf:
            assert item == "type"
            return self._type_file

    root = [
        _Node("can0", _Leaf("280")),
        _Node("eth0", _Leaf("1")),
        _Node("bad", _Leaf("xx")),
        _Node("err", _Leaf(exc=OSError("oops"))),
    ]
    mapping = {
        "/sys/class/net/vcan0/mtu": _Leaf("72"),
        "/sys/class/net": types.SimpleNamespace(iterdir=lambda: iter(root)),
    }
    monkeypatch.setattr(module, "Path", lambda path: mapping[path])

    assert iface._read_iface_mtu() == 72
    assert module.SocketCANInterface.list_interfaces() == ["can0"]

    broken_root = types.SimpleNamespace(iterdir=lambda: (_ for _ in ()).throw(OSError("no sysfs")))
    monkeypatch.setattr(module, "Path", lambda _path: broken_root)
    assert module.SocketCANInterface.list_interfaces() == []


async def test_tx_loop_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)

    success = _make_iface(module)
    success_entry = _entry(10, 1, 100, b"abc")
    success._tx = _TxScript(success, [success_entry])
    success_loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: success_loop)
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=0)))

    async def wait_success(coro: object, timeout: float) -> None:
        del timeout
        await cast(Awaitable[object], coro)
        success._closed = True

    monkeypatch.setattr(module.asyncio, "wait_for", wait_success)
    await success._tx_loop()
    assert success_loop.sent_frames
    assert success_entry.job.future.result() is None

    cancelled = _make_iface(module)

    class _CancelledQueue:
        async def pop(self) -> TxEntry:
            raise asyncio.CancelledError

    cancelled._tx = _CancelledQueue()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    with pytest.raises(asyncio.CancelledError):
        await cancelled._tx_loop()

    post_get_close = _make_iface(module)
    post_get_close._tx = _TxScript(
        post_get_close, [lambda: _close_then_return(post_get_close, _entry(11, 1, 100, b"x"))]
    )
    post_get_loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: post_get_loop)
    await post_get_close._tx_loop()
    assert post_get_loop.sent_frames == []

    aborted = _make_iface(module)  # The tail of an already-failed transfer is skipped, not sent.
    aborted_entry = _entry(17, 1, 100, b"x")
    aborted_entry.job.abort(SendError("already failed"))
    aborted._tx = _TxScript(aborted, [aborted_entry])
    aborted_loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: aborted_loop)
    await aborted._tx_loop()
    assert aborted_loop.sent_frames == []

    expired = _make_iface(module)
    expired_entry = _entry(12, 1, 0, b"x")
    expired._tx = _TxScript(expired, [expired_entry])
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=1)))
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    await expired._tx_loop()
    with pytest.raises(SendError):
        await expired_entry.job.future

    timeout_zero = _make_iface(module)
    timeout_zero._tx = _TxScript(timeout_zero, [_entry(13, 1, 1, b"x")])
    times = iter([Instant(ns=0), Instant(ns=2)])
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: next(times)))
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    await timeout_zero._tx_loop()

    timeout_retry = _make_iface(module)
    retry_entry = _entry(14, 1, 100, b"x")
    timeout_retry._tx = _TxScript(timeout_retry, [retry_entry])
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=0)))
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())

    async def wait_timeout(_coro: object, timeout: float) -> None:
        del timeout
        if hasattr(_coro, "close"):
            _coro.close()  # type: ignore[call-arg]
        raise asyncio.TimeoutError

    async def sleep_timeout(_delay: float) -> None:
        timeout_retry._closed = True

    monkeypatch.setattr(module.asyncio, "wait_for", wait_timeout)
    monkeypatch.setattr(module.asyncio, "sleep", sleep_timeout)
    await timeout_retry._tx_loop()
    assert timeout_retry._tx.requeued == [retry_entry]
    assert not retry_entry.job.settled  # Still owed.

    transient_retry = _make_iface(module)
    transient_entry = _entry(15, 1, 100, b"x")
    transient_retry._tx = _TxScript(transient_retry, [transient_entry])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())

    async def wait_transient(_coro: object, timeout: float) -> None:
        del timeout
        if hasattr(_coro, "close"):
            _coro.close()  # type: ignore[call-arg]
        raise OSError(errno.EAGAIN, "again")

    async def sleep_transient(_delay: float) -> None:
        transient_retry._closed = True

    monkeypatch.setattr(module.asyncio, "wait_for", wait_transient)
    monkeypatch.setattr(module.asyncio, "sleep", sleep_transient)
    await transient_retry._tx_loop()
    assert transient_retry._tx.requeued == [transient_entry]
    assert not transient_entry.job.settled

    permanent_fail = _make_iface(module)
    permanent_entry = _entry(16, 1, 100, b"x")
    permanent_fail._tx = _TxScript(permanent_fail, [permanent_entry])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())

    async def wait_permanent(_coro: object, timeout: float) -> None:
        del timeout
        if hasattr(_coro, "close"):
            _coro.close()  # type: ignore[call-arg]
        raise OSError(errno.EINVAL, "bad")

    monkeypatch.setattr(module.asyncio, "wait_for", wait_permanent)
    await permanent_fail._tx_loop()
    assert permanent_fail._closed is True
    assert isinstance(permanent_fail._failure, OSError)
    with pytest.raises(ClosedError):  # A fatal error fails the in-flight transfer.
        await permanent_entry.job.future

    repeated = _make_iface(module)
    repeated._fail(OSError("first"))
    first = repeated._failure
    repeated._closed = False
    repeated._fail(OSError("second"))
    assert repeated._failure is first


def _close_then_return(iface: object, entry: TxEntry) -> TxEntry:
    iface._closed = True  # type: ignore[attr-defined]
    return entry


async def test_tx_loop_drops_oversized_classic_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    """An oversized frame on a Classic-CAN interface is dropped in-loop, not allowed to kill the TX task."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)

    iface = _make_iface(module)  # fd=False -> Classic CAN, so a >8-byte payload is unencodable.
    oversized = _entry(20, 1, 100, b"0" * 9)
    ok = _entry(21, 2, 100, b"abc")
    iface._tx = _TxScript(iface, [oversized, ok])
    loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=0)))

    async def wait_ok(coro: object, timeout: float) -> None:
        del timeout
        await cast(Awaitable[object], coro)

    monkeypatch.setattr(module.asyncio, "wait_for", wait_ok)
    await iface._tx_loop()

    assert len(loop.sent_frames) == 1  # Oversized frame dropped; the following valid frame still sent.
    assert iface._tx.requeued == []  # The oversized frame was not re-queued.
    with pytest.raises(SendError):
        await oversized.job.future
    assert ok.job.future.result() is None
    assert iface._failure is None  # And it was not treated as an interface failure.


async def test_expired_frame_aborts_the_whole_transfer(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tail of an expired transfer is useless to the far side, so it must not be emitted."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)

    job = new_tx_job(2, Instant(ns=0))  # Deadline already in the past.
    head, tail = TxEntry(30, 1, b"aa", job), TxEntry(30, 2, b"bb", job)
    iface._tx = _TxScript(iface, [head, tail])
    loop = _FakeLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=1)))

    await iface._tx_loop()

    assert loop.sent_frames == []
    assert iface._tx.popped == 2  # The tail was skipped, not left stranded.
    with pytest.raises(SendError):
        await job.future


async def test_close_resolves_the_in_flight_transfer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A popped-but-unsent transfer must also be failed by close(), or its publisher waits forever."""
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)

    sending = asyncio.Event()
    release = asyncio.Event()

    class _BlockingLoop(_FakeLoop):
        async def sock_sendall(self, _sock: object, frame: bytes) -> None:
            self.sent_frames.append(frame)
            sending.set()
            await release.wait()

    loop = _BlockingLoop()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)

    future = iface._tx.push(0x10, [b"a", b"b"], Instant.now() + 30.0)
    task = asyncio.create_task(iface._tx_loop())
    await asyncio.wait_for(sending.wait(), timeout=5.0)  # Frame 1 is off the queue and mid-send.
    assert iface._tx.qsize() == 1  # Frame 1 is only reachable as in-flight.

    iface.close()
    with pytest.raises(ClosedError):
        await asyncio.wait_for(future, timeout=5.0)

    release.set()
    await asyncio.wait_for(task, timeout=5.0)
