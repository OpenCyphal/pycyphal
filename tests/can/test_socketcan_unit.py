from __future__ import annotations

import asyncio
import errno
from pathlib import Path
import sys
import types
from typing import Any, Awaitable, cast

import pytest

from pycyphal2 import ClosedError, Instant
from pycyphal2.can import Filter, TimestampedFrame

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


class _QueueScript:
    def __init__(self, iface: object, items: list[object]) -> None:
        self._iface = iface
        self._items = list(items)
        self.requeued: list[tuple[int, int, int, bytes]] = []

    async def get(self) -> tuple[int, int, int, bytes]:
        if self._items:
            item = self._items.pop(0)
            if callable(item):
                out = item()
                assert isinstance(out, tuple)
                return out
            assert isinstance(item, tuple)
            return item
        self._iface._closed = True  # type: ignore[attr-defined]
        return 0, 0, 0, b""

    def get_nowait(self) -> tuple[int, int, int, bytes]:
        if not self._items:
            raise asyncio.QueueEmpty
        item = self._items.pop(0)
        assert isinstance(item, tuple)
        return item

    def put_nowait(self, item: tuple[int, int, int, bytes]) -> None:
        self.requeued.append(item)


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
    iface._tx_seq = 0
    iface._tx_queue = asyncio.PriorityQueue()
    iface._tx_task = None
    return iface


def test_socketcan_import_guard_rejects_non_linux(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ImportError, match="SocketCAN is available only on Linux"):
        _load_socketcan_module(monkeypatch, platform="darwin")


def test_socketcan_init_fd_and_classic_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, created = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)

    monkeypatch.setattr(module.SocketCANInterface, "_read_iface_mtu", lambda self: module._CAN_FD_MTU)
    fd_iface = module.SocketCANInterface("vcan0")
    fd_sock = created[-1]
    assert fd_iface.name == "vcan0"
    assert fd_iface.fd is True
    assert ("setsockopt", fake_socket.SOL_CAN_RAW, fake_socket.CAN_RAW_FD_FRAMES, 1) in fd_sock.calls
    assert "vcan0" in repr(fd_iface)

    monkeypatch.setattr(module.SocketCANInterface, "_read_iface_mtu", lambda self: module._CAN_CLASSIC_MTU)
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
    iface.enqueue(123, [memoryview(b"a")], deadline)
    iface.enqueue(123, [memoryview(b"b")], deadline)
    assert len(loop.created_tasks) == 1
    assert iface._tx_seq == 2
    assert iface._tx_queue.qsize() == 2

    iface.purge()
    assert iface._tx_queue.qsize() == 0
    iface.purge()

    task = iface._tx_task
    assert isinstance(task, _TaskStub)
    iface.close()
    iface.close()
    assert task.cancelled is True

    closed = _make_iface(module, closed=True)
    closed.purge()


async def test_receive_retries_after_decode_drop_and_raises_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_socket, _ = _make_socket_module()
    module = _load_socketcan_module(monkeypatch, socket_module=fake_socket)
    iface = _make_iface(module)
    good = module._CAN_FRAME_STRUCT.pack(fake_socket.CAN_EFF_FLAG | 0x123, 2, b"ab".ljust(8, b"\x00"))
    loop = _FakeLoop(recv=[b"\x00", good])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: loop)

    frame = await iface.receive()
    assert frame.id == 0x123
    assert frame.data == b"ab"

    failing = _make_iface(module)
    failing_loop = _FakeLoop(recv=[OSError("rx failed")])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: failing_loop)
    with pytest.raises(ClosedError, match="receive failed"):
        await failing.receive()
    assert failing._closed is True
    assert isinstance(failing._failure, OSError)

    cancelled = _make_iface(module)
    cancelled_loop = _FakeLoop(recv=[asyncio.CancelledError()])
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: cancelled_loop)
    with pytest.raises(asyncio.CancelledError):
        await cancelled.receive()


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
    with pytest.raises(ClosedError, match="not CAN FD-capable"):
        classic._encode(123, b"012345678")

    encoded_classic = classic._encode(123, b"abc")
    assert len(encoded_classic) == module._CAN_CLASSIC_MTU

    fd_iface = _make_iface(module, fd=True)
    encoded_fd = fd_iface._encode(456, b"012345678")
    assert len(encoded_fd) == module._CAN_FD_MTU

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
    success._tx_queue = _QueueScript(success, [(10, 1, 100, b"abc")])
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

    cancelled = _make_iface(module)

    class _CancelledQueue:
        async def get(self) -> tuple[int, int, int, bytes]:
            raise asyncio.CancelledError

    cancelled._tx_queue = _CancelledQueue()
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    with pytest.raises(asyncio.CancelledError):
        await cancelled._tx_loop()

    post_get_close = _make_iface(module)
    post_get_close._tx_queue = _QueueScript(
        post_get_close, [lambda: _close_then_return(post_get_close, (11, 1, 100, b"x"))]
    )
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    await post_get_close._tx_loop()

    expired = _make_iface(module)
    expired._tx_queue = _QueueScript(expired, [(12, 1, 0, b"x")])
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: Instant(ns=1)))
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    await expired._tx_loop()

    timeout_zero = _make_iface(module)
    timeout_zero._tx_queue = _QueueScript(timeout_zero, [(13, 1, 1, b"x")])
    times = iter([Instant(ns=0), Instant(ns=2)])
    monkeypatch.setattr(module.Instant, "now", staticmethod(lambda: next(times)))
    monkeypatch.setattr(module.asyncio, "get_running_loop", lambda: _FakeLoop())
    await timeout_zero._tx_loop()

    timeout_retry = _make_iface(module)
    timeout_retry._tx_queue = _QueueScript(timeout_retry, [(14, 1, 100, b"x")])
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
    assert timeout_retry._tx_queue.requeued == [(14, 1, 100, b"x")]

    transient_retry = _make_iface(module)
    transient_retry._tx_queue = _QueueScript(transient_retry, [(15, 1, 100, b"x")])
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
    assert transient_retry._tx_queue.requeued == [(15, 1, 100, b"x")]

    permanent_fail = _make_iface(module)
    permanent_fail._tx_queue = _QueueScript(permanent_fail, [(16, 1, 100, b"x")])
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

    repeated = _make_iface(module)
    repeated._fail(OSError("first"))
    first = repeated._failure
    repeated._closed = False
    repeated._fail(OSError("second"))
    assert repeated._failure is first


def _close_then_return(iface: object, item: tuple[int, int, int, bytes]) -> tuple[int, int, int, bytes]:
    iface._closed = True  # type: ignore[attr-defined]
    return item
