from __future__ import annotations

import asyncio
from abc import ABC
from collections.abc import Callable

import pytest

from pycyphal2 import ClosedError, Instant
from pycyphal2.can import Filter, TimestampedFrame
from pycyphal2.can._media_slcan import encode_init_sequence
from pycyphal2.can.webserial import AsyncSerialPort, WebSerialSLCANInterface

_ACK = b"\r"
_NACK = b"\x07"
_INIT_1M = [b"C\r", b"S8\r", b"O\r"]
_INIT_250K = [b"C\r", b"S5\r", b"O\r"]


class _FakeAsyncSerial(AsyncSerialPort):
    def __init__(self, reads: list[bytes | BaseException] | None = None) -> None:
        self.reads = list(reads or [])
        self.writes: list[bytes] = []
        self.write_errors: list[BaseException] = []
        self.closed = False
        self._read_waiter: asyncio.Future[bytes] | None = None

    async def read(self) -> bytes:
        if self.reads:
            item = self.reads.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        loop = asyncio.get_running_loop()
        self._read_waiter = loop.create_future()
        try:
            return await self._read_waiter
        finally:
            # A timed-out (cancelled) read must not leave an orphaned waiter, else a later feed()
            # would resolve a future nobody awaits and drop the byte. Reset so feed() falls back to
            # the queue. This mirrors the purge draining input via short-timeout reads.
            self._read_waiter = None

    async def write(self, data: bytes) -> None:
        self.writes.append(bytes(data))
        if self.write_errors:
            item = self.write_errors.pop(0)
            raise item

    async def close(self) -> None:
        self.closed = True
        if self._read_waiter is not None and not self._read_waiter.done():
            self._read_waiter.set_result(b"")

    def feed(self, data: bytes) -> None:
        if self._read_waiter is not None and not self._read_waiter.done():
            self._read_waiter.set_result(data)
            self._read_waiter = None
        else:
            self.reads.append(data)


async def _wait_for(predicate: Callable[[], bool], timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("predicate did not become true within timeout")


def _writes_reach(port: _FakeAsyncSerial, count: int) -> Callable[[], bool]:
    return lambda: len(port.writes) >= count


async def _start(
    port: _FakeAsyncSerial, *, bitrate: int | None = 1_000_000, name: str = "webserial"
) -> WebSerialSLCANInterface:
    """Construct the interface and drive the deinit/purge/init handshake to completion.

    The close (write 0) is fire-and-forget and its response is purged; each subsequent init command
    is acknowledged reactively, after its write appears, to mirror a real adapter.
    """
    iface = WebSerialSLCANInterface(port, name=name, bitrate=bitrate)
    for i in range(len(encode_init_sequence(bitrate))):
        await _wait_for(_writes_reach(port, i + 2))  # writes: [close, cmd_0, ..., cmd_i]
        port.feed(_ACK)
    return iface


def test_async_serial_port_is_abc() -> None:
    assert issubclass(AsyncSerialPort, ABC)


def test_webserial_interface_properties_and_sync_close() -> None:
    async def run() -> tuple[WebSerialSLCANInterface, _FakeAsyncSerial]:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port, name="slcan-web")
        assert iface.name == "slcan-web"
        assert iface.fd is False
        assert repr(iface) == "WebSerialSLCANInterface('slcan-web', fd=False)"
        return iface, port

    iface, port = asyncio.run(run())
    iface.close()  # No running loop anymore: close must still tear down the port.
    iface.close()
    assert port.closed is True


def test_webserial_initializes_slcan_channel_with_bitrate() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port, bitrate=250_000)

        assert port.writes == _INIT_250K

        iface.close()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_webserial_initializes_without_bitrate() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port, bitrate=None)

        assert port.writes == [b"C\r", b"O\r"]  # No bitrate command is emitted.

        iface.close()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_webserial_nonstandard_bitrate_passed_through() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port, bitrate=123_456)

        assert port.writes == [b"C\r", b"S123456\r", b"O\r"]  # Sent as-is, not rejected.

        iface.close()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_webserial_purges_stale_input_before_open() -> None:
    async def run() -> None:
        # A frame buffered by the adapter under the old config must be discarded by the purge.
        port = _FakeAsyncSerial([b"T000007FF155\r"])
        iface = await _start(port)

        port.feed(b"T000001231AA\r")  # Fresh frame, after open.
        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)

        assert frame.id == 0x123
        assert frame.data == b"\xaa"
        iface.close()

    asyncio.run(run())


def test_webserial_initialization_nack_closes_interface() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port, bitrate=1_000_000)

        await _wait_for(lambda: len(port.writes) > 1)  # Close, then the bitrate command.
        port.feed(_NACK)

        await _wait_for(lambda: port.closed)
        assert port.writes == [b"C\r", b"S8\r"]
        with pytest.raises(ClosedError, match="failed") as exc_info:
            iface.filter([Filter.promiscuous()])
        assert isinstance(exc_info.value.__cause__, OSError)

    asyncio.run(run())


def test_enqueue_writes_expected_slcan_bytes() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        iface.enqueue(0x123, [memoryview(b"\xaa"), memoryview(b"")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == len(_INIT_1M) + 2)
        assert port.writes == [*_INIT_1M, b"T000001231AA\r", b"T000001230\r"]

        iface.close()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_receive_returns_timestamped_frame_and_drops_malformed_input() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        port.feed(b"bad\rT000001231AA\r")

        before = Instant.now()
        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)
        after = Instant.now()

        assert frame == TimestampedFrame(id=0x123, data=b"\xaa", timestamp=frame.timestamp)
        assert before.ns <= frame.timestamp.ns <= after.ns
        iface.close()

    asyncio.run(run())


def test_receive_accepts_slcan_optional_suffix() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        port.feed(b"T10AE6EFF8000000FF000000A07071Lvendor\r")

        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)

        assert frame.id == 0x10AE6EFF
        assert frame.data == b"\x00\x00\x00\xff\x00\x00\x00\xa0"
        iface.close()

    asyncio.run(run())


def test_receive_filter_is_noop() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        iface.filter([Filter(id=0x456, mask=0x1FFFFFFF)])
        port.feed(b"T000001231AA\rT000004561BB\r")

        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)

        assert frame.id == 0x123
        assert frame.data == b"\xaa"
        iface.close()

    asyncio.run(run())


def test_expired_deadline_is_dropped() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)

        iface.enqueue(0x123, [memoryview(b"\xaa")], Instant.now() + (-1.0))
        iface.enqueue(0x124, [memoryview(b"\xbb")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == len(_INIT_1M) + 1)
        assert port.writes == [*_INIT_1M, b"T000001241BB\r"]
        iface.close()

    asyncio.run(run())


def test_purge_drops_pending_tx() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)

        iface.enqueue(0x123, [memoryview(b"\xaa")], Instant.now() + 10.0)
        iface.purge()
        iface.enqueue(0x124, [memoryview(b"\xbb")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == len(_INIT_1M) + 1)
        assert port.writes == [*_INIT_1M, b"T000001241BB\r"]
        iface.close()

    asyncio.run(run())


def test_close_unblocks_pending_receive_and_operations_raise() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port)
        task = asyncio.create_task(iface.receive())
        await asyncio.sleep(0)

        iface.close()

        with pytest.raises(ClosedError, match="closed"):
            await asyncio.wait_for(task, timeout=1.0)
        with pytest.raises(ClosedError, match="closed"):
            iface.enqueue(0x123, [memoryview(b"")], Instant.now())
        with pytest.raises(ClosedError, match="closed"):
            iface.filter([Filter.promiscuous()])
        iface.purge()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_read_failure_closes_interface() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        port.reads.append(OSError("rx failed"))

        with pytest.raises(ClosedError, match="receive failed") as exc_info:
            await asyncio.wait_for(iface.receive(), timeout=1.0)
        assert isinstance(exc_info.value.__cause__, OSError)

        with pytest.raises(ClosedError, match="failed") as closed_info:
            iface.filter([Filter.promiscuous()])
        assert isinstance(closed_info.value.__cause__, OSError)
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_write_failure_closes_interface() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = await _start(port)
        port.write_errors.append(OSError("tx failed"))

        iface.enqueue(0x123, [memoryview(b"\xaa")], Instant.now() + 1.0)

        await _wait_for(lambda: port.closed)
        with pytest.raises(ClosedError, match="failed") as exc_info:
            iface.enqueue(0x123, [memoryview(b"")], Instant.now())
        assert isinstance(exc_info.value.__cause__, OSError)

    asyncio.run(run())


def test_enqueue_validation() -> None:
    async def run() -> None:
        iface = WebSerialSLCANInterface(_FakeAsyncSerial())

        with pytest.raises(ValueError, match="Invalid CAN identifier"):
            iface.enqueue(-1, [memoryview(b"")], Instant.now())
        with pytest.raises(ValueError, match="Invalid CAN data length"):
            iface.enqueue(0x123, [memoryview(bytes(range(9)))], Instant.now())

        iface.close()

    asyncio.run(run())
