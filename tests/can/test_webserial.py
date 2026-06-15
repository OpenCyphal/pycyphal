from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from pycyphal2 import ClosedError, Instant
from pycyphal2.can import Filter, TimestampedFrame
from pycyphal2.can.webserial import AsyncSerialPort, WebSerialSLCANInterface


class _FakeAsyncSerial:
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
        return await self._read_waiter

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


def test_async_serial_port_protocol_runtime_check() -> None:
    assert isinstance(_FakeAsyncSerial(), AsyncSerialPort)


def test_webserial_interface_properties_and_sync_close() -> None:
    port = _FakeAsyncSerial()
    iface = WebSerialSLCANInterface(port, name="slcan-web")

    assert iface.name == "slcan-web"
    assert iface.fd is False
    assert repr(iface) == "WebSerialSLCANInterface('slcan-web', fd=False)"

    iface.close()
    iface.close()
    assert port.closed is True


def test_enqueue_writes_expected_slcan_bytes() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port)
        iface.enqueue(0x123, [memoryview(b"\xAA"), memoryview(b"")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == 2)
        assert port.writes == [b"T000001231AA\r", b"T000001230\r"]

        iface.close()
        await _wait_for(lambda: port.closed)

    asyncio.run(run())


def test_receive_returns_timestamped_frame_and_drops_malformed_input() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial([b"bad\rT000001231AA\r"])
        iface = WebSerialSLCANInterface(port)

        before = Instant.now()
        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)
        after = Instant.now()

        assert frame == TimestampedFrame(id=0x123, data=b"\xAA", timestamp=frame.timestamp)
        assert before.ns <= frame.timestamp.ns <= after.ns
        iface.close()

    asyncio.run(run())


def test_receive_accepts_slcan_timestamp_suffix() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial([b"T10AE6EFF8000000FF000000A07071\r"])
        iface = WebSerialSLCANInterface(port)

        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)

        assert frame.id == 0x10AE6EFF
        assert frame.data == b"\x00\x00\x00\xFF\x00\x00\x00\xA0"
        iface.close()

    asyncio.run(run())


def test_receive_applies_local_filters() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial([b"T000001231AA\rT000004561BB\r"])
        iface = WebSerialSLCANInterface(port)
        iface.filter([Filter(id=0x456, mask=0x1FFFFFFF)])

        frame = await asyncio.wait_for(iface.receive(), timeout=1.0)

        assert frame.id == 0x456
        assert frame.data == b"\xBB"
        iface.close()

    asyncio.run(run())


def test_expired_deadline_is_dropped() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port)

        iface.enqueue(0x123, [memoryview(b"\xAA")], Instant.now() + (-1.0))
        iface.enqueue(0x124, [memoryview(b"\xBB")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == 1)
        assert port.writes == [b"T000001241BB\r"]
        iface.close()

    asyncio.run(run())


def test_purge_drops_pending_tx() -> None:
    async def run() -> None:
        port = _FakeAsyncSerial()
        iface = WebSerialSLCANInterface(port)

        iface.enqueue(0x123, [memoryview(b"\xAA")], Instant.now() + 10.0)
        iface.purge()
        iface.enqueue(0x124, [memoryview(b"\xBB")], Instant.now() + 1.0)

        await _wait_for(lambda: len(port.writes) == 1)
        assert port.writes == [b"T000001241BB\r"]
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
        port = _FakeAsyncSerial([OSError("rx failed")])
        iface = WebSerialSLCANInterface(port)

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
        port.write_errors.append(OSError("tx failed"))
        iface = WebSerialSLCANInterface(port)

        iface.enqueue(0x123, [memoryview(b"\xAA")], Instant.now() + 1.0)

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
