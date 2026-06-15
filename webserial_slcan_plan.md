# WebSerial SLCAN CAN Interface Plan

This plan outlines how to add and test a CAN interface backend for running `pycyphal2` inside a browser through
Pyodide, using WebSerial to communicate with an SLCAN-compatible CAN adapter.

## 1. Add a Private SLCAN Codec

Create `src/pycyphal2/can/_slcan.py`.

Keep this module pure Python and dependency-free. It should handle only SLCAN byte/text encoding and decoding, without
knowing about WebSerial or `asyncio`.

Suggested shape:

```python
def encode_frame(identifier: int, data: bytes) -> bytes: ...


class SLCANParser:
    def feed(self, chunk: bytes) -> list[Frame]: ...
```

Handle:

- Classic extended data frames: `T{ID:08X}{DLC}{DATA}\r`.
- Optional CANDapter extended alias: `x...`.
- Drop standard frames, remote frames, malformed hex, DLC/data mismatch, and overlength payloads.
- Bound the internal receive buffer so malformed input cannot grow memory forever.

Do not copy python-can code. Use its SLCAN behavior as a reference only.

## 2. Define a Minimal Async Serial Protocol

Create `src/pycyphal2/can/webserial.py`.

Avoid binding the interface directly to JavaScript globals. Define a small typed protocol that can be implemented by a
Pyodide WebSerial adapter and by CPython test doubles:

```python
class AsyncSerialPort(Protocol):
    async def read(self) -> bytes: ...

    async def write(self, data: bytes) -> None: ...

    async def close(self) -> None: ...
```

Then implement:

```python
class WebSerialSLCANInterface(Interface):
    def __init__(self, port: AsyncSerialPort, *, name: str = "webserial") -> None: ...
```

This keeps the backend testable under normal CPython and avoids requiring browser objects at import time.

## 3. Implement Interface Semantics

Mirror the general structure of `src/pycyphal2/can/pythoncan.py`, but keep it fully async and thread-free.

Core behavior:

- `name`: return the configured interface name.
- `fd`: return `False`.
- `filter(filters)`: store filters locally; SLCAN has no portable acceptance-filter command.
- `enqueue(id, data, deadline)`: push frames into an `asyncio.PriorityQueue`.
- `_tx_loop()`: pop queued frames, enforce deadlines, encode SLCAN, and `await port.write(...)`.
- RX path: read serial chunks, feed the parser, locally apply filters, and enqueue `TimestampedFrame` instances.
- `purge()`: clear queued TX frames.
- `close()`: cancel tasks, unblock pending `receive()`, close the port, and remain idempotent.
- Failures: record the first failure and raise `ClosedError`, following the existing backend pattern.

Use `tests/can/_support.py` as the behavior model for local filter matching.

## 4. Keep Browser Wiring Separate

Browser permission flow should not be embedded into the importable backend.

Add either:

- a small `from_webserial_port(...)` classmethod, or
- a separate example/helper module.

The application should perform `navigator.serial.requestPort()` from JavaScript or Pyodide-side glue code, then pass an
already-open async serial adapter into `WebSerialSLCANInterface`.

This matters because WebSerial depends on browser support, secure context, permissions policy, and user activation.

## 5. Unit-Test the Codec

Add `tests/can/test_slcan.py`.

Cover:

- Encode empty, 1-byte, and 8-byte Classic frames.
- Encode maximum extended ID `0x1FFFFFFF`.
- Reject payloads larger than 8 bytes.
- Parse fragmented input, such as `b"T000001"` followed by the rest of the line.
- Parse multiple frames in one chunk.
- Drop malformed lines without raising.
- Drop standard and remote frames.
- Drop DLC/data mismatch.

These tests should be pure synchronous tests.

## 6. Unit-Test the Interface With Fake Serial

Add `tests/can/test_webserial.py`.

Create a fake async serial port:

```python
class FakeAsyncSerial:
    writes: list[bytes]

    async def read(self) -> bytes: ...

    async def write(self, data: bytes) -> None: ...

    async def close(self) -> None: ...
```

Test:

- `enqueue()` writes expected SLCAN bytes.
- `receive()` returns `TimestampedFrame`.
- Filters are applied locally.
- Expired deadlines are dropped.
- `purge()` drops pending TX.
- Malformed RX input is ignored and the next valid frame is received.
- `close()` is idempotent.
- Pending `receive()` unblocks on close.
- Read/write failure becomes `ClosedError`.

## 7. Add Transport-Level Fake Bus Tests

Build a fake SLCAN serial hub connecting two fake serial ports. When one side writes a `T...` frame, the hub feeds that
line into the other side's read queue.

Add transport tests equivalent to the important SocketCAN smoke tests:

- Pub/sub smoke.
- Unicast smoke.
- Self-loopback behavior, if supported/configurable.
- Node-ID reroll collision case, if the fake hub supports loopback.

This verifies that `CANTransport.new(WebSerialSLCANInterface(...))` works end to end.

## 8. Add an Optional Manual Browser Smoke Example

Add an example or concise documentation snippet, not a mandatory test.

The example should show:

- The browser app obtains a WebSerial port from JavaScript.
- The app wraps the port in the async serial protocol adapter.
- The app creates `CANTransport.new(WebSerialSLCANInterface(...))`.

Do not put real WebSerial/hardware tests into normal `nox`; they are permission-driven and browser-dependent.

## 9. Update Documentation and Public Import Notes

Update `src/pycyphal2/can/__init__.py` documentation to mention the new backend.

Do not eagerly import the backend from `pycyphal2.can.__init__`, matching the existing backend pattern. Users should
import it explicitly:

```python
from pycyphal2.can.webserial import WebSerialSLCANInterface
```

## 10. Verify

Run focused tests first:

```bash
pytest tests/can/test_slcan.py tests/can/test_webserial.py
```

Then run the project acceptance command:

```bash
nox
```

Per repository policy, the feature is not complete until plain `nox` passes.
