# WebSerial SLCAN Browser Smoke Test

This is a manual smoke test for `pycyphal2.can.webserial.WebSerialSLCANInterface` in a Pyodide browser runtime.
It is intentionally not part of `nox` because it depends on a browser permission prompt, WebSerial support, and real
SLCAN/CAN hardware.

## Preconditions

- Browser with WebSerial support, such as a Chromium-based browser.
- Secure context. `http://localhost` is acceptable for local development.
- An SLCAN-compatible serial CAN adapter connected to a real CAN bus.
- Another CAN node on the bus, or an adapter/firmware mode that echoes transmitted frames.
- A local `pycyphal2` wheel served over HTTP.

Build and serve the wheel from the repository root:

```bash
python -m build
python -m http.server 8000
```

Open the smoke page from `http://localhost:8000/...` so the wheel and page share the same origin.

## Smoke Page

Use this as the page body. Adjust the Pyodide version, wheel URL, serial baud rate, and SLCAN bitrate command for the
adapter under test.

```html
<!doctype html>
<meta charset="utf-8">
<title>pycyphal2 WebSerial SLCAN Smoke</title>

<button id="run">Connect and run smoke</button>
<pre id="log"></pre>

<script src="https://cdn.jsdelivr.net/pyodide/v0.27.7/full/pyodide.js"></script>
<script type="module">
const log = (line) => {
  document.getElementById("log").textContent += `${line}\n`;
};

let port = null;
let reader = null;
let writer = null;

async function writeText(text) {
  const data = new TextEncoder().encode(text);
  await writer.write(data);
}

globalThis.pycyphalSerialRead = async () => {
  const { value, done } = await reader.read();
  if (done || value === undefined) {
    return new Uint8Array();
  }
  return value;
};

globalThis.pycyphalSerialWrite = async (data) => {
  await writer.write(Uint8Array.from(data));
};

globalThis.pycyphalSerialClose = async () => {
  try {
    if (reader !== null) {
      await reader.cancel();
      reader.releaseLock();
      reader = null;
    }
  } finally {
    try {
      if (writer !== null) {
        writer.releaseLock();
        writer = null;
      }
    } finally {
      if (port !== null) {
        await port.close();
        port = null;
      }
    }
  }
};

async function connectSerial() {
  if (!("serial" in navigator)) {
    throw new Error("WebSerial is not available in this browser");
  }

  port = await navigator.serial.requestPort();
  await port.open({ baudRate: 115200 });
  reader = port.readable.getReader();
  writer = port.writable.getWriter();

  // Common LAWICEL/SLCAN setup: close channel, set 500 kbit/s, open channel.
  // Change S6 if the bus uses another rate: S4=125k, S5=250k, S6=500k, S8=1M.
  await writeText("C\r");
  await writeText("S6\r");
  await writeText("O\r");
}

document.getElementById("run").addEventListener("click", async () => {
  try {
    log("Loading Pyodide...");
    const pyodide = await loadPyodide();
    await pyodide.loadPackage("micropip");

    log("Installing pycyphal2 wheel...");
    const micropip = pyodide.pyimport("micropip");
    await micropip.install("./dist/pycyphal2-0.0.0-py3-none-any.whl");

    log("Opening serial adapter...");
    await connectSerial();

    log("Running Python smoke...");
    await pyodide.runPythonAsync(`
import asyncio

import js
from pyodide.ffi import to_js

from pycyphal2 import Instant, Priority
from pycyphal2.can import CANTransport
from pycyphal2.can.webserial import WebSerialSLCANInterface


class BrowserSerialPort:
    async def read(self) -> bytes:
        chunk = await js.pycyphalSerialRead()
        return bytes(chunk.to_py())

    async def write(self, data: bytes) -> None:
        await js.pycyphalSerialWrite(to_js(list(data)))

    async def close(self) -> None:
        await js.pycyphalSerialClose()


async def main() -> None:
    iface = WebSerialSLCANInterface(BrowserSerialPort(), name="browser-slcan")
    transport = CANTransport.new(iface)

    arrivals = []
    listener = transport.subject_listen(1234, arrivals.append)
    writer = transport.subject_advertise(1234)
    try:
        await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hello from browser")
        print("published one Cyphal/CAN message on subject 1234")

        # This receive check needs another node to transmit subject 1234, or adapter echo/loopback.
        for _ in range(500):
            if arrivals:
                break
            await asyncio.sleep(0.01)
        if arrivals:
            print(f"received subject 1234 from node {arrivals[0].remote_id}: {arrivals[0].message!r}")
        else:
            print("no subject 1234 arrival observed within 5 seconds")
    finally:
        listener.close()
        writer.close()
        transport.close()


await main()
`);
    log("Smoke completed. Check browser console for Python output.");
  } catch (ex) {
    log(`FAILED: ${ex.stack || ex}`);
    try {
      await globalThis.pycyphalSerialClose();
    } catch {
      // Ignore cleanup failures in the smoke page.
    }
  }
});
</script>
```

## Expected Result

- The browser asks for serial-port permission after the button click.
- The adapter receives `C`, bitrate, and `O` SLCAN commands.
- Python creates `WebSerialSLCANInterface` and `CANTransport`.
- A Cyphal/CAN message is published on subject `1234`.
- If another CAN node or adapter echo produces a subject `1234` transfer, the arrival is printed in the console.

## Notes

- Replace `./dist/pycyphal2-0.0.0-py3-none-any.whl` with the actual wheel filename in `dist/`.
- The example configures Classic CAN only; the current SLCAN backend intentionally has no CAN FD support.
- The interface does not configure SLCAN adapters by itself. The smoke page sends `C`, `S6`, and `O` before constructing
  `WebSerialSLCANInterface`.
- If receive times out, first verify that the CAN bus has another active node or that the adapter emits loopback frames.

## References

- MDN Web Serial API: https://developer.mozilla.org/en-US/docs/Web/API/Web_Serial_API
- MDN `SerialPort.open()`: https://developer.mozilla.org/en-US/docs/Web/API/SerialPort/open
- Pyodide type translations and JS proxy behavior: https://pyodide.org/en/stable/usage/type-conversions.html
