import sys
import typing
import asyncio
import logging
import subprocess
import pytest

from pycyphal.transport import Timestamp
from pycyphal.transport.can.media import Envelope, DataFrame, FrameFormat, FilterConfiguration
from pycyphal.transport.can.media.socketcand import SocketcandMedia

if sys.platform != "linux":  # pragma: no cover
    pytest.skip("Socketcand test skipped because the system is not GNU/Linux", allow_module_level=True)


GIBIBYTE = 1024**3

MEMORY_LIMIT = 8 * GIBIBYTE
"""
The test suite artificially limits the amount of consumed memory in order to avoid triggering the OOM killer
should a test go crazy and eat all memory.
"""

_logger = logging.getLogger(__name__)

socketcand = None


@pytest.fixture()
def _configure_host_environment() -> None:
    print("configurng host environment")

    def execute(
        *cmd: typing.Any, ensure_success: bool = True, cwd: typing.Optional[str] = None, daemon: bool = False
    ) -> typing.Tuple[int, str, str]:
        cmd = tuple(map(str, cmd))
        out = None
        if cwd is None:
            out = subprocess.run(  # pylint: disable=subprocess-run-check
                cmd,
                encoding="utf8",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            out = subprocess.run(  # pylint: disable=subprocess-run-check
                cmd, encoding="utf8", stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd
            )

        stdout, stderr = out.stdout, out.stderr
        _logger.debug("%s stdout:\n%s", cmd, stdout)
        _logger.debug("%s stderr:\n%s", cmd, stderr)
        if out.returncode != 0 and ensure_success:  # pragma: no cover
            raise subprocess.CalledProcessError(out.returncode, cmd, stdout, stderr)
        assert isinstance(stdout, str) and isinstance(stderr, str)
        return out.returncode, stdout, stderr

    if sys.platform.startswith("linux"):
        import resource  # pylint: disable=import-error

        _logger.info("Limiting process memory usage to %.1f GiB", MEMORY_LIMIT / GIBIBYTE)
        resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))

        # Set up virtual SocketCAN interfaces.
        execute("sudo", "modprobe", "can")
        execute("sudo", "modprobe", "can_raw")
        execute("sudo", "modprobe", "vcan")
        execute("sudo", "ip", "link", "add", "dev", "vcan3", "type", "vcan", ensure_success=False)
        execute("sudo", "ip", "link", "set", "vcan3", "mtu", 72)  # Enable both Classic CAN and CAN FD.
        execute("sudo", "ip", "link", "set", "up", "vcan3")

        # build and install socketcand
        execute("sudo", "apt-get", "install", "-y", "autoconf")
        execute("git", "clone", "https://github.com/linux-can/socketcand.git")
        execute("./autogen.sh", cwd="socketcand")
        execute("./configure", cwd="socketcand")
        execute("make", cwd="socketcand")
        execute("sudo", "make", "install", cwd="socketcand")

        # start socketcand daemon in background so test can continue
        socketcand = subprocess.Popen(["socketcand", "-i", "vcan3", "-l", "lo"])

    yield None
    if sys.platform.startswith("linux"):
        socketcand.kill()
        execute("sudo", "rm", "-r", "socketcand")


@pytest.mark.asyncio
async def _unittest_can_socketcand(_configure_host_environment) -> None:
    asyncio.get_running_loop().slow_callback_duration = 5.0

    media_a = SocketcandMedia("vcan3", "127.0.0.1")
    media_b = SocketcandMedia("vcan3", "127.0.0.1")

    assert media_a.mtu == 8
    assert media_b.mtu == 8
    assert media_a.interface_name == "socketcand"
    assert media_b.interface_name == "socketcand"
    assert media_a.channel_name == "vcan3"
    assert media_b.channel_name == "vcan3"
    assert media_a.host_name == "127.0.0.1"
    assert media_b.host_name == "127.0.0.1"
    assert media_a.port_name == 29536
    assert media_b.port_name == 29536
    assert media_a.number_of_acceptance_filters == media_b.number_of_acceptance_filters
    assert media_a._maybe_thread is None
    assert media_b._maybe_thread is None

    rx_a: typing.List[typing.Tuple[Timestamp, Envelope]] = []
    rx_b: typing.List[typing.Tuple[Timestamp, Envelope]] = []

    def on_rx_a(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        nonlocal rx_a
        frames = list(frames)
        print("RX A:", frames)
        rx_a += frames

    def on_rx_b(frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        nonlocal rx_b
        frames = list(frames)
        print("RX B:", frames)
        rx_b += frames

    media_a.start(on_rx_a, False)
    media_b.start(on_rx_b, False)

    assert media_a._maybe_thread is not None
    assert media_b._maybe_thread is not None

    await asyncio.sleep(2.0)  # This wait is needed to ensure that the RX thread handles read timeout properly

    ts_begin = Timestamp.now()
    await media_b.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 0xBADC0FE, bytearray(range(8))), loopback=True),
            Envelope(DataFrame(FrameFormat.EXTENDED, 0x12345678, bytearray(range(0))), loopback=False),
            Envelope(DataFrame(FrameFormat.BASE, 0x123, bytearray(range(6))), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    await asyncio.sleep(0.1)
    ts_end = Timestamp.now()

    print("rx_a:", rx_a)
    # Three received from another part
    assert len(rx_a) == 3
    for ts, _f in rx_a:
        assert ts_begin.monotonic_ns <= ts.monotonic_ns <= ts_end.monotonic_ns
        assert ts_begin.system_ns <= ts.system_ns <= ts_end.system_ns

    rx_external = list(filter(lambda x: True, rx_a))

    assert rx_external[0][1].frame.identifier == 0xBADC0FE
    assert rx_external[0][1].frame.data == bytearray(range(8))
    assert rx_external[0][1].frame.format == FrameFormat.EXTENDED

    assert rx_external[1][1].frame.identifier == 0x12345678
    assert rx_external[1][1].frame.data == bytearray(range(0))
    assert rx_external[1][1].frame.format == FrameFormat.EXTENDED

    assert rx_external[2][1].frame.identifier == 0x123
    assert rx_external[2][1].frame.data == bytearray(range(6))
    assert rx_external[2][1].frame.format == FrameFormat.BASE

    print("rx_b:", rx_b)
    # Two messages are loopback and were copied
    assert len(rx_b) == 2

    rx_loopback = list(filter(lambda x: True, rx_b))

    assert rx_loopback[0][1].frame.identifier == 0xBADC0FE
    assert rx_loopback[0][1].frame.data == bytearray(range(8))
    assert rx_loopback[0][1].frame.format == FrameFormat.EXTENDED

    assert rx_loopback[1][1].frame.identifier == 0x123
    assert rx_loopback[1][1].frame.data == bytearray(range(6))
    assert rx_loopback[1][1].frame.format == FrameFormat.BASE

    media_a.close()
    media_b.close()
