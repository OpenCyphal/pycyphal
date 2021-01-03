# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import sys
import typing
import logging
import subprocess
import pytest

# The fixture is imported here to make it visible to other tests in this suite.
from .dsdl.conftest import generated_packages as generated_packages  # noqa  # pylint: disable=unused-import


GIBIBYTE = 1024 ** 3

MEMORY_LIMIT = 4 * GIBIBYTE
"""
The test suite artificially limits the amount of consumed memory in order to avoid triggering the OOM killer
should a test go crazy and eat all memory.
"""

_logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)  # type: ignore
def _configure_host_environment() -> None:
    def execute(*cmd: typing.Any, ensure_success: bool = True) -> typing.Tuple[int, str, str]:
        cmd = tuple(map(str, cmd))
        out = subprocess.run(  # pylint: disable=subprocess-run-check
            cmd,
            encoding="utf8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = out.stdout, out.stderr
        _logger.debug("%s stdout:\n%s", cmd, stdout)
        _logger.debug("%s stderr:\n%s", cmd, stderr)
        if out.returncode != 0 and ensure_success:  # pragma: no cover
            raise subprocess.CalledProcessError(out.returncode, cmd, stdout, stderr)
        assert isinstance(stdout, str) and isinstance(stderr, str)
        return out.returncode, stdout, stderr

    if sys.platform.startswith("linux"):
        import resource

        _logger.info("Limiting process memory usage to %.1f GiB", MEMORY_LIMIT / GIBIBYTE)
        resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))

        # Set up virtual SocketCAN interfaces.
        execute("sudo", "modprobe", "can")
        execute("sudo", "modprobe", "can_raw")
        execute("sudo", "modprobe", "vcan")
        for idx in range(3):
            iface = f"vcan{idx}"
            execute("sudo", "ip", "link", "add", "dev", iface, "type", "vcan", ensure_success=False)
            execute("sudo", "ip", "link", "set", iface, "mtu", 72)  # Enable both Classic CAN and CAN FD.
            execute("sudo", "ip", "link", "set", "up", iface)

    if sys.platform.startswith("win"):
        import ctypes

        # Reconfigure the system timer to run at a higher resolution. This is desirable for the real-time tests.
        t = ctypes.c_ulong()
        ctypes.WinDLL("NTDLL.DLL").NtSetTimerResolution(5000, 1, ctypes.byref(t))
        _logger.info("System timer resolution: %.3f ms", t.value / 10e3)
