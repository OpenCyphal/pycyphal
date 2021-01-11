# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import sys
import shutil
import typing
import logging
import subprocess


_logger = logging.getLogger(__name__)


class BackgroundChildProcess:
    r"""
    A wrapper over :class:`subprocess.Popen`.
    This wrapper allows collection of stdout upon completion. At first I tried using a background reader
    thread that was blocked on ``stdout.readlines()``, but that solution ended up being dysfunctional because
    it is fundamentally incompatible with internal stdio buffering in the monitored process which
    we have absolutely no control over from our local process. Sure, there exist options to suppress buffering,
    such as the ``-u`` flag in Python or the PYTHONUNBUFFERED env var, but they would make the test environment
    unnecessarily fragile, so I opted to use a simpler approach where we just run the process until it kicks
    the bucket and then loot the output from its dead body.

    >>> p = BackgroundChildProcess('ping', '127.0.0.1')
    >>> p.wait(0.5)
    Traceback (most recent call last):
    ...
    subprocess.TimeoutExpired: ...
    >>> p.kill()
    """

    def __init__(self, *args: str, environment_variables: typing.Optional[typing.Dict[str, str]] = None):
        cmd = _make_process_args(*args)
        _logger.info("Starting in background: %s with env vars: %s", args, environment_variables)

        if sys.platform.startswith("win"):
            # If the current process group is used, CTRL_C_EVENT will kill the parent and everyone in the group!
            creationflags: int = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            creationflags = 0

        # Buffering must be DISABLED, otherwise we can't read data on Windows after the process is interrupted.
        # For some reason stdout is not flushed at exit there.
        self._inferior = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            encoding="utf8",
            env=_get_env(environment_variables),
            creationflags=creationflags,
            bufsize=0,
        )

    @staticmethod
    def cli(*args: str, environment_variables: typing.Optional[typing.Dict[str, str]] = None) -> BackgroundChildProcess:
        """
        A convenience factory for running the CLI tool.
        """
        return BackgroundChildProcess("python", "-m", "pyuavcan", *args, environment_variables=environment_variables)

    def wait(self, timeout: float, interrupt: typing.Optional[bool] = False) -> typing.Tuple[int, str]:
        if interrupt and self._inferior.poll() is None:
            self.interrupt()
        stdout = self._inferior.communicate(timeout=timeout)[0]
        exit_code = int(self._inferior.returncode)
        return exit_code, stdout

    def kill(self) -> None:
        self._inferior.kill()

    def interrupt(self) -> None:
        import signal

        try:
            self._inferior.send_signal(signal.SIGINT)
        except ValueError:  # pragma: no cover
            # On Windows, SIGINT is not supported, and CTRL_C_EVENT does nothing.
            self._inferior.send_signal(getattr(signal, "CTRL_BREAK_EVENT"))

    @property
    def pid(self) -> int:
        return int(self._inferior.pid)

    @property
    def alive(self) -> bool:
        return self._inferior.poll() is None


def _get_env(environment_variables: typing.Optional[typing.Dict[str, str]] = None) -> typing.Dict[str, str]:
    # Buffering must be DISABLED, otherwise we can't read data on Windows after the process is interrupted.
    # For some reason stdout is not flushed at exit there.
    env = {
        "PYTHONUNBUFFERED": "1",
    }
    env.update(environment_variables or {})
    return env


def _make_process_args(executable: str, *args: str) -> typing.Sequence[str]:
    # On Windows, the path lookup is not performed so we have to find the executable manually.
    # On GNU/Linux it doesn't matter so we do it anyway for consistency.
    resolved = shutil.which(executable)
    if not resolved:  # pragma: no cover
        raise RuntimeError(f"Cannot locate executable: {executable}")
    executable = resolved
    return list(map(str, [executable] + list(args)))
