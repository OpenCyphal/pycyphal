#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import os
import sys
import shutil
import typing
import logging
import pathlib
import subprocess
from subprocess import CalledProcessError as CalledProcessError


DEMO_DIR = pathlib.Path(__file__).absolute().parent.parent / 'demo'


_logger = logging.getLogger(__name__)


def run_process(*args: str, timeout: typing.Optional[float] = None) -> str:
    r"""
    This is a wrapper over :func:`subprocess.check_output`.
    It adds all directories containing runnable scripts (the CLI tool and the demos) to PATH to make them invokable.

    :param args: The args to run.

    :param timeout: Give up waiting if the command could not be completed in this much time and raise TimeoutExpired.
        No limit by default.

    :return: stdout of the command.

    >>> run_process('ping', '127.0.0.1', timeout=0.1)
    Traceback (most recent call last):
    ...
    subprocess.TimeoutExpired: ...
    """
    cmd = _make_process_args(*args)
    _logger.info('Running process with timeout=%s: %s', timeout if timeout is not None else 'inf', ' '.join(cmd))
    # Can't use shell=True with timeout; see https://stackoverflow.com/questions/36952245/subprocess-timeout-failure
    stdout = subprocess.check_output(cmd,
                                     stderr=sys.stderr,
                                     timeout=timeout,
                                     encoding='utf8',
                                     env=_get_env())
    assert isinstance(stdout, str)
    return stdout


def run_cli_tool(*args: str, timeout: typing.Optional[float] = None) -> str:
    """
    A wrapper over :func:`run_process` that runs the CLI tool with the specified arguments.
    """
    return run_process('python', '-m', 'pyuavcan', *args,
                       timeout=timeout)


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
        _logger.info('Starting background child process: %s', ' '.join(cmd))

        try:  # Windows-specific.
            # If the current process group is used, CTRL_C_EVENT will kill the parent and everyone in the group!
            creationflags: int = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore
        except AttributeError:  # Not on Windows.
            creationflags = 0

        # Buffering must be DISABLED, otherwise we can't read data on Windows after the process is interrupted.
        # For some reason stdout is not flushed at exit there.
        self._inferior = subprocess.Popen(cmd,
                                          stdout=subprocess.PIPE,
                                          stderr=sys.stderr,
                                          encoding='utf8',
                                          env=_get_env(environment_variables),
                                          creationflags=creationflags,
                                          bufsize=0)

    @staticmethod
    def cli(*args: str, environment_variables: typing.Optional[typing.Dict[str, str]] = None) -> BackgroundChildProcess:
        """
        A convenience factory for running the CLI tool.
        """
        return BackgroundChildProcess('python', '-m', 'pyuavcan', *args, environment_variables=environment_variables)

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
            self._inferior.send_signal(signal.CTRL_BREAK_EVENT)

    @property
    def pid(self) -> int:
        return int(self._inferior.pid)

    @property
    def alive(self) -> bool:
        return self._inferior.poll() is None

    def __del__(self) -> None:
        if self._inferior.poll() is None:
            self._inferior.kill()


def _get_env(environment_variables: typing.Optional[typing.Dict[str, str]] = None) -> typing.Dict[str, str]:
    env = os.environ.copy()
    # Buffering must be DISABLED, otherwise we can't read data on Windows after the process is interrupted.
    # For some reason stdout is not flushed at exit there.
    env['PYTHONUNBUFFERED'] = '1'
    env.update(environment_variables or {})
    return env


def _make_process_args(executable: str, *args: str) -> typing.Sequence[str]:
    # On Windows, the path lookup is not performed so we have to find the executable manually.
    # On GNU/Linux it doesn't matter so we do it anyway for consistency.
    resolved = shutil.which(executable)
    if not resolved:  # pragma: no cover
        raise RuntimeError(f'Cannot locate executable: {executable}')
    executable = resolved
    return list(map(str, [executable] + list(args)))
