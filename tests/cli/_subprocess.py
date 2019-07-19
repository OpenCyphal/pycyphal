#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import queue
import typing
import logging
import pathlib
import subprocess

# noinspection PyProtectedMember
import pyuavcan._cli as _cli


_CLI_TOOL_DIR = pathlib.Path(_cli.__file__).absolute().parent

_DEMO_DIR = pathlib.Path(__file__).absolute().parent.parent / 'demo'


_logger = logging.getLogger(__name__)


def run_process(*args: str, timeout: typing.Optional[float] = None) -> str:
    r"""
    This is a wrapper over :func:`subprocess.check_output`.
    It adds all directories containing runnable scripts (the CLI tool and the demos) to PATH to make them invokable.

    :param args: The args to run.

    :param timeout: Give up waiting if the command could not be completed in this much time and raise TimeoutExpired.
        No limit by default.

    :return: stdout of the command.

    >>> run_process('echo', 'Hello world!')
    'Hello world!\n'
    >>> run_process('ping', '127.0.0.1', timeout=0.1)
    Traceback (most recent call last):
    ...
    subprocess.TimeoutExpired: ...
    """
    _logger.info('Running process with timeout=%s: %s', timeout if timeout is not None else 'inf', ' '.join(args))

    # Can't use shell=True with timeout; see https://stackoverflow.com/questions/36952245/subprocess-timeout-failure
    stdout = subprocess.check_output(args,                  # type: ignore
                                     stderr=sys.stderr,
                                     timeout=timeout,
                                     encoding='utf8',
                                     env=_get_env())
    assert isinstance(stdout, str)
    return stdout


class BackgroundChildProcess:
    r"""
    A wrapper over :class:`subprocess.Popen`.
    This wrapper allows collection of stdout upon completion. At first I tried using a background reader
    thread that was blocked on ``stdout.readlines()``, but that solution ended up being dysfunctional because
    it is fundamentally incompatible with internal stdio buffering in the monitored process which
    we have absolutely no control over from our local process. Sure, there exist options to suppress buffering,
    such as the ``-u`` flag in Python or the PYTHONUNBUFFERED env var, but they would make the test environment
    unnecessarily fragile, so I opted to use a simpler approach where we just run the process until it's dead
    and then loot the output from its dead body.

    >>> p = BackgroundChildProcess('echo', 'Hello world!')
    >>> p.wait(0.1)
    (0, 'Hello world!\n')
    >>> p = BackgroundChildProcess('sleep', '999')
    >>> p.wait(1)
    Traceback (most recent call last):
    ...
    subprocess.TimeoutExpired: ...
    >>> p.kill()
    """

    def __init__(self, *args: str):
        _logger.info('Starting background child process: %s', ' '.join(args))

        self._inferior = subprocess.Popen(args,
                                          stdout=subprocess.PIPE,
                                          stderr=sys.stderr,
                                          encoding='utf8',
                                          env=_get_env())

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
        self._inferior.send_signal(signal.SIGINT)

    @property
    def pid(self) -> int:
        return int(self._inferior.pid)

    def __del__(self) -> None:
        if self._inferior.poll() is None:
            self._inferior.kill()


def _get_env() -> typing.Dict[str, str]:
    env = os.environ.copy()
    for p in [_CLI_TOOL_DIR, _DEMO_DIR]:  # Order matters; our directories are PREPENDED.
        env['PATH'] = os.pathsep.join([str(p), env['PATH']])
    return env
