#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import queue
import typing
import pathlib
import threading
import subprocess

# noinspection PyProtectedMember
import pyuavcan._cli as _cli


_CLI_TOOL_DIR = pathlib.Path(_cli.__file__).absolute().parent

_DEMO_DIR = pathlib.Path(__file__).absolute().parent.parent / 'demo'


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

    >>> p = BackgroundChildProcess('echo', 'Hello world!')
    >>> p.pop_stdout_lines(0.1)
    ['Hello world!\n']
    >>> p.wait(999)
    0
    >>> p = BackgroundChildProcess('sleep', '999')
    >>> p.pop_stdout_lines()
    []
    >>> p.wait(1)
    Traceback (most recent call last):
    ...
    subprocess.TimeoutExpired: ...
    >>> p.kill()
    """

    def __init__(self, *args: str):
        self._inferior = subprocess.Popen(args,
                                          stdout=subprocess.PIPE,
                                          encoding='utf8',
                                          stderr=sys.stderr,
                                          env=_get_env())

        self._stdout_lines: queue.Queue[str] = queue.Queue()

        self._stdout_thread = threading.Thread(target=self._reader_thread_func, daemon=True)
        self._stdout_thread.start()

    def pop_stdout_lines(self, timeout: typing.Optional[float] = None) -> typing.Sequence[str]:
        out: typing.List[str] = []
        while True:
            try:
                out.append(self._stdout_lines.get(block=timeout is not None, timeout=timeout))
            except queue.Empty:
                break
        return out

    def wait(self, timeout: float) -> int:
        return self._inferior.wait(timeout)

    def kill(self) -> None:
        self._inferior.kill()

    def _reader_thread_func(self) -> None:
        while True:
            line = self._inferior.stdout.readline()
            if line:
                self._stdout_lines.put_nowait(line)
            else:
                break

    def __del__(self):
        if self._inferior.poll() is None:
            self._inferior.kill()


def _get_env() -> typing.Dict[str, str]:
    env = os.environ.copy()
    for p in [_CLI_TOOL_DIR, _DEMO_DIR]:
        env['PATH'] += f'{os.pathsep}{p}'
    return env
