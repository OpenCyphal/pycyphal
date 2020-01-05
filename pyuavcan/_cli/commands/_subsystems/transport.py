#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import inspect
import logging
import argparse
import pyuavcan
from ._base import SubsystemFactory
from .._paths import OUTPUT_TRANSFER_ID_MAP_DIR, OUTPUT_TRANSFER_ID_MAP_MAX_AGE


_logger = logging.getLogger(__name__)


class TransportFactory(SubsystemFactory):
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            '--transport', '--tr',
            metavar='EXPRESSION',
            action='append',
            help=f'''
A Python expression that yields a transport instance upon evaluation.
If the expression fails to evaluate or yields anything that is not a
transport instance, the command fails. If the argument is provided
more than once, a redundant transport instance will be constructed
automatically.

Please read the PyUAVCAN's API documentation to learn about the available
transports and how their instances can be constructed.

All nested submodules under "pyuavcan.transport" are imported before the
expression is evaluated, so the expression itself does not need to explicitly
import anything.

Examples:
    pyuavcan.transport.can.CANTransport(pyuavcan.transport.can.media.socketcan.SocketCANMedia('vcan0', 64), 42)
    pyuavcan.transport.loopback.LoopbackTransport(None)
    pyuavcan.transport.serial.SerialTransport("/dev/ttyUSB0", None, baudrate=115200)
    pyuavcan.transport.udp.UDPTransport('127.255.255.255/8')

Such long expressions are hard to type, so the following entities are also
pre-imported into the global namespace for convenience:
    - All direct submodules of "pyuavcan.transport" are wildcard-imported.
      For example, "pyuavcan.transport.can" is also available as "can".
    - All classes that implement "pyuavcan.transport.Transport" are
      wildcard-imported under their original name but without the
      shared "Transport" suffix. For example, the transport class
      "pyuavcan.transport.loopback.LoopbackTransport" is also available as
      "Loopback".
More shortcuts may be added in the future.

The following examples yield configurations that are equivalent to the above:
    CAN(can.media.socketcan.SocketCANMedia('vcan0',64),42)
    Loopback(None)
    Serial("/dev/ttyUSB0",None,baudrate=115200)
    UDP('127.255.255.255/8')

This is still a lot to type, so it might make sense to store frequently used
configurations into environment variables and expand them as necessary.

Observe that the node-ID for the local node is to be configured here as well,
because per the UAVCAN architecture, this is a transport-layer property.
If desired, a usable node-ID value can be automatically found using the
command "pick-node-id"; read its help for usage information (it's useful for
various automation scripts and similar tasks).

The command-line tool stores the output transfer-ID map on disk, keyed by
the node-ID and the OS resource associated with the transport; the path is:
{OUTPUT_TRANSFER_ID_MAP_DIR}
The map files are managed automatically. They can be removed to reset all
transfer-ID counters to zero. Files that are more than {OUTPUT_TRANSFER_ID_MAP_MAX_AGE}
seconds old are no longer used.
'''.strip())

    def construct_subsystem(self, args: argparse.Namespace) -> pyuavcan.transport.Transport:
        context = _make_evaluation_context()
        _logger.debug('Expression evaluation context: %r', list(context.keys()))

        trs: typing.List[pyuavcan.transport.Transport] = []
        for expression in args.transport:
            t = _evaluate_transport_expr(expression, context)
            _logger.info('Expression %r yields %r', expression, t)
            trs.append(t)

        if len(trs) < 1:
            raise ValueError('No transports specified')
        elif len(trs) == 1:
            return trs[0]  # Non-redundant transport
        else:
            from pyuavcan.transport.redundant import RedundantTransport
            rt = RedundantTransport()
            for t in trs:
                rt.attach_inferior(t)
            assert rt.inferiors == trs
            return rt


def _evaluate_transport_expr(expression: str, context: typing.Dict[str, typing.Any]) -> pyuavcan.transport.Transport:
    out = eval(expression, context)
    if isinstance(out, pyuavcan.transport.Transport):
        return out
    else:
        raise ValueError(f'The expression {expression!r} yields an instance of {type(out).__name__}. '
                         f'Expected an instance of pyuavcan.transport.Transport.')


def _make_evaluation_context() -> typing.Dict[str, typing.Any]:
    # This import is super slow, so we do it as late as possible.
    # Doing this when generating command-line arguments would be disastrous for performance.
    # noinspection PyTypeChecker
    pyuavcan.util.import_submodules(pyuavcan.transport)

    # Populate the context with all references that may be useful for the transport expression.
    context: typing.Dict[str, typing.Any] = {
        'pyuavcan': pyuavcan,
    }

    # Pre-import transport modules for convenience.
    for name, module in inspect.getmembers(pyuavcan.transport, inspect.ismodule):
        if not name.startswith('_'):
            context[name] = module

    # Pre-import transport classes for convenience.
    transport_base = pyuavcan.transport.Transport
    # Suppressing MyPy false positive: https://github.com/python/mypy/issues/5374
    for cls in pyuavcan.util.iter_descendants(transport_base):  # type: ignore
        if not cls.__name__.startswith('_') and cls is not transport_base:
            name = cls.__name__.rpartition(transport_base.__name__)[0]
            assert name
            context[name] = cls

    return context
