# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import os
import typing
import inspect
import logging
import argparse
import pyuavcan
from ._base import SubsystemFactory
from .._paths import OUTPUT_TRANSFER_ID_MAP_DIR, OUTPUT_TRANSFER_ID_MAP_MAX_AGE


_logger = logging.getLogger(__name__)


_ENV_VAR_NAME = "PYUAVCAN_CLI_TRANSPORT"


class TransportFactory(SubsystemFactory):
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--transport",
            "--tr",
            metavar="EXPRESSION",
            action="append",
            help=f"""
A Python expression that yields a transport instance upon evaluation. If the expression fails to evaluate or yields
anything that is not a transport instance, the command fails. If the argument is provided more than once, a redundant
transport instance will be constructed automatically. If and only if the transport arguments are not provided,
the transport configuration will be picked up from the environment variable {_ENV_VAR_NAME}.

Read PyUAVCAN API documentation to learn about the available transports and how their instances can be constructed.

All nested submodules under "pyuavcan.transport" are imported before the expression is evaluated, so the expression
itself does not need to explicitly import anything. Transports whose dependencies are not installed are silently
skipped; that is, if SerialTransport depends on PySerial but it is not installed, an expression that attempts to
configure a UAVCAN/serial transport would fail to evaluate.

Examples:
    pyuavcan.transport.can.CANTransport(pyuavcan.transport.can.media.socketcan.SocketCANMedia('vcan0', 64), 42)
    pyuavcan.transport.loopback.LoopbackTransport(None)
    pyuavcan.transport.serial.SerialTransport("/dev/ttyUSB0", None, baudrate=115200)
    pyuavcan.transport.udp.UDPTransport('127.42.0.123', anonymous=True)

Such long expressions are hard to type, so the following entities are also pre-imported into the global namespace
for convenience:
    - All direct submodules of "pyuavcan.transport" are wildcard-imported. For example, "pyuavcan.transport.can"
      is also available as "can".
    - All classes that implement "pyuavcan.transport.Transport" are wildcard-imported under their original name but
      without the shared "Transport" suffix. For example, "pyuavcan.transport.loopback.LoopbackTransport" is also
      available as "Loopback".
More shortcuts may be added in the future.

The following examples yield configurations that are equivalent to the above:
    CAN(can.media.socketcan.SocketCANMedia('vcan0',64),42)
    Loopback(None)
    Serial("/dev/ttyUSB0",None,baudrate=115200)
    UDP('127.42.0.123',anonymous=True)

It is often more convenient to use the environment variable instead of typing the arguments because they tend to be
complex and are usually reused without modification. The variable may contain either a single transport expression,
in which case a non-redundant transport instance would be constructed:
    {_ENV_VAR_NAME}='Loopback(None)'
...or it may be a Python list/tuple, in which case a redundant transport will be constructed, unless the sequence
contains only one element:
    {_ENV_VAR_NAME}="UDP('127.42.0.123'), Serial('/dev/ttyUSB0',None,baudrate=115200)"

Observe that the node-ID for the local node is to be configured here as well, because per the UAVCAN architecture,
this is a transport-layer property. If desired, a usable node-ID value can be automatically found using the command
"pick-node-id"; read its help for usage information (it's useful for various automation scripts and similar tasks).

The command-line tool stores the output transfer-ID map on disk keyed by the node-ID; the current local path is:
{OUTPUT_TRANSFER_ID_MAP_DIR}
The map files are managed automatically. They can be removed to reset all transfer-ID counters to zero. Files that
are more than {OUTPUT_TRANSFER_ID_MAP_MAX_AGE} seconds old are no longer used.
""".strip(),
        )

    def construct_subsystem(self, args: argparse.Namespace) -> pyuavcan.transport.Transport:
        context = _make_evaluation_context()
        trs: typing.List[pyuavcan.transport.Transport] = []
        if args.transport is not None:
            _logger.info(
                "Configuring the transport from command line arguments; environment variable %s is ignored",
                _ENV_VAR_NAME,
            )
            for expression in args.transport:
                trs += _evaluate_transport_expr(expression, context)
        else:
            _logger.info(
                "Command line arguments do not specify the transport configuration; "
                "trying the environment variable %s instead",
                _ENV_VAR_NAME,
            )
            expression = os.environ.get(_ENV_VAR_NAME, None)
            if expression:
                trs = _evaluate_transport_expr(expression, context)

        _logger.info("Resulting transport configuration: %r", trs)
        if len(trs) < 1:
            raise ValueError("No transports specified")
        elif len(trs) == 1:
            return trs[0]  # Non-redundant transport
        else:
            from pyuavcan.transport.redundant import RedundantTransport

            rt = RedundantTransport()
            for t in trs:
                rt.attach_inferior(t)
            assert rt.inferiors == trs
            return rt


def _evaluate_transport_expr(
    expression: str, context: typing.Dict[str, typing.Any]
) -> typing.List[pyuavcan.transport.Transport]:
    out = eval(expression, context)
    _logger.debug("Expression %r yields %r", expression, out)
    if isinstance(out, pyuavcan.transport.Transport):
        return [out]
    elif isinstance(out, (list, tuple)) and all(isinstance(x, pyuavcan.transport.Transport) for x in out):
        return list(out)
    else:
        raise ValueError(
            f"The expression {expression!r} yields an instance of {type(out).__name__!r}. "
            f"Expected an instance of pyuavcan.transport.Transport or a list thereof."
        )


def _make_evaluation_context() -> typing.Dict[str, typing.Any]:
    def handle_import_error(parent_module_name: str, ex: ImportError) -> None:
        try:
            tr = parent_module_name.split(".")[2]
        except LookupError:
            tr = parent_module_name
        _logger.info("Transport %r is not available due to the missing dependency %r", tr, ex.name)

    # This import is super slow, so we do it as late as possible.
    # Doing this when generating command-line arguments would be disastrous for performance.
    # noinspection PyTypeChecker
    pyuavcan.util.import_submodules(pyuavcan.transport, error_handler=handle_import_error)

    # Populate the context with all references that may be useful for the transport expression.
    context: typing.Dict[str, typing.Any] = {
        "pyuavcan": pyuavcan,
    }

    # Expose pre-imported transport modules for convenience.
    for name, module in inspect.getmembers(pyuavcan.transport, inspect.ismodule):
        if not name.startswith("_"):
            context[name] = module

    # Pre-import transport classes for convenience.
    transport_base = pyuavcan.transport.Transport
    # Suppressing MyPy false positive: https://github.com/python/mypy/issues/5374
    for cls in pyuavcan.util.iter_descendants(transport_base):  # type: ignore
        if not cls.__name__.startswith("_") and cls is not transport_base:
            name = cls.__name__.rpartition(transport_base.__name__)[0]
            assert name
            context[name] = cls

    _logger.debug("Transport expression evaluation context (on the next line):\n%r", context)
    return context
