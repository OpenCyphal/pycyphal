#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import re
import typing
import logging
import pkgutil
import argparse
import importlib
import pyuavcan.transport


_logger = logging.getLogger(__name__)


def add_argument_transport(parser: argparse.ArgumentParser) -> None:
    # TODO: better interface; see below
    parser.add_argument(
        '--transport', '-T',
        metavar='TRANSPORT_SPEC',
        action='append',
        required=True,
        help='''
Transport construction expression.
Specify more than once to use redundant transports.
Example:
    can.CANTransport(can.media.socketcan.SocketCANMedia('vcan0',mtu=64))
'''.strip(),
    )


def construct_transport(specs: typing.Iterable[str]) -> pyuavcan.transport.Transport:
    trans: typing.List[pyuavcan.transport.Transport] = []
    try:
        for s in specs:
            trans.append(_eval_spec(s))
    except Exception:
        for t in trans:
            try:
                t.close()
            except Exception as ex:
                _logger.exception('Could not close transport %s: %s', t, ex)
        raise

    _logger.debug(f'Specs {specs!r} yielded the following transports: {trans!r}')
    if len(trans) < 1:
        raise ValueError('No transports specified')
    elif len(trans) == 1:
        return trans[0]  # Non-redundant transport
    else:
        raise NotImplementedError('Sorry, redundant transport construction is not yet implemented')


def _eval_spec(spec: str) -> pyuavcan.transport.Transport:
    module_names = re.findall(r'([a-z]\w*(?:\.[a-zA-Z_]\w*)*)\.[a-zA-Z_]\w*', spec)
    _logger.debug('The transport spec string %r requires importing the following modules: %r', spec, module_names)
    local_refs: typing.Dict[str, typing.Any] = {
        'pyuavcan.transport': pyuavcan.transport,
    }
    for mn in module_names:
        local_refs[mn] = importlib.import_module('.' + mn, pyuavcan.transport.__name__)
    # Eval is unsafe. Build a custom safe parser later. Should be trivial even with regexps; there's also parsimonious.
    transport = eval(spec, local_refs)
    if not isinstance(transport, pyuavcan.transport.Transport):
        raise ValueError(f'The transport spec string does not define a valid transport: {spec!r}')
    return transport


# Force import all transport implementations and all recursive sub-modules.
# This is necessary to discover all transport implementations and their inferior entities such as media support classes.
for loader, module_name, is_pkg in pkgutil.walk_packages(pyuavcan.transport.__path__):
    if is_pkg:
        loader.find_module(module_name).load_module(module_name)
