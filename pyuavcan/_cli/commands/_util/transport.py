#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import re
import typing
import logging
import argparse
import importlib
import itertools
import dataclasses
import pyuavcan.transport
from .yaml import YAMLLoader


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class _IfaceArgument:
    argparse_dest: str
    constructor:   typing.Callable[[str], pyuavcan.transport.Transport]


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Adds arguments for all supported transports and interfaces to the specified parser.
    """
    for name, value in globals().items():
        if name.startswith('_add_args_for_'):
            value(parser)


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
    # Eval is unsafe. Build a custom safe parser later. Should be trivial even with regexps; there's also Parsimonious.
    transport = eval(spec, local_refs)
    if not isinstance(transport, pyuavcan.transport.Transport):
        raise ValueError(f'The transport spec string does not define a valid transport: {spec!r}')
    return transport


def make_arg_sequence_parser(*type_default_pairs: typing.Tuple[typing.Type[object], typing.Any]) \
        -> typing.Callable[[str], typing.Sequence[typing.Any]]:
    r"""
    Constructs a callable that transforms a comma-separated list of arguments into the form specified by the
    sequence of (type, default) tuples, or raises a ValueError if the input arguments are non-conforming.
    The type constructor must be able to accept the default value.

    >>> make_arg_sequence_parser()('')
    []
    >>> make_arg_sequence_parser((int, 123), (float, -15))('12')
    [12, -15.0]
    >>> make_arg_sequence_parser((int, 123), (float, -15))('12, 16, "abc"')
    Traceback (most recent call last):
    ...
    ValueError: Expected at most 2 values, found 3 in '12, 16, "abc"'
    """
    # Config validation - abort if default can't be accepted by the type constructor.
    try:
        _ = [ty(default) for ty, default in type_default_pairs]  # type: ignore
    except Exception:
        raise ValueError(f'Invalid arg spec: {type_default_pairs!r}')

    def do_parse(arg: str) -> typing.Sequence[typing.Any]:
        values = YAMLLoader().load(f'[ {arg} ]')
        if len(values) <= len(type_default_pairs):
            return [
                ty(val if val is not None else default)
                for val, (ty, default) in itertools.zip_longest(values, type_default_pairs)
            ]
        else:
            raise ValueError(f'Expected at most {len(type_default_pairs)} values, found {len(values)} in {arg!r}')
    return do_parse


# The following functions are invoked automatically during argument list construction.
# Manual registration is not required; however, every function must use the same name prefix to be discoverable.
# The full (non-abridged) argument name pattern should be as follows:
#   --iface-<transport-name>[-media-name][-further-specifiers]
# Abridged names may be arbitrary.

def _add_args_for_can(parser: argparse.ArgumentParser) -> None:
    from pyuavcan.transport.can import CANTransport
    from pyuavcan.transport.can.media.socketcan import SocketCANMedia

    parser.add_argument(
        '--iface-can-socketcan-2.0', '--socketcan2',
        dest='transport',
        metavar='IFACE_NAME',
        type=lambda iface_name: CANTransport(SocketCANMedia(iface_name, 8)),
    )

    parser.add_argument(
        '--iface-can-socketcan-fd', '--socketcanfd',
    )
