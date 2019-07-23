#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import typing
import logging
import argparse
import itertools
import pyuavcan.transport
from .._yaml import YAMLLoader
from ._base import SubsystemFactory


_logger = logging.getLogger(__name__)


class TransportFactory(SubsystemFactory):
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        for ini in _INITIALIZERS:
            ini(parser)

    def construct_subsystem(self, args: argparse.Namespace) -> pyuavcan.transport.Transport:
        trans: typing.List[pyuavcan.transport.Transport] = args.transport
        if not trans:
            raise ValueError('At least one transport must be specified.')
        assert isinstance(trans, list)
        assert all(map(lambda t: isinstance(t, pyuavcan.transport.Transport), trans))

        _logger.debug(f'Using the following transports: {trans!r}')
        if len(trans) < 1:
            raise ValueError('No transports specified')
        elif len(trans) == 1:
            return trans[0]  # Non-redundant transport
        else:
            # TODO: initialize a RedundantTransport!
            raise NotImplementedError('Sorry, redundant transport construction is not yet implemented')


def _make_arg_sequence_parser(*type_default_pairs: typing.Tuple[typing.Type[object], typing.Any]) \
        -> typing.Callable[[str], typing.Sequence[typing.Any]]:
    r"""
    Constructs a callable that transforms a comma-separated list of arguments into the form specified by the
    sequence of (type, default) tuples, or raises a ValueError if the input arguments are non-conforming.
    The type constructor must be able to accept the default value unless it's None.

    >>> _make_arg_sequence_parser()('')
    []
    >>> _make_arg_sequence_parser((int, 123), (float, -15))('12')
    [12, -15.0]
    >>> _make_arg_sequence_parser((int, 123), (float, -15))('12, 16, "abc"')
    Traceback (most recent call last):
    ...
    ValueError: Expected at most 2 values, found 3 in '12, 16, "abc"'
    """
    # Config validation - abort if default can't be accepted by the type constructor.
    try:
        _ = [ty(default) for ty, default in type_default_pairs if default is not None]  # type: ignore
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


def _add_args_for_can(parser: argparse.ArgumentParser) -> None:
    from pyuavcan.transport.can import CANTransport
    from pyuavcan.transport.can.media.socketcan import SocketCANMedia

    socketcan_parser = _make_arg_sequence_parser((str, ''), (int, 64))

    def construct_socketcan_transport(arg_seq: str) -> CANTransport:
        iface_name, mtu = socketcan_parser(arg_seq)
        return CANTransport(SocketCANMedia(iface_name, mtu=mtu))

    parser.add_argument(
        '--iface-can-socketcan', '--socketcan',
        action='append',
        dest='transport',
        metavar='IFACE_NAME[,MTU]',
        type=construct_socketcan_transport,
        help=f"""
Use CAN transport over SocketCAN. Arguments:
    - Interface name, string, mandatory; e.g.: "can0".
    - Maximum transmission unit, int; optional, defaults to 64 bytes;
      MTU value of 8 bytes selects CAN 2.0.
Examples:
    --socketcan=vcan0,8     # Selects CAN 2.0
    --socketcan=vcan0       # Selects CAN FD with MTU 64 bytes
""".strip())


def _add_args_for_loopback(parser: argparse.ArgumentParser) -> None:
    from pyuavcan.transport.loopback import LoopbackTransport
    parser.add_argument(
        '--iface-loopback', '--loopback',
        action='append_const',
        dest='transport',
        const=LoopbackTransport(),
        help=f"""
Use process-local loopback transport. This transport is only useful for
testing. It is not possible to exchange data between different nodes and/or
processes using this transport.
""".strip())


# When writing initializers, the full (non-abridged) argument name pattern should be as follows:
#   --iface-<transport-name>[-media-name][-further-specifiers]
# Abridged names may be arbitrary.
# The result shall be stored into the field "transport" and the action shall be "append" or "append_const".
#
# TODO: This approach is fragile and does not scale well because it requires much manual coding per transport/media.
#
# Consider defining an URI scheme per transport, add a static factory method per transport implementation? Roughly:
#   <transport>://<transport-specific initialization arguments>
# For example:
#   can:///dev/ttyACM0:slcan
#   can://vcan0:socketcan?mtu=32
#   serial:///dev/ttyACM0
#
# While generic and extensible, URI are hard to type manually, which harms usability. Not good. Should we search for a
# middle ground solution that would combine the genericity of URI and conciseness of hand-coded arguments? We could,
# perhaps, invent a custom spec string format? It could be as simple as a sequence of comma-separated parameters:
#   can,socketcan,/dev/ttyACM0,64
# The spec string could be made a valid YAML string by adding square brackets on either side, so that quoted strings
# could be used:
#   can,socketcan,"~/serial-port-name,with-comma",64
_INITIALIZERS: typing.Sequence[typing.Callable[[argparse.ArgumentParser], None]] = [
    _add_args_for_can,
    _add_args_for_loopback,
]
