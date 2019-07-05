#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import decimal
import asyncio
import logging
import argparse
import argparse_utils
import pyuavcan
import contextlib
from . import _base, _transport, _port_spec, _formatter


INFO = _base.CommandInfo(
    help='''
Subscribe to the specified subject, receive and print messages into stdout.
This command does not instantiate a local node; the bus is accessed directly
at the presentation layer, so many instances can be cheaply executed
concurrently to subscribe to multiple message streams.
'''.strip(),
    examples=f'''
pyuavcan sub uavcan.node.Heartbeat.1.0
'''.strip(),
    aliases=[
        'sub',
    ]
)


_logger = logging.getLogger(__name__)


def register_arguments(parser: argparse.ArgumentParser) -> None:
    _transport.add_argument_transport(parser)

    parser.add_argument(
        'subject_spec',
        metavar='[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR',
        nargs='+',
        help='''
A set of full message type names with version and optional subject-ID for each.
The subject-ID can be omitted if a fixed one is defined for the data type.
If multiple subjects are selected, a synchronizing subscription will be used,
reporting received messages in synchronous groups.
Examples:
    1234.uavcan.node.Heartbeat.1.0 (using subject-ID 1234)
    uavcan.node.Heartbeat.1.0 (using the fixed subject-ID 32085)
'''.strip(),
    )

    # noinspection PyTypeChecker
    parser.add_argument(
        '--format',
        default=next(iter(_formatter.Format)),
        action=argparse_utils.enum_action(_formatter.Format),
        help='''
The format of the data printed into stdout. The final representation is
constructed from an intermediate "builtin-based" representation, which is
a simplified form that is stripped of the detailed DSDL type information,
like JSON. For the background info please read the PyUAVCAN documentation
on builtin-based representations.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--with-metadata', '-M',
        action='store_true',
        help='''
Emit metadata together with each message.
'''.strip(),
    )

    parser.add_argument(
        '--count', '-C',
        type=int,
        metavar='NATURAL',
        help='''
Exit automatically after this many messages (or synchronous message groups)
have been received. No limit by default.
'''.strip(),
    )


def execute(args: argparse.Namespace) -> None:
    transport = _transport.construct_transport(args.transport)
    subject_specs = [_port_spec.construct_port_id_and_type(ds) for ds in args.subject_spec]
    asyncio.get_event_loop().run_until_complete(
        _run(transport=transport,
             subject_specs=subject_specs,
             formatter=_formatter.make_formatter(args.format),
             with_metadata=args.with_metadata,
             count=int(args.count) if args.count is not None else (2 ** 63))
    )


async def _run(transport:     pyuavcan.transport.Transport,
               subject_specs: typing.List[typing.Tuple[int, typing.Type[pyuavcan.dsdl.CompositeObject]]],
               formatter:     _formatter.Formatter,
               with_metadata: bool,
               count:         int) -> None:
    if len(subject_specs) < 1:
        raise ValueError('Nothing to do: no subjects specified')
    elif len(subject_specs) == 1:
        subject_id, dtype = subject_specs[0]
    else:
        # TODO: add support for multi-subject synchronous subscribers https://github.com/UAVCAN/pyuavcan/issues/65
        raise NotImplementedError('Multi-subject subscription is not yet implemented, sorry!')

    _logger.debug(f'Starting the subscriber with transport={transport}, subject_specs={subject_specs}, '
                  f'formatter={formatter}, with_metadata={with_metadata}')

    pres = pyuavcan.presentation.Presentation(transport)
    with contextlib.closing(pres):
        async for msg, transfer in pres.make_subscriber(dtype, subject_id):
            assert isinstance(transfer, pyuavcan.transport.TransferFrom)
            outer: typing.Dict[int, typing.Dict[str, typing.Any]] = {}

            bi: typing.Dict[str, typing.Any] = {}  # We use updates to ensure proper dict ordering: metadata before data
            if with_metadata:
                bi.update({
                    '_transfer_': {
                        'timestamp': {
                            'system':    transfer.timestamp.system.quantize(_1EM6),
                            'monotonic': transfer.timestamp.monotonic.quantize(_1EM6),
                        },
                        'priority':       transfer.priority.name.lower(),
                        'transfer_id':    transfer.transfer_id,
                        'source_node_id': transfer.source_node_id,  # None if anonymous
                    },
                })
            bi.update(pyuavcan.dsdl.to_builtin(msg))
            outer[subject_id] = bi

            print(formatter(outer))

            count -= 1
            if count <= 0:
                _logger.info('Reached the specified message count, stopping')
                break


_1EM6 = decimal.Decimal('0.000001')
