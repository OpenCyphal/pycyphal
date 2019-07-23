#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import argparse
import contextlib
import pyuavcan
from . import _util, _subsystems
from ._base import Command, SubsystemFactory


_logger = logging.getLogger(__name__)


class SubscribeCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ['subscribe', 'sub']

    @property
    def help(self) -> str:
        return '''
Subscribe to the specified subject, receive and print messages into stdout.
This command does not instantiate a local node; the bus is accessed directly
at the presentation layer, so many instances can be cheaply executed
concurrently to subscribe to multiple message streams.
'''.strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return f'''
pyuavcan sub uavcan.node.Heartbeat.1.0
'''.strip()

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return [
            _subsystems.transport.TransportFactory(),
            _subsystems.formatter.FormatterFactory(),
        ]

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
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
'''.strip())
        parser.add_argument(
            '--with-metadata', '-M',
            action='store_true',
            help='''
Emit metadata together with each message.
'''.strip())
        parser.add_argument(
            '--count', '-C',
            type=int,
            metavar='NATURAL',
            help='''
Exit automatically after this many messages (or synchronous message groups)
have been received. No limit by default.
'''.strip())

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        transport, formatter = subsystems
        assert isinstance(transport, pyuavcan.transport.Transport)
        assert callable(formatter)

        subject_specs = [_util.construct_port_id_and_type(ds) for ds in args.subject_spec]
        asyncio.get_event_loop().run_until_complete(
            _run(transport=transport,
                 subject_specs=subject_specs,
                 formatter=formatter,
                 with_metadata=args.with_metadata,
                 count=int(args.count) if args.count is not None else (2 ** 64))
        )
        return 0


async def _run(transport:     pyuavcan.transport.Transport,
               subject_specs: typing.List[typing.Tuple[int, typing.Type[pyuavcan.dsdl.CompositeObject]]],
               formatter:     _subsystems.formatter.Formatter,
               with_metadata: bool,
               count:         int) -> None:
    if len(subject_specs) < 1:
        raise ValueError('Nothing to do: no subjects specified')
    elif len(subject_specs) == 1:
        subject_id, dtype = subject_specs[0]
    else:
        # TODO: add support for multi-subject synchronous subscribers https://github.com/UAVCAN/pyuavcan/issues/65
        raise NotImplementedError('Multi-subject subscription is not yet implemented, sorry!')

    _logger.info(f'Starting the subscriber with transport={transport}, subject_specs={subject_specs}, '
                 f'formatter={formatter}, with_metadata={with_metadata}')

    pres = pyuavcan.presentation.Presentation(transport)
    with contextlib.closing(pres):
        async for msg, transfer in pres.make_subscriber(dtype, subject_id):
            assert isinstance(transfer, pyuavcan.transport.TransferFrom)
            outer: typing.Dict[int, typing.Dict[str, typing.Any]] = {}

            bi: typing.Dict[str, typing.Any] = {}  # We use updates to ensure proper dict ordering: metadata before data
            if with_metadata:
                bi['_metadata_'] = _util.convert_transfer_metadata_to_builtin(transfer)
            bi.update(pyuavcan.dsdl.to_builtin(msg))
            outer[subject_id] = bi

            print(formatter(outer))

            count -= 1
            if count <= 0:
                _logger.info('Reached the specified message count, stopping')
                break
