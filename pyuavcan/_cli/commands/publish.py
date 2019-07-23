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
import argparse_utils
import pyuavcan
from . import _util, _subsystems
from ._yaml import YAMLLoader
from ._base import Command, SubsystemFactory


_logger = logging.getLogger(__name__)


class PublishCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ['publish', 'pub']

    @property
    def help(self) -> str:
        return '''
Publish messages of the specified subject with the fixed contents.
The local node will also publish heartbeat and respond to GetInfo,
unless it is configured to be anonymous.
'''.strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return '''
pyuavcan pub uavcan.diagnostic.Record.1.0 '{text: "Hello world!"}'
'''.strip()

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return [
            _subsystems.node.NodeFactory(node_name_suffix=self.names[0], allow_anonymous=True),
        ]

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            'subject_spec',
            metavar='[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR YAML_FIELDS',
            nargs='*',
            help='''
The full message type name with version and optional subject-ID, followed
by the YAML (or JSON, which is a subset of YAML)-formatted contents of the
message (separated by whitespace). Missing fields will be left at their
default values. Use empty dict as "{}" to construct a default-initialized
message. For more info about the YAML representation, read the PyUAVCAN
documentation on builtin-based representations.

The subject-ID can be omitted if a fixed one is defined for the data type.

The number of such pairs can be arbitrary; all defined messages will be
published synchronously. If no such pairs are specified, nothing will be
published, unless the local node is not anonymous. Per the specification,
a non-anonymous node must publish heartbeat; this requirement is respected.
Additionally, the recommended standard service uavcan.node.GetInfo is served.

Examples:
    1234.uavcan.diagnostic.Record.1.0 '{"text": "Hello world!"}'
    uavcan.diagnostic.Record.1.0 '{"text": "Hello world!"}'
'''.strip())
        parser.add_argument(
            '--period', '-P',
            type=float,
            default=1.0,
            metavar='SECONDS',
            help='''
Message publication period. All messages are published synchronously, so
the period setting applies to all specified subjects. Besides, the period
of heartbeat is defined as min((--period), MAX_PUBLICATION_PERIOD); i.e.,
unless this value exceeds the maximum period defined for heartbeat by the
specification, it is used for heartbeat as well. Note that anonymous nodes
do not publish heartbeat, see the local node-ID argument for more info.

The send timeout for all publishers will equal the publication period.

Default: %(default)s
'''.strip())
        parser.add_argument(
            '--count', '-C',
            type=int,
            default=1,
            metavar='NATURAL',
            help='''
Number of synchronous publication cycles before exiting normally.
The duration therefore equals (--period) * (--count).
Default: %(default)s
'''.strip())
        parser.add_argument(
            '--priority',
            default=pyuavcan.presentation.DEFAULT_PRIORITY,
            action=argparse_utils.enum_action(pyuavcan.transport.Priority),
            help='''
Priority of published message transfers. Applies to the heartbeat as well.
Default: %(default)s
'''.strip())
        parser.add_argument(
            '--transfer-id',
            default=0,
            type=int,
            help='''
The initial transfer-ID value. The same initial value will be shared for all
subjects, including heartbeat. You will need to increment this value manually
if you're publishing on the same subject repeatedly in a short period of time.

The protocol stack will compute the modulus automatically as necessary; e.g.,
in the case of a transport where the transfer-ID modulo equals 32, supplying
123 here would result in the transfer-ID value of 123 %% 32 = 27.

Default: %(default)s
'''.strip())

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        asyncio.get_event_loop().run_until_complete(self._do_execute(args, subsystems))
        return 0

    @staticmethod
    async def _do_execute(args: argparse.Namespace, subsystems: typing.Sequence[object]) -> None:
        import pyuavcan.application
        node, = subsystems
        assert isinstance(node, pyuavcan.application.Node)

        with contextlib.closing(node):
            node.heartbeat_publisher.priority = args.priority
            node.heartbeat_publisher.period = \
                min(pyuavcan.application.heartbeat_publisher.Heartbeat.MAX_PUBLICATION_PERIOD, args.period)
            node.heartbeat_publisher.publisher.transfer_id_counter.override(args.transfer_id)

            raw_ss = args.subject_spec
            if len(raw_ss) % 2 != 0:
                raise argparse.ArgumentError('Mismatching arguments: '
                                             'each subject specifier must be matched with its field specifier.')
            publications: typing.List[Publication] = []
            for subject_spec, field_spec in (raw_ss[i:i + 2] for i in range(0, len(raw_ss), 2)):
                publications.append(Publication(subject_spec=subject_spec,
                                                field_spec=field_spec,
                                                presentation=node.presentation,
                                                transfer_id=args.transfer_id,
                                                priority=args.priority,
                                                send_timeout=args.period))
            _logger.info('Publication set: %r', publications)

            # All set! Run the publication loop until the specified number of publications is done.
            node.start()

            sleep_until = asyncio.get_event_loop().time()
            for c in range(int(args.count)):
                out = await asyncio.gather(*[p.publish() for p in publications])
                assert len(out) == len(publications)
                assert all(isinstance(x, bool) for x in out)
                if not all(out):
                    _logger.error('The following publications have timed out:\n\t',
                                  '\n\t'.join(f'#{idx}: {publications[idx]}' for idx, res in enumerate(out) if not res))

                sleep_until += float(args.period)
                _logger.info('Publication cycle %d of %d completed; sleeping for %.3f seconds',
                             c + 1, args.count, sleep_until - asyncio.get_event_loop().time())
                await asyncio.sleep(sleep_until - asyncio.get_event_loop().time())


class Publication:
    _YAML_LOADER = YAMLLoader()

    def __init__(self,
                 subject_spec: str,
                 field_spec:   str,
                 presentation: pyuavcan.presentation.Presentation,
                 transfer_id:  int,
                 priority:     pyuavcan.transport.Priority,
                 send_timeout: float):
        subject_id, dtype = _util.construct_port_id_and_type(subject_spec)
        content = self._YAML_LOADER.load(field_spec)

        self._message = pyuavcan.dsdl.update_from_builtin(dtype(), content)
        self._publisher = presentation.make_publisher(dtype, subject_id)
        self._publisher.priority = priority
        self._publisher.transfer_id_counter.override(transfer_id)
        self._publisher.send_timeout = send_timeout

    async def publish(self) -> bool:
        return await self._publisher.publish(self._message)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._message}, {self._publisher})'
