#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import time
import typing
import asyncio
import logging
import argparse
import contextlib

import argparse_utils

import pyuavcan
from . import _util
from ._util.yaml import YAMLLoader, YAMLDumper
from . import dsdl_generate_packages


INFO = _util.base.CommandInfo(
    help='''
Publish messages of the specified subject with the fixed contents.
The local node will also publish heartbeat and respond to GetInfo,
unless it is configured to be anonymous.
'''.strip(),
    examples='''
pyuavcan pub uavcan.diagnostic.Record.1.0 '{text: "Hello world!"}'
'''.strip(),
    aliases=[
        'pub',
    ]
)


_logger = logging.getLogger(__name__)


def register_arguments(parser: argparse.ArgumentParser) -> None:
    _util.transport.add_arguments(parser)

    parser.add_argument(
        'subject_spec',
        metavar='[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR YAML_FIELDS',
        nargs='*',
        help='''
The full message type name with version and optional subject-ID, followed
by the YAML (or JSON, which is a subset of YAML)-formatted contents of the
message. Missing fields will be left at their default values. Use empty dict
as "{}" to construct a default-initialized message. For more info about the
YAML representation, read the PyUAVCAN documentation on builtin-based
representations.

The subject-ID can be omitted if a fixed one is defined for the data type.

The number of such pairs can be arbitrary; all defined messages will be
published synchronously. If no such pairs are specified, nothing will be
published, unless the local node is not anonymous. Per the specification,
a non-anonymous node must publish heartbeat; this requirement is respected.
Additionally, the recommended standard service uavcan.node.GetInfo is served.

Examples:
    1234.uavcan.diagnostic.Record.1.0 '{"text": "Hello world!"}'
    uavcan.diagnostic.Record.1.0 '{"text": "Hello world!"}'
'''.strip(),
    )

    parser.add_argument(
        '--local-node-id', '-L',
        metavar='NODE_ID_OR_DIRECTIVE',
        default='none',
        help=f'''
Node-ID to use for the requested operation.
Accepted values are non-negative integers or special directives:

- An integer is directly used as the local node-ID.

- Directive "none" does not assign any node-ID to the local node. On most
  transports this results in the node running in the anonymous mode; some
  transports, however, may have a default node-ID value assigned by the
  transport layer, in which case that node-ID will be used. Beware that
  anonymous transfers may have limitations; for example, some transports
  don't support multi-frame anonymous transfers.

- Directive "auto" triggers a simplified unsafe automatic node-ID look-up
  before the command is executed: listen for heartbeat messages for a few
  seconds, randomly pick a node-ID value not currently in use, and use it.
  This allocation method is collision-prone and should not be used in
  production; it is suitable only for development and testing. If the
  transport provides a default node-ID, that node-ID will be used and this
  directive will have no effect.

Default: %(default)s
'''.strip(),
    )

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
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--count', '-C',
        type=int,
        default=1,
        metavar='NATURAL',
        help='''
Number of synchronous publication cycles before exiting normally.
The duration therefore equals (--period) * (--count).
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--priority',
        default=pyuavcan.presentation.DEFAULT_PRIORITY,
        action=argparse_utils.enum_action(pyuavcan.transport.Priority),
        help='''
Priority of published message transfers. Applies to the heartbeat as well.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--transfer-id',
        default=0,
        type=int,
        help='''
The initial transfer-ID value. The same initial value will be shared for all
subjects, including heartbeat.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--heartbeat-fields',
        default='{}',
        metavar='YAML_FIELDS',
        type=_YAML_LOADER.load,
        help='''
Value of the heartbeat message uavcan.node.Heartbeat published by the node.
The uptime will be overridden so specifying it here will have no effect.
Has no effect if the node is anonymous (i.e., without a local node-ID)
because anonymous nodes do not publish their heartbeat.

For more info about the YAML representation, read the PyUAVCAN documentation
on builtin-based representations.

Unless overridden, the following defaults are used:
- Mode operational.
- Health nominal.
- Vendor-specific status code equals the process ID (PID) of the command.
Default: %(default)s
'''.strip(),
    )

    parser.add_argument(
        '--node-info-fields',
        default='{}',
        type=_YAML_LOADER.load,
        metavar='YAML_FIELDS',
        help=f'''
Value of the node info response uavcan.node.GetInfo returned by the node.
This argument overrides the following defaults per-field:
{YAMLDumper().dumps(_NODE_INFO_FIELDS)}
For more info about the YAML representation, read the PyUAVCAN documentation
on builtin-based representations.
Default: %(default)s
'''.strip(),
    )


class Publication:
    def __init__(self,
                 subject_spec: str,
                 field_spec:   str,
                 presentation: pyuavcan.presentation.Presentation,
                 transfer_id:  int,
                 priority:     pyuavcan.transport.Priority):
        subject_id, dtype = _util.port_spec.construct_port_id_and_type(subject_spec)
        content = _YAML_LOADER.load(field_spec)

        self._message = pyuavcan.dsdl.update_from_builtin(dtype(), content)
        self._publisher = presentation.make_publisher(dtype, subject_id)
        self._publisher.priority = priority
        self._publisher.transfer_id_counter.override(transfer_id)

    async def publish(self) -> None:
        await self._publisher.publish(self._message)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._message}, {self._publisher})'


def execute(args: argparse.Namespace) -> None:
    asyncio.get_event_loop().run_until_complete(_do_execute(args))


async def _do_execute(args: argparse.Namespace) -> None:
    # If the application submodule fails to import with an import error, the standard DSDL data type package
    # probably needs to be generated first, which we suggest the user to do.
    try:
        from pyuavcan import application
    except ImportError:
        raise ImportError(dsdl_generate_packages.make_usage_suggestion_text('uavcan')) from None

    # Construct the node instance.
    node_info_fields = _NODE_INFO_FIELDS.copy()
    node_info_fields.update(args.node_info_fields)
    node_info = pyuavcan.dsdl.update_from_builtin(application.NodeInfo(), node_info_fields)
    _logger.info('Node info: %r', node_info)
    node = application.Node(_util.transport.construct_transport(args.transport), info=node_info)

    with contextlib.closing(node):
        # Configure the heartbeat publisher.
        if args.heartbeat_fields.pop('uptime', None) is not None:
            _logger.warning('Specifying uptime has no effect because it will be overridden by the node.')
        node.heartbeat_publisher.health = \
            args.heartbeat_fields.pop('health', application.heartbeat_publisher.Health.NOMINAL)
        node.heartbeat_publisher.mode = \
            args.heartbeat_fields.pop('mode', application.heartbeat_publisher.Mode.OPERATIONAL)
        node.heartbeat_publisher.vendor_specific_status_code = args.heartbeat_fields.pop(
            'vendor_specific_status_code',
            os.getpid() & (2 ** min(pyuavcan.dsdl.get_model(application.heartbeat_publisher.Heartbeat)
                                    ['vendor_specific_status_code'].data_type.bit_length_set) - 1)
        )
        node.heartbeat_publisher.priority = args.priority
        node.heartbeat_publisher.period = min(application.heartbeat_publisher.Heartbeat.MAX_PUBLICATION_PERIOD,
                                              args.period)
        node.heartbeat_publisher.publisher.transfer_id_counter.override(args.transfer_id)
        _logger.info('Node heartbeat: %r', node.heartbeat_publisher.make_message())
        if args.heartbeat_fields:
            raise ValueError(f'Unrecognized heartbeat fields: {args.heartbeat_fields}')

        # Configure the node-ID.
        try:
            local_node_id = int(args.local_node_id)
        except ValueError:
            pass  # TODO DIRECTIVES
        else:
            node.set_local_node_id(local_node_id)

        # Configure the publication set.
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
                                            priority=args.priority))
        _logger.info('Publication set: %r', publications)

        # All set! Run the publication loop until the specified number of publications is done.
        sleep_until = time.monotonic()
        for c in range(int(args.count)):
            await asyncio.gather(*[p.publish() for p in publications])
            sleep_until += float(args.period)
            _logger.info('Publication cycle %d of %d completed; sleeping for %.3f seconds',
                         c + 1, args.count, sleep_until - time.monotonic())
            await asyncio.sleep(sleep_until - time.monotonic())


_YAML_LOADER = YAMLLoader()
_NODE_INFO_FIELDS = _util.node.make_default_node_info_fields_for_command_module(__name__)
