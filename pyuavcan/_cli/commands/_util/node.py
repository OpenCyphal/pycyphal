#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import re
import typing
import logging
import argparse
import pyuavcan
from .yaml import YAMLLoader, YAMLDumper
from . import transport


_logger = logging.getLogger(__name__)


def add_arguments(parser:              argparse.ArgumentParser,
                  command_module_name: str,
                  allow_anonymous:     bool) -> None:
    transport.add_arguments(parser)

    if not allow_anonymous:
        local_node_id_epilogue = '''
If not specified, the default node-ID of the specified transport will be used,
if such is specified. If the transport does not have a pre-defined node-ID,
the command will fail.
'''.strip()
    else:
        local_node_id_epilogue = '''
If not specified, no node-ID will be assigned to the local node. On most
transports this results in the node running in the anonymous mode; some
transports, however, may have a default node-ID value assigned by the
transport layer, in which case that node-ID will be used. Beware that
anonymous transfers may have limitations; for example, some transports
don't support multi-frame anonymous transfers.
'''.strip()
    parser.add_argument(
        '--local-node-id', '-L',
        metavar='NATURAL',
        type=int,
        help=f'''
Node-ID to use for the requested operation. Also see the command pick-node-id.
Valid values range from zero (inclusive) to a transport-specific upper limit.

{local_node_id_epilogue}
'''.strip(),
    )
    parser.set_defaults(allow_anonymous=allow_anonymous)

    parser.add_argument(
        '--heartbeat-fields',
        default='{}',
        metavar='YAML_FIELDS',
        type=YAMLLoader().load,
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

    node_info_fields = _make_default_node_info_fields(command_module_name)

    def construct_node_info_fields(text: str) -> typing.Dict[str, typing.Any]:
        out = node_info_fields.copy()
        out.update(YAMLLoader().load(text))
        return out

    parser.add_argument(
        '--node-info-fields',
        default='{}',
        type=construct_node_info_fields,
        metavar='YAML_FIELDS',
        help=f'''
Value of the node info response uavcan.node.GetInfo returned by the node.
This argument overrides the following defaults per-field:

{YAMLDumper().dumps(node_info_fields).strip()}

For more info about the YAML representation, read the PyUAVCAN documentation
on builtin-based representations.
Default: %(default)s
'''.strip(),
    )


def construct_node(args: argparse.Namespace) -> typing.Any:
    from pyuavcan import application
    node_info = pyuavcan.dsdl.update_from_builtin(application.NodeInfo(), args.node_info_fields)
    _logger.info('Node info: %r', node_info)
    node = application.Node(transport.construct_transport(args), info=node_info)
    try:
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
        _logger.info('Node heartbeat: %r', node.heartbeat_publisher.make_message())
        if args.heartbeat_fields:
            raise ValueError(f'Unrecognized heartbeat fields: {args.heartbeat_fields}')

        # Configure the node-ID.
        if args.local_node_id is not None:
            node.set_local_node_id(args.local_node_id)
        else:
            if not args.allow_anonymous and node.local_node_id is None:
                raise ValueError('The specified transport does not have a predefined node-ID, '
                                 'and the command cannot be used with an anonymous node. '
                                 'Please specify the node-ID explicitly, or use a different transport.')
        return node
    except Exception:
        node.close()
        raise


def _make_default_node_info_fields(command_module_name: str) -> typing.Dict[str, typing.Any]:
    command_name = command_module_name.split('.')[-1].replace('-', '_')
    if not re.match(r'[a-z][a-z0-9_]*[a-z0-9]', command_name):
        raise ValueError(f'Poorly chosen command name: {command_name!r}')

    return {
        'protocol_version': {
            'major': pyuavcan.UAVCAN_SPECIFICATION_VERSION[0],
            'minor': pyuavcan.UAVCAN_SPECIFICATION_VERSION[1],
        },
        'software_version': {
            'major': pyuavcan.__version_info__[0],
            'minor': pyuavcan.__version_info__[1],
        },
        'name': 'org.uavcan.pyuavcan.cli.' + command_name,
    }
