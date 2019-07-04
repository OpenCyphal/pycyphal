#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging
import argparse
import pyuavcan
from . import _base, _transport


INFO = _base.CommandInfo(
    help='''
Subscribe to the specified subject, receive and print messages into stdout.
'''.strip(),
    examples=f'''
pyuavcan sub uavcan.node.Heartbeat.1
'''.strip(),
    aliases=[
        'sub',
    ]
)


_logger = logging.getLogger(__name__)


def register_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        'data_id',
        metavar='[subject_id.]full_data_type_name.version_major[.version_minor]',
        help='''
The full data type name with version and optional subject ID.
If the subject ID is not specified, the fixed subject ID will be used,
if such is available for the selected version of the data type. If the
subject ID is not specified and the fixed subject ID is not defined for the
selected data type version, the command will exit immediately with an error.
The minor version number may be omitted, in which case the newest known
version will be used. Examples:
    1234.uavcan.node.Heartbeat.1.0
    uavcan.node.Heartbeat.1
'''.strip(),
    )
    format_choices = ['yaml', 'json-line', 'tsv']
    parser.add_argument(
        '--format',
        choices=format_choices,
        default=format_choices[0],
        help='''
The representation of the data printed into stdout.
Default: %(default)s
'''.strip(),
    )
    _transport.add_argument_transport(parser)


def execute(args: argparse.Namespace) -> int:
    transport = _transport.construct_transport(args.transport)
    print(transport)
    return 1
