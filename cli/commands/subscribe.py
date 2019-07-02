#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import logging
import argparse
import pyuavcan
from . import _base


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
        'full_data_type_name_with_version',
        help='''
The full data type name with version. The minor version number may be omitted,
in which case the newest known version will be used.
Example: uavcan.node.Heartbeat.1.0
'''.strip(),
    )
    parser.add_argument(
        'subject_id',
        type=int,
        nargs='?',
        help='''
The subject ID to subscribe to. If not specified, the fixed subject ID will
be used, if such is available for the selected version of the data type. If
the argument is not specified and the fixed subject ID is not defined for the
selected data type, the command will exit immediately with an error.
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


def execute(args: argparse.Namespace) -> int:
    full_data_type_name_with_version = str(args.full_data_type_name_with_version)
    subject_id: typing.Optional[int] = int(args.subject_id) if args.subject_id is not None else None
    print('full_data_type_name_with_version:', full_data_type_name_with_version)
    print('subject_id:                      ', subject_id)
    # TODO implement
    return 1
