#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import re
import typing
import pyuavcan


def make_default_node_info_fields_for_command_module(command_module_name: str) -> typing.Dict[str, typing.Any]:
    command_name = command_module_name.split('.')[-1].replace('-', '_')
    if not re.match(r'[a-z][a-z0-9_]*[a-z0-9]', command_name):
        raise ValueError(f'Poorly chosen command name: {command_name!r}')

    return {
        'protocol_version': {
            'major': pyuavcan.UAVCAN_SPECIFICATION_VERSION[0],
            'minor': pyuavcan.UAVCAN_SPECIFICATION_VERSION[1],
        },
        'software_version': {
            'major': pyuavcan.__version__[0],
            'minor': pyuavcan.__version__[1],
        },
        'name': 'org.uavcan.pyuavcan.cli.' + command_name,
    }
