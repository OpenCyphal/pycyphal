#!/usr/bin/env python3
#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
I've drafted up this custom hack instead of using sphinx-argparse because it's broken (generated ReST is
not syntactically correct) and does not support parallel build.
"""

import textwrap
import subprocess

# noinspection PyProtectedMember
import pyuavcan._cli as cli


HEADER_SUFFIX = '\n' + '.' * 80 + '\n\n'


def indent(text: str) -> str:
    return textwrap.indent(text, ' ' * 3)


def print_output(command_arguments: str) -> None:
    print('::', end='\n\n')
    print(indent(f'$ pyuavcan {command_arguments}'))
    print(indent(subprocess.check_output(f'python -m pyuavcan {command_arguments}',
                                         encoding='utf8',
                                         shell=True)).replace('__main__.py ', 'pyuavcan ', 1),
          end='\n\n')


print('General help' + HEADER_SUFFIX)
print_output('--version')
print_output('--help')


for cls in cli.commands.get_available_command_classes():
    cmd = cls()
    print(f'Subcommand ``{cmd.names[0]}``' + HEADER_SUFFIX)
    print_output(f'{cmd.names[0]} --help')
