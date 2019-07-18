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

import pathlib
import textwrap
import subprocess

# noinspection PyProtectedMember
import pyuavcan._cli as cli


EXECUTABLE = pathlib.Path(cli.__file__).absolute().parent / 'pyuavcan'

HEADER_SUFFIX = '\n' + '.' * 80 + '\n\n'


def indent(text: str) -> str:
    return textwrap.indent(text, ' ' * 3)


def print_output(command_arguments: str) -> None:
    print('::', end='\n\n')
    print(indent(f'$ {EXECUTABLE.name} {command_arguments}'))
    print(indent(subprocess.check_output(f'{EXECUTABLE} {command_arguments}',
                                         encoding='utf8',
                                         shell=True)),
          end='\n\n')


print('General help' + HEADER_SUFFIX)
print_output('--version')
print_output('--help')


for sub in cli.commands.COMMANDS:
    print(f'Subcommand ``{sub.name}``' + HEADER_SUFFIX)
    print_output(f'{sub.name} --help')
