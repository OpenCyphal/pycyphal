#!/usr/bin/env python3
#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import re
import typing
import textwrap
import dataclasses
import configparser
import pyuavcan

HEADER_SUFFIX = '\n' + '.' * 80 + '\n'

cp = configparser.ConfigParser()
cp.read('../setup.cfg')
extras: typing.Dict[str, str] = dict(cp['options.extras_require'])


print(f'Example: ``pip install pyuavcan[{list(extras)[0]},{list(extras)[-1]}]``')
print()


@dataclasses.dataclass(frozen=True)
class TransportOption:
    name:        str
    module_name: str
    class_name:  str
    extras:      typing.Dict[str, str]


transport_options: typing.List[TransportOption] = []

# noinspection PyTypeChecker
pyuavcan.util.import_submodules(pyuavcan.transport)
for cls in pyuavcan.util.iter_descendants(pyuavcan.transport.Transport):
    transport_name = cls.__module__.split('.')[2]   # pyuavcan.transport.X
    relevant_extras = {
        k: v
        for k, v in extras.items()
        if k.startswith(f'transport_{transport_name}')
    }

    transport_module_name = re.sub(r'\._[_a-zA-Z0-9]*', '', cls.__module__)
    transport_class_name = transport_module_name + '.' + cls.__name__

    transport_options.append(TransportOption(name=transport_name,
                                             module_name=transport_module_name,
                                             class_name=transport_class_name,
                                             extras=relevant_extras))

for to in transport_options:
    print(f'{to.name} transport' + HEADER_SUFFIX)
    print(f'This transport is implemented in the module :mod:`{to.module_name}`, class :class:`{to.class_name}`.')
    if to.extras:
        print('The following installation options are available for this transport:')
        print()
        for key, deps in to.extras.items():
            print(f'{key}')
            print('   This option pulls the following dependencies:', end='\n\n')
            print('   .. code-block::', end='\n\n')
            print(textwrap.indent(deps.strip(), ' ' * 6), end='\n\n')
    else:
        print('This transport requires no installation dependencies, it is always available.')
    print()

other_extras = {
    k: v
    for k, v in extras.items()
    if not k.startswith(f'transport_')
}
if other_extras:
    print('Non-transport-related installation options' + HEADER_SUFFIX)
    for key, deps in other_extras.items():
        print(f'{key}')
        print('   This option pulls the following dependencies:', end='\n\n')
        print('   .. code-block::', end='\n\n')
        print(textwrap.indent(deps.strip(), ' ' * 6), end='\n\n')
    print()
