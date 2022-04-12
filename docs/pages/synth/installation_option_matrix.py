#!/usr/bin/env python3
# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import re
import typing
import textwrap
import dataclasses
import configparser
import pycyphal

HEADER_SUFFIX = "\n" + "." * 80 + "\n"

cp = configparser.ConfigParser()
cp.read("../setup.cfg")
extras: typing.Dict[str, str] = dict(cp["options.extras_require"])


print("If you need full-featured library, use this and read no more::", end="\n\n")
print(f'   pip install pycyphal[{",".join(extras.keys())}]', end="\n\n")
print("If you want to know what exactly you are installing, read on.", end="\n\n")


@dataclasses.dataclass(frozen=True)
class TransportOption:
    name: str
    class_name: str
    extras: typing.Dict[str, str]


transport_options: typing.List[TransportOption] = []

# noinspection PyTypeChecker
pycyphal.util.import_submodules(pycyphal.transport)
for cls in pycyphal.util.iter_descendants(pycyphal.transport.Transport):
    transport_name = cls.__module__.split(".")[2]  # pycyphal.transport.X
    relevant_extras: typing.Dict[str, str] = {}
    for k in list(extras.keys()):
        if k.startswith(f"transport-{transport_name}"):
            relevant_extras[k] = extras.pop(k)

    transport_module_name = re.sub(r"\._[_a-zA-Z0-9]*", "", cls.__module__)
    transport_class_name = transport_module_name + "." + cls.__name__

    transport_options.append(
        TransportOption(name=transport_name, class_name=transport_class_name, extras=relevant_extras)
    )

for to in transport_options:
    print(f"{to.name} transport" + HEADER_SUFFIX)
    print(f"This transport is implemented by :class:`{to.class_name}`.")
    if to.extras:
        print("The following installation options are available:")
        print()
        for key, deps in to.extras.items():
            print(f"{key}")
            print("   This option pulls the following dependencies::", end="\n\n")
            print(textwrap.indent(deps.strip(), " " * 6), end="\n\n")
    else:
        print("This transport has no installation dependencies.")
    print()

other_extras: typing.Dict[str, str] = {}
for k in list(extras.keys()):
    if not k.startswith(f"transport_"):
        other_extras[k] = extras.pop(k)

if other_extras:
    print("Other installation options" + HEADER_SUFFIX)
    print("These installation options are not related to any transport.", end="\n\n")
    for key, deps in other_extras.items():
        print(f"{key}")
        print("   This option pulls the following dependencies:", end="\n\n")
        print("   .. code-block::", end="\n\n")
        print(textwrap.indent(deps.strip(), " " * 6), end="\n\n")
    print()

if extras:
    raise RuntimeError(
        f"No known transports to match the following installation options (typo?): " f"{list(extras.keys())}"
    )
