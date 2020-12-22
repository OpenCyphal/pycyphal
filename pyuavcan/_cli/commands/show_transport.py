# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import argparse
import pyuavcan
from ._base import Command, SubsystemFactory


class ShowTransportCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ["show-transport"]

    @property
    def help(self) -> str:
        return """
Show transport usage documentation and exit.
""".strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return None

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return []

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        del parser

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        import pydoc

        # noinspection PyTypeChecker
        pyuavcan.util.import_submodules(pyuavcan.transport)
        fill_width = 120
        transport_base = pyuavcan.transport.Transport
        # Suppressing MyPy false positive: https://github.com/python/mypy/issues/5374
        for cls in pyuavcan.util.iter_descendants(transport_base):  # type: ignore
            if not cls.__name__.startswith("_") and cls is not transport_base:
                public_module = cls.__module__.split("._")[0]
                public_name = public_module + "." + cls.__name__
                print("=" * fill_width)
                print(public_name.center(fill_width, " "))
                print("-" * fill_width)
                print(cls.__doc__)
                print(pydoc.text.document(cls.__init__))
                print()
        return 0
