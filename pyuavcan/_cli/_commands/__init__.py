#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import types
import typing
import argparse
from ._base import CommandInfo as CommandInfo
from ._base import DEFAULT_DSDL_GENERATED_PACKAGES_DIR as DEFAULT_DSDL_GENERATED_PACKAGES_DIR


class Command:
    def __init__(self, module: types.ModuleType):
        self._module = module
        assert self.info.help, 'Malformed module'

    @property
    def name(self) -> str:
        return self._module.__name__.split('.')[-1].replace('_', '-')

    @property
    def info(self) -> CommandInfo:
        obj = getattr(self._module, 'INFO')
        assert isinstance(obj, CommandInfo), 'Malformed module'
        return obj

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        obj = getattr(self._module, 'register_arguments')
        assert callable(obj), 'Malformed module'
        obj(parser)

    def execute(self, args: argparse.Namespace) -> int:
        obj = getattr(self._module, 'execute')
        assert callable(obj), 'Malformed module'
        out = obj(args)
        return int(out) if out is not None else 0

    def __repr__(self) -> str:
        return f'Command(module={self._module}, name={self.name!r}, aliases={self.info.aliases})'


def _load_commands() -> typing.List[Command]:
    import pathlib
    import importlib

    out: typing.List[Command] = []
    for mod in pathlib.Path(__file__).parent.iterdir():
        if mod.name.endswith('.py') and not mod.name.startswith('_'):
            module_name = mod.stem
            module = importlib.import_module('.' + module_name, __name__)
            out.append(Command(module))

    return out


COMMANDS = _load_commands()
