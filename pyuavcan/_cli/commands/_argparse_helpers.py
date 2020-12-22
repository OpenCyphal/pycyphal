# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import enum
import typing
import argparse


def make_enum_action(enum_type: typing.Type[enum.Enum]) -> typing.Type[argparse.Action]:
    mapping: typing.Dict[str, typing.Any] = {}
    for e in enum_type:
        mapping[e.name.lower()] = e

    class ArgparseEnumAction(argparse.Action):
        # noinspection PyShadowingBuiltins
        def __init__(
            self,
            option_strings: typing.Sequence[str],
            dest: str,
            nargs: typing.Union[int, str, None] = None,
            const: typing.Any = None,
            default: typing.Any = None,
            type: typing.Any = None,
            choices: typing.Any = None,
            required: bool = False,
            help: typing.Optional[str] = None,
            metavar: typing.Any = None,
        ):
            def type_proxy(x: str) -> typing.Any:
                """A proxy is needed because a method of an unhashable type is unhashable."""
                return mapping.get(x)

            if type is None:
                type = type_proxy

            if choices is None:
                choices = [_NamedChoice(key, value) for key, value in mapping.items()]

            super(ArgparseEnumAction, self).__init__(
                option_strings,
                dest,
                nargs=nargs,
                const=const,
                default=default,
                type=type,
                choices=choices,
                required=required,
                help=help,
                metavar=metavar,
            )

        def __call__(
            self,
            parser: argparse.ArgumentParser,
            namespace: argparse.Namespace,
            values: typing.Union[str, typing.Sequence[typing.Any], None],
            option_string: typing.Optional[str] = None,
        ) -> None:
            setattr(namespace, self.dest, values)

    return ArgparseEnumAction


class _NamedChoice:
    def __init__(self, key: str, value: typing.Any):
        self.key = key
        self.value = value

    def __eq__(self, other: object) -> bool:
        return bool(self.value == other)

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return self.key
