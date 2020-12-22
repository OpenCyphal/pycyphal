# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import enum
import typing
import logging
import argparse
from .._yaml import YAMLDumper  # Reaching to an upper-level module like this is not great, do something about it.
from .._argparse_helpers import make_enum_action
from ._base import SubsystemFactory


Formatter = typing.Callable[[typing.Dict[int, typing.Dict[str, typing.Any]]], str]

_logger = logging.getLogger(__name__)


class FormatterFactory(SubsystemFactory):
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        # noinspection PyTypeChecker
        parser.add_argument(
            "--format",
            "-F",
            default=next(iter(_Format)),
            action=make_enum_action(_Format),
            help="""
The format of the data printed into stdout. The final representation is constructed from an intermediate
"builtin-based" representation, which is a simplified form that is stripped of the detailed DSDL type information,
like JSON. For the background info please read the PyUAVCAN documentation on builtin-based representations.

YAML is the default option as it is easy to process for humans and other machines alike. Each YAML-formatted object
is separated from its siblings by an explicit document start marker: "---".

JSON output is optimized for machine parsing, strictly one object per line.

TSV (tab separated values) output is intended for use with third-party software such as computer algebra systems or
spreadsheet processors.

Default: %(default)s
""".strip(),
        )

    def construct_subsystem(self, args: argparse.Namespace) -> Formatter:
        return {
            _Format.YAML: _make_yaml_formatter,
            _Format.JSON: _make_json_formatter,
            _Format.TSV: _make_tsv_formatter,
        }[args.format]()


class _Format(enum.Enum):
    YAML = enum.auto()
    JSON = enum.auto()
    TSV = enum.auto()


def _make_yaml_formatter() -> Formatter:
    dumper = YAMLDumper(explicit_start=True)
    return lambda data: dumper.dumps(data)


def _make_json_formatter() -> Formatter:
    # We prefer simplejson over the standard json because the native json lacks important capabilities:
    #  - simplejson preserves dict ordering, which is very important for UX.
    #  - simplejson supports Decimal.
    import simplejson as json

    return lambda data: json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _make_tsv_formatter() -> Formatter:
    # TODO print into a TSV (tab separated values, like CSV with tabs instead of commas).
    # The TSV format should place one scalar per column for ease of parsing by third-party software.
    # Variable-length entities such as arrays should expand into the maximum possible number of columns?
    # Unions should be represented by adjacent groups of columns where only one such group contains values?
    # We may need to obtain the full type information here in order to build the final representation.
    # Sounds complex. Search for better ways later. We just need a straightforward way of dumping data into a
    # standard tabular format for later processing using third-party software.
    raise NotImplementedError("Sorry, the TSV formatter is not yet implemented")


def _unittest_formatter() -> None:
    obj = {
        2345: {
            "abc": {
                "def": [
                    123,
                    456,
                ],
            },
            "ghi": 789,
        }
    }
    assert (
        FormatterFactory().construct_subsystem(argparse.Namespace(format=_Format.YAML))(obj)
        == """---
2345:
  abc:
    def:
    - 123
    - 456
  ghi: 789
"""
    )
    assert (
        FormatterFactory().construct_subsystem(argparse.Namespace(format=_Format.JSON))(obj)
        == '{"2345":{"abc":{"def":[123,456]},"ghi":789}}'
    )
