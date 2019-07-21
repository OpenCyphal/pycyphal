#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import enum
import typing
import logging
import argparse
import argparse_utils


Formatter = typing.Callable[[typing.Dict[int, typing.Dict[str, typing.Any]]], str]

_logger = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    # noinspection PyTypeChecker
    parser.add_argument(
        '--format', '-F',
        default=next(iter(_Format)),
        action=argparse_utils.enum_action(_Format),
        help='''
The format of the data printed into stdout. The final representation is
constructed from an intermediate "builtin-based" representation, which is
a simplified form that is stripped of the detailed DSDL type information,
like JSON. For the background info please read the PyUAVCAN documentation
on builtin-based representations.
Default: %(default)s
        '''.strip(),
    )


def construct_formatter(args: argparse.Namespace) -> Formatter:
    return {
        _Format.YAML: _make_yaml_formatter,
        _Format.JSON: _make_json_formatter,
        _Format.TSV:  _make_tsv_formatter,
    }[args.format]()


class _Format(enum.Enum):
    YAML = enum.auto()
    JSON = enum.auto()
    TSV = enum.auto()


def _make_yaml_formatter() -> Formatter:
    from .yaml import YAMLDumper
    dumper = YAMLDumper(explicit_start=True)
    return lambda data: dumper.dumps(data)


def _make_json_formatter() -> Formatter:
    import simplejson as json
    return lambda data: json.dumps(data, ensure_ascii=False, separators=(',', ':'))


def _make_tsv_formatter() -> Formatter:
    # TODO print into a TSV (tab separated values, like CSV with tabs instead of commas).
    # The TSV format should place one scalar per column for ease of parsing by third-party software.
    # Variable-length entities such as arrays should expand into the maximum possible number of columns?
    # Unions should be represented by adjacent groups of columns where only one such group contains values?
    # We may need to obtain the full type information here in order to build the final representation.
    # Sounds complex. Search for better ways later. We just need a straightforward way of dumping data into a
    # standard tabular format for later processing using third-party software.
    raise NotImplementedError('Sorry, the TSV formatter is not yet implemented')


def _unittest_formatter() -> None:
    obj = {
        12345: {
            'abc': {
                'def': [123, 456, ],
            },
            'ghi': 789,
        }
    }
    assert construct_formatter(argparse.Namespace(format=_Format.YAML))(obj) == """---
12345:
  abc:
    def:
    - 123
    - 456
  ghi: 789
"""
    assert construct_formatter(argparse.Namespace(format=_Format.JSON))(obj) == \
        '{"12345":{"abc":{"def":[123,456]},"ghi":789}}'
