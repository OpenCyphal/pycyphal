#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import time
import logging
import argparse


_logger = logging.getLogger(__name__)


def main() -> None:
    from ._commands import DEFAULT_DSDL_GENERATED_PACKAGES_DIR
    sys.path.insert(0, str(DEFAULT_DSDL_GENERATED_PACKAGES_DIR))

    logging.basicConfig(stream=sys.stderr,
                        level=logging.WARNING,
                        format='%(asctime)s %(process)5d %(levelname)-8s %(name)s: %(message)s')
    try:
        exit(_main_impl())
    except KeyboardInterrupt:
        _logger.info('Interrupted')
        _logger.debug('Stack trace where the program has been interrupted', exc_info=True)
        exit(1)
    except Exception as ex:
        print('Error (run with -v for more info): %s:' % type(ex).__name__, ex, file=sys.stderr)
        _logger.info('Unhandled exception: %s', ex, exc_info=True)
        exit(1)


def _construct_argument_parser() -> argparse.ArgumentParser:
    from . import _commands
    from pyuavcan import __version__

    root_parser = argparse.ArgumentParser(
        description='''
A command line tool for diagnostics and management of UAVCAN networks.
This tool is built on top of PyUAVCAN -- a full-featured Python implementation
of the UAVCAN stack for high-level operating systems.
Find documentation and support at https://uavcan.org.
'''.strip(),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Register common arguments
    root_parser.add_argument(
        '--version', '-V',
        action='version',
        version=f'%(prog)s {__version__}',
        help='''
Print the PyUAVCAN version string and exit.
The tool is versioned synchronously with the PyUAVCAN library.
'''.strip(),
    )
    root_parser.add_argument(
        '--verbose', '-v',
        action='count',
        help='Increase the verbosity of the output. Twice for extra verbosity.',
    )

    # Register commands
    subparsers = root_parser.add_subparsers()
    for cmd in _commands.COMMANDS:
        if cmd.info.examples:
            epilog = 'Examples:\n' + cmd.info.examples
        else:
            epilog = ''

        parser = subparsers.add_parser(
            cmd.name,
            help=cmd.info.help,
            epilog=epilog,
            aliases=cmd.info.aliases,
            formatter_class=argparse.RawTextHelpFormatter,
        )

        cmd.register_arguments(parser)
        parser.set_defaults(func=cmd.execute)

    return root_parser


def _main_impl() -> int:
    from . import _commands

    args = _construct_argument_parser().parse_args()

    logging.root.setLevel({
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }.get(args.verbose or 0, logging.DEBUG))

    _logger.debug('Available command modules: %s', _commands.COMMANDS)
    _logger.debug('Parsed args: %s', args)

    if hasattr(args, 'func'):
        started_at = time.monotonic()
        result = args.func(args)
        _logger.debug('Command executed in %.1f seconds', time.monotonic() - started_at)
        assert isinstance(result, int)
        return result
    else:
        print('No command specified, nothing to do. Run with --help for usage help. '
              'Online support: https://forum.uavcan.org.', file=sys.stderr)
        print('Available commands:', file=sys.stderr)
        for cmd in _commands.COMMANDS:
            text = f'\t{cmd.name}'
            if cmd.info.aliases:
                text += f' (aliases: {", ".join(cmd.info.aliases)})'
            print(text, file=sys.stderr)
        return 1
