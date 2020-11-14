#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import time
import typing
import logging
import argparse
# noinspection PyCompatibility
from . import commands

_logger = logging.getLogger(__name__)

_LOG_FORMAT = '%(asctime)s %(process)5d %(levelname)-8s %(name)s: %(message)s'


def main() -> None:
    logging.basicConfig(format=_LOG_FORMAT)  # Using the default log level; it will be overridden later.

    try:
        exit(_main_impl())
    except KeyboardInterrupt:
        _logger.info('Interrupted')
        _logger.debug('Stack trace where the program has been interrupted', exc_info=True)
        exit(1)
    except AssertionError:
        raise  # Re-raise directly in order to have the stack trace printed. The user is not expected to see this.
    except Exception as ex:
        print('Error: %s:' % type(ex).__name__, ex, file=sys.stderr)
        _logger.info('Unhandled exception: %s', ex, exc_info=True)
        exit(1)


def _main_impl() -> int:
    command_instances: typing.Sequence[commands.Command] = [cls() for cls in commands.get_available_command_classes()]

    args = _construct_argument_parser(command_instances).parse_args()

    _configure_logging(args.verbose)

    _logger.debug('Available commands: %s', command_instances)
    _logger.debug('Parsed args: %s', args)

    # It is a common use case when the user generates DSDL packages in the current directory and then runs the CLI
    # tool in it. Do not require the user to manually export PYTHONPATH=. by extending it with the CWD automatically.
    sys.path.append(os.getcwd())
    _logger.debug('sys.path: %r', sys.path)

    if hasattr(args, 'func'):
        started_at = time.monotonic()
        try:
            result = args.func(args)
        except ImportError as ex:
            # If the application submodule fails to import with an import error, a DSDL data type package
            # probably needs to be generated first, which we suggest the user to do.
            from .commands.dsdl_generate_packages import DSDLGeneratePackagesCommand
            raise ImportError(DSDLGeneratePackagesCommand.make_usage_suggestion_text(ex.name or ''))

        _logger.debug('Command executed in %.1f seconds', time.monotonic() - started_at)
        assert isinstance(result, int)
        return result
    else:
        print('No command specified, nothing to do. Run with --help for usage help. '
              'Online support: https://forum.uavcan.org.', file=sys.stderr)
        print('Available commands:', file=sys.stderr)
        for cmd in command_instances:
            text = f'\t{cmd.names[0]}'
            if len(cmd.names) > 1:
                text += f' (aliases: {", ".join(cmd.names[1:])})'
            print(text, file=sys.stderr)
        return 1


def _construct_argument_parser(command_instances: typing.Sequence[commands.Command]) -> argparse.ArgumentParser:
    from pyuavcan import __version__

    root_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=r'''
         __   __   _______   __   __   _______   _______   __   __
        |  | |  | /   _   \ |  | |  | /   ____| /   _   \ |  \ |  |
        |  | |  | |  |_|  | |  | |  | |  |      |  |_|  | |   \|  |
        |  |_|  | |   _   | \  \_/  / |  |____  |   _   | |  |\   |
        \_______/ |__| |__|  \_____/  \_______| |__| |__| |__| \__|
            |      |            |         |      |         |
        ----o------o------------o---------o------o---------o-------

PyUAVCAN CLI -- a command line tool for diagnostics and management of UAVCAN networks.
PyUAVCAN is a Python library implementing the UAVCAN stack for high-level operating systems (GNU/Linux, Windows, macOS)
supporting different transport protocols (UAVCAN/CAN, UAVCAN/UDP/IP, UAVCAN/serial, etc).

This tool is designed for use either directly by humans or from automation scripts.

Read the docs: https://pyuavcan.readthedocs.io
Ask questions: https://forum.uavcan.org
'''.strip('\r\n'))

    # Register common arguments
    root_parser.add_argument(
        '--version', '-V',
        action='version',
        version=f'%(prog)s {__version__}',
        help='''
Print the PyUAVCAN version string and exit. The tool is versioned synchronously with the PyUAVCAN library.
'''.strip())
    root_parser.add_argument(
        '--verbose', '-v',
        action='count',
        help='Increase the verbosity of the output. Twice for extra verbosity.',
    )

    # Register commands
    subparsers = root_parser.add_subparsers()
    for cmd in command_instances:
        if cmd.examples:
            epilog = 'Examples:\n' + cmd.examples
        else:
            epilog = ''

        parser = subparsers.add_parser(
            cmd.names[0],
            help=cmd.help,
            epilog=epilog,
            aliases=cmd.names[1:],
            formatter_class=argparse.RawTextHelpFormatter,
        )
        cmd.register_arguments(parser)
        for sf in cmd.subsystem_factories:
            sf.register_arguments(parser)

        parser.set_defaults(func=_make_executor(cmd))

    return root_parser


def _make_executor(cmd: commands.Command) -> typing.Callable[[argparse.Namespace], int]:
    def execute(args: argparse.Namespace) -> int:
        subsystems: typing.List[object] = []
        for sf in cmd.subsystem_factories:
            try:
                ss = sf.construct_subsystem(args)
            except Exception as ex:
                raise RuntimeError(f'Subsystem factory {type(sf).__name__!r} for command {cmd.names[0]!r} '
                                   f'has failed: {ex}')
            else:
                subsystems.append(ss)
        _logger.debug('Invoking %r with subsystems %r and arguments %r', cmd, subsystems, args)
        return cmd.execute(args, subsystems)

    return execute


def _configure_logging(verbosity_level: int) -> None:
    """
    Until this function is invoked we're running the bootstrap default configuration.
    This function changes the configuration to use the correct production settings as specified.
    """
    log_level = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }.get(verbosity_level or 0, logging.DEBUG)

    logging.root.setLevel(log_level)

    try:
        # This is not listed among the deps because the availability on other platforms is questionable and it's not
        # actually required at all. See https://stackoverflow.com/a/16847935/1007777.
        import coloredlogs
        # The level spec applies to the handler, not the root logger! This is different from basicConfig().
        coloredlogs.install(level=log_level, fmt=_LOG_FORMAT)
    except Exception as ex:
        _logger.debug('Colored logs are not available: %s: %s', type(ex), ex)
        _logger.info('Consider installing "coloredlogs" from PyPI to make log messages look better')

    # Handle special cases one by one.
    if log_level < logging.INFO:
        logging.getLogger('pydsdl').setLevel(logging.INFO)  # Too much low-level logs from PyDSDL.
