#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import typing


def _make_transport_options_for_cli() -> typing.Iterable[typing.Sequence[str]]:
    """
    Sensible transport configurations supported by the CLI to test against.
    Don't forget to extend when adding support for new transports.
    """
    # Interfaces that are supported only on GNU/Linux.
    if sys.platform == 'linux':
        yield '--socketcan=vcan0',

    # Interfaces supported on all systems.
    from tests.transport.serial import VIRTUAL_BUS_URI
    yield f'--serial={VIRTUAL_BUS_URI}',


TRANSPORT_ARGS_OPTIONS = list(_make_transport_options_for_cli())
