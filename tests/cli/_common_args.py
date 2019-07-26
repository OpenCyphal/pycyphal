#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
import typing


def make_iface_args() -> typing.Sequence[str]:
    """
    Constructs the list of command-line arguments specifying which interfaces to use for testing.
    We could also add a random element here.

    When adding support for new transports to the CLI, make sure to update this function so that
    your transports are tested against, too.
    """
    import pytest
    # TODO: Emit redundant transports when supported.
    if sys.platform == 'linux':
        return '--socketcan=vcan0',
    else:
        pytest.skip('CLI test skipped because it does not yet support non-GNU/Linux-based systems. Please fix.')
