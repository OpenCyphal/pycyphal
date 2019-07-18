#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from ._common import run_process, BackgroundChildProcess
# noinspection PyProtectedMember
from pyuavcan._cli.commands import dsdl_generate_packages
from tests.dsdl.conftest import TEST_DATA_TYPES_DIR, PUBLIC_REGULATED_DATA_TYPES_DIR


def _unittest_cli() -> None:
    demo_proc = BackgroundChildProcess('basic_usage.py')

    # Generate DSDL namespace "sirius_cyber_corp"
    run_process('pyuavcan', '-v', 'dsdl-gen-pkg',
                str(TEST_DATA_TYPES_DIR / 'sirius_cyber_corp'),
                '--lookup', dsdl_generate_packages.DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "test"
    run_process('pyuavcan', '-v', 'dsdl-gen-pkg',
                str(TEST_DATA_TYPES_DIR / 'test'),
                '--lookup', dsdl_generate_packages.DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL)

    # Generate DSDL namespace "uavcan"
    run_process('pyuavcan', '-v', 'dsdl-gen-pkg', str(PUBLIC_REGULATED_DATA_TYPES_DIR / 'uavcan'))

    # TODO: publication and subscription

    demo_proc.kill()
