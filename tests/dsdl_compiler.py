#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import logging
import pathlib
import shutil
# noinspection PyProtectedMember
from pyuavcan.dsdl._compiler import generate_package_from_dsdl_namespace, _SOURCE_DIRECTORY


def _unittest_dsdl_compiler() -> None:
    # Suppress unnecessary logging from PyDSDL, there's too much of it and we don't want it to interfere
    logging.getLogger('pydsdl').setLevel(logging.WARNING)

    root_ns = _SOURCE_DIRECTORY.parent / pathlib.Path('_public_regulated_data_types') / pathlib.Path('uavcan')

    parent_dir = _SOURCE_DIRECTORY.parent.parent / pathlib.Path('.dsdl_generated')
    if parent_dir.exists():  # pragma: no cover
        shutil.rmtree(parent_dir, ignore_errors=True)
    parent_dir.mkdir(parents=True, exist_ok=True)

    pkg_dir = generate_package_from_dsdl_namespace(parent_dir, root_ns, [])

    assert pkg_dir.name.endswith('uavcan')
