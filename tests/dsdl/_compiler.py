#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pytest

import pyuavcan.dsdl

from .conftest import TEST_DATA_TYPES_DIR, PUBLIC_REGULATED_DATA_TYPES_DIR


def _unittest_bad_usage() -> None:
    with pytest.raises(TypeError):
        pyuavcan.dsdl.generate_package(TEST_DATA_TYPES_DIR, TEST_DATA_TYPES_DIR)  # type: ignore
