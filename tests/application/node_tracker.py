#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import pyuavcan


# noinspection PyProtectedMember
@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_node_tracker(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    assert generated_packages
