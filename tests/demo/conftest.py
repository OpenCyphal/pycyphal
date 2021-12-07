# Copyright (c) 2021 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from typing import Iterator
import os
import pytest
from .. import DEMO_DIR


@pytest.fixture()
def cd_to_demo() -> Iterator[None]:
    restore_to = os.getcwd()
    os.chdir(DEMO_DIR)
    yield
    os.chdir(restore_to)
