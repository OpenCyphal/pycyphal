#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import pytest
import logging

# The fixture is imported here to make it visible to other tests in this suite.
from .dsdl.conftest import generated_packages as generated_packages  # noqa


GIBIBYTE = 1024 ** 3

MEMORY_LIMIT = 4 * GIBIBYTE
"""
The test suite artificially limits the amount of consumed memory in order to avoid triggering the OOM killer
should a test go crazy and eat all memory.
"""

_logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)  # type: ignore
def _configure_memory_limit() -> None:
    if sys.platform == "linux":
        import resource

        _logger.info("Limiting process memory usage to %.1f GiB", MEMORY_LIMIT / GIBIBYTE)
        resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
