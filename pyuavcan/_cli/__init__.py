#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from ._main import main as main

# This is exported for automatic documentation generation.
# noinspection PyCompatibility
from . import commands as commands

# Exported for unit tests.
from .commands import DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL as DEFAULT_PUBLIC_REGULATED_DATA_TYPES_ARCHIVE_URL
