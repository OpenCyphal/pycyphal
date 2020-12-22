# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
This module does not implement a transport, and it is not a part of the abstract transport model.
It contains a collection of software components implementing common logic reusable
with different transport implementations.
It is expected that some transport implementations may be unable to rely on these.

This module is unlikely to be useful for a regular library user (not a developer).
"""

from . import crc as crc
from . import high_overhead_transport as high_overhead_transport

from ._refragment import refragment as refragment
