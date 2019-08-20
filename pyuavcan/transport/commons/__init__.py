#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This is not a transport, and it is not a part of the abstract model.
This submodule contains a collection of software components implementing common logic reusable
with different transport implementations.
Some transport implementations may be too specialized to rely on these, which is expected and natural.
"""

from . import crc as crc
from . import high_overhead_transport as high_overhead_transport

from ._refragment import refragment as refragment
