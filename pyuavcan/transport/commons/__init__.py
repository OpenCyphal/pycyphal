#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This is not a transport.
This submodule contains a collection of software components implementing common logic reusable
with different transport implementations.
Some transport implementations may be too specialized to rely on these, which is expected and natural.
"""

from . import crc

from ._refragment import refragment as refragment

from ._high_overhead_transfer_reassembler import HighOverheadTransferReassembler as HighOverheadTransferReassembler
