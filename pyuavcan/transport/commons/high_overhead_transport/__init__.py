#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This module contains common classes and algorithms used in a certain category of transports
which we call **High Overhead Transports**.
They are designed for highly capable mediums where packets are large and the data transfer speeds are high.

For example, UDP, Serial, and IEEE 802.15.4 transports are high-overhead transports.
CAN, on the other hand, is not a high-overhead transport;
none of the entities defined in this module can be used with CAN.
"""

from ._frame_base import FrameBase as FrameBase
from ._transfer_reassembler import TransferReassembler as TransferReassembler
from ._common import TransferCRC as TransferCRC
