# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
This module contains implementations of various CRC algorithms used by the transports.

`32-Bit Cyclic Redundancy Codes for Internet Applications (Philip Koopman)
<https://users.ece.cmu.edu/~koopman/networks/dsn02/dsn02_koopman.pdf>`_.
"""

from ._base import CRCAlgorithm as CRCAlgorithm
from ._crc16_ccitt import CRC16CCITT as CRC16CCITT
from ._crc32c import CRC32C as CRC32C
from ._crc64we import CRC64WE as CRC64WE
