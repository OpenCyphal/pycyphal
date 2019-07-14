#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This transport module contains no media abstraction submodules because the media abstraction
is handled directly by the PySerial library and the underlying operating system.

.. inheritance-diagram:: pyuavcan.transport.serial._serial
   :parts: 1
"""

from ._serial import SerialTransport as SerialTransport
