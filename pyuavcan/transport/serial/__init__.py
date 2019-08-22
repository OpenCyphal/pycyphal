#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This transport module contains no media sublayers because the media abstraction
is handled directly by the PySerial library and the underlying operating system.

For testing, use serial-over-TCP tunneling implemented in PySerial
(details: https://pythonhosted.org/pyserial/url_handlers.html#socket)
with ncat in TCP broker mode
(details: https://nmap.org/ncat/guide/ncat-broker.html)::

    ncat --broker --listen -p 50905

This closely emulates an RS-485 bus one could say.

Inheritance diagram:

.. inheritance-diagram:: pyuavcan.transport.serial._serial
   :parts: 1
"""

from ._serial import SerialTransport as SerialTransport

from ._session import SerialSession as SerialSession
from ._session import SerialInputSession as SerialInputSession
from ._session import SerialOutputSession as SerialOutputSession
from ._session import SerialFeedback as SerialFeedback
from ._session import SerialInputStatistics as SerialInputStatistics

from ._frame import SerialFrame as SerialFrame
