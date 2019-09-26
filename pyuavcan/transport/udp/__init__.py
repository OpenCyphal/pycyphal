#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This transport module contains no media sublayers because the media abstraction
is handled directly by the standard UDP/IP stack of the underlying operating system.


Tooling
+++++++

Run UAVCAN networks on the local loopback interface (``127.x.x.x``, ``::1``).
The loopback interface does not support broadcasting, however, so it can't be used on its own.
There is a helper script in the UDP transport test directory which can be used for broadcast emulation.
However, this approach is not recommended; instead, consider testing on a physical network with real broadcast
capability.

Use Wireshark for monitoring and inspection.

Use netcat for trivial monitoring; e.g., listen to a UDP port like this: ``nc -ul 48469``.


Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pyuavcan.transport.udp._udp
                         pyuavcan.transport.udp._frame
                         pyuavcan.transport.udp._session._input
                         pyuavcan.transport.udp._session._output
                         pyuavcan.transport.udp._demultiplexer
   :parts: 1
"""

from ._udp import UDPTransport as UDPTransport
from ._udp import UDPTransportStatistics as UDPTransportStatistics

from ._session import UDPInputSession as UDPInputSession
from ._session import PromiscuousUDPInputSession as PromiscuousUDPInputSession
from ._session import SelectiveUDPInputSession as SelectiveUDPInputSession

from ._session import UDPInputSessionStatistics as UDPInputSessionStatistics
from ._session import PromiscuousUDPInputSessionStatistics as PromiscuousUDPInputSessionStatistics
from ._session import SelectiveUDPInputSessionStatistics as SelectiveUDPInputSessionStatistics

from ._session import UDPOutputSession as UDPOutputSession
from ._session import UDPFeedback as UDPFeedback

from ._frame import UDPFrame as UDPFrame

from ._port_mapping import map_data_specifier_to_udp_port as map_data_specifier_to_udp_port

from ._demultiplexer import DemultiplexerStatistics
