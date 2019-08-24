#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
This transport module contains no media sublayers because the media abstraction
is handled directly by the `PySerial <https://pypi.org/project/pyserial>`_
library and the underlying operating system.


TCP/IP tunneling
++++++++++++++++

For testing or experimentation it is often convenient to use a virtual link instead of a real one.

The underlying PySerial library includes support for many auxiliary features that help with testing
(please read its user documentation, particularly the section on URL handlers).
One of such features is the support for tunneling of raw serial data over TCP connections,
which can be leveraged for local testing without accessing any physical serial ports.
This option can be accessed by specifying the URI of the form ``socket://<address>:<port>``
instead of a real serial port name when establishing the connection.

The location specified in the URL must point to the TCP server port that will forward the data
to and from the other end of the link.
While such a server can be trivially coded manually by the developer,
it is possible to avoid the effort by relying on the TCP connection brokering mode available in
`Ncat <https://nmap.org/ncat/>`_ (which is a part of the Nmap project, thanks Fyodor).

For example, one could set up the TCP broker as follows
(add ``-v`` to see what's happening; more info at https://nmap.org/ncat/guide/ncat-broker.html)
(the port number is chosen at random here)::

    ncat --broker --listen -p 50905

And then use a serial transport with ``socket://localhost:50905``.
All nodes whose transports are configured like that will be able to communicate with each other,
as if they were connected to the same bus.
Essentially, this can be seen as a virtualized RS-485 bus,
where same concerns regarding medium access coordination apply.

The location of the URI doesn't have to be ``localhost``, of course --
one can use this approach to link UAVCAN nodes via conventional IP networks.


Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pyuavcan.transport.serial._serial
                         pyuavcan.transport.serial._frame
                         pyuavcan.transport.serial._session._base
                         pyuavcan.transport.serial._session._input
                         pyuavcan.transport.serial._session._output
   :parts: 1
"""

from ._serial import SerialTransport as SerialTransport
from ._serial import SerialTransportStatistics as SerialTransportStatistics

from ._session import SerialSession as SerialSession
from ._session import SerialInputSession as SerialInputSession
from ._session import SerialOutputSession as SerialOutputSession
from ._session import SerialFeedback as SerialFeedback
from ._session import SerialInputSessionStatistics as SerialInputSessionStatistics

from ._frame import SerialFrame as SerialFrame
