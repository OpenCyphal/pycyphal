# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

"""
Cyphal/serial transport overview
++++++++++++++++++++++++++++++++

The Cyphal/serial transport is designed for byte-level communication channels, such as:

- TCP/IP
- UART, RS-422/232
- USB CDC ACM

It may also be suited for raw transport log storage.

This transport module contains no media sublayers because the media abstraction
is handled directly by the `PySerial <https://pypi.org/project/pyserial>`_
library and the underlying operating system.

For the full protocol definition, please refer to the `Cyphal Specification <https://opencyphal.org/specification>`_.


Forward error correction (FEC)
++++++++++++++++++++++++++++++

This transport supports optional FEC through full duplication of transfers.
This feature is discussed in detail in the documentation for the UDP transport :mod:`pycyphal.transport.udp`.


Usage
+++++

..  doctest::
    :hide:

    >>> import tests
    >>> tests.asyncio_allow_event_loop_access_from_top_level()
    >>> from tests import doctest_await

>>> import asyncio
>>> import pycyphal
>>> import pycyphal.transport.serial
>>> tr = pycyphal.transport.serial.SerialTransport('loop://', local_node_id=1234, baudrate=115200)
>>> tr.local_node_id
1234
>>> tr.serial_port.baudrate
115200
>>> pm = pycyphal.transport.PayloadMetadata(1024)
>>> ds = pycyphal.transport.MessageDataSpecifier(2345)
>>> pub = tr.get_output_session(pycyphal.transport.OutputSessionSpecifier(ds, None), pm)
>>> sub = tr.get_input_session(pycyphal.transport.InputSessionSpecifier(ds, None), pm)
>>> doctest_await(pub.send(pycyphal.transport.Transfer(pycyphal.transport.Timestamp.now(),
...                                                    pycyphal.transport.Priority.LOW,
...                                                    1111,
...                                                    fragmented_payload=[]),
...                        asyncio.get_event_loop().time() + 1.0))
True
>>> doctest_await(sub.receive(asyncio.get_event_loop().time() + 1.0))
TransferFrom(..., transfer_id=1111, ...)
>>> tr.close()


Tooling
+++++++

Serial data logging
~~~~~~~~~~~~~~~~~~~

The underlying PySerial library provides a convenient method of logging exchange through a serial port into a file.
To invoke this feature, embed the name of the serial port into the URI ``spy:///dev/ttyUSB0?file=dump.txt``,
where ``/dev/ttyUSB0`` is the name of the serial port, ``dump.txt`` is the name of the log file.


TCP/IP tunneling
~~~~~~~~~~~~~~~~

For testing or experimentation it is often convenient to use a virtual link instead of a real one.
The underlying PySerial library supports tunneling of raw serial data over TCP connections,
which can be leveraged for local testing without accessing any physical serial ports.
This option can be accessed by specifying the URI of the form ``socket://<address>:<port>``
instead of a real serial port name when establishing the connection.

The location specified in the URL must point to the TCP server port that will forward the data
to and from the other end of the link. For this purpose PyCyphal includes ``cyphal-serial-broker``.
Alternatively, ncat (which is a part of the `Nmap <https://nmap.org>`_ project, thanks Fyodor)
has the broker mode.

For example, one could use ``cyphal-serial-broker`` as follows (the port number is chosen at random here)::

    cyphal-serial-broker -p 50906

And then use a serial transport with ``socket://127.0.0.1:50905``
(N.B.: using ``localhost`` may significantly increase initialization latency on Windows due to slow DNS lookup).
All nodes whose transports are configured like that will be able to communicate with each other,
as if they were connected to the same bus.

The location of the URI doesn't have to be local, of course --
one can use this approach to link Cyphal nodes via conventional IP networks.

The exchange over the virtual bus can be dumped trivially for analysis::

    nc localhost 50905 > dump.bin


Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pycyphal.transport.serial._serial
                         pycyphal.transport.serial._frame
                         pycyphal.transport.serial._session._base
                         pycyphal.transport.serial._session._input
                         pycyphal.transport.serial._session._output
                         pycyphal.transport.serial._tracer
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

from ._tracer import SerialCapture as SerialCapture
from ._tracer import SerialTracer as SerialTracer
from ._tracer import SerialErrorTrace as SerialErrorTrace
from ._tracer import SerialOutOfBandTrace as SerialOutOfBandTrace
