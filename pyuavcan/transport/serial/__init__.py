#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
UAVCAN/Serial transport overview
++++++++++++++++++++++++++++++++

The UAVCAN/Serial transport is experimental and is not yet part of the UAVCAN specification.
Future revisions may break wire compatibility until the transport is formally specified.
Context: https://forum.uavcan.org/t/alternative-transport-protocols/324, also see the discussion at
https://forum.uavcan.org/t/yukon-design-megathread/390/115?u=pavel.kirienko.

The UAVCAN/Serial transport is designed for OSI L1 byte-level serial links:

- UART, RS-232/485/422 (the recommended rates are: 115200 bps, 921600 bps, 3 Mbps, 10 Mbps, 100 Mbps);
  copper or fiber optics.
- USB CDC ACM.

It is also suitable for raw transport log storage, because one-dimensional flat binary files are structurally
similar to serial byte-level links.

This transport module contains no media sublayers because the media abstraction
is handled directly by the `PySerial <https://pypi.org/project/pyserial>`_
library and the underlying operating system.

The serial transport supports all transfer categories:

+--------------------+--------------------------+---------------------------+
| Supported transfers| Unicast                  | Broadcast                 |
+====================+==========================+===========================+
|**Message**         | Yes                      | Yes                       |
+--------------------+--------------------------+---------------------------+
|**Service**         | Yes                      | Banned by Specification   |
+--------------------+--------------------------+---------------------------+


Protocol definition
+++++++++++++++++++

The packet header is defined as follows (byte and bit ordering in this definition follow the DSDL specification:
least significant byte first, most significant bit first)::

    uint8   version              # Always zero. Discard the frame if not.
    uint8   priority             # 0 = highest, 7 = lowest; the rest are unused.
    uint16  source node ID       # 0xFFFF = anonymous.
    uint16  destination node ID  # 0xFFFF = broadcast.
    uint16  data specifier

    uint64  data type hash
    uint64  transfer ID

    uint32  frame index EOT      # MSB set if last frame of the transfer; i.e., 0x8000_0000 if single-frame transfer.
    uint32  header CRC           # CRC-32C (Castagnoli) of the header (all fields above).

For message frames, the data specifier field contains the subject-ID value,
so that the most significant bit is always cleared.
For service frames, the most significant bit (15th) is always set,
and the second-to-most-significant bit (14th) is set for response transfers only;
the remaining 14 least significant bits contain the service-ID value.

Total header size: 32 bytes (256 bits).

The header is prepended before the frame payload; the resulting structure is
encoded into its serialized form using the following packet format (influenced by HDLC, SLIP, POPCOP):

+-------------------------+--------------+---------------+--------------------------------+-------------------------+
| Frame delimiter **0x9E**|Escaped header|Escaped payload| Escaped CRC-32C of the payload | Frame delimiter **0x9E**|
+=========================+==============+===============+================================+=========================+
| 1 byte                  | 32..64 bytes | >=0 bytes     | 4..8 bytes                     | 1 byte                  |
+-------------------------+--------------+---------------+--------------------------------+-------------------------+
| Single-byte frame       | The following bytes are      | Four bytes long, little-endian | Same frame delimiter as |
| delimiter **0x9E**.     | escaped: **0x9E** (frame     | byte order; bytes 0x9E (frame  | at the start.           |
| Begins a new frame and  | delimiter); **0x8E**         | delimiter) and 0x8E (escape    | Terminates the current  |
| possibly terminates the | (escape character). An       | character) are escaped like in | frame and possibly      |
| previous frame.         | escaped byte is bitwise      | the header/payload. The CRC is | begins the next frame.  |
|                         | inverted and prepended with  | computed over the unescaped    |                         |
|                         | the escape character 0x8E.   | (i.e., original form) payload, |                         |
|                         | For example: byte 0x9E is    | not including the header       |                         |
|                         | transformed into 0x8E        | (because the header has a      |                         |
|                         | followed by 0x71.            | dedicated CRC).                |                         |
+-------------------------+------------------------------+--------------------------------+-------------------------+

There are no magic bytes in this format because the strong CRC and the data type hash field render the
format sufficiently recognizable. The worst case overhead exceeds 100% if every byte of the payload and the CRC
is either 0x9E or 0x8E. Despite the overhead, this format is still considered superior to the alternatives
since it is robust and guarantees a constant recovery time. Consistent-overhead byte stuffing (COBS) is sometimes
employed for similar tasks, but it should be understood that while it offers a substantially lower overhead,
it undermines the synchronization recovery properties of the protocol. There is a somewhat relevant discussion
at https://github.com/vedderb/bldc/issues/79.

The format can share the same serial medium with ASCII text exchanges such as command-line interfaces or
real-time logging. The special byte values employed by the format do not belong to the ASCII character set.

The last four bytes of a multi-frame transfer payload contain the CRC32C (Castagnoli) hash of the transfer
payload in little-endian byte order.
The multi-frame transfer logic (decomposition and reassembly) is implemented in a separate
transport-agnostic module :mod:`pyuavcan.transport.commons.high_overhead_transport`.

Note that we use CRC-32C (Castagnoli) as the header/frame CRC instead of CRC-32K2 (Koopman-2)
which is superior at short data blocks offering the Hamming distance of 6 as opposed to 4.
This is because Castagnoli is superior for transfer CRC which is often sufficiently long
to flip the balance in favor of Castagnoli rather than Koopman.
We could use Koopman for the header/frame CRC and keep Castagnoli for the transfer CRC,
but such diversity is harmful because it would require implementers to keep two separate CRC tables
which may be costly in embedded applications and may deteriorate the performance of CPU caches.

**Despite the fact that the support for multi-frame transfers is built into the transport layer,
it should not be relied on and it may be removed later.** The reason is that serial links do not have native support
for framing, and as such, it is possible to configure the MTU to be arbitrarily high to avoid multi-frame transfers
completely. **The lack of multi-frame transfers simplifies implementations drastically, which is important for
deeply-embedded systems. As such, all serial transfers should be single-frame transfers.**


Unreliable links and temporal redundancy
++++++++++++++++++++++++++++++++++++++++

The serial transport supports the deterministic data loss mitigation option,
where a transfer can be repeated several times to reduce the probability of its loss.
This feature is discussed in detail in the documentation for the UDP transport :mod:`pyuavcan.transport.udp`.


Usage
+++++

>>> import pyuavcan
>>> import pyuavcan.transport.serial
>>> tr = pyuavcan.transport.serial.SerialTransport('loop://', local_node_id=1234, baudrate=115200)
>>> tr.local_node_id
1234
>>> tr.serial_port.baudrate
115200
>>> pm = pyuavcan.transport.PayloadMetadata(0x_bad_c0ffee_0dd_f00d, 1024)
>>> ds = pyuavcan.transport.MessageDataSpecifier(12345)
>>> pub = tr.get_output_session(pyuavcan.transport.OutputSessionSpecifier(ds, None), pm)
>>> sub = tr.get_input_session(pyuavcan.transport.InputSessionSpecifier(ds, None), pm)
>>> await_ = tr.loop.run_until_complete
>>> await_(pub.send_until(pyuavcan.transport.Transfer(pyuavcan.transport.Timestamp.now(),
...                                                   pyuavcan.transport.Priority.LOW,
...                                                   1111,
...                                                   fragmented_payload=[]),
...                       tr.loop.time() + 1.0))
True
>>> await_(sub.receive_until(tr.loop.time() + 1.0))
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
to and from the other end of the link.
While such a server can be trivially coded manually by the developer,
it is possible to avoid the effort by relying on the TCP connection brokering mode available in
Ncat (which is a part of the `Nmap <https://nmap.org>`_ project, thanks Fyodor).

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

The exchange over the virtual bus can be dumped trivially for analysis::

    nc localhost 50905 > dump.bin


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
from ._stream_parser import StreamParser as StreamParser
