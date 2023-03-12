# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

r"""
Cyphal/UDP transport overview
+++++++++++++++++++++++++++++

The Cyphal/UDP transport is essentially a trivial stateless UDP blaster based on IP multicasting.
This transport is intended for low-latency, high-throughput switched Ethernet networks with complex topologies.
In the spirit of Cyphal, it is designed to be simple and robust;
much of the data handling work is offloaded to the standard underlying UDP/IP stack.
Both IPv4 and IPv6 are supported by this design,
although it is expected that the advantages of IPv6 over IPv4 are less relevant in an intravehicular setting.

Cyphal/UDP supports anonymous transfers (i.e., transfers without a source node-ID) with one limitation:
an anonymous node is only able to send Message transfers (but not Service transfers).

This transport module contains no media sublayers because the media abstraction
is handled directly by the standard UDP/IP stack of the underlying operating system.

Per the Cyphal transport model provided in the Cyphal specification, the following transfer categories are supported:

+--------------------+--------------------------+---------------------------+
| Supported transfers| Point-to-point           | Point-to-many             |
+====================+==========================+===========================+
|**Message**         | No                       | Yes                       |
+--------------------+--------------------------+---------------------------+
|**Service**         | Yes                      | Banned by Specification   |
+--------------------+--------------------------+---------------------------+


Protocol definition
+++++++++++++++++++

The entirety of the session specifier (:class:`pycyphal.transport.SessionSpecifier`)
is reified through the standard UDP/IP stack without any special extensions.
The transfer-ID, transfer priority, and the multi-frame transfer reassembly metadata are allocated in the
Cyphal-specific UDP datagram header.

There are two data types that model Cyphal/UDP protocol data: :class:`UDPFrame` and :class:`RawPacket`.
The latter is never used during normal operation but only during on-line capture sessions
for reporting captured packets (see :class:`UDPCaptured`).

Cyphal uses a single UDP port for all transfers (9382).

For more background information on how Cyphal/UDP came to be, please see the following thread in the OpenCyphal forum:
https://forum.opencyphal.org/t/1765


IP address mapping
++++++++++++++++++

Message transfers
~~~~~~~~~~~~~~~~~

Message transfers are executed as IP multicast transfers.
The IPv4 multicast group address is computed statically as follows::

            fixed            subject-ID (Message)
          (15 bits)     res. (15 bits)
       ______________   | ______________
      /              \  v/              \
      11101111.00000000.0sssssss.ssssssss
      \__/      ^     ^
    (4 bits)  Cyphal SNM
      IPv4     UDP
    multicast address
     prefix   version
                \_______________________/
                       (23 bits)
              collision-free multicast
                 addressing limit of
                Ethernet MAC for IPv4

SNM: Service, not Message

From the most significant bit to the least significant bit, the IPv4 multicast group address components are as follows:

- IPv4 multicast prefix is defined by RFC 1112.

- The following 5 bits are set to 0b11110 by this Specification. The motivation is as follows:

  - Setting the four least significant bits of the most significant byte to 0b1111 moves the address range
    into the administratively-scoped range (239.0.0.0/8, RFC 2365),
    which ensures that there may be no conflicts with well-known multicast groups.

  - Setting the most significant bit of the second octet to zero ensures that there may be no conflict
    with reserved sub-ranges within the administratively-scoped range.
    The resulting range 239.0.0.0/9 is entirely ad-hoc defined.

  - Fixing the 5+4=9 most significant bits of the multicast group address ensures that the variability
    is confined to the 23 least significant bits of the address only,
    which is desirable because the IPv4 Ethernet MAC layer does not differentiate beyond the
    23 least significant bits of the multicast group address
    (i.e., addresses that differ only in the 9 MSb collide at the MAC layer,
    which is unacceptable in a real-time system; see RFC 1112 section 6.4).
    Without this limitation, an engineer deploying a network might inadvertently create a configuration that
    causes MAC-layer collisions which may be difficult to detect.

- The next 6 bits complete the fixed part of the multicast group address, with the most significant bit
  defining the Cyphal UDP address version (this can be used in case we want to make changes to the endpoint
  mapping).

- Last but not least, the remaining 17 bits are used to encode:

  - SNM: Service, not Message (1 bit), which is used to differentiate between a Message and Service address.
    Set to zero in case of Message.

  - 1 reserved bit for future use.

  - The 15-bit subject-ID of the Message.

Per RFC 1112, the default TTL is 1, which is unacceptable.
Therefore, publishers should use the TTL value of 16 by default,
which is chosen as a sensible default suitable for any intravehicular network.

Per RFC 1112, in order to emit a multicast packet, a limited level-1 implementation without the full support of
IGMP and multicast-specific packet handling policies is sufficient.

Due to the dependency on the dynamic IGMP configuration,
a newly configured subscriber may not immediately receive data from the subject --
a brief *subscription initialization latency* may occur (typically it is well under one second).
This is because the underlying IP stack needs to inform the network switch/router about its interest in a particular
multicast group by sending an IGMP membership report first.
A high-integrity application may choose to rely on a static switch configuration,
in which case no initialization delay will take place.

Example::

    Fixed prefix:       11101111 0000000x xxxxxxxx xxxxxxxx

    Service,    :       xxxxxxxx xxxxxxx0 xxxxxxxx xxxxxxxx
    not Message

    Reserved:           xxxxxxxx xxxxxxxx 0xxxxxxx xxxxxxxx

    Subject-ID (=42):   xxxxxxxx xxxxxxxx x0000000 00101010

    Multicast group:    11101111 00000000 00000000 00101010
                             239        0        0       42


Service transfers
~~~~~~~~~~~~~~~~~

Service transfers are also executed as IP multicast transfers.
The IPv4 multicast group address is computed statically as follows::

            fixed
          (15 bits)
       ______________
      /              \
      11101111.00000001.ssssssss.ssssssss
      \__/      ^     ^ \_______________/
    (4 bits)  Cyphal SNM    (16 bits)
      IPv4     UDP          destination node-ID
    multicast address
     prefix   version
                \_______________________/
                       (23 bits)
              collision-free multicast
                 addressing limit of
                Ethernet MAC for IPv4

Service transfers are distinguished from message transfers by the least significant bit of the second octet.
The 2 last octets define the destination node-ID of the service transfer.

Example::

    Fixed prefix:       11101111 0000000x xxxxxxxx xxxxxxxx

    Service,    :       xxxxxxxx xxxxxxx1 xxxxxxxx xxxxxxxx
    not Message

    Reserved:           xxxxxxxx xxxxxxxx 0xxxxxxx xxxxxxxx

    Subject-ID (=42):   xxxxxxxx xxxxxxxx x0000000 00101010

    Multicast group:    11101111 00000000 00000000 00101010
                             239        1        0       42

Datagram header format
~~~~~~~~~~~~~~~~~~~~~~

Every Cyphal/UDP frame contains the following header before the payload,
encoded in the little-endian byte order, expressed here in the DSDL notation::

    uint8 version           # =1 in this revision; ignore frame otherwise.
    uint8 priority          # Like in CAN: 0 -- highest priority, 7 -- lowest priority.
    uint16 source_node_id   # Cyphal node-ID of the origin.
    uint32 frame_index_eot  # MSB is set if the current frame is the last frame of the transfer.
    uint64 transfer_id      # The transfer-ID never overflows.
    void64                  # This space may be used later for runtime type identification.

    uint4 version                   # <- 1
    void4
    uint3 priority                  # Duplicates QoS for ease of access; 0 -- highest, 7 -- lowest.
    void5
    uint16 source_node_id
    uint16 destination_node_id
    uint16 data_specifier           # Like in Cyphal/serial: subject-ID | (service-ID + RNR (Request, Not Response))
    uint64 transfer_id
    uint31 frame_index              # Index of the current frame within the current transfer.
    bool end_of_transfer
    uint16 user_data
    # Opaque application-specific data with user-defined semantics. Generic implementations should ignore
    uint16 header_crc
    @assert _offset_ / 8 == {24}    # Fixed-size 24-byte header with natural alignment for each field ensured.
    @sealed

In the case of a Message frame, the ``data_specifier`` field contains the subject-ID of the message
(15 least significant bits) and the remaining most significant bit represents SNM.

In the case of a Service frame, the ``data_specifier`` field contains the service-ID of the service
(14 least significant bits) and the remaining two most significant bits represent RNR and SNM
(second and most significant bits respectively).

Also see the documentation for :class:`UDPFrame`.

Please note: in addition to ``header_crc``, multi-frame transfers contain four bytes of CRC32-C (Castagnoli)
at the end of the payload computed over the entire transfer payload (payload_crc).
For more info on multi-frame transfers, please see
:class:`pycyphal.transport.commons.high_overhead_transport.TransferReassembler`.

Unreliable networks and temporal redundancy
+++++++++++++++++++++++++++++++++++++++++++

For unreliable networks, deterministic data loss mitigation is supported.
This measure is only available for service transfers, not for message transfers due to their different semantics.
If the probability of a frame loss exceeds the desired reliability threshold,
the transport can be configured to repeat every outgoing service transfer a specified number of times,
on the assumption that the probability of losing any given frame is uncorrelated (or weakly correlated)
with that of its neighbors.

Assuming that the probability of transfer loss ``P`` is time-invariant,
the influence of the multiplier ``M`` can be approximated as ``P' = P^M``.
For example, given a network that successfully delivers 99% of transfers,
and the probabilities of adjacent transfer loss are uncorrelated,
the multiplication factor of 2 can increase the link reliability up to ``100% - (100% - 99%)^2 = 99.99%``.

The duplicates are emitted immediately following the original transfer.
For example, suppose that a service transfer contains three frames, F0 to F2,
and the service transfer multiplication factor is two,
then the resulting frame sequence would be as follows::

    F0      F1      F2      F0      F1      F2
    \_______________/       \_______________/
       main copy             redundant copy
     (TX timestamp)      (never TX-timestamped)

    ------------------ time ------------------>

As shown on the diagram, if the transmission timestamping is requested, only the first copy is timestamped.
Further, any errors occurring during the transmission of redundant copies
may be silently ignored by the stack, provided that the main copy is transmitted successfully.

The resulting behavior in the provided example is that the transport network may
lose up to three unique frames without affecting the application.
In the following example, the frames F0 and F2 of the main copy are lost, but the transfer survives::

    F0 F1 F2 F0 F1 F2
    |  |  |  |  |  |
    x  |  x  |  |  \_____ F2 __________________________
       |     |  \________ F1 (redundant, discarded) x  \
       |     \___________ F0 ________________________  |
       \_________________ F1 ______________________  \ |
                                                   \ | |
    ----- time ----->                              v v v
                                                reassembled
                                                multi-frame
                                                 transfer

Removal of duplicate transfers at the opposite end of the link is natively guaranteed by the Cyphal protocol;
no special activities are needed there (read the Cyphal Specification for background).

For time-deterministic (real-time) networks this strategy is preferred over the conventional
confirmation-retry approach (e.g., the TCP model) because it results in more predictable
network load, lower worst-case latency, and is stateless (participants do not make assumptions
about the state of other agents involved in data exchange).


Implementation-specific details
+++++++++++++++++++++++++++++++

Usage
+++++

..  doctest::
    :hide:

    >>> import tests
    >>> tests.asyncio_allow_event_loop_access_from_top_level()
    >>> from tests import doctest_await

Create two transport instances -- one with a node-ID, one anonymous:

>>> import asyncio
>>> import pycyphal
>>> import pycyphal.transport.udp
>>> tr_0 = pycyphal.transport.udp.UDPTransport(local_ip_address='127.0.0.1', local_node_id=10)
>>> tr_0.local_ip_address
IPv4Address('127.0.0.1')
>>> tr_0.local_node_id
10
>>> tr_1 = pycyphal.transport.udp.UDPTransport(local_ip_address='127.0.0.1', local_node_id=None) # Anonymous is only for listening.
>>> tr_1.local_node_id is None
True

Create an output and an input session:

>>> pm = pycyphal.transport.PayloadMetadata(1024)
>>> ds = pycyphal.transport.MessageDataSpecifier(42)
>>> pub = tr_0.get_output_session(pycyphal.transport.OutputSessionSpecifier(ds, None), pm)
>>> pub.socket.getpeername()   # UDP port is fixed, and the multicast group address is computed as shown above.
('239.0.0.42', 9382)
>>> sub = tr_1.get_input_session(pycyphal.transport.InputSessionSpecifier(ds, None), pm)

Send a transfer from one instance to the other:

>>> doctest_await(pub.send(pycyphal.transport.Transfer(pycyphal.transport.Timestamp.now(),
...                                                    pycyphal.transport.Priority.LOW,
...                                                    transfer_id=1111,
...                                                    fragmented_payload=[]),
...                        asyncio.get_event_loop().time() + 1.0))
True
>>> doctest_await(sub.receive(asyncio.get_event_loop().time() + 1.0))
TransferFrom(..., transfer_id=1111, ...)
>>> tr_0.close()
>>> tr_1.close()

TODO Add Service example


Tooling
+++++++

Run Cyphal networks on the local loopback interface (``127.x.y.z/8``) or create virtual interfaces for testing.

Use Wireshark for monitoring and inspection.

Use netcat for trivial monitoring; e.g., listen to a UDP port like this: ``nc -ul 48469``.

List all open UDP ports on the local machine: ``netstat -vpaun`` (GNU/Linux).


Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pycyphal.transport.udp._udp
                         pycyphal.transport.udp._frame
                         pycyphal.transport.udp._session._input
                         pycyphal.transport.udp._session._output
                         pycyphal.transport.udp._tracer
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

from ._ip import message_data_specifier_to_multicast_group as message_data_specifier_to_multicast_group
from ._ip import service_node_id_to_multicast_group as service_node_id_to_multicast_group
from ._ip import LinkLayerPacket as LinkLayerPacket

from ._tracer import IPPacket as IPPacket
from ._tracer import IPv4Packet as IPv4Packet
from ._tracer import IPv6Packet as IPv6Packet
from ._tracer import UDPIPPacket as UDPIPPacket
from ._tracer import UDPCapture as UDPCapture
from ._tracer import UDPTracer as UDPTracer
from ._tracer import UDPErrorTrace as UDPErrorTrace
