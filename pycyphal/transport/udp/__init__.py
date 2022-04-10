# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

r"""
Cyphal/UDP transport overview
+++++++++++++++++++++++++++++

The Cyphal/UDP transport is experimental and is not yet part of the Cyphal specification.
Future revisions may break wire compatibility until the transport is formally specified.
Context: https://forum.opencyphal.org/t/324/45?u=pavel.kirienko.

The Cyphal/UDP transport is essentially a trivial stateless UDP blaster based on IP multicasting.
This transport is intended for low-latency, high-throughput switched Ethernet networks with complex topologies.
In the spirit of Cyphal, it is designed to be simple and robust;
much of the data handling work is offloaded to the standard underlying UDP/IP stack.
Both IPv4 and IPv6 are supported by this design,
although it is expected that the advantages of IPv6 over IPv4 are less relevant in an intravehicular setting.

The concept of anonymous transfer is not defined for Cyphal/UDP;
in this transport, in order to be able to emit a transfer, the node shall have a valid node-ID value.
This means that an anonymous Cyphal/UDP node can only listen to network traffic
(i.e., can subscribe to subjects) but cannot transmit anything.
If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.

This transport module contains no media sublayers because the media abstraction
is handled directly by the standard UDP/IP stack of the underlying operating system.

Per the Cyphal transport model provided in the Cyphal specification, the following transfer categories are supported:

+--------------------+--------------------------+---------------------------+
| Supported transfers| Unicast                  | Broadcast                 |
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

+---------------------------------------+---------------------------------------------------------------------------+
| Parameter                             | Manifested in                                                             |
+=======================================+===========================================================================+
| Transfer priority                     |                                                                           |
+---------------------------------------+ UDP datagram payload (frame header)                                       |
| Transfer-ID                           |                                                                           |
+-------------------+-------------------+---------------------------------------------------------------------------+
|                   | Route specifier   | 16 least significant bits of the IP address                               |
| Session specifier +-------------------+---------------------------------------------------------------------------+
|                   | Data specifier    | For message transfers: 16 least significant bits of the                   |
|                   |                   | multicast group address.                                                  |
|                   |                   | For service transfers: UDP destination port number.                       |
+-------------------+-------------------+---------------------------------------------------------------------------+

There are two data types that model Cyphal/UDP protocol data: :class:`UDPFrame` and :class:`RawPacket`.
The latter is never used during normal operation but only during on-line capture sessions
for reporting captured packets (see :class:`UDPCaptured`).


IP address mapping
~~~~~~~~~~~~~~~~~~

The IPv4 address of a node is structured as follows::

   xxxxxxxx.xddddddd.nnnnnnnn.nnnnnnnn
   \________/\_____/ \_______________/
    (9 bits) (7 bits)     (16 bits)
     prefix  subnet-ID     node-ID

Incoming traffic from IP addresses whose 16 most significant bits are different is rejected;
this behavior enables co-existence of multiple independent Cyphal/UDP networks along with other UDP protocols
on the same network.

The *subnet-ID* is used to differentiate independent Cyphal/UDP transport networks sharing the same IP network
(e.g., multiple Cyphal/UDP networks running on localhost or on some physical network).
This is similar to the domain identifier in DDS.
This value is not used anywhere else in the protocol other than in the construction of the multicast group address,
as will be shown below.


Message transfers
~~~~~~~~~~~~~~~~~

Message transfers are executed as IP multicast transfers.
The IPv4 multicast group address is computed statically as follows::

       fixed         reserved
      (9 bits)       (3 bits)
      ________          _
     /        \        / \
     11101111.0ddddddd.000sssss.ssssssss
     \__/      \_____/    \____________/
   (4 bits)    (7 bits)      (13 bits)
     IPv4      subnet-ID     subject-ID
   multicast   \_______________________/
    prefix             (23 bits)
               collision-free multicast
                  addressing limit of
                 Ethernet MAC for IPv4

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

- The following 7 bits (the least significant bits of the second octet) are used to differentiate
  independent Cyphal/UDP networks sharing the same physical IP network.
  Since the 9 most significant bits of the node IP address are not represented in the multicast group address,
  nodes whose IP addresses differ only by the 9 MSb are not distinguished by Cyphal/UDP.
  This limitation does not appear to be significant, though, because such configurations are easy to avoid.
  It follows that there may be up to 128 independent Cyphal/UDP networks sharing the same IP subnet.

- The following 16 bits define the data specifier:

  - 3 bits reserved for future use.

  - 13 bits represent the subject-ID as-is.

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

    Node IP address:    01111111 00000010 00000000 00001000
                             127        2        0        8

    Subject-ID:                              00010 00101010

    Multicast group:    11101111 00000010 00000010 00101010
                             239        2        2       42

Example::

    Node IP address:    11000000 10101000 00000000 00000001
                             192      168        0        1

    Subject-ID:                              00010 00101010

    Multicast group:    11101111 00101000 00000010 00101010
                             239       40        2       42


Service transfers
~~~~~~~~~~~~~~~~~

Service transfers are executed as regular IP unicast transfers.

The service data specifier (:class:`pycyphal.transport.ServiceDataSpecifier`)
is manifested on the wire as the destination UDP port number;
the mapping function is implemented in :func:`udp_port_from_data_specifier`.
The source port number can be arbitrary (ephemeral), its value is ignored.

Cyphal uses a wide range of UDP ports.
UDP/IP stacks that comply with the IANA ephemeral port range recommendations are expected to be
compatible with this; otherwise, there may be port assignment conflicts.
This, however, is not a problem for any major modern OS.


Datagram header format
~~~~~~~~~~~~~~~~~~~~~~

Every Cyphal/UDP frame contains the following header before the payload,
encoded in the little-endian byte order, expressed here in the DSDL notation::

    uint8 version           # =0 in this revision; ignore frame otherwise.
    uint8 priority          # Like in CAN: 0 -- highest priority, 7 -- lowest priority.
    void16                  # Set to zero when transmitting, ignore when receiving.
    uint32 frame_index_eot  # MSB is set if the current frame is the last frame of the transfer.
    uint64 transfer_id      # The transfer-ID never overflows.
    void64                  # This space may be used later for runtime type identification.

The 31 least significant bits of the field ``frame_index_eot`` contain the frame index within the current transfer;
the most significant bit (31st) is set if the current frame is the last frame of the transfer.
Also see the documentation for :class:`UDPFrame`.

Multi-frame transfers contain four bytes of CRC32-C (Castagnoli) at the end computed over the entire transfer payload.
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

Applications relying on this particular transport implementation will be unable to detect a node-ID conflict on
the bus because the implementation discards all traffic originating from its own IP address.
This is a very environment-specific edge case resulting from certain peculiarities of the Berkeley socket API.
Other implementations of Cyphal/UDP (particularly those for embedded systems) may not have this limitation.


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
>>> tr_0 = pycyphal.transport.udp.UDPTransport('127.9.1.42')
>>> tr_0.local_node_id                                             # Derived from the IP address: (1 << 8) + 42 = 298.
298
>>> tr_1 = pycyphal.transport.udp.UDPTransport('127.9.15.254', local_node_id=None)  # Anonymous is only for listening.
>>> tr_1.local_node_id is None
True

Create an output and an input session:

>>> pm = pycyphal.transport.PayloadMetadata(1024)
>>> ds = pycyphal.transport.MessageDataSpecifier(111)
>>> pub = tr_0.get_output_session(pycyphal.transport.OutputSessionSpecifier(ds, None), pm)
>>> pub.socket.getpeername()   # UDP port is fixed, and the multicast group address is computed as shown above.
('239.9.0.111', 16383)
>>> sub = tr_1.get_input_session(pycyphal.transport.InputSessionSpecifier(ds, None), pm)

Send a transfer from one instance to the other:

>>> doctest_await(pub.send(pycyphal.transport.Transfer(pycyphal.transport.Timestamp.now(),
...                                                    pycyphal.transport.Priority.LOW,
...                                                    1111,
...                                                    fragmented_payload=[]),
...                        asyncio.get_event_loop().time() + 1.0))
True
>>> doctest_await(sub.receive(asyncio.get_event_loop().time() + 1.0))
TransferFrom(..., transfer_id=1111, ...)
>>> tr_0.close()
>>> tr_1.close()


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
                         pycyphal.transport.udp._socket_reader
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

from ._ip import IP_ADDRESS_NODE_ID_MASK as IP_ADDRESS_NODE_ID_MASK
from ._ip import SUBJECT_PORT as SUBJECT_PORT
from ._ip import node_id_to_unicast_ip as node_id_to_unicast_ip
from ._ip import unicast_ip_to_node_id as unicast_ip_to_node_id
from ._ip import message_data_specifier_to_multicast_group as message_data_specifier_to_multicast_group
from ._ip import multicast_group_to_message_data_specifier as multicast_group_to_message_data_specifier
from ._ip import service_data_specifier_to_udp_port as service_data_specifier_to_udp_port
from ._ip import udp_port_to_service_data_specifier as udp_port_to_service_data_specifier
from ._ip import LinkLayerPacket as LinkLayerPacket

from ._tracer import IPPacket as IPPacket
from ._tracer import IPv4Packet as IPv4Packet
from ._tracer import IPv6Packet as IPv6Packet
from ._tracer import UDPIPPacket as UDPIPPacket
from ._tracer import UDPCapture as UDPCapture
from ._tracer import UDPTracer as UDPTracer
from ._tracer import UDPErrorTrace as UDPErrorTrace
