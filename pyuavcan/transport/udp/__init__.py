#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

r"""
UAVCAN/UDP transport overview
+++++++++++++++++++++++++++++

The UAVCAN/UDP transport is experimental and is not yet part of the UAVCAN specification.
Future revisions may break wire compatibility until the transport is formally specified.
Context: https://forum.uavcan.org/t/alternative-transport-protocols/324.

The UAVCAN/UDP transport is essentially a trivial stateless UDP blaster.
This transport is intended for low-latency, high-throughput switched Ethernet networks with complex topologies.
In the spirit of UAVCAN, it is designed to be simple and robust;
much of the data handling work is offloaded to the standard underlying UDP/IP stack.
Both IPv4 and IPv6 are supported.

This transport module contains no media sublayers because the media abstraction
is handled directly by the standard UDP/IP stack of the underlying operating system.

The UDP transport supports all transfer categories:

+--------------------+--------------------------+---------------------------+
| Supported transfers| Unicast                  | Broadcast                 |
+====================+==========================+===========================+
|**Message**         | Yes                      | Yes                       |
+--------------------+--------------------------+---------------------------+
|**Service**         | Yes                      | Banned by Specification   |
+--------------------+--------------------------+---------------------------+


Protocol definition
+++++++++++++++++++

The entirety of the session specifier (:class:`pyuavcan.transport.SessionSpecifier`)
is reified through the standard UDP/IP stack without any special extensions.
The transfer-ID, transfer priority, and the multi-frame transfer reassembly metadata are allocated in the
UAVCAN-specific UDP datagram header.

+---------------------------------------+---------------------------------------+
| Parameter                             | Manifested in                         |
+=======================================+=======================================+
| Transfer priority                     |                                       |
+---------------------------------------+ UDP datagram payload (frame header)   |
| Transfer-ID                           |                                       |
+-------------------+-------------------+---------------------------------------+
|                   | Route specifier   | IP address (least significant bits)   |
| Session specifier +-------------------+---------------------------------------+
|                   | Data specifier    | UDP destination port number           |
+-------------------+-------------------+---------------------------------------+


UDP port mapping
~~~~~~~~~~~~~~~~

The data specifier (:class:`pyuavcan.transport.DataSpecifier`)
is manifested on the wire as the destination UDP port number;
the mapping function is implemented in :func:`udp_port_from_data_specifier`.
The source port number can be arbitrary (ephemeral), its value is ignored.

UAVCAN uses a wide range of UDP ports: [15360, 24575].
UDP/IP stacks that comply with the IANA ephemeral port range recommendations are expected to be
compatible with this; otherwise, there may be port assignment conflicts.
All new versions of MS Windows starting with Vista and Server 2008 are compatible with the IANA recommendations.
Many versions of GNU/Linux, however, are not, but it can be fixed by manual reconfiguration:
https://stackoverflow.com/questions/28573390/how-to-view-and-edit-the-ephemeral-port-range-on-linux.


IP address mapping
~~~~~~~~~~~~~~~~~~

The node-ID of a node is the value of its host address (i.e., IP address with the subnet bits zeroed out);
the bits above the :attr:`NODE_ID_BIT_LENGTH`-th bit shall be zero::

    IPv4 address:   127.000.012.123/8
    Subnet mask:    255.000.000.000
    Host mask:      000.255.255.255
                    \_/ \_________/
                  subnet    host
                 address   address
                             \____/
                           node-ID=3195

    IPv6 address:   fe80:0000:0000:0000:0000:0000:0000:0c7b%enp6s0/64
    Subnet mask:    ffff:ffff:ffff:ffff:0000:0000:0000:0000
    Host mask:      0000:0000:0000:0000:ffff:ffff:ffff:ffff
                    \_________________/ \_________________/
                      subnet address        host address
                                                       \__/
                                                    node-ID=3195

An IP address that does not match the above requirement cannot be mapped to a node-ID value.
Nodes that are configured with such IP addresses are considered anonymous.
Incoming traffic from IP addresses that cannot be mapped to a valid node-ID value is rejected;
this behavior enables co-existence of UAVCAN/UDP with other UDP protocols on the same network.

The concept of anonymous transfer is not defined for UDP/IP;
in this transport, in order to be able to emit a transfer, the node shall have a valid node-ID value.
This means that an anonymous UAVCAN/UDP node can only listen to broadcast
network traffic (i.e., can subscribe to subjects) but cannot transmit anything.
If address auto-configuration is desired, lower-level solutions should be used, such as DHCP.

Both IPv4 and IPv6 are supported with minimal differences, although IPv6 is not expected to be useful in
a vehicular network because virtually none of its advantages are relevant there,
and the increased overhead is detrimental to the network's latency and throughput.
If IPv6 is used, the flow-ID of UAVCAN packets is set to zero.


Datagram header format
~~~~~~~~~~~~~~~~~~~~~~

Every UAVCAN/UDP frame contains the following header before the payload,
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
:class:`pyuavcan.transport.commons.high_overhead_transport.TransferReassembler`.


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

Removal of duplicate transfers at the opposite end of the link is natively guaranteed by the UAVCAN protocol;
no special activities are needed there (read the UAVCAN Specification for background).

For time-deterministic (real-time) networks this strategy is preferred over the conventional
confirmation-retry approach (e.g., the TCP model) because it results in more predictable
network load, lower worst-case latency, and is stateless (participants do not make assumptions
about the state of other agents involved in data exchange).


Implementation-specific details
+++++++++++++++++++++++++++++++

Applications relying on this particular transport implementation will be unable to detect a node-ID conflict on
the bus because the implementation discards all broadcast traffic originating from its own IP address.
This is a very environment-specific edge case resulting from certain peculiarities of the Berkeley socket API.
Other implementations of UAVCAN/UDP (particularly those for embedded systems) may not have this limitation.


Usage
+++++

Create two transport instances -- one with a node-ID, one anonymous:

>>> import pyuavcan
>>> import pyuavcan.transport.udp
>>> tr_0 = pyuavcan.transport.udp.UDPTransport('127.0.1.42/8')
>>> tr_0.local_node_id                                               # Derived from the IP address: (1 << 8) + 42 = 298.
298
>>> tr_1 = pyuavcan.transport.udp.UDPTransport('127.255.255.255/8')  # Anonymous, for listening purposes only.
>>> tr_1.local_node_id is None
True

Create an output and an input session:

>>> pm = pyuavcan.transport.PayloadMetadata(1024)
>>> ds = pyuavcan.transport.MessageDataSpecifier(2345)
>>> pub = tr_0.get_output_session(pyuavcan.transport.OutputSessionSpecifier(ds, None), pm)
>>> pub.socket.getpeername()   # UDP port number derived from the subject ID: 2345 + 16384 = 18729
('127.255.255.255', 18729)
>>> sub = tr_1.get_input_session(pyuavcan.transport.InputSessionSpecifier(ds, None), pm)

Send a transfer from one instance to another:

>>> await_ = tr_1.loop.run_until_complete
>>> await_(pub.send_until(pyuavcan.transport.Transfer(pyuavcan.transport.Timestamp.now(),
...                                                   pyuavcan.transport.Priority.LOW,
...                                                   1111,
...                                                   fragmented_payload=[]),
...                       tr_1.loop.time() + 1.0))
True
>>> await_(sub.receive_until(tr_1.loop.time() + 1.0))
TransferFrom(..., transfer_id=1111, ...)
>>> tr_0.close()
>>> tr_1.close()


Tooling
+++++++

Run UAVCAN networks on the local loopback interface (``127.x.y.z/8``) or create virtual interfaces for testing.

Use Wireshark for monitoring and inspection.

Use netcat for trivial monitoring; e.g., listen to a UDP port like this: ``nc -ul 48469``.

List all open UDP ports on the local machine: ``netstat -vpaun`` (GNU/Linux).


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

from ._port_mapping import udp_port_from_data_specifier as udp_port_from_data_specifier

from ._demultiplexer import UDPDemultiplexerStatistics as UDPDemultiplexerStatistics
