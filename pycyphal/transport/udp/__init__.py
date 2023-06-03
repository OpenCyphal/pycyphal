# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

r"""
Cyphal/UDP transport overview
+++++++++++++++++++++++++++++

Please refer to the appropriate section of the `Cyphal Specification <https://opencyphal.org/specification>`_
for the definition of the Cyphal/UDP transport.

This transport module contains no media sublayers because the media abstraction
is handled directly by the standard UDP/IP stack of the underlying operating system.


Forward error correction (FEC)
++++++++++++++++++++++++++++++

For unreliable networks, optional forward error correction (FEC) is supported by this implementation.
This measure is only available for service transfers, not for message transfers due to their different semantics.
If the probability of a frame loss exceeds the desired reliability threshold,
the transport can be configured to repeat every outgoing service transfer a specified number of times,
on the assumption that the probability of losing any given frame is uncorrelated (or weakly correlated)
with that of its neighbors.
Assuming that the probability of transfer loss ``P`` is time-invariant,
the influence of the FEC multiplier ``M`` can be approximated as ``P' = P^M``.

Duplicates are emitted immediately following the original transfer.
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
no special activities are needed there (refer to the Cyphal Specification for background).

For time-deterministic (real-time) networks this strategy is preferred over the conventional
confirmation-retry approach (e.g., the TCP model) because it results in more predictable
network load, lower worst-case latency, and is stateless (participants do not make assumptions
about the state of other agents involved in data exchange).


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
>>> tr_1 = pycyphal.transport.udp.UDPTransport(local_ip_address='127.0.0.1',
...                                            local_node_id=None) # Anonymous is only for listening.
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


Tooling
+++++++

Run Cyphal networks on the local loopback interface (``127.0.0.1``) or create virtual interfaces for testing.

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
