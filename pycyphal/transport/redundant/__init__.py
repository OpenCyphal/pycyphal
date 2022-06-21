# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

"""
Redundant pseudo-transport overview
+++++++++++++++++++++++++++++++++++

Native support for redundant transports is one of the core features of Cyphal.
The class :class:`RedundantTransport` implements this feature within PyCyphal.
It works by aggregating zero or more instances of :class:`pycyphal.transport.Transport`
into a *composite* that implements the redundant transport management logic as defined in the Cyphal specification:

- Every outgoing transfer is replicated into all of the available redundant interfaces.
- Incoming transfers are deduplicated so that the local node receives at most one copy of each unique transfer
  received from the bus.

There exist two approaches to implementing transport-layer redundancy.
The differences are confined to the specifics of a particular implementation, they are not manifested on the bus
-- nodes exhibit identical behavior regardless of the chosen strategy:

- **Frame-level redundancy.**
  In this case, multiple redundant interfaces are managed by the same transport state machine.
  This strategy is more efficient in the sense of computing power and memory resources required to
  accommodate a given amount of networking workload compared to the alternative.
  Its limitation is that the redundant transports shall implement the same protocol (e.g., CAN),
  and all involved transports shall be configured to use the same MTU.

- **Transfer-level redundancy.**
  In this case, redundant interfaces are managed one level of abstraction higher:
  not at the level of separate *transport frames*, but at the level of complete *Cyphal transfers*
  (if these terms sound unfamiliar, please read the Cyphal specification).
  This approach complicates the data flow inside the library, but it supports *dissimilar transport redundancy*,
  allowing one to aggregate transports implementing different protocols (e.g., UDP with serial,
  possibly with different MTU).
  Dissimilar redundancy is often sought in high-reliability/safety-critical applications,
  as reviewed in https://forum.opencyphal.org/t/557.

In accordance with its design goals, PyCyphal implements the transfer-level redundancy management strategy
since it offers greater flexibility and a wider set of available design options.
It is expected though that real-time embedded applications may often find frame-level redundancy preferable.

This implementation uses the term *inferior* to refer to a member of a redundant group:

- *Inferior transport* is a transport that belongs to a redundant transport group.
- *Inferior session* is a transport session that is owned by an inferior transport.

Whenever a redundant transport is requested to construct a new session,
it does so by initializing an instance of :class:`RedundantInputSession` or :class:`RedundantOutputSession`.
The constructed instance then holds a set of inferior sessions, one from each inferior transport,
all sharing the same session specifier (:class:`pycyphal.transport.SessionSpecifier`).
The resulting relationship between inferior transports and inferior sessions can be conceptualized
as a matrix where columns represent inferior transports and rows represent sessions:

+-----------+---------------+---------------+---------------+---------------+
|           |  Transport 0  |  Transport 1  |      ...      |  Transport M  |
+===========+===============+===============+===============+===============+
| Session 0 |     S0T0      |     S0T1      |      ...      |     S0Tm      |
+-----------+---------------+---------------+---------------+---------------+
| Session 1 |     S1T0      |     S1T1      |      ...      |     S1Tm      |
+-----------+---------------+---------------+---------------+---------------+
|    ...    |      ...      |      ...      |      ...      |      ...      |
+-----------+---------------+---------------+---------------+---------------+
| Session N |     SnT0      |     SnT1      |      ...      |     SnTm      |
+-----------+---------------+---------------+---------------+---------------+

Attachment/detachment of a transport is modeled as an addition/removal of a column;
likewise, construction/retirement of a session is modeled as an addition/removal of a row.
While the construction of a row or a column is in progress, the matrix resides in an inconsistent state.
If any error occurs in the process, the matrix is rolled back to the previous consistent state,
and the already-constructed sessions of the new vector are retired.

Existing redundant sessions retain validity across any changes in the matrix configuration.
Logic that relies on a redundant instance is completely shielded from any changes in the underlying transport
configuration, meaning that the entire underlying transport structure may be swapped out with a completely
different one without affecting the higher levels.
A practical extreme case is where a redundant transport is constructed with zero inferior transports,
its session instances are configured, and the inferior transports are added later.
This is expected to be useful for long-running applications that have to retain the presentation-level structure
across changes in the transport configuration done on-the-fly without stopping the application.

Since the redundant transport itself also implements the interface :class:`pycyphal.transport.Transport`,
it technically could be used as an inferior of another redundant transport instance,
although the practicality of such arrangement is questionable.
Attaching a redundant transport as an inferior of itself is expressly prohibited and results in an error.


Inferior aggregation restrictions
+++++++++++++++++++++++++++++++++

Transports are categorized into one of the following two categories by the value of their transfer-ID (TID) modulo
(i.e., the transfer-ID overflow period).

Transports where the set of transfer-ID values contains less than 2**48 (``0x_1_0000_0000_0000``)
distinct elements are said to have *cyclic transfer-ID*.
In such transports, the value of the transfer-ID increases steadily starting from zero,
incremented once per emitted transfer, until the highest value is reached,
then the value is wrapped over to zero::

    modulo
         /|   /|   /|
        / |  / |  / |
       /  | /  | /  | /
      /   |/   |/   |/
    0 ----------------->
            time


Transports where the set of transfer-ID values is larger are said to have *monotonic transfer-ID*.
In such transports, the set is considered to be large enough to be inexhaustible for any practical application,
hence a wrap-over to zero is expected to never occur.
(For example, a Cyphal/UDP transport operating over a 10 GbE link at the theoretical throughput limit of
14.9 million transfers per second will exhaust the set in approx. 153 years in the worst case.)

Monotonic transports impose a higher data overhead per frame due to the requirement to accommodate a
sufficiently wide integer field for the transfer-ID value.
Their advantage is that transfer-ID values carried over inferior transports of a redundant group are guaranteed
to remain in-phase for the entire lifetime of the network.
The importance of this guarantee can be demonstrated with the following counter-example of two transports
leveraging different transfer-ID modulo for the same session,
where the unambiguous mapping between their transfer-ID values is lost
with the beginning of the epoch B1 after the first overflow::

    A0    A1    A2    A3
        /|    /|    /|
       / |   / |   / |   /
      /  |  /  |  /  |  /
     /   | /   | /   | /
    /    |/    |/    |/

    B0   B1   B2   B3   B4
       /|   /|   /|   /|
      / |  / |  / |  / |
     /  | /  | /  | /  | /
    /   |/   |/   |/   |/
    ---------------------->
             time

The phase ambiguity of cyclic-TID transports results in the following hard requirements:

1. Inferior transports under the same redundant transport instance shall belong to the same TID monotonicity category:
   either all cyclic or all monotonic.
2. In the case where the inferiors utilize cyclic TID counters, the TID modulo shall be identical for all inferiors.

The implementation raises an error if an attempt is made to violate any of the above requirements.
The TID monotonicity category of an inferior is determined by querying
:attr:`pycyphal.transport.Transport.protocol_parameters`.


Transmission
++++++++++++

As stated in the Specification, every emitted transfer shall be replicated into all available redundant interfaces.
The rest of the logic does not concern wire compatibility, and hence it is implementation-defined.

This implementation applies an optimistic result aggregation policy where it considers a transmission successful
if at least one inferior was able to successfully complete it.
The handling of time-outs, exceptions, and other edge cases is described in detail in the documentation for
:class:`RedundantOutputSession`.

Every outgoing transfer will be serialized and transmitted by each inferior independently from each other.
This may result in different number of transport frames emitted if the inferiors are configured to use
different MTU, or if they implement different transport protocols.

Inferiors compute the modulus of the transfer-ID according to the protocol they implement
independently from each other;
however, despite the independent computation, it is guaranteed that they will always arrive at the same
final transfer-ID value thanks to the aggregation restrictions introduced earlier.
This guarantee is paramount for service calls, because Cyphal requires the caller to match a service response
with the appropriate request state by comparing its transfer-ID value,
which in turn requires that the logic that performs such matching is aware about the transfer-ID modulo in use.


Reception
+++++++++

Received transfers need to be deduplicated (dereplicated) so that the higher layers of the protocol stack
would not receive each unique transfer more than once (as demanded by the Specification).

Transfer reception and deduplication are managed by the class :class:`RedundantInputSession`.
There exist two deduplication strategies, chosen automatically depending on the TID monotonicity category
of the inferiors
(as described earlier, it is enforced that all inferiors in a redundant group belong to the same
TID monotonicity category).

The cyclic-TID deduplication strategy picks a transport interface at random and stays with it as long as
the interface keeps delivering transfers.
If the currently used interface ceases to deliver transfers, the strategy may switch to another one,
thus manifesting the automatic fail-over.
The cyclic-TID strategy cannot utilize more than one interface simultaneously due to the risk of
transfer duplication induced by a possible transport latency disbalance
(this is discussed at https://github.com/OpenCyphal/specification/issues/8 and in the Specification).

The monotonic-TID deduplication strategy always picks the first transfer to arrive.
This approach provides instant fail-over in the case of an interface failure and
ensures that the worst case transfer latency is bounded by the latency of the best-performing transport.

The following two swim lane diagrams should illustrate the difference.
First, the case of cyclic-TID::

    A   B     Deduplicated
    |   |     |
    T0  |     T0     <-- First transfer received from transport A.
    T1  T0    T1     <-- Transport B is auto-assigned as a back-up.
    T2  T1    T2     <-- Up to this point the transport functions normally.
    X   T2    |      <-- Transport A fails here.
        T3    |      <-- Valid transfers from transport B are ignored due to the mandatory fail-over delay.
        ...   |
        Tn    Tn     <-- After the delay, the deduplicator switches over to the back-up transport.
        Tn+1  Tn+1   <-- Now, the roles of the back-up transport and the main transport are swapped.
        Tn+2  Tn+2

Monotonic-TID::

    A   B     Deduplicated
    |   |     |
    T0  |     T0    <-- The monotonic-TID strategy always picks the first transfer to arrive.
    T1  T0    T1    <-- All available interfaces are always considered.
    T2  T1    T2    <-- The result is that the transfer latency is defined by the best-performing transport.
    |   T2    |     <-- Here, the latency of transport A has increased temporarily.
    |   T3    T3    <-- The deduplication strategy reacts by picking the next transfer from transport B.
    T3  X     |     <-- Shall one transport fail, the deduplication strategy fails over immediately.
    T4        T4

Anonymous transfers are a special case:
a deduplicator has to keep local state per session in order to perform its functions;
since anonymous transfers are fundamentally stateless, they are always accepted unconditionally.
The implication is that redundant transfers may be replicated.
This behavior is due to the design of the protocol and is not specific to this implementation.


Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pycyphal.transport.redundant._redundant_transport
                         pycyphal.transport.redundant._error
                         pycyphal.transport.redundant._session._base
                         pycyphal.transport.redundant._session._input
                         pycyphal.transport.redundant._session._output
   :parts: 1


Usage
+++++

..  doctest::
    :hide:

    >>> import tests
    >>> tests.asyncio_allow_event_loop_access_from_top_level()
    >>> from tests import doctest_await

A freshly constructed redundant transport is empty.
Redundant transport instances are intentionally designed to be very mutable,
allowing one to reconfigure them freely on-the-fly to support the needs of highly dynamic applications.
Such flexibility allows one to do things that are illegal per the Cyphal specification,
such as changing the node-ID while the node is running, so beware.

>>> tr = RedundantTransport()
>>> tr.inferiors  # By default, there are none.
[]

It is possible to begin creating session instances immediately, before configuring the inferiors.
Any future changes will update all dependent session instances automatically.

>>> from pycyphal.transport import OutputSessionSpecifier, InputSessionSpecifier, MessageDataSpecifier
>>> from pycyphal.transport import PayloadMetadata, Transfer, Timestamp, Priority, ProtocolParameters
>>> pm = PayloadMetadata(1024)
>>> s0 = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), pm)
>>> s0.inferiors    # No inferior transports; hence, no inferior sessions.
[]

If we attempted to transmit or receive a transfer while there are no inferiors, the call would just time out.

In this example, we will be experimenting with the loopback transport.
Below we are attaching a new inferior transport instance; the session instances are updated automatically.

>>> from pycyphal.transport.loopback import LoopbackTransport
>>> lo_0 = LoopbackTransport(local_node_id=42)
>>> tr.attach_inferior(lo_0)
>>> tr.inferiors
[LoopbackTransport(...)]
>>> s0.inferiors
[LoopbackOutputSession(...)]

Add another inferior and another session:

>>> lo_1 = LoopbackTransport(local_node_id=42)
>>> tr.attach_inferior(lo_1)
>>> s1 = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), pm)
>>> len(tr.inferiors)
2
>>> len(s0.inferiors)  # Updated automatically.
2
>>> len(s1.inferiors)
2
>>> assert tr.inferiors[0].output_sessions[0] is s0.inferiors[0]    # Navigating the session matrix.
>>> assert tr.inferiors[1].output_sessions[0] is s0.inferiors[1]
>>> assert tr.inferiors[0].input_sessions[0] is s1.inferiors[0]
>>> assert tr.inferiors[1].input_sessions[0] is s1.inferiors[1]

A simple exchange test (remember this is a loopback, so we get back whatever we send):

>>> import asyncio
>>> doctest_await(s0.send(Transfer(Timestamp.now(), Priority.LOW, 1111, fragmented_payload=[]),
...                       asyncio.get_event_loop().time() + 1.0))
True
>>> doctest_await(s1.receive(asyncio.get_event_loop().time() + 1.0))
RedundantTransferFrom(..., transfer_id=1111, fragmented_payload=[], ...)

Inject a failure into one inferior.
The redundant transport will continue to function with the other inferior; an error message will be logged:

.. The 'doctest: +SKIP' is needed because PyTest is broken. If a failure is actually injected,
.. the transport will be logging errors, which in turn break the PyTest's doctest plugin.
.. This is a known bug which is documented here: https://github.com/pytest-dev/pytest/issues/5908.
.. When that is fixed (I suppose it should be by PyTest v6?), please, remove this comment and the 'doctest: +SKIP'.

>>> lo_0.output_sessions[0].exception = RuntimeError('Injected failure')  # doctest: +SKIP
>>> doctest_await(s0.send(Transfer(Timestamp.now(), Priority.LOW, 1112, fragmented_payload=[]),
...                       asyncio.get_event_loop().time() + 1.0))
True
>>> doctest_await(s1.receive(asyncio.get_event_loop().time() + 1.0))   # Still works.
RedundantTransferFrom(..., transfer_id=1112, fragmented_payload=[], ...)

Inferiors that are no longer needed can be detached.
The redundant transport cleans up after itself by closing all inferior sessions in the detached transport.

>>> tr.detach_inferior(lo_0)
>>> len(tr.inferiors)   # Yup, removed.
1
>>> len(s0.inferiors)   # And the existing session instances are updated.
1
>>> len(s1.inferiors)   # Indeed they are.
1

One cannot mix inferiors with incompatible TID monotonicity or different node-ID.
For example, it is not possible to use CAN with UDP in the same redundant group.

>>> lo_0 = LoopbackTransport(local_node_id=42)
>>> lo_0.protocol_parameters = ProtocolParameters(transfer_id_modulo=32, max_nodes=128, mtu=8)
>>> tr.attach_inferior(lo_0)                        # TID monotonicity mismatch.    #doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
InconsistentInferiorConfigurationError: The new inferior shall use monotonic transfer-ID counters...
>>> tr.attach_inferior(LoopbackTransport(local_node_id=None))  # Node-ID mismatch.  #doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
InconsistentInferiorConfigurationError: The inferior has a different node-ID...

The parameters of a redundant transport are computed from the inferiors.
If the inferior set is changed, the transport parameters may also be changed.
This may create unexpected complications because parameters of real transports are generally immutable,
so it is best to avoid unnecessary runtime transformations unless required by the business logic.

>>> tr.local_node_id
42
>>> tr.protocol_parameters
ProtocolParameters(...)
>>> tr.close()                  # All inferiors and all sessions are closed.
>>> tr.inferiors
[]
>>> tr.local_node_id is None
True
>>> tr.protocol_parameters
ProtocolParameters(transfer_id_modulo=0, max_nodes=0, mtu=0)

..  doctest::
    :hide:

    >>> doctest_await(asyncio.sleep(1.0))  # Let pending tasks terminate before the loop is closed.

A redundant transport can be used with just one inferior to implement ad-hoc PnP allocation as follows:
the transport is set up with an anonymous inferior which is disposed of upon completing the allocation procedure;
the new inferior is then installed in the place of the old one configured to use the newly allocated node-ID value.
"""

from ._redundant_transport import RedundantTransport as RedundantTransport
from ._redundant_transport import RedundantTransportStatistics as RedundantTransportStatistics

from ._session import RedundantSession as RedundantSession
from ._session import RedundantInputSession as RedundantInputSession
from ._session import RedundantOutputSession as RedundantOutputSession

from ._session import RedundantSessionStatistics as RedundantSessionStatistics
from ._session import RedundantFeedback as RedundantFeedback

from ._error import InconsistentInferiorConfigurationError as InconsistentInferiorConfigurationError

from ._tracer import RedundantCapture as RedundantCapture
from ._tracer import RedundantDuplicateTransferTrace as RedundantDuplicateTransferTrace
from ._tracer import RedundantTracer as RedundantTracer
