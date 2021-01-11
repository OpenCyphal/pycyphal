# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
Abstract transport model
++++++++++++++++++++++++

The transport layer submodule defines a high-level interface that abstracts transport-specific implementation
details from the transport-agnostic library core.
The main component is the interface class :class:`pyuavcan.transport.Transport`
accompanied by several auxiliary entities encapsulating and modeling different aspects of the
UAVCAN protocol stack, particularly:

- :class:`pyuavcan.transport.Session`
- :class:`pyuavcan.transport.Transfer`
- :class:`pyuavcan.transport.DataSpecifier`
- :class:`pyuavcan.transport.SessionSpecifier`
- :class:`pyuavcan.transport.PayloadMetadata`
- :class:`pyuavcan.transport.Priority`

These classes are specifically designed to map well onto the UAVCAN v1 transport layer model
(first discussed in this post: https://forum.uavcan.org/t/alternative-transport-protocols/324).
The following transfer metadata taxonomy table is the essence of the model;
one can map it onto aforementioned auxiliary definitions:

+----------------------+-------------------+-------------------+---------------------------------------+
|  Transfer            |                   |                   |                                       |
|  metadata taxonomy   |     Messages      |     Services      |              Comments                 |
+======================+===================+===================+=======================================+
|                      |          Transfer priority            | Not used above the transport layer.   |
+----------+-----------+-------------------+-------------------+---------------------------------------+
|          | Route     |                   |  Source node-ID   | Transport route information. If the   |
|          | specifier |  Source node-ID   +-------------------+ destination node-ID is not provided,  |
|          |           |                   |Destination node-ID| broadcast is implied.                 |
|Session   +-----------+-------------------+-------------------+---------------------------------------+
|specifier |           |                Kind                   | Contained information: kind of        |
|          | Data      +-------------------+-------------------+ transfer (message or service);        |
|          | specifier |                   |    Service-ID     | subject-ID for messages;              |
|          |           |    Subject-ID     +---------+---------+ service-ID with request/response      |
|          |           |                   | Request |Response | role selector for services.           |
+----------+-----------+-------------------+---------+---------+---------------------------------------+
|                      |             Transfer-ID               | Transfer sequence number.             |
+----------------------+---------------------------------------+---------------------------------------+


Sessions
++++++++

PyUAVCAN transport heavily relies on the concept of *session*.
In PyUAVCAN, session represents a **flow of data through the network defined by a particular
session specifier that either originates or terminates at the local node**.
Whenever the application desires to establish communication
(such as subscribing to a subject or invoking a service),
it commands the transport layer to open a particular session.
The session abstraction is sufficiently high-level to permit efficient mapping to features
natively available to concrete transport implementations.
For example, the UAVCAN/CAN transport uses the set of active input sessions to automatically compute the
optimal hardware acceptance filter configuration;
the UAVCAN/UDP transport can map sessions onto UDP port numbers,
establishing close equivalence between sessions and Berkeley sockets.

There can be at most one session per session specifier.
When a transport is requested to provide a session, it will first check if there is one for the specifier,
and return the existing one if so; otherwise, a new session will be created, stored, and returned.
Once created, the session will remain active until explicitly closed, or until the transport instance
that owns it is closed.

An output session that doesn't have a remote node-ID specified is called a *broadcast session*;
the opposite is called a *unicast session*.

An input session that doesn't have a remote node-ID specified is called a *promiscuous session*,
meaning that it accepts transfers with matching *data specifier* from any remote node.
An input session where a remote node-ID is specified is called a *selective session*;
such a session accepts transfers from a particular remote node-ID only.
Selective sessions are useful for service transfers.

From the above description it is easy to see that a set of transfers that are valid for a given
selective session is a subset of transfers that are valid for a given promiscuous session
sharing the same data specifier.
For example, consider two sessions sharing a data specifier *D*,
one of which is promiscuous and the other is selective bound to remote node-ID *N*.
Suppose that a transfer matching the data specifier *D* is received by the local node from remote node *N*,
thereby matching both sessions.
In cases like this,
**the transport implementation is required to deliver the received transfer into both matching sessions**.
The order (whether selective or promiscuous is served first) is implementation-defined.


Sniffing/snooping and tracing
+++++++++++++++++++++++++++++

Set up live capture on a transport using :meth:`Transport.begin_capture`.
We are using the loopback transport here for demonstration but other transports follow the same interface:

>>> from pyuavcan.transport import Capture
>>> from pyuavcan.transport.loopback import LoopbackTransport
>>> captured_events = []
>>> def on_capture(cap: Capture) -> None:
...     captured_events.append(cap)
>>> tr = LoopbackTransport(None)
>>> tr.begin_capture(on_capture)

Multiple different transports can be set up to deliver capture events into the same handler since they all
share the same transport-agnostic API.
This way, heterogeneous redundant transports can write and parse a single shared log file.

Emit a random transfer and see it captured:

>>> from pyuavcan.transport import MessageDataSpecifier, PayloadMetadata, OutputSessionSpecifier, Transfer
>>> from pyuavcan.transport import Timestamp, Priority
>>> import asyncio
>>> await_ = asyncio.get_event_loop().run_until_complete
>>> ses = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(1234), None), PayloadMetadata(1024))
>>> await_(ses.send(Transfer(Timestamp.now(), Priority.LOW, 1234567890, [memoryview(b'abc')]),
...                 monotonic_deadline=asyncio.get_event_loop().time() + 1.0))
True
>>> captured_events
[LoopbackCapture(...priority=LOW, transfer_id=1234567890...)]

The captured events can be processed afterwards: logged, displayed, or reconstructed into high-level events.
The latter is done with the help of :class:`Tracer` instantiated using the static factory method
:meth:`Transport.make_tracer`:

>>> tracer = LoopbackTransport.make_tracer()
>>> tracer.update(captured_events[0])  # Captures could be read from live network or from a log file, for instance.
TransferTrace(...priority=LOW, transfer_id=1234567890...)


Implementing new transports
+++++++++++++++++++++++++++

New transports can be added trivially by subclassing :class:`pyuavcan.transport.Transport`.
This module contains several nested submodules providing standard transport implementations
according to the UAVCAN specification (e.g., the UAVCAN/CAN transport) alongside with experimental implementations.

Each specific transport implementation included in the library shall reside in its own separate
submodule under :mod:`pyuavcan.transport`.
The name of the submodule should be the lowercase name of the transport.
The name of the implementation class that inherits from :class:`pyuavcan.transport.Transport`
should begin with capitalized name of the submodule followed by ``Transport``.
If the new transport contains a media sub-layer, the media interface class should be at
``pyuavcan.transport.*.media.Media``, where the asterisk is the transport name placeholder;
the media sub-layer should follow the same organization patterns as the transport layer.
See the UAVCAN/CAN transport as an example.

Implementations included in the library are never auto-imported, nor do they need to be.
The same should be true for transport-specific media sub-layers.
The application is required to explicitly import the transport (and media sub-layer) implementations that are needed.
A highly generic, transport-agnostic application may benefit from the helper functions available in
:mod:`pyuavcan.util`, designed specifically to ease discovery and use of entities defined in submodules that
are not auto-imported and whose names are not known in advance.

Users can define their custom transports and/or media sub-layers outside of the library scope.
The library itself does not care about the location of its components.


Class inheritance diagram
+++++++++++++++++++++++++

Below is the class inheritance diagram for this module (trivial classes may be omitted):

.. inheritance-diagram:: pyuavcan.transport._transport
                         pyuavcan.transport._error
                         pyuavcan.transport._session
                         pyuavcan.transport._data_specifier
                         pyuavcan.transport._transfer
                         pyuavcan.transport._payload_metadata
                         pyuavcan.transport._tracer
   :parts: 1
"""

# Please keep the imports well-ordered because it affects the generated documentation.

# Core transport.
from ._transport import Transport as Transport
from ._transport import ProtocolParameters as ProtocolParameters
from ._transport import TransportStatistics as TransportStatistics

# Transport model auxiliaries.
from ._transfer import Transfer as Transfer
from ._transfer import TransferFrom as TransferFrom
from ._transfer import Priority as Priority

from ._data_specifier import DataSpecifier as DataSpecifier
from ._data_specifier import MessageDataSpecifier as MessageDataSpecifier
from ._data_specifier import ServiceDataSpecifier as ServiceDataSpecifier

from ._session import SessionSpecifier as SessionSpecifier
from ._session import InputSessionSpecifier as InputSessionSpecifier
from ._session import OutputSessionSpecifier as OutputSessionSpecifier
from ._session import Session as Session
from ._session import InputSession as InputSession
from ._session import OutputSession as OutputSession

from ._payload_metadata import PayloadMetadata as PayloadMetadata

# Low-level entities.
from ._session import SessionStatistics as SessionStatistics
from ._session import Feedback as Feedback

from ._timestamp import Timestamp as Timestamp

from ._transfer import FragmentedPayload as FragmentedPayload

# Exceptions.
from ._error import TransportError as TransportError
from ._error import UnsupportedSessionConfigurationError as UnsupportedSessionConfigurationError
from ._error import OperationNotDefinedForAnonymousNodeError as OperationNotDefinedForAnonymousNodeError
from ._error import InvalidTransportConfigurationError as InvalidTransportConfigurationError
from ._error import InvalidMediaConfigurationError as InvalidMediaConfigurationError
from ._error import ResourceClosedError as ResourceClosedError

# Analysis API.
from ._tracer import Capture as Capture
from ._tracer import CaptureCallback as CaptureCallback
from ._tracer import AlienSessionSpecifier as AlienSessionSpecifier
from ._tracer import AlienTransferMetadata as AlienTransferMetadata
from ._tracer import AlienTransfer as AlienTransfer
from ._tracer import Trace as Trace
from ._tracer import ErrorTrace as ErrorTrace
from ._tracer import TransferTrace as TransferTrace
from ._tracer import Tracer as Tracer

# Reusable components.
from . import commons as commons
