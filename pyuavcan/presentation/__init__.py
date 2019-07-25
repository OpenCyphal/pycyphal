#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

r"""
The presentation layer provides a high-level object-oriented interface on top of the transport layer.
This is the level of abstraction presented to the user of the library.
That is, when creating a new publisher or another network session, the calling code will interact
directly with the presentation layer.
The application layer, if used, serves as a thin proxy with some of the commonly used
high-level protocol functions implemented rather than adding any new abstraction on top.

The presentation layer uses the term *presentation layer session*, or just *session*,
to refer to an instance of publisher, subscriber, service client, or service server
for a specific subject or service (see the inheritance diagram).
The presentation layer allows the application to create multiple presentation layer session instances
concurrently that access the same underlying transport layer instance, taking care of all of the related
data management and synchronization issues automatically.
This enables minimal logical coupling between different components
of the application that have to rely on the same UAVCAN network resource.
For example, when the application creates more than one subscriber for a given subject, the presentation
layer will distribute received messages into every subscription instance requested by the application.
Likewise, different components of the application may publish messages over the same subject
or invoke the same service on the same remote server node.

The main entity of the presentation layer is the class :class:`pyuavcan.presentation.Presentation`;
the following demo shows how it can be used.
This example is based on a simple loopback transport that does not interact with the outside world at all
(it doesn't even perform any kind of IO with the OS), which makes it well-suited for demo needs.

>>> import tests; tests.dsdl.generate_packages()  # DSDL generation not shown; see the pyuavcan.dsdl docs for info.
[...]
>>> import uavcan.node, uavcan.diagnostic         # Import what we need from DSDL-generated packages.
>>> import pyuavcan.transport.loopback            # Import the demo transport implementation.
>>> transport = pyuavcan.transport.loopback.LoopbackTransport()  # Use your real transport instead.
>>> presentation = pyuavcan.presentation.Presentation(transport)

Having prepared a presentation layer controller, we can create presentation-layer sessions.
They are the main points bus access for the application. Let's start with a publisher and a subscriber:

>>> pub_record = presentation.make_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
>>> sub_record = presentation.make_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)

Publish a message and receive it also (the loopback transport just sends it back):

>>> import asyncio
>>> run_until_complete = asyncio.get_event_loop().run_until_complete
>>> record = uavcan.diagnostic.Record_1_0(
...     severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.INFO),
...     text='Neither man nor animal can be influenced by anything but suggestion.')
>>> run_until_complete(pub_record.publish(record))  # publish() returns False on timeout.
True
>>> message, metadata = run_until_complete(sub_record.receive_with_transfer())
>>> message.text.tobytes().decode()  # Calling .tobytes().decode() won't be needed when DSDL supports strings natively.
'Neither man nor animal can be influenced by anything but suggestion.'
>>> metadata.transfer_id, metadata.source_node_id, metadata.timestamp
(0, None, Timestamp(system_ns=..., monotonic_ns=...))

We can use custom subject-ID with any data type, even if there is a fixed subject-ID provided
(the background is explained in Specification, please read it). Here is an example; we also show here
that when a receive call times out, it returns None:

>>> sub_record_custom = presentation.make_subscriber(uavcan.diagnostic.Record_1_0, subject_id=12345)
>>> run_until_complete(sub_record_custom.receive_for(timeout=0.5))  # Times out and returns None.

You can see above that the node-ID of the received transfer metadata is None,
that's because it is actually an anonymous transfer, and it is so because our node is an anonymous node;
i.e., it doesn't have a node-ID.

Next we're going to create a service. Services can't be used with anonymous nodes, so let's assign a node-ID first:

>>> presentation.transport.local_node_id is None    # Yup, our node is anonymous.
True
>>> presentation.transport.set_local_node_id(1234)  # The set of valid node-ID values is transport-dependent.

Having assigned a node-ID, let's set up a service and invoke it:

>>> async def on_request(request: uavcan.node.ExecuteCommand_1_0.Request,
...                      metadata: pyuavcan.presentation.ServiceRequestMetadata) \
...         -> uavcan.node.ExecuteCommand_1_0.Response:
...     print(f'Received command {request.command} from node {metadata.client_node_id}')
...     return uavcan.node.ExecuteCommand_1_0.Response(uavcan.node.ExecuteCommand_1_0.Response.STATUS_BAD_COMMAND)
>>> srv_exec_command = presentation.get_server_with_fixed_service_id(uavcan.node.ExecuteCommand_1_0)
>>> srv_exec_command.serve_in_background(on_request)
>>> client_exec_command = presentation.make_client_with_fixed_service_id(uavcan.node.ExecuteCommand_1_0,
...                                                                      server_node_id=1234)
>>> request_object = uavcan.node.ExecuteCommand_1_0.Request(
...     uavcan.node.ExecuteCommand_1_0.Request.COMMAND_BEGIN_SOFTWARE_UPDATE,
...     '/path/to/the/firmware/image.bin')
>>> received_response = run_until_complete(client_exec_command.call(request_object))
Received command 65533 from node 1234
>>> received_response
uavcan.node.ExecuteCommand.Response.1.0(status=3)

Methods that receive data from the network return None on timeout.
For example, here we create a client for a nonexistent service; the call times out and returns None:

>>> bad_client = presentation.make_client(uavcan.node.ExecuteCommand_1_0,
...                                       service_id=234,       # There is no such service.
...                                       server_node_id=321)   # There is no such server.
>>> bad_client.response_timeout = 0.1                           # Override the default.
>>> bad_client.priority = pyuavcan.transport.Priority.HIGH      # Override the default.
>>> run_until_complete(bad_client.call(request_object))         # Times out and returns None.

Inheritance diagram for the presentation layer is shown below.
Classes named ``*Impl`` are not accessible to the user; their instances are managed automatically by the
presentation controller.

.. inheritance-diagram:: pyuavcan.presentation._session._publisher
                         pyuavcan.presentation._session._subscriber
                         pyuavcan.presentation._session._server
                         pyuavcan.presentation._session._client
                         pyuavcan.presentation._session._error
   :parts: 1
"""

from ._presentation import Presentation as Presentation

from ._session import Publisher as Publisher
from ._session import Subscriber as Subscriber
from ._session import Client as Client
from ._session import Server as Server

from ._session import SubscriberStatistics as SubscriberStatistics
from ._session import ClientStatistics as ClientStatistics
from ._session import ServerStatistics as ServerStatistics
from ._session import ServiceRequestMetadata as ServiceRequestMetadata
from ._session import ServiceRequestHandler as ServiceRequestHandler

from ._session import PresentationSession as PresentationSession
from ._session import MessageTypedSession as MessageTypedSession
from ._session import ServiceTypedSession as ServiceTypedSession

from ._session import OutgoingTransferIDCounter as OutgoingTransferIDCounter
from ._session import TypedSessionClosedError as TypedSessionClosedError
from ._session import RequestTransferIDVariabilityExhaustedError as RequestTransferIDVariabilityExhaustedError
from ._session import DEFAULT_PRIORITY as DEFAULT_PRIORITY
from ._session import DEFAULT_SERVICE_REQUEST_TIMEOUT as DEFAULT_SERVICE_REQUEST_TIMEOUT
