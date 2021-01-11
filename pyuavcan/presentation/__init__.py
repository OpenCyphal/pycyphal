# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

# noinspection PyUnresolvedReferences
r"""
Presentation layer overview
+++++++++++++++++++++++++++

The presentation layer provides a high-level object-oriented interface on top of the transport layer.
This is the highest level of abstraction available to the user of the library.
When creating a new port instance (e.g., a publisher), the calling code will always interact
directly with the presentation layer.
The application layer functions provided on top of the presentation layer by the respective submodule are
entirely optional; it is expected that some applications will bypass the application layer entirely.

The presentation layer uses the term *port* to refer to an instance of publisher, subscriber, service client,
or service server for a specific subject or service (see the inheritance diagram below).
The presentation layer allows the application to create multiple ports that access the same underlying transport
layer instance concurrently, taking care of all related data management and synchronization issues automatically.
This minimizes the logical coupling between different components
of the application that have to rely on the same UAVCAN network resource.
For example, when the application creates more than one subscriber for a given subject, the presentation
layer will distribute received messages into every subscription instance requested by the application.
Likewise, different components of the application may publish messages over the same subject
or invoke the same service on the same remote server node.

Inheritance diagram for the presentation layer is shown below.
Classes named ``*Impl`` are not accessible to the user; their instances are managed automatically by the
presentation layer controller class.
Trivial types may be omitted from the diagram.

.. inheritance-diagram:: pyuavcan.presentation._port._publisher
                         pyuavcan.presentation._port._subscriber
                         pyuavcan.presentation._port._server
                         pyuavcan.presentation._port._client
                         pyuavcan.presentation._port._error
   :parts: 1


Usage example
+++++++++++++

The main entity of the presentation layer is the class :class:`pyuavcan.presentation.Presentation`;
the following demo shows how it can be used.
This example is based on a simple loopback transport that does not interact with the outside world
(it doesn't perform IO with the OS), which makes it well-suited for demo needs.

>>> import tests; tests.dsdl.generate_packages()  # DSDL generation not shown; see the pyuavcan.dsdl docs for info.
[...]
>>> import uavcan.node, uavcan.diagnostic         # Import what we need from DSDL-generated packages.
>>> import pyuavcan.transport.loopback            # Import the demo transport implementation.
>>> transport = pyuavcan.transport.loopback.LoopbackTransport(None)  # Use your real transport instead.
>>> presentation = pyuavcan.presentation.Presentation(transport)

Having prepared a presentation layer controller, we can create *ports*.
They are the main points of network access for the application.
Let's start with a publisher and a subscriber:

>>> pub_record = presentation.make_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_1)
>>> sub_record = presentation.make_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_1)

Publish a message and receive it also (the loopback transport just returns all outgoing transfers back):

>>> import asyncio
>>> run_until_complete = asyncio.get_event_loop().run_until_complete
>>> record = uavcan.diagnostic.Record_1_1(
...     severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.INFO),
...     text='Neither man nor animal can be influenced by anything but suggestion.')
>>> run_until_complete(pub_record.publish(record))  # publish() returns False on timeout.
True
>>> message, metadata = run_until_complete(sub_record.receive_for(timeout=0.5))
>>> message.text.tobytes().decode()  # Calling .tobytes().decode() won't be needed when DSDL supports strings natively.
'Neither man nor animal can be influenced by anything but suggestion.'
>>> metadata.transfer_id, metadata.source_node_id, metadata.timestamp
(0, None, Timestamp(system_ns=..., monotonic_ns=...))

We can use custom subject-ID with any data type, even if there is a fixed subject-ID provided
(the background is explained in Specification, please read it).
Here is an example; we also show here that when a receive call times out, it returns None:

>>> sub_record_custom = presentation.make_subscriber(uavcan.diagnostic.Record_1_1, subject_id=2345)
>>> run_until_complete(sub_record_custom.receive_for(timeout=0.5))  # Times out and returns None.

You can see above that the node-ID of the received transfer metadata is None,
that's because it is actually an anonymous transfer, and it is so because our node is an anonymous node;
i.e., it doesn't have a node-ID.

>>> presentation.transport.local_node_id is None    # Yup, it's anonymous.
True

Next we're going to create a service.
Services can't be used with anonymous nodes (which is natural -- how do you send a unicast transfer
to an anonymous node?), so we'll have to create a new transport with a node-ID of its own.

>>> transport = pyuavcan.transport.loopback.LoopbackTransport(1234)  # The range of valid values is transport-dependent.
>>> presentation = pyuavcan.presentation.Presentation(transport)  # Start anew, this time not anonymous.
>>> presentation.transport.local_node_id
1234

Generally, anonymous nodes are useful in two cases:

1. You only need to listen and you know that you are not going to emit any transfers
   (no point tinkering with node-ID if you're not going to use it anyway).

2. You need to allocate a node-ID using the plug-and-play autoconfiguration protocol.
   In this case, you would normally create a transport, run the PnP allocation procedure to obtain a node-ID value
   from the PnP allocator, and then replace your transport instance with a new one (similar to what we just did here)
   initialized with the node-ID value provided by the PnP allocator.


Having configured the node-ID, let's set up a service and invoke it:

>>> async def on_request(request: uavcan.node.ExecuteCommand_1_1.Request,
...                      metadata: pyuavcan.presentation.ServiceRequestMetadata) \
...         -> uavcan.node.ExecuteCommand_1_1.Response:
...     print(f'Received command {request.command} from node {metadata.client_node_id}')
...     return uavcan.node.ExecuteCommand_1_1.Response(uavcan.node.ExecuteCommand_1_1.Response.STATUS_BAD_COMMAND)
>>> srv_exec_command = presentation.get_server_with_fixed_service_id(uavcan.node.ExecuteCommand_1_1)
>>> srv_exec_command.serve_in_background(on_request)
>>> client_exec_command = presentation.make_client_with_fixed_service_id(uavcan.node.ExecuteCommand_1_1,
...                                                                      server_node_id=1234)
>>> request_object = uavcan.node.ExecuteCommand_1_1.Request(
...     uavcan.node.ExecuteCommand_1_1.Request.COMMAND_BEGIN_SOFTWARE_UPDATE,
...     '/path/to/the/firmware/image.bin')
>>> received_response, response_transfer = run_until_complete(client_exec_command.call(request_object))
Received command 65533 from node 1234
>>> received_response
uavcan.node.ExecuteCommand.Response.1.1(status=3)

Methods that receive data from the network return None on timeout.
For example, here we create a client for a nonexistent service; the call times out and returns None:

>>> bad_client = presentation.make_client(uavcan.node.ExecuteCommand_1_1,
...                                       service_id=234,       # There is no such service.
...                                       server_node_id=321)   # There is no such server.
>>> bad_client.response_timeout = 0.1                           # Override the default.
>>> bad_client.priority = pyuavcan.transport.Priority.HIGH      # Override the default.
>>> run_until_complete(bad_client.call(request_object))         # Times out and returns None.
"""

from ._presentation import Presentation as Presentation

from ._port import Publisher as Publisher
from ._port import Subscriber as Subscriber
from ._port import Client as Client
from ._port import Server as Server

from ._port import SubscriberStatistics as SubscriberStatistics
from ._port import ClientStatistics as ClientStatistics
from ._port import ServerStatistics as ServerStatistics
from ._port import ServiceRequestMetadata as ServiceRequestMetadata
from ._port import ServiceRequestHandler as ServiceRequestHandler

from ._port import Port as Port
from ._port import MessagePort as MessagePort
from ._port import ServicePort as ServicePort

from ._port import OutgoingTransferIDCounter as OutgoingTransferIDCounter
from ._port import PortClosedError as PortClosedError
from ._port import RequestTransferIDVariabilityExhaustedError as RequestTransferIDVariabilityExhaustedError
from ._port import DEFAULT_PRIORITY as DEFAULT_PRIORITY
from ._port import DEFAULT_SERVICE_REQUEST_TIMEOUT as DEFAULT_SERVICE_REQUEST_TIMEOUT
