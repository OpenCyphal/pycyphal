# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

# noinspection PyUnresolvedReferences
r"""
Application layer overview
++++++++++++++++++++++++++

The application module contains the application-layer API.
This module is not imported automatically because it depends on the transpiled DSDL namespace ``uavcan``.
The DSDL namespace can be either transpiled manually or lazily ad-hoc; see :mod:`pyuavcan.dsdl` for related docs.


Node class
++++++++++

The abstract class :class:`pyuavcan.application.Node` models a UAVCAN node ---
it is one of the main entities of the library, along with its factory :meth:`make_node`.
The application uses its Node instance to interact with the network:
create publications/subscriptions, invoke and serve RPC-services.


Constructing a node
^^^^^^^^^^^^^^^^^^^

..  doctest::
    :hide:

    >>> import os
    >>> os.environ["UAVCAN__NODE__ID__NATURAL16"]                   = "42"
    >>> os.environ["UAVCAN__PUB__MEASURED_VOLTAGE__ID__NATURAL16"]  = "6543"
    >>> os.environ["UAVCAN__SUB__POSITION_SETPOINT__ID__NATURAL16"] = "6544"
    >>> os.environ["UAVCAN__SRV__LEAST_SQUARES__ID__NATURAL16"]     = "123"
    >>> os.environ["UAVCAN__CLN__LEAST_SQUARES__ID__NATURAL16"]     = "123"
    >>> os.environ["UAVCAN__LOOPBACK__BIT"]                         = "1"

    >>> import asyncio
    >>> await_ = asyncio.get_event_loop().run_until_complete

Create a node using the factory :meth:`make_node` and start it:

>>> import pyuavcan.application
>>> import uavcan.node                                  # Transcompiled DSDL namespace (see pyuavcan.dsdl).
>>> node_info = pyuavcan.application.NodeInfo(          # This is an alias for uavcan.node.GetInfo.Response.
...     software_version=uavcan.node.Version_1_0(major=1, minor=0),
...     name="org.uavcan.pyuavcan.docs",
... )
>>> node = pyuavcan.application.make_node(node_info)    # Some of the fields in node_info are set automatically.
>>> node.start()

..  doctest::
    :hide:

    >>> for k in os.environ:
    ...     if "__" in k:
    ...         del os.environ[k]

The node instance we just started will periodically publish ``uavcan.node.Heartbeat`` and ``uavcan.node.port.List``,
respond to ``uavcan.node.GetInfo`` and ``uavcan.register.Access``/``uavcan.register.List``,
and do some other standard things -- read the docs for :class:`Node` for details.

Now we can create ports --- that is, instances of
:class:`pyuavcan.presentation.Publisher`,
:class:`pyuavcan.presentation.Subscriber`,
:class:`pyuavcan.presentation.Client`,
:class:`pyuavcan.presentation.Server`
--- to interact with the network.
To create a new port you need to specify its type and name
(the name can be omitted if a fixed port-ID is defined for the data type).


Publishers and subscribers
^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a publisher and publish a message:

>>> import uavcan.si.unit.voltage
>>> pub_voltage = node.make_publisher(uavcan.si.unit.voltage.Scalar_1_0, "measured_voltage")
>>> pub_voltage.publish_soon(uavcan.si.unit.voltage.Scalar_1_0(402.15))     # Publish message asynchronously.
>>> await_(pub_voltage.publish(uavcan.si.unit.voltage.Scalar_1_0(402.15)))  # Or synchronously.
True

Create a subscription and receive a message from it:

..  doctest::
    :hide:

    >>> import uavcan.si.unit.length
    >>> pub = node.presentation.make_publisher(uavcan.si.unit.length.Vector3_1_0, 6544)
    >>> pub.publish_soon(uavcan.si.unit.length.Vector3_1_0([42.0, 15.4, -8.7]))

>>> import uavcan.si.unit.length
>>> sub_position = node.make_subscriber(uavcan.si.unit.length.Vector3_1_0, "position_setpoint")
>>> msg, metadata = await_(sub_position.receive_for(timeout=0.5))
>>> msg.meter[0], msg.meter[1], msg.meter[2]                            # Some payload in the message we received.
(42.0, 15.4, -8.7)
>>> metadata.source_node_id, metadata.priority, metadata.transfer_id    # Metadata for the message.
(42, <Priority.NOMINAL: 4>, 0)


RPC-service clients and servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Define an RPC-service of an application-specific type:

>>> from sirius_cyber_corp import PerformLinearLeastSquaresFit_1_0  # An application-specific DSDL definition.
>>> async def solve_linear_least_squares(
...     request: PerformLinearLeastSquaresFit_1_0.Request,
...     metadata: pyuavcan.presentation.ServiceRequestMetadata,
... ) -> PerformLinearLeastSquaresFit_1_0.Response:                 # Business logic.
...     import numpy as np
...     x = np.array([p.x for p in request.points])
...     y = np.array([p.y for p in request.points])
...     s, *_ = np.linalg.lstsq(np.vstack([x, np.ones(len(x))]).T, y, rcond=None)
...     return PerformLinearLeastSquaresFit_1_0.Response(slope=s[0], y_intercept=s[1])
>>> srv_least_squares = node.get_server(PerformLinearLeastSquaresFit_1_0, "least_squares")
>>> srv_least_squares.serve_in_background(solve_linear_least_squares)  # Run the server in a background task.

Invoke the service we defined above assuming that it is served by node 42:

>>> from sirius_cyber_corp import PointXY_1_0
>>> cln_least_sq = node.make_client(PerformLinearLeastSquaresFit_1_0, 42, "least_squares")
>>> req = PerformLinearLeastSquaresFit_1_0.Request([PointXY_1_0(10, 1), PointXY_1_0(20, 2)])
>>> response, metadata = await_(cln_least_sq.call(req))
>>> round(response.slope, 1), round(response.y_intercept, 1)
(0.1, 0.0)

Here is another example showcasing the use of a standard service with a fixed port-ID:

>>> client_node_info = node.make_client(uavcan.node.GetInfo_1_0, 42)    # Port name is not required.
>>> response, metadata = await_(client_node_info.call(uavcan.node.GetInfo_1_0.Request()))
>>> response.software_version
uavcan.node.Version.1.0(major=1, minor=0)


Registers and application settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, you are probably wondering, how come we just created a node without specifying which transport it should use,
its node-ID, or even the subject-IDs and service-IDs?
Where did these values come from?

These values were read from from the *registers*, as defined in the UAVCAN Specification
(chapter "Application layer", section "Register interface").
Those familiar with ROS will find similarities with the *ROS Parameter Server*.

The registers are named values that keep various settings and parameters of the node.
The factory :meth:`make_node` we used above just reads the registers and figures out how to construct
the node from that: which transport to use, the node-ID, the subject-IDs, and so on.
Any UAVCAN application is also expected to keep its own configuration parameters in the registers so that
it can be reconfigured and controlled at runtime via UAVCAN.

The registry of the node instance can be accessed via :attr:`Node.registry` which is an instance of
:class:`pyuavcan.application.register.Registry`:

>>> int(node.registry["uavcan.node.id"])        # Standard registers defined by UAVCAN are named like "uavcan.*"
42
>>> node.presentation.transport.local_node_id   # Yup, indeed, the node-ID is picked up from the register.
42
>>> int(node.registry["uavcan.pub.measured_voltage.id"])    # This is where we got the subject-ID from.
6543
>>> pub_voltage.port_id
6543
>>> int(node.registry["uavcan.sub.position_setpoint.id"])   # And so on.
6544
>>> str(node.registry["uavcan.sub.position_setpoint.type"]) # Subscription type is automatically exposed via registry.
'uavcan.si.unit.length.Vector3.1.0'

Every port created by the application (publisher, subscriber, etc.) is automatically exposed via the register
interface to uphold the logic of the register-based UAVCAN introspection API.
The application therefore should not attempt to create new ports using the presentation-layer API because that
would circumvent the introspection services.

The node instance also implements the register network service (``uavcan.register.Access``, ``uavcan.register.List``)
so other network participants can access the registry of the local node and reconfigure it.

Registers can be created in two ways:
by passing defaults to :func:`make_node` (use this metod to define application-specific configs, more on this later),
and via :meth:`Node.new_register` (use this to expose volatile states) like so:

>>> from pyuavcan.application.register import Value, Real64  # Convenience aliases for uavcan.register.Value, etc.
>>> import numpy as np
>>> node.new_register("my_application.estimator.state_vector",      # Not stored, but computed at every invocation
...                   lambda: Value(real64=Real64(np.random.random((4, 1)).flatten())))
>>> node.registry["my_application.estimator.state_vector"].floats   # Some random things.
[..., ..., ..., ...]

But the above does not explain where did the example get the register values from.
There are three places:

- **The register file** which contains a simple key-value database table.
  If the file does not exist (like at the first run), it is automatically created.
  If no file location is provided when invoking :meth:`make_node`,
  the registry is stored in memory so that all state is lost when the node is closed.

- **The environment variables.**
  The mapping between register names and environment variables is documented in
  :func:`pyuavcan.application.register.parse_environment_variables`.
  When the environment variables are parsed, the values stored in the register file are automatically updated.

- **The schema definition (default values).**
  The application can pass default register values to ensure that the register file contains them and that they are
  of the correct type.
  The defaults are created before the environment variables are parsed to ensure that the registers are of the
  type defined by the application.
  Registers that already exist in the file under a wrong type are automatically converted to
  the correct type defined in the schema.
  *Do not use this feature for setting default node-ID or port-IDs.*

..  doctest::
    :hide:

    >>> import os
    >>> os.environ["UAVCAN__NODE__ID__NATURAL16"]                   = "42"
    >>> os.environ["UAVCAN__PUB__MEASURED_VOLTAGE__ID__NATURAL16"]  = "6543"
    >>> os.environ["UAVCAN__SUB__OPTIONAL_PORT__ID__NATURAL16"]     = "65535"
    >>> os.environ["UAVCAN__UDP__IP__STRING"]                       = "127.63.0.0"
    >>> os.environ["UAVCAN__SERIAL__PORT__STRING"]                  = "socket://localhost:50905"
    >>> os.environ["UAVCAN__DIAGNOSTIC__SEVERITY__REAL64"]          = "3.1"
    >>> os.environ["M__MOTOR__INDUCTANCE_DQ__REAL64"]               = "0.12 0.13"

>>> import os
>>> for k in os.environ:  # Suppose that the following environment variables were passed to our process:
...     if "__" in k:
...         print(k.ljust(47), os.environ[k])
UAVCAN__NODE__ID__NATURAL16                     42
UAVCAN__PUB__MEASURED_VOLTAGE__ID__NATURAL16    6543
UAVCAN__SUB__OPTIONAL_PORT__ID__NATURAL16       65535
UAVCAN__UDP__IP__STRING                         127.63.0.0
UAVCAN__SERIAL__PORT__STRING                    socket://localhost:50905
UAVCAN__DIAGNOSTIC__SEVERITY__REAL64            3.1
M__MOTOR__INDUCTANCE_DQ__REAL64                 0.12 0.13
>>> node = pyuavcan.application.make_node(
...     node_info,
...     "registers.db",     # The file will be created if doesn't exist.
...     {                   # Configure default logging severity and a custom register.
...         "uavcan.diagnostic.severity": Value(natural16=pyuavcan.application.register.Natural16([2])),
...         "custom.register": Value(real64=pyuavcan.application.register.Real64([1.23, -8.15])),
...     },
... )
>>> node.id
42
>>> node.presentation.transport     # Heterogeneously redundant transport: UDP+Serial, as specified in env vars.
RedundantTransport(UDPTransport('127.63.0.42', ...), SerialTransport('socket://localhost:50905', ...))
>>> pub_voltage = node.make_publisher(uavcan.si.unit.voltage.Scalar_1_0, "measured_voltage")
>>> pub_voltage.port_id
6543
>>> pub_voltage.close()
>>> list(node.registry["uavcan.diagnostic.severity"].value.natural16.value)     # Type automatically converted!
[3]
>>> node.registry["custom.register"].floats                                     # Default values.
[1.23, -8.15]
>>> node.registry["m.motor.inductance_dq"].floats                               # Application parameters.
[0.12, 0.13]
>>> node.make_subscriber(uavcan.si.unit.voltage.Scalar_1_0, "optional_port")  # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
...
MissingRegisterError: 'uavcan.sub.optional_port.id'
>>> node.close()

As mentioned above, when the schema type is changed, existing values are type-converted and
updated in the register file automatically:

>>> node = pyuavcan.application.make_node(
...     node_info,
...     "registers.db",     # The file was just created above.
...     {                   # Notice that the type is now different!
...         "custom.register": Value(integer8=pyuavcan.application.register.Integer8([99, -88])),
...     },
... )
>>> node.registry["custom.register"].floats     # The old values are used but the type is now integer8.
[1.0, -8.0]

..  doctest::
    :hide:

    >>> for k in os.environ:
    ...     if "__" in k:
    ...         del os.environ[k]
    >>> node.close()

Naturally, in order to launch a node one would need to export the required environment variables.
While this can be done trivially using a shell script or something similar,
we recommend using the UAVCAN orchestrator implemented in the Yakut command-line tool
(those familiar with ROS will find certain parallels with roslaunch).
It allows one to define UAVCAN network configuration in YAML files with first-class support for passing
registers via environment variables.


Application-layer function implementations
++++++++++++++++++++++++++++++++++++++++++

As mentioned in the description of the Node class, it provides certain bare-minumum standard application-layer
functionality like publishing heartbeats, responding to GetInfo, serving the register API, etc.
More complex capabilities are to be set up by the user as needed; some of them are:

.. autosummary::
   pyuavcan.application.diagnostic.DiagnosticSubscriber
   pyuavcan.application.diagnostic.DiagnosticPublisher
   pyuavcan.application.node_tracker.NodeTracker
   pyuavcan.application.plug_and_play.Allocatee
   pyuavcan.application.plug_and_play.Allocator
"""

from ._node import Node as Node, NodeInfo as NodeInfo

from ._node_factory import make_node as make_node

from ._transport_factory import make_transport as make_transport

from . import register as register
