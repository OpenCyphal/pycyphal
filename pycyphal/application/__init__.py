# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# noinspection PyUnresolvedReferences
r"""
Application layer overview
++++++++++++++++++++++++++

The application module contains the application-layer API.
This module is not imported automatically because it depends on the transpiled DSDL namespace ``uavcan``.
The DSDL namespace can be either transpiled manually or lazily ad-hoc; see :mod:`pycyphal.dsdl` for related docs.


Node class
++++++++++

The abstract class :class:`pycyphal.application.Node` models a Cyphal node ---
it is one of the main entities of the library, along with its factory :meth:`make_node`.
The application uses its Node instance to interact with the network:
create publications/subscriptions, invoke and serve RPC-services.


Constructing a node
^^^^^^^^^^^^^^^^^^^

..  doctest::
    :hide:

    >>> import os
    >>> os.environ["UAVCAN__NODE__ID"]                   = "42"
    >>> os.environ["UAVCAN__PUB__MEASURED_VOLTAGE__ID"]  = "6543"
    >>> os.environ["UAVCAN__SUB__POSITION_SETPOINT__ID"] = "6544"
    >>> os.environ["UAVCAN__SRV__LEAST_SQUARES__ID"]     = "123"
    >>> os.environ["UAVCAN__CLN__LEAST_SQUARES__ID"]     = "123"
    >>> os.environ["UAVCAN__LOOPBACK"]                   = "1"
    >>> import tests
    >>> tests.asyncio_allow_event_loop_access_from_top_level()
    >>> from tests import doctest_await

Create a node using the factory :meth:`make_node` and start it:

>>> import pycyphal.application
>>> import uavcan.node                                  # Transcompiled DSDL namespace (see pycyphal.dsdl).
>>> node_info = pycyphal.application.NodeInfo(          # This is an alias for uavcan.node.GetInfo.Response.
...     software_version=uavcan.node.Version_1(major=1, minor=0),
...     name="org.uavcan.pycyphal.docs",
... )
>>> node = pycyphal.application.make_node(node_info)    # Some of the fields in node_info are set automatically.
>>> node.start()

The node instance we just started will periodically publish ``uavcan.node.Heartbeat`` and ``uavcan.node.port.List``,
respond to ``uavcan.node.GetInfo`` and ``uavcan.register.Access``/``uavcan.register.List``,
and do some other standard things -- read the docs for :class:`Node` for details.

Now we can create ports --- that is, instances of
:class:`pycyphal.presentation.Publisher`,
:class:`pycyphal.presentation.Subscriber`,
:class:`pycyphal.presentation.Client`,
:class:`pycyphal.presentation.Server`
--- to interact with the network.
To create a new port you need to specify its type and name
(the name can be omitted if a fixed port-ID is defined for the data type).


Publishers and subscribers
^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a publisher and publish a message (here and below, ``doctest_await`` substitutes for the ``await`` statement):

>>> import uavcan.si.unit.voltage
>>> pub_voltage = node.make_publisher(uavcan.si.unit.voltage.Scalar_1, "measured_voltage")
>>> pub_voltage.publish_soon(uavcan.si.unit.voltage.Scalar_1(402.15))            # Publish message asynchronously.
>>> doctest_await(pub_voltage.publish(uavcan.si.unit.voltage.Scalar_1(402.15)))  # Or synchronously.
True

Create a subscription and receive a message from it:

..  doctest::
    :hide:

    >>> import uavcan.si.unit.length
    >>> pub = node.presentation.make_publisher(uavcan.si.unit.length.Vector3_1, 6544)
    >>> pub.publish_soon(uavcan.si.unit.length.Vector3_1([42.0, 15.4, -8.7]))

>>> import uavcan.si.unit.length
>>> sub_position = node.make_subscriber(uavcan.si.unit.length.Vector3_1, "position_setpoint")
>>> msg = doctest_await(sub_position.get(timeout=0.5))              # None if timed out.
>>> msg.meter[0], msg.meter[1], msg.meter[2]                        # Some payload in the message we received.
(42.0, 15.4, -8.7)


RPC-service clients and servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Define an RPC-service of an application-specific type:

>>> from sirius_cyber_corp import PerformLinearLeastSquaresFit_1    # An application-specific DSDL definition.
>>> async def solve_linear_least_squares(                           # Refer to the Demo chapter for the DSDL sources.
...     request: PerformLinearLeastSquaresFit_1.Request,
...     metadata: pycyphal.presentation.ServiceRequestMetadata,
... ) -> PerformLinearLeastSquaresFit_1.Response:                   # Business logic.
...     import numpy as np
...     x = np.array([p.x for p in request.points])
...     y = np.array([p.y for p in request.points])
...     s, *_ = np.linalg.lstsq(np.vstack([x, np.ones(len(x))]).T, y, rcond=None)
...     return PerformLinearLeastSquaresFit_1.Response(slope=s[0], y_intercept=s[1])
>>> srv_least_squares = node.get_server(PerformLinearLeastSquaresFit_1, "least_squares")
>>> srv_least_squares.serve_in_background(solve_linear_least_squares)  # Run the server in a background task.

Invoke the service we defined above assuming that it is served by node 42:

>>> from sirius_cyber_corp import PointXY_1
>>> cln_least_sq = node.make_client(PerformLinearLeastSquaresFit_1, 42, "least_squares")
>>> req = PerformLinearLeastSquaresFit_1.Request([PointXY_1(10, 1), PointXY_1(20, 2)])
>>> response = doctest_await(cln_least_sq(req))                         # None if timed out.
>>> round(response.slope, 1), round(response.y_intercept, 1)
(0.1, 0.0)

Here is another example showcasing the use of a standard service with a fixed port-ID:

>>> client_node_info = node.make_client(uavcan.node.GetInfo_1, 42)    # Port name is not required.
>>> response = doctest_await(client_node_info(uavcan.node.GetInfo_1.Request()))
>>> response.software_version
uavcan.node.Version.1.0(major=1, minor=0)


Registers and application settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You are probably wondering, how come we just created a node without specifying which transport it should use,
its node-ID, or even the subject-IDs and service-IDs?
Where did these values come from?

They were read from from the *registry* --- a key-value configuration parameter storage [#parameter_server]_
defined in the Cyphal Specification, chapter *Application layer*, section *Register interface*.
The factory :meth:`make_node` we used above just reads the registers and figures out how to construct
the node from that: which transport to use, the node-ID, the subject-IDs, and so on.
Any Cyphal application is also expected to keep its own configuration parameters in the registers so that
it can be reconfigured and controlled at runtime via Cyphal.

The registry of the local node can be accessed via :attr:`Node.registry` which is an instance of class
:class:`pycyphal.application.register.Registry`:

>>> int(node.registry["uavcan.node.id"])        # Standard registers defined by Cyphal are named like "uavcan.*"
42
>>> node.id                                     # Yup, indeed, the node-ID is picked up from the register.
42
>>> int(node.registry["uavcan.pub.measured_voltage.id"])    # This is where we got the subject-ID from.
6543
>>> pub_voltage.port_id
6543
>>> int(node.registry["uavcan.sub.position_setpoint.id"])   # And so on.
6544
>>> str(node.registry["uavcan.sub.position_setpoint.type"]) # Port types are automatically exposed via registry, too.
'uavcan.si.unit.length.Vector3.1.0'

Every port created by the application (publisher, subscriber, etc.) is automatically exposed via the register
interface as prescribed by the Specification [#avoid_presentation_layer]_.

New registers (application-specific registers in particular) can be created using
:meth:`pycyphal.application.register.Registry.setdefault`:

>>> from pycyphal.application.register import Value, Real64  # Convenience aliases for uavcan.register.Value, etc.
>>> gains = node.registry.setdefault("my_app.controller.pid_gains", Real64([1.3, 0.8, 0.05]))   # Explicit real64 here.
>>> gains.floats
[1.3, 0.8, 0.05]
>>> import numpy as np
>>> node.registry.setdefault("my_app.estimator.state_vector",       # Not stored, but computed at every invocation.
...                          lambda: np.random.random(4)).floats    # Deduced type: real64.
[..., ..., ..., ...]

But the above does not explain where did the example get the register values from.
There are two places:

- **The register file** which contains a simple key-value database table.
  If the file does not exist (like at the first run), it is automatically created.
  If no file location is provided when invoking :meth:`make_node`,
  the registry is stored in memory so that all state is lost when the node is closed.

- **The environment variables.**
  A register like ``m.motor.inductance_dq`` can be assigned via environment variable ``M__MOTOR__INDUCTANCE_DQ``
  (the mapping is documented in the standard RPC-service ``uavcan.register.Access``).
  The value of an environment variable is a space-separated list of values (in case of arrays), or a plain string.
  The environment variables are checked once when the node is constructed, and also whenever a new register is
  created using :meth:`pycyphal.application.register.Registry.setdefault`.

..  doctest::
    :hide:

    >>> node.close()
    >>> import os
    >>> for k in os.environ:
    ...     if "__" in k:
    ...         del os.environ[k]
    >>> os.environ["UAVCAN__NODE__ID"]                   = "42"
    >>> os.environ["UAVCAN__PUB__MEASURED_VOLTAGE__ID"]  = "6543"
    >>> os.environ["UAVCAN__SUB__OPTIONAL_PORT__ID"]     = "65535"
    >>> os.environ["UAVCAN__UDP__IFACE"]                 = "127.63.0.0"
    >>> os.environ["UAVCAN__SERIAL__IFACE"]              = "socket://127.0.0.1:50905"
    >>> os.environ["UAVCAN__DIAGNOSTIC__SEVERITY"]       = "3.1"
    >>> os.environ["M__MOTOR__INDUCTANCE_DQ"]            = "0.12 0.13"

>>> import os
>>> for k in os.environ:  # Suppose that the following environment variables were passed to our process:
...     if "__" in k:
...         print(k.ljust(40), os.environ[k])
UAVCAN__NODE__ID                         42
UAVCAN__PUB__MEASURED_VOLTAGE__ID        6543
UAVCAN__SUB__OPTIONAL_PORT__ID           65535
UAVCAN__UDP__IFACE                       127.63.0.0
UAVCAN__SERIAL__IFACE                    socket://127.0.0.1:50905
UAVCAN__DIAGNOSTIC__SEVERITY             3.1
M__MOTOR__INDUCTANCE_DQ                  0.12 0.13
>>> node = pycyphal.application.make_node(node_info, "registers.db")  # The file will be created if doesn't exist.
>>> node.id
42
>>> node.presentation.transport     # Heterogeneously redundant transport: UDP+Serial, as specified in env vars.
RedundantTransport(UDPTransport('127.63.0.42', ...), SerialTransport('socket://127.0.0.1:50905', ...))
>>> pub_voltage = node.make_publisher(uavcan.si.unit.voltage.Scalar_1, "measured_voltage")
>>> pub_voltage.port_id
6543
>>> int(node.registry["uavcan.diagnostic.severity"])                            # This is a standard register.
3
>>> node.registry.setdefault("m.motor.inductance_dq", [1.23, -8.15]).floats     # The value is taken from environment!
[0.12, 0.13]
>>> node.registry.setdefault("m.motor.flux_linkage_dq", [1.23, -8.15]).floats   # No environment variable for this one.
[1.23, -8.15]
>>> node.registry["m.motor.inductance_dq"] = [1.9, 6]                           # Assign new value.
>>> node.registry["m.motor.inductance_dq"].floats
[1.9, 6.0]
>>> node.make_subscriber(uavcan.si.unit.voltage.Scalar_1, "optional_port")      # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
...
PortNotConfiguredError: 'uavcan.sub.optional_port.id'
>>> node.close()

..  doctest::
    :hide:

    >>> for k in os.environ:
    ...     if "__" in k:
    ...         del os.environ[k]
    >>> node.close()        # Ensure idempotency.

Per the Specification, a port-ID of 65535 (0xFFFF) represents an unconfigured port,
as illustrated in the above snippet.


Application-layer function implementations
++++++++++++++++++++++++++++++++++++++++++

As mentioned in the description of the Node class, it provides certain bare-minumum standard application-layer
functionality like publishing heartbeats, responding to GetInfo, serving the register API, etc.
More complex capabilities are to be set up by the user as needed; some of them are:

..  autosummary::
    pycyphal.application.diagnostic.DiagnosticSubscriber
    pycyphal.application.node_tracker.NodeTracker
    pycyphal.application.plug_and_play.Allocatee
    pycyphal.application.plug_and_play.Allocator
    pycyphal.application.file.FileServer
    pycyphal.application.file.FileClient


..  [#parameter_server]
    Those familiar with ROS may find similarities with the *ROS Parameter Server*,
    except that each node keeps its own registers locally instead of relying on a remote centralized provider.

..  [#avoid_presentation_layer]
    The application therefore should not attempt to create new ports using the presentation-layer API because that
    would circumvent the introspection services.
"""

from ._node import Node as Node, NodeInfo as NodeInfo, PortNotConfiguredError as PortNotConfiguredError

from ._node_factory import make_node as make_node

from ._transport_factory import make_transport as make_transport

from ._registry_factory import make_registry as make_registry

from . import register as register


class NetworkTimeoutError(TimeoutError):
    """
    API calls below the application layer return None on timeout.
    Some of the application-layer API calls raise this exception instead.
    """
