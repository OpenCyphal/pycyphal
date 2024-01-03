.. _demo:

Demo
====

This section demonstrates how to build `Cyphal <https://opencyphal.org>`_ applications using PyCyphal.
It has been tested against GNU/Linux and Windows; it is also expected to work with any other major OS.
The document is arranged as follows:

- In the first section we introduce a couple of custom data types to illustrate how they can be dealt with.

- The second section shows a simple demo node that implements a temperature controller
  and provides a custom RPC-service.

- The third section provides a hands-on illustration of the data distribution functionality of Cyphal with the help
  of Yakut --- a command-line utility for diagnostics and debugging of Cyphal networks.

- The fourth section adds a second node that simulates the plant whose temperature is controlled by the first one.

- The last section explains how to perform orchestration and configuration management of Cyphal networks.

You are expected to be familiar with terms like *Cyphal node*, *DSDL*, *subject-ID*, *RPC-service*.
If not, skim through the `Cyphal Guide <https://opencyphal.org/guide>`_ first.

If you want to follow along, :ref:`install PyCyphal <installation>` and switch to a new directory (``~/pycyphal-demo``) before continuing.


DSDL definitions
----------------

Every Cyphal application depends on the standard DSDL definitions located in the namespace ``uavcan``.
The standard namespace is part of the *regulated* namespaces maintained by the OpenCyphal project.
Grab your copy from git::

    git clone https://github.com/OpenCyphal/public_regulated_data_types

The demo relies on two vendor-specific data types located in the root namespace ``sirius_cyber_corp``.
The root namespace directory layout is as follows::

    sirius_cyber_corp/                              # root namespace directory
        PerformLinearLeastSquaresFit.1.0.dsdl       # service type definition
        PointXY.1.0.dsdl                            # nested message type definition

Type ``sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0``,
file ``sirius_cyber_corp/PerformLinearLeastSquaresFit.1.0.dsdl``:

.. literalinclude:: /../demo/custom_data_types/sirius_cyber_corp/PerformLinearLeastSquaresFit.1.0.dsdl
   :linenos:

Type ``sirius_cyber_corp.PointXY.1.0``,
file ``sirius_cyber_corp/PointXY.1.0.dsdl``:

.. literalinclude:: /../demo/custom_data_types/sirius_cyber_corp/PointXY.1.0.dsdl
   :linenos:


First node
----------

Copy-paste the source code given below into a file named ``demo_app.py``.
For the sake of clarity, move the custom DSDL root namespace directory ``sirius_cyber_corp/``
that we created above into ``custom_data_types/``.
You should end up with the following directory structure::

    pycyphal-demo/
        custom_data_types/
            sirius_cyber_corp/                          # Created in the previous section
                PerformLinearLeastSquaresFit.1.0.dsdl
                PointXY.1.0.dsdl
        public_regulated_data_types/                    # Clone from git
            uavcan/                                     # The standard DSDL namespace
                ...
            ...
        demo_app.py                                     # The thermostat node script

``CYPHAL_PATH`` should contain a list to all the paths where the DSDL root namespace directories are to be found
(be sure to modify the values to match your environment):

..  code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal-demo/custom_data_types:$HOME/pycyphal-demo/public_regulated_data_types"

Here comes ``demo_app.py``:

.. literalinclude:: /../demo/demo_app.py
   :linenos:

The following graph should give a rough visual overview of how the applications within the ``demo_app`` node
are structured:

.. graphviz::

    digraph G {
        subgraph cluster {
            label = "42:org:opencyphal.pycyphal.demo.demo_app";
            node [shape=box]

            subgraph cluster_5 {
                label = "least_squares";
                least_squares_service[label="sirius_cyber_corp.PerformLinearLeastSquaresFit_1", shape=hexagon, style=filled]
                sirius_cyber_corp_PerformLinearLeastSquaresFit_1_Request_123[label="123:sirius_cyber_corp.PerformLinearLeastSquaresFit_1.Request", style=filled]
                sirius_cyber_corp_PerformLinearLeastSquaresFit_1_Response_123[label="123:sirius_cyber_corp.PerformLinearLeastSquaresFit_1.Response", style=filled]
            }
            sirius_cyber_corp_PerformLinearLeastSquaresFit_1_Request_123 -> least_squares_service
            least_squares_service -> sirius_cyber_corp_PerformLinearLeastSquaresFit_1_Response_123

            subgraph cluster_4 {
                label = "heater_voltage";
                heater_voltage_node[label="uavcan.si.unit.voltage.Scalar_1", shape=trapezium, style=filled]
                uavcan_si_unit_voltage_Scalar[label="2347:uavcan.si.unit.voltage.Scalar", style=filled]
            }
            heater_voltage_node -> uavcan_si_unit_voltage_Scalar

            subgraph cluster_3 {
                label = "temperature_measurement";
                uavcan_si_unit_voltage_scalar_2346[label="2346:uavcan.si.unit.voltage.Scalar",style=filled]
                temperature_measurement_node[label="uavcan.si.sample.temperature.Scalar_1", shape=invtrapezium, style=filled]
            }
            uavcan_si_unit_voltage_scalar_2346 -> temperature_measurement_node

            subgraph cluster_2 {
                label = "temperature_setpoint";
                uavcan_si_sample_temperature_scalar_2345[label="2345:uavcan.si.sample.temperature.Scalar",style=filled]
                temperature_setpoint_node[label="uavcan.si.unit.temperature.Scalar_1", shape=invtrapezium, style=filled]
            }
            uavcan_si_sample_temperature_scalar_2345 -> temperature_setpoint_node

            subgraph cluster_1 {
                label = "heartbeat_publisher";
                heartbeat_publisher_node[label="uavcan.node.Hearbeat.1.0", shape=trapezium, style=filled]
                uavcan_node_heartbeat[label="uavcan.node.heartbeat",style=filled]
            }
            heartbeat_publisher_node -> uavcan_node_heartbeat

        }

    }

.. graphviz::
    :caption: Legend

      digraph G {
          node [shape=box]

          message_publisher_node[label="Message-publisher", shape=trapezium, style=filled]
          message_subscriber_node[label="Message-subscriber", shape=invtrapezium, style=filled]
          service_node[label="Service", shape=hexagon, style=filled]
          type_node[label="subject/service id:type", style=filled]

      }

If you just run the script as-is,
you will notice that it fails with an error referring to some *missing registers*.

As explained in the comments (and --- in great detail --- in the Cyphal Specification),
registers are basically named values that keep various configuration parameters of the local Cyphal node (application).
Some of these parameters are used by the business logic of the application (e.g., PID gains);
others are used by the Cyphal stack (e.g., port-IDs, node-ID, transport configuration, logging, and so on).
Registers of the latter category are all named with the same prefix ``uavcan.``,
and their names and semantics are regulated by the Specification to ensure consistency across the ecosystem.

So the application fails with an error that says that it doesn't know how to reach the Cyphal network it is supposed
to be part of because there are no registers to read that information from.
We can resolve this by passing the correct register values via environment variables:

..  code-block:: sh

    export UAVCAN__NODE__ID=42                           # Set the local node-ID 42 (anonymous by default)
    export UAVCAN__UDP__IFACE=127.0.0.1                  # Use Cyphal/UDP transport via localhost
    export UAVCAN__SUB__TEMPERATURE_SETPOINT__ID=2345    # Subject "temperature_setpoint"    on ID 2345
    export UAVCAN__SUB__TEMPERATURE_MEASUREMENT__ID=2346 # Subject "temperature_measurement" on ID 2346
    export UAVCAN__PUB__HEATER_VOLTAGE__ID=2347          # Subject "heater_voltage"          on ID 2347
    export UAVCAN__SRV__LEAST_SQUARES__ID=123            # Service "least_squares"           on ID 123
    export UAVCAN__DIAGNOSTIC__SEVERITY=2                # This is optional to enable logging via Cyphal

    python demo_app.py                                   # Run the application!

The snippet is valid for sh/bash/zsh; if you are using PowerShell on Windows, replace ``export`` with ``$env:``
and take values into double quotes.
Further snippets will not include this remark.

An environment variable ``UAVCAN__SUB__TEMPERATURE_SETPOINT__ID`` sets register ``uavcan.sub.temperature_setpoint.id``,
and so on.

..  tip::

    Specifying the environment variables manually is inconvenient.
    A better option is to store the configuration you use often into a shell file,
    and then source that when necessary into your active shell session like ``source my_env.sh``
    (this is similar to Python virtualenv).
    See Yakut user manual for practical examples.

In PyCyphal, registers are normally stored in the *register file*, in our case it's ``demo_app.db``
(the Cyphal Specification does not regulate how the registers are to be stored, this is an implementation detail).
Once you started the application with a specific configuration, it will store the values in the register file,
so the next time you can run it without passing any environment variables at all.

The registers of any Cyphal node are exposed to other network participants via the standard RPC-services
defined in the standard DSDL namespace ``uavcan.register``.
This means that other nodes on the network can reconfigure our demo application via Cyphal directly,
without the need to resort to any secondary management interfaces.
This is equally true for software nodes like our demo application and deeply embedded hardware nodes.

When you execute the commands above, you should see the script running.
Leave it running and move on to the next section.

..  tip:: Just-in-time vs. ahead-of-time DSDL compilation

    The script will transpile the required DSDL namespaces just-in-time at launch.
    While this approach works for some applications, those that are built for redistribution at large (e.g., via PyPI)
    may benefit from compiling DSDL ahead-of-time (at build time)
    and including the compilation outputs into the redistributable package.
    Ahead-of-time DSDL compilation can be trivially implemented in ``setup.py``:

    .. literalinclude:: /../demo/setup.py
       :linenos:


Poking the node using Yakut
---------------------------

The demo is running now so we can interact with it and see how it responds.
We could write another script for that using PyCyphal, but in this section we will instead use
`Yakut <https://github.com/OpenCyphal/yakut>`_ --- a simple CLI tool for diagnostics and management of Cyphal networks.
You will need to open a couple of new terminal sessions now.

If you don't have Yakut installed on your system yet, install it now by following its documentation.

Yakut (also) needs to know where the DSDL files are located, this is done via the ``CYPHAL_PATH`` environment variable:

.. code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal-demo/custom_data_types:$HOME/pycyphal-demo/public_regulated_data_types"

The commands shown later need to operate on the same network as the demo.
Earlier we configured the demo to use Cyphal/UDP via the localhost interface.
So, for Yakut, we can export this configuration to let it run on the same network anonymously:

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.0.0.1  # We don't export the node-ID, so it will remain anonymous.

To listen to the demo's heartbeat and diagnostics,
launch the following in a new terminal and leave it running (``y`` is a convenience shortcut for ``yakut``):

..  code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal-demo/custom_data_types:$HOME/pycyphal-demo/public_regulated_data_types"
    export UAVCAN__UDP__IFACE=127.0.0.1
    y sub --with-metadata uavcan.node.heartbeat uavcan.diagnostic.record    # You should see heartbeats

Now let's see how the simple thermostat node operates.
Launch another subscriber to see the published voltage command (it is not going to print anything yet):

..  code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal-demo/custom_data_types:$HOME/pycyphal-demo/public_regulated_data_types"
    export UAVCAN__UDP__IFACE=127.0.0.1
    y sub 2347:uavcan.si.unit.voltage.scalar --redraw       # Prints nothing.

And publish the setpoint along with the measurement (process variable):

..  code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal-demo/custom_data_types:$HOME/pycyphal-demo/public_regulated_data_types"
    export UAVCAN__UDP__IFACE=127.0.0.1
    export UAVCAN__NODE__ID=111         # We need a node-ID to publish messages properly
    y pub --count=10 2345:uavcan.si.unit.temperature.scalar   250 \
                     2346:uavcan.si.sample.temperature.scalar 'kelvin: 240'

You should see the voltage subscriber that we just started print something along these lines:

..  code-block:: yaml

    ---
    2347: {volt: 1.1999999284744263}
    # And so on...

Okay, the thermostat is working.
If you change the setpoint (via subject-ID 2345) or measurement (via subject-ID 2346),
you will see the published command messages (subject-ID 2347) update accordingly.

One important feature of the register interface is that it allows one to monitor internal states of the application,
which is critical for debugging.
In some way it is similar to performance counters or tracing probes:

..  code-block:: sh

    y r 42 thermostat.error     # Read register

We will see the current value of the temperature error registered by the thermostat.
If you run the last command with ``-dd`` (d for detailed), you will see the register metadata:

..  code-block:: yaml

    real64:
      value: [10.0]
    _meta_: {mutable: false, persistent: false}

``mutable: false`` says that this register cannot be modified and ``persistent: false`` says that
it is not committed to any persistent storage (like a register file).
Together they mean that the value is computed at runtime dynamically.

We can use the very same interface to query or modify the configuration parameters.
For example, we can change the PID gains of the thermostat:

..  code-block:: sh

    y r 42 thermostat.pid.gains       # read current values
    y r 42 thermostat.pid.gains 2 0 0 # write new values

Which returns ``[2.0, 0.0, 0.0]``, meaning that the new value was assigned successfully.
Observe that the register server does implicit type conversion to the type specified by the application (our script).
The Cyphal Specification does not require this behavior, though, so some simpler nodes (embedded systems in particular)
may just reject mis-typed requests.

If you restart the application now, you will see it use the updated PID gains.

Now let's try the linear regression service:

.. code-block:: sh

    # The following commands do the same thing but differ in verbosity/explicitness:
    y call 42 123:sirius_cyber_corp.PerformLinearLeastSquaresFit 'points: [{x: 10, y: 3}, {x: 20, y: 4}]'
    y q 42 least_squares '[[10, 3], [20, 4]]'

The response should look like:

..  code-block:: yaml

    123: {slope: 0.1, y_intercept: 2.0}

And the diagnostic subscriber we started in the beginning (type ``uavcan.diagnostic.Record``) should print a message.


Second node
-----------

To make this tutorial more hands-on, we are going to add another node and make it interoperate with the first one.
As the first node implements a basic thermostat, the second one simulates the plant whose temperature is
controlled by the thermostat.
Put the following into ``plant.py`` in the same directory:

.. literalinclude:: /../demo/plant.py
   :linenos:

In graph form, the new node looks as follows:

.. graphviz::

    digraph G {

        subgraph cluster {
            label = "43:org:opencyphal.pycyphal.demo.plant";
            node [shape=box]

            subgraph cluster_3 {
                label = "voltage";
                uavcan_si_unit_voltage_scalar_2347[label="2347:uavcan.si.unit.voltage.Scalar",style=filled]
                voltage_node[label="uavcan.si.sample.voltage.Scalar_1", shape=invtrapezium, style=filled]
            }
            uavcan_si_unit_voltage_scalar_2347 -> voltage_node

            subgraph cluster_2 {
                label = "temperature";
                temperature_setpoint_node[label="uavcan.si.unit.temperature.Scalar_1", shape=trapezium, style=filled]
                uavcan_si_sample_temperature_scalar_2346[label="2346:uavcan.si.sample.temperature.Scalar",style=filled]
            }
            temperature_setpoint_node -> uavcan_si_sample_temperature_scalar_2346

            subgraph cluster_1 {
                label = "heartbeat_publisher";
                heartbeat_publisher_node[label="uavcan.node.Hearbeat.1.0", shape=trapezium, style=filled]
                uavcan_node_heartbeat[label="uavcan.node.heartbeat", style=filled]
            }
            heartbeat_publisher_node -> uavcan_node_heartbeat

        }

    }

You may launch it if you want, but you will notice that tinkering with registers by way of manual configuration
gets old fast.
The next section introduces a better way.


Orchestration
-------------

..  attention::

    Yakut Orchestrator is in the alpha stage.
    Breaking changes may be introduced between minor versions until Yakut v1.0 is released.
    Freeze the minor version number to avoid unexpected changes.

    Yakut Orchestrator does not support Windows at the moment.

Manual management of environment variables and node processes may work in simple setups, but it doesn't really scale.
Practical cyber-physical systems require a better way of managing Cyphal networks that may simultaneously include
software nodes executed on the local or remote computers along with specialized bare-metal nodes running on
dedicated hardware.

One solution to this is Yakut Orchestrator --- an interpreter of a simple YAML-based domain-specific language
that allows one to define process groups and conveniently manage them as a single entity.
The language comes with a user-friendly syntax for managing Cyphal registers.
Those familiar with ROS may find it somewhat similar to *roslaunch*.

The following orchestration file (orc-file) ``launch.orc.yaml`` does this:

- Compiles two DSDL namespaces: the standard ``uavcan`` and the custom ``sirius_cyber_corp``.
  If they are already compiled, this step is skipped.

- When compilation is done, the two applications are launched.
  Be sure to stop the first script if it is still running!

- Aside from the applications, a couple of diagnostic processes are started as well.
  A setpoint publisher will command the thermostat to drive the plant to the specified temperature.

The orchestrator runs everything concurrently, but *join statements* are used to enforce sequential execution as needed.
The first process to fail (that is, exit with a non-zero code) will bring down the entire *composition*.
*Predicate* scripts ``?=`` are allowed to fail though --- this is used to implement conditional execution.

The syntax allows the developer to define regular environment variables along with register names.
The latter are translated into environment variables when starting a process.

.. literalinclude:: /../demo/launch.orc.yaml
   :linenos:
   :language: yaml

Terminate the first node before continuing since it is now managed by the orchestration script we just wrote.
Ensure that the node script files are named ``demo_app.py`` and ``plant.py``,
otherwise the orchestrator won't find them.

The orc-file can be executed as ``yakut orc launch.orc.yaml``, or simply ``./launch.orc.yaml``
(use ``--verbose`` to see which environment variables are passed to each launched process).
Having started it, you should see roughly the following output appear in the terminal,
indicating that the thermostat is driving the plant towards the setpoint:

..  code-block:: yaml

    ---
    2346:
      _meta_: {ts_system: 1651773332.157150, ts_monotonic: 3368.421244, source_node_id: 43, transfer_id: 0, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773332156343}
      kelvin: 300.0
    ---
    8184:
      _meta_: {ts_system: 1651773332.162746, ts_monotonic: 3368.426840, source_node_id: 42, transfer_id: 0, priority: optional, dtype: uavcan.diagnostic.Record.1.1}
      timestamp: {microsecond: 1651773332159267}
      severity: {value: 2}
      text: 'root: Application started with PID gains: 0.100 0.000 0.000'
    ---
    2346:
      _meta_: {ts_system: 1651773332.157150, ts_monotonic: 3368.421244, source_node_id: 43, transfer_id: 1, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773332657040}
      kelvin: 300.0
    ---
    2346:
      _meta_: {ts_system: 1651773332.657383, ts_monotonic: 3368.921476, source_node_id: 43, transfer_id: 2, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773333157512}
      kelvin: 300.0
    ---
    2346:
      _meta_: {ts_system: 1651773333.158257, ts_monotonic: 3369.422350, source_node_id: 43, transfer_id: 3, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773333657428}
      kelvin: 300.73126220703125
    ---
    2346:
      _meta_: {ts_system: 1651773333.657797, ts_monotonic: 3369.921891, source_node_id: 43, transfer_id: 4, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773334157381}
      kelvin: 301.4406433105469
    ---
    2346:
      _meta_: {ts_system: 1651773334.158120, ts_monotonic: 3370.422213, source_node_id: 43, transfer_id: 5, priority: high, dtype: uavcan.si.sample.temperature.Scalar.1.0}
      timestamp: {microsecond: 1651773334657390}
      kelvin: 302.1288757324219
    # And so on. Notice how the temperature is rising slowly towards the setpoint at 450 K!

You can run ``yakut monitor`` to see what is happening on the network.
(Don't forget to set ``UAVCAN__UDP__IFACE`` or similar depending on your transport.)

.. tip:: macOS

    Monitoring the network using ``yakut monitor``, requires using root while preserving your environment variables:

    .. code-block:: sh

        sudo -E yakut monitor

As an exercise, consider this:

- Run the same composition over CAN by changing the transport configuration registers at the top of the orc-file.
  The full set of transport-related registers is documented at :func:`pycyphal.application.make_transport`.

- Implement saturation management by publishing the ``saturation`` flag over a dedicated subject
  and subscribing to it from the thermostat node.

- Use Wireshark (capture filter expression: ``(udp or igmp) and src net 127.9.0.0/16``)
  or candump (like ``candump -decaxta any``) to inspect the network exchange.
