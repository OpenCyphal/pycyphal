.. _demo:

Demo
====

It is recommended to at least skim through the *UAVCAN Guide* beforehand.
See `uavcan.org <https://uavcan.org>`_ for details.

This demo has been tested against GNU/Linux and Windows; it is also expected to work with any other major OS.


Custom data types
-----------------

The demo relies on two vendor-specific data types located in the root namespace ``sirius_cyber_corp``.
The root namespace directory layout is as follows::

    sirius_cyber_corp/                              <-- root namespace directory
        PerformLinearLeastSquaresFit.1.0.uavcan     <-- service type definition
        PointXY.1.0.uavcan                          <-- nested message type definition

The referenced DSDL definitions are provided below.

``sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0``:

.. literalinclude:: /../demo/custom_data_types/sirius_cyber_corp/PerformLinearLeastSquaresFit.1.0.uavcan
   :linenos:

``sirius_cyber_corp.PointXY.1.0``:

.. literalinclude:: /../demo/custom_data_types/sirius_cyber_corp/PointXY.1.0.uavcan
   :linenos:


Application source code
-----------------------

The demo relies on the custom data types presented above.
To run it, copy-paste its source code into a file on your computer and update the DSDL paths to match your environment.
The public regulated data types can be cloned from https://github.com/UAVCAN/public_regulated_data_types .

.. literalinclude:: /../demo/demo_app.py
   :linenos:


Just-in-time vs. ahead-of-time DSDL compilation
+++++++++++++++++++++++++++++++++++++++++++++++

The demo application will transpile the required DSDL namespaces just-in-time at launch.
While this approach works for some applications, those that are built for redistribution at large (e.g., via PyPI)
may benefit from compiling DSDL ahead-of-time (at build time)
and including the compilation outputs into the redistributable package.

Ahead-of-time DSDL compilation can be trivially implemented in ``setup.py``:

.. literalinclude:: /../demo/setup.py
   :linenos:


Running the application
-----------------------

If you just run the demo application as-is,
you will notice that it fails with an error referring to some *missing registers*.

As explained in the comments (and --- in great detail --- in the UAVCAN Specification),
registers are basically named values that keep various configuration parameters of the local UAVCAN node (application).
Some of these parameters are used by the business logic of the application (e.g., PID gains);
others are used by the UAVCAN stack (e.g., port-IDs, node-ID, transport configuration, logging, and so on).
Registers of the latter category are all named with the same prefix ``uavcan.``,
and their names and semantics are regulated by the Specification to ensure consistency across the ecosystem.

So the application fails with an error that says that it doesn't know how to reach the UAVCAN network it is supposed
to be part of because there are no registers to read that information from.
We can resolve this by passing the correct register values via environment variables:

..  code-block:: sh

    export UAVCAN__NODE__ID=42                           # Set the local node-ID 42 (anonymous by default)
    export UAVCAN__UDP__IFACE=127.9.0.0                  # Use UAVCAN/UDP transport via 127.9.0.42 (sic!)
    export UAVCAN__SUB__TEMPERATURE_SETPOINT__ID=2345    # Subject "temperature_setpoint"    on ID 2345
    export UAVCAN__SUB__TEMPERATURE_MEASUREMENT__ID=2346 # Subject "temperature_measurement" on ID 2346
    export UAVCAN__PUB__HEATER_VOLTAGE__ID=2347          # Subject "heater_voltage"          on ID 2347
    export UAVCAN__SRV__LEAST_SQUARES__ID=123            # Service "least_squares"           on ID 123
    export UAVCAN__DIAGNOSTIC__SEVERITY=2                # This is optional to enable logging via UAVCAN

    python demo_app.py                                   # Run the application!

The snippet is valid for sh/bash/zsh; if you are using PowerShell on Windows, replace ``export`` with ``$env:``.
Further snippets will not include this remark.

An environment variable ``UAVCAN__SUB__TEMPERATURE_SETPOINT__ID`` sets register ``uavcan.sub.temperature_setpoint.id``,
and so on.

In PyUAVCAN, registers are normally stored in the *register file*, in our case it's ``my_registers.db``
(the UAVCAN Specification does not regulate how the registers are to be stored, this is an implementation detail).
Once you started the application with a specific configuration, it will store the values in the register file,
so the next time you can run it without passing any environment variables at all.

The registers of any UAVCAN node are exposed to other network participants via the standard RPC-services
defined in the standard DSDL namespace ``uavcan.register``.
This means that other nodes on the network can reconfigure our demo application via UAVCAN directly,
without the need to resort to any secondary management interfaces.
This is equally true for software nodes like our demo application and deeply embedded hardware nodes.


Poking the application using Yakut
----------------------------------

The demo is running now so we can interact with it and see how it responds.
We could write another script for that using PyUAVCAN, but in this section we will instead use
`Yakut <https://github.com/UAVCAN/yakut>`_ --- a simple CLI tool for diagnostics and management of UAVCAN networks.


How to use Yakut
++++++++++++++++

If you don't have Yakut installed on your system yet, install it now by following its documentation.

Yakut requires us to compile our DSDL namespaces beforehand using ``yakut compile`` (update paths as necessary):

.. code-block:: sh

    yakut compile  custom_data_types/sirius_cyber_corp  public_regulated_data_types/uavcan

The outputs will be stored in the current working directory.
If you decided to change the working directory or move the compilation outputs,
make sure to export the ``YAKUT_PATH`` environment variable pointing to the correct location.

The commands shown later need to operate on the same network as the demo.
Earlier we configured the demo to use UAVCAN/UDP via 127.9.0.42.
So, for Yakut, we can export this configuration to let it run on the same network anonymously:

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.9.0.0  # We don't export the node-ID, so it will remain anonymous.


Interacting with the application
++++++++++++++++++++++++++++++++

To listen to the demo's heartbeat and diagnostics, run the following commands in new terminals:

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.9.0.0
    yakut sub uavcan.node.Heartbeat.1.0     # You should see heartbeats being printed continuously.

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.9.0.0
    yakut sub uavcan.diagnostic.Record.1.1  # This one will not show anything yet -- read on.

Now we can actually see how the simple thermostat node is operating.
Add another subscriber to see the published voltage command:

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.9.0.0
    yakut sub -M 2347:uavcan.si.unit.voltage.Scalar.1.0

And publish the setpoint along with measurement (process variable):

..  code-block:: sh

    export UAVCAN__UDP__IFACE=127.9.0.0
    export UAVCAN__NODE__ID=111         # We need a node-ID to publish messages
    yakut pub --count 10 2345:uavcan.si.unit.temperature.Scalar.1.0   'kelvin: 250' \
                         2346:uavcan.si.sample.temperature.Scalar.1.0 'kelvin: 240'

You should see the voltage subscriber (subject-ID 2347) print something along these lines:

..  code-block:: yaml

    ---
    2347:
      volt: 1.1999999284744263

    # And so on...

Okay, the thermostat is working.
If you change the setpoint (subject-ID 2345) or measurement (subject-ID 2346),
you will see the published command messages (subject-ID 2347) update accordingly.

One important feature of the register interface is that it allows one to monitor internal states of the application,
which is critical for debugging.
In some way it is similar to performance counters or tracing probes:

..  code-block:: sh

    yakut call 42 uavcan.register.Access.1.0 'name: {name: thermostat.error}'

We will see the current value of the temperature error registered by the thermostat:

..  code-block:: yaml

    ---
    384:
      timestamp:
        microsecond: 0
      mutable: false
      persistent: false
      value:
        real32:
          value:
          - 10.0

Field ``mutable: false`` says that this register cannot be modified and ``persistent: false`` says that
it is not committed to any persistent storage (like a register file).
Together they mean that the value is computed at runtime dynamically.

We can use the very same interface to query or modify the configuration parameters.
For example, we can change the PID gains of the thermostat:

..  code-block:: sh

    yakut call 42 uavcan.register.Access.1.0 '{name: {name: thermostat.pid.gains}, value: {integer8: {value: [2, 0, 0]}}}'

Which results in:

..  code-block:: yaml

    ---
    384:
      timestamp:
        microsecond: 0
      mutable: true
      persistent: true
      value:
        real64:
          value:
          - 2.0
          - 0.0
          - 0.0

An attentive reader would notice that the assigned value was of type ``integer8``, whereas the result is ``real64``.
This is because the register server does implicit type conversion to the type specified by the application.
The UAVCAN Specification does not require this behavior, though, so some simpler nodes (embedded systems in particular)
may just reject mis-typed requests.

If you restart the application now, you will see it use the updated PID gains.

Now let's try the linear regression service:

.. code-block:: sh

    yakut call 42 123:sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0 'points: [{x: 10, y: 3}, {x: 20, y: 4}]'

The response should look like:

..  code-block:: yaml

    ---
    123:
      slope: 0.1
      y_intercept: 2.0

And the diagnostic subscriber we started in the beginning should print a log record.


Orchestration
-------------

..  attention::

    Yakut Orchestrator is in the alpha stage.
    Breaking changes may be introduced between minor versions until Yakut v1.0 is released.
    Freeze the minor version number to avoid unexpected changes.

    Yakut Orchestrator does not support Windows at the moment.

Manual management of environment variables and node processes may work in simple setups, but it doesn't really scale.
Practical cyber-physical systems require a better way of managing UAVCAN networks that may simultaneously include
software nodes executed on the local or remote computers along with specialized bare-metal nodes running on
dedicated hardware.

One solution to this is Yakut Orchestrator --- an interpreter of a simple YAML-based domain-specific language
that allows one to define process groups and conveniently manage them as a single entity.
The language comes with a user-friendly syntax for managing UAVCAN registers.
Those familiar with ROS may find it somewhat similar to *roslaunch*.


Second application
++++++++++++++++++

To make this section more hands-on, we are going to add another application and make it interoperate with the first one.
As the first application implements a basic thermostat, the second one simulates the plant whose temperature is
controlled by the thermostat.

.. literalinclude:: /../demo/plant.py
   :linenos:


Writing the orc-file
++++++++++++++++++++

The following orchestration file (orc-file) ``launch.orc.yaml`` does this:

- Compiles two DSDL namespaces: the standard ``uavcan`` and the custom ``sirius_cyber_corp``.
  If they are already compiled, this step is skipped.

- When compilation is done, the two applications are launched.

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

The orc-file can be executed as ``yakut orc launch.orc.yaml``, or simply ``./launch.orc.yaml``
(use ``--verbose`` to see which environment variables are passed to each launched process).
Having started it, you should see roughly the following output appear in the terminal,
indicating that the thermostat is driving the plant towards the setpoint:

..  code-block:: yaml

    ---
    8184:
      _metadata_:
        timestamp:
          system: 1614489567.052270
          monotonic: 4864.397568
        priority: optional
        transfer_id: 0
        source_node_id: 42
      timestamp:
        microsecond: 1614489567047461
      severity:
        value: 2
      text: 'root: Application started with PID gains: 0.100 0.000 0.000'

    {"2346":{"timestamp":{"microsecond":1614489568025004},"kelvin":300.0}}
    {"2346":{"timestamp":{"microsecond":1614489568524508},"kelvin":300.7312622070312}}
    {"2346":{"timestamp":{"microsecond":1614489569024634},"kelvin":301.4406433105469}}
    {"2346":{"timestamp":{"microsecond":1614489569526189},"kelvin":302.1288757324219}}

    # And so on. Notice how the temperature is rising slowly towards the setpoint at 450 K!

As an exercise, consider this:

- Run the same composition over CAN by changing the transport configuration registers at the top of the orc-file.
  The full set of transport-related registers is documented at :func:`pyuavcan.application.make_transport`.

- Implement saturation management by publishing the ``saturation`` flag over a dedicated subject
  and subscribing to it from the thermostat node.

- Use Wireshark (capture filter expression: ``(udp or igmp) and src net 127.9.0.0/16``)
  or candump (like ``candump -decaxta any``) to inspect the network exchange.
