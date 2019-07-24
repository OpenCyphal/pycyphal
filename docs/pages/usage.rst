.. _basic_usage:

Basic usage demo
================

Custom data types
-----------------

The following application-specific data types are used in the demo.

``sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0``:

.. literalinclude:: /../tests/dsdl/namespaces/sirius_cyber_corp/PerformLinearLeastSquaresFit.1.0.uavcan
   :linenos:


``sirius_cyber_corp.PointXY.1.0``:

.. literalinclude:: /../tests/dsdl/namespaces/sirius_cyber_corp/PointXY.1.0.uavcan
   :linenos:


Source code
-----------

The source code relies on the custom data types presented above.
In order to run the demo, please copy-paste its source code into a file on your computer
and update the DSDL paths to match your environment.

.. literalinclude:: /../tests/demo/basic_usage.py
   :linenos:


Evaluating the demo using the command-line tool
-----------------------------------------------

First, we need to make sure that the required DSDL-generated packages are available for the command-line tool.
Suppose that the application-specific data types listed above are located at ``../dsdl/namespaces/``,
and that instead of using a local copy of the public regulated data types we prefer to download it from the
repository. This is the command:

.. code-block:: sh

    uc dsdl-gen-pkg ../dsdl/namespaces/sirius_cyber_corp/ https://github.com/UAVCAN/public_regulated_data_types/archive/a532bfa7.zip

That's it. If you want to know what exactly has been done, rerun the command with ``-v`` (V for Verbose).

The DSDL packages have been stored on your computer in a directory known to the CLI tool, so now we can use them.
Start the demo application shown above and leave it running.
In a new terminal, run the following commands to listen to the demo's heartbeat or its diagnostics:

.. code-block:: sh

    uc sub uavcan.node.Heartbeat.1.0 --with-metadata --socketcan=vcan0 --count=3
    uc sub uavcan.diagnostic.Record.1.0 --with-metadata --socketcan=vcan0

The latter may not output anything because the demo application is not doing anything interesting,
so it has nothing to report. Keep the command running, and open a yet another terminal, whereat run this:

.. code-block:: sh

    uc call 42 123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0 '{points: [{x: 10, y: 1}, {x: 20, y: 2}]}' --local-node-id=11 --socketcan=vcan0,8

Observe that we have specified the interface as ``vcan0,8``. The number eight is important as it tells the CAN
transport that it should use CAN 2.0 (where the maximum transmission unit is eight bytes per its specification).
Use of CAN 2.0 is in turn important because our demo application is configured to use that protocol;
per SocketCAN logic, an application that is configured to use CAN 2.0 can't receive CAN FD frames.
This is just an implementation detail that is not really related to UAVCAN,
but it is to the SocketCAN stack we're using in this example.

Once you've executed the last command, you should see a diagnostic message being emitted in the other terminal.
Now let's publish temperature:

.. code-block:: sh

    uc pub 12345.uavcan.si.temperature.Scalar.1.0 '{kelvin: 123.456}' --count=2 --socketcan=vcan0,8

You will see the demo application emit two more diagnostic messages.
