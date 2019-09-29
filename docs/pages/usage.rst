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

Generating data type packages from DSDL
+++++++++++++++++++++++++++++++++++++++

First, we need to make sure that the required DSDL-generated packages are available for the command-line tool.
Suppose that the application-specific data types listed above are located at ``../dsdl/namespaces/``,
and that instead of using a local copy of the public regulated data types we prefer to download it from the
repository. This is the command:

.. code-block:: sh

    uc dsdl-gen-pkg ../dsdl/namespaces/sirius_cyber_corp/ https://github.com/UAVCAN/public_regulated_data_types/archive/a532bfa7.zip

That's it. If you want to know what exactly has been done, rerun the command with ``-v`` (V for Verbose).

The DSDL packages have been stored on your computer in a directory known to the CLI tool, so now we can use them.


Configuring the transport
+++++++++++++++++++++++++

The commands shown later have to be instructed to use the same transport interface as the demo.
Please use one of the following options depending on your demo configuration:

- ``--tr="UDP('127.0.0.111/8')"`` --
  UDP/IP transport on localhost. Local node-ID 111.

- ``--tr="Serial('socket://loopback:50905',111)"`` --
  serial transport emulated over a TCP/IP tunnel instead of a real serial port (use Ncat for TCP connection brokering).
  Local node-ID 111.

- ``--tr="CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111)"`` --
  virtual CAN bus via SocketCAN (GNU/Linux systems only).
  Local node-ID 111.


Running the application
+++++++++++++++++++++++

Start the demo application shown above and leave it running.
In a new terminal, run the following commands to listen to the demo's heartbeat or its diagnostics
(don't forget to specify transport):

.. code-block:: sh

    uc sub uavcan.node.Heartbeat.1.0 --with-metadata --count=3
    uc sub uavcan.diagnostic.Record.1.0 --with-metadata

The latter may not output anything because the demo application is not doing anything interesting,
so it has nothing to report.
Keep the command running, and open a yet another terminal, whereat run this:

.. code-block:: sh

    uc call 42 123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0 '{points: [{x: 10, y: 1}, {x: 20, y: 2}]}'

Once you've executed the last command, you should see a diagnostic message being emitted in the other terminal.
Now let's publish temperature:

.. code-block:: sh

    uc pub 12345.uavcan.si.temperature.Scalar.1.0 '{kelvin: 123.456}' --count=2

You will see the demo application emit two more diagnostic messages.
