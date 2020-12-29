.. _demo_app:

Demo application
================

The reader is assumed to have at least skimmed through *The UAVCAN Guide* beforehand.
See `uavcan.org <https://uavcan.org>`_ for details.

This demo has been tested against GNU/Linux and Windows; it is also expected to work with any major OS.


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


Source code
-----------

The demo relies on the custom data types presented above.
In order to run the demo, please copy-paste its source code into a file on your computer
and update the DSDL paths to match your environment.

.. literalinclude:: /../demo/demo_app.py
   :linenos:


Evaluating the demo using Yakut command-line tool
-------------------------------------------------

`Yakut <https://github.com/UAVCAN/yakut>`_ is a simple CLI tool for diagnostics and management of UAVCAN networks
built on PyUAVCAN.
Please refer to Yakut docs to see how to get it running on your system.


Compiling DSDL
++++++++++++++

We need to compile DSDL namespaces before using them with Yakut.
Suppose that the application-specific data types listed above are located under ``custom_data_types``,
and the public regulated data types are under ``public_regulated_data_types``.
This is the command:

.. code-block:: sh

    yakut compile  custom_data_types/sirius_cyber_corp  public_regulated_data_types/uavcan

Outputs are stored in the current working directory, so now we can use them.
If you decided to change the working directory or move the compilation outputs,
make sure to update the ``YAKUT_PATH`` environment variable.

This command is actually a thin wrapper over the `Nunavut DSDL transpiler <https://github.com/UAVCAN/nunavut>`_.

If you want to know what exactly has been done, rerun the command with ``-v`` (V for Verbose).
As always, use ``--help`` to get the full usage information.


Configuring the transport
+++++++++++++++++++++++++

The commands shown later have to be instructed to use the same transport interface as the demo.
In this example we configure the transport using the environment variable ``YAKUT_TRANSPORT``,
but it is also possible to use the ``--transport`` command line argument if found more convenient
(the syntax is identical).

Use one of the following initialization expressions depending on your demo configuration:

- ``"UDP('127.0.0.111')"`` -- UDP/IP on loopback. Local node-ID 111.

- ``"Serial('socket://loopback:50905',111)"`` --
  UAVCAN/serial emulated over a TCP/IP tunnel instead of a real serial port (use Ncat for TCP connection brokering).
  Local node-ID 111.

- ``"CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111)"`` --
  virtual CAN bus via SocketCAN (GNU/Linux systems only). Local node-ID 111.

Redundant transports can be configured by specifying multiple comma-separated expressions:

- ``"UDP('127.0.0.111'), Serial('socket://loopback:50905',111)"`` --
  dissimilar double redundancy, UDP plus serial.

- ``"CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111), CAN(can.media.socketcan.SocketCANMedia('vcan1',32),111), CAN(can.media.socketcan.SocketCANMedia('vcan2',64),111)"`` --
  triple redundant CAN bus, classic CAN with CAN FD.

Complete example if you are using bash/sh/zsh or similar:

.. code-block:: sh

    export YAKUT_TRANSPORT="UDP('127.0.0.111')"

If you are using PowerShell:

.. code-block:: ps1

    $env:YAKUT_TRANSPORT="UDP('127.0.0.111')"


Running the application
+++++++++++++++++++++++

Start the demo application shown above and leave it running.
To listen to the demo's heartbeat or its diagnostics, run the following commands in a new terminal:

.. code-block:: sh

    yakut sub uavcan.node.Heartbeat.1.0 --count=3
    yakut sub uavcan.diagnostic.Record.1.1

The latter may not output anything because the demo application is not doing anything interesting,
so it has nothing to report.
Keep the command running, and open a yet another terminal, whereat run this:

.. code-block:: sh

    yakut call 42 123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0 'points: [{x: 10, y: 1}, {x: 20, y: 2}]'

Once you've executed the last command, you should see a diagnostic message being emitted in the other terminal.
Now let's publish temperature:

.. code-block:: sh

    yakut pub 12345.uavcan.si.sample.temperature.Scalar.1.0 '{kelvin: 123.456}' --count=2

You will see the demo application emit two more diagnostic messages.

If you want to see what exactly is happening under the hood,
export the environment variable ``PYUAVCAN_LOGLEVEL=DEBUG`` before starting the process.
This will slow down the library significantly.
