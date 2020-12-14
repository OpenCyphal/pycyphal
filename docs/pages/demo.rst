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

.. literalinclude:: /../tests/dsdl/namespaces/sirius_cyber_corp/PerformLinearLeastSquaresFit.1.0.uavcan
   :linenos:


``sirius_cyber_corp.PointXY.1.0``:

.. literalinclude:: /../tests/dsdl/namespaces/sirius_cyber_corp/PointXY.1.0.uavcan
   :linenos:


Source code
-----------

The demo relies on the custom data types presented above.
In order to run the demo, please copy-paste its source code into a file on your computer
and update the DSDL paths to match your environment.

.. literalinclude:: /../tests/demo/demo_app.py
   :linenos:


Evaluating the demo using the command-line tool
-----------------------------------------------

Generating data type packages from DSDL
+++++++++++++++++++++++++++++++++++++++

First, we need to make sure that the required DSDL-generated packages are available for the command-line tool.
Suppose that the application-specific data types listed above are located at ``../dsdl/namespaces/``,
and that instead of using a local copy of the public regulated data types we prefer to download them from the
repository. This is the command:

.. code-block:: sh

    uvc dsdl-gen-pkg ../dsdl/namespaces/sirius_cyber_corp/ https://github.com/UAVCAN/public_regulated_data_types/archive/master.zip

That's it.
The DSDL-generated packages have been stored in the current working directory, so now we can use them.
If you decided to change the working directory, please make sure to update the ``PYTHONPATH`` environment
variable to include the path where the generated packages are stored, otherwise you won't be able to import them.
Alternatively, you can just move the generated packages to a new location (they are location-invariant)
or just generate them anew where needed.

If you want to know what exactly has been done, rerun the command with ``-v`` (V for Verbose).
As always, use ``--help`` to get the full usage information.


Configuring the transport
+++++++++++++++++++++++++

The commands shown later have to be instructed to use the same transport interface as the demo.
In this example we configure the transport using the environment variable ``PYUAVCAN_CLI_TRANSPORT``,
but it is also possible to use the ``--tr`` command line argument if found more convenient
(the syntax is identical).

Please use one of the following transport configuration expressions depending on your demo configuration:

- ``"UDP('127.0.0.111')"`` --
  UDP/IP transport on localhost. Local node-ID 111.

- ``"Serial('socket://loopback:50905',111)"`` --
  serial transport emulated over a TCP/IP tunnel instead of a real serial port (use Ncat for TCP connection brokering).
  Local node-ID 111.

- ``"CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111)"`` --
  virtual CAN bus via SocketCAN (GNU/Linux systems only).
  Local node-ID 111.

Redundant transports can be configured by specifying multiple comma-separated expressions (bracketed list is also ok)
(or by specifying the ``--tr`` option more than once if the command line arguments are used instead
of the environment variable):

- ``"UDP('127.0.0.111'), Serial('socket://loopback:50905',111)"`` --
  dissimilar double redundancy, UDP plus serial.

- ``"CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111), CAN(can.media.socketcan.SocketCANMedia('vcan1',32),111), CAN(can.media.socketcan.SocketCANMedia('vcan2',64),111)"`` --
  triple redundant CAN bus, classic CAN with CAN FD.

Specifying a single transport using the list notation is also acceptable --
this case is handled as if there was no list notation used: ``[a] == a``.
For more info on command line arguments, see chapter :ref:`cli`.

If you are using bash/sh/zsh or similar, the syntax to set the variable is:

.. code-block:: sh

    export PYUAVCAN_CLI_TRANSPORT="Loopback(None)"  # Using LoopbackTransport as an example

If you are using PowerShell:

.. code-block:: ps1

    $env:PYUAVCAN_CLI_TRANSPORT="Loopback(None), Loopback(None)"


Running the application
+++++++++++++++++++++++

Start the demo application shown above and leave it running.
To listen to the demo's heartbeat or its diagnostics, run the following commands in a new terminal:

.. code-block:: sh

    uvc sub uavcan.node.Heartbeat.1.0 --with-metadata --count=3
    uvc sub uavcan.diagnostic.Record.1.1 --with-metadata

The latter may not output anything because the demo application is not doing anything interesting,
so it has nothing to report.
Keep the command running, and open a yet another terminal, whereat run this:

.. code-block:: sh

    uvc call 42 123.sirius_cyber_corp.PerformLinearLeastSquaresFit.1.0 '{points: [{x: 10, y: 1}, {x: 20, y: 2}]}'

Once you've executed the last command, you should see a diagnostic message being emitted in the other terminal.
Now let's publish temperature:

.. code-block:: sh

    uvc pub 12345.uavcan.si.sample.temperature.Scalar.1.0 '{kelvin: 123.456}' --count=2

You will see the demo application emit two more diagnostic messages.

If you want to see what exactly is happening under the hood,
set the environment variable ``PYUAVCAN_LOGLEVEL=DEBUG`` before starting the process.
This will slow down the library significantly.
