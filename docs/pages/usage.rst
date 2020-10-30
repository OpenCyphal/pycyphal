.. _basic_usage:

Basic usage demo
================

The reader is assumed to have read the **UAVCAN v1** specification beforehand.
See `uavcan.org <https://uavcan.org>`_ for details.


Custom data types
-----------------

The demo relies on two vendor-specific data types located in the root namespace ``sirius_cyber_corp``.
The root namespace directory layout is as follows::

    sirius_cyber_corp/                              <-- root namespace directory
        PerformLinearLeastSquaresFit.1.0.uavcan     <-- service type definition
        PointXY.1.0.uavcan                          <-- nested message type definition

If this doesn't look familiar, please read the UAVCAN specification first.
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

.. literalinclude:: /../tests/demo/basic_usage.py
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
Please use one of the following options depending on your demo configuration:

- ``--tr="UDP('127.0.0.111/8')"`` --
  UDP/IP transport on localhost. Local node-ID 111.

- ``--tr="Serial('socket://loopback:50905',111)"`` --
  serial transport emulated over a TCP/IP tunnel instead of a real serial port (use Ncat for TCP connection brokering).
  Local node-ID 111.

- ``--tr="CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111)"`` --
  virtual CAN bus via SocketCAN (GNU/Linux systems only).
  Local node-ID 111.

Redundant transports can be configured by specifying the ``--tr`` option more than once:

- ``--tr="UDP('127.0.0.111/8')" --tr="Serial('socket://loopback:50905',111)"`` --
  dissimilar double redundancy, UDP plus serial.

- ``--tr="CAN(can.media.socketcan.SocketCANMedia('vcan0',8),111)"``
  ``--tr="CAN(can.media.socketcan.SocketCANMedia('vcan1',32),111)"``
  ``--tr="CAN(can.media.socketcan.SocketCANMedia('vcan2',64),111)"`` --
  triple redundant CAN bus, classic CAN with CAN FD.


Running the application
+++++++++++++++++++++++

Start the demo application shown above and leave it running.
In a new terminal, run the following commands to listen to the demo's heartbeat or its diagnostics
(don't forget to specify transport):

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
