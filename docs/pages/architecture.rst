.. _architecture:

Architecture
============

Overview
--------

PyUAVCAN is a full-featured implementation of the `UAVCAN protocol stack <https://uavcan.org>`_
intended for non-embedded, user-facing applications such as GUI software, diagnostic tools,
automation scripts, prototypes, and various R&D cases.
It is designed to support **GNU/Linux**, **MS Windows**, and **macOS** as first-class target platforms.

The reader should understand the basics of UAVCAN and be familiar with
`asynchronous programming in Python <https://docs.python.org/3/library/asyncio.html>`_
to understand this documentation.

The library consists of several loosely coupled submodules,
each implementing a well-segregated part of the protocol:

- :mod:`pyuavcan.dsdl` -- DSDL language support: transcompilation (code generation) and object serialization.
  This module is a thin wrapper over `Nunavut <https://github.com/UAVCAN/nunavut/>`_.

- :mod:`pyuavcan.transport` -- the abstract UAVCAN transport layer model and several
  concrete transport implementations (UAVCAN/CAN, UAVCAN/UDP, UAVCAN/serial, etc.).
  This submodule exposes a relatively low-level API where data is represented as serialized blocks of bytes.
  Users may build custom concrete transports based on this module as well.
  *Typical applications are not expected to use this API directly.*

- :mod:`pyuavcan.presentation` -- this layer binds the transport layer together with DSDL serialization logic,
  providing a high-level object-oriented API.
  At this layer, data is represented as instances of well-structured Python classes
  auto-generated from their DSDL source definitions.
  *Typical applications are not expected to use this API directly.*

- :mod:`pyuavcan.application` -- this is the top-level API provided by this library that applications should rely on.
  The factory :func:`pyuavcan.application.make_node` is the main entry point of the library.

- :mod:`pyuavcan.util` -- this is just a loosely organized collection of various utility functions and classes
  that are used across the library. User applications may benefit from them also.
  Every other submodule depends on this one.

.. note::
   In order to use this library the user should at least skim through the API docs for
   :mod:`pyuavcan.application` and check out the :ref:`demo`.

The overall structure of the library and its mapping onto the UAVCAN protocol is shown on the following diagram:

.. image:: /_static/arch-non-redundant.svg

Every submodule is imported automatically, excepting application layer and concrete transport implementation
submodules -- those must be imported explicitly by the user::

    >>> import pyuavcan
    >>> pyuavcan.dsdl.serialize         # OK, the DSDL submodule is auto-imported.
    <function serialize at ...>
    >>> pyuavcan.transport.can          # Not the transport-specific modules though.
    Traceback (most recent call last):
    ...
    AttributeError: module 'pyuavcan.transport' has no attribute 'can'
    >>> import pyuavcan.transport.can   # Import the necessary transports explicitly before use.
    >>> import pyuavcan.transport.serial
    >>> import pyuavcan.application     # Likewise the application layer -- it depends on DSDL generated classes.


Transport layer
---------------

The UAVCAN protocol itself is designed to support different transports such as CAN bus (UAVCAN/CAN),
UDP/IP (UAVCAN/UDP), raw serial links (UAVCAN/serial), and so on.
Generally, a real-time safety-critical implementation of UAVCAN would support a limited subset of
transports defined by the protocol (often just one) in order to reduce the validation & verification efforts.
PyUAVCAN is different -- it is created for user-facing software rather than reliable deeply embedded systems;
that is, PyUAVCAN can't be put onboard a vehicle, but it can be put onto the computer of an engineer or a researcher
building said vehicle to help them implement, understand, validate, verify, and diagnose its onboard network.
Hence, PyUAVCAN trades off simplicity and constrainedness (desirable for embedded systems)
for extensibility and repurposeability (desirable for user-facing software).

The library consists of a transport-agnostic core which implements the higher levels of the UAVCAN protocol,
DSDL code generation, and object serialization.
The core defines an abstract *transport model* which decouples it from transport-specific logic.
The main component of the abstract transport model is the interface class :class:`pyuavcan.transport.Transport`,
accompanied by several auxiliary definitions available in the same module :mod:`pyuavcan.transport`.

The concrete transports implemented in the library are contained in nested submodules;
here is the full list of them:

.. computron-injection::
   :filename: synth/transport_summary.py

Users can implement their own custom transports by subclassing :class:`pyuavcan.transport.Transport`.

Whenever the API documentation refers to *monotonic time*, the time system of
:meth:`asyncio.AbstractEventLoop.time` is implied.
Per asyncio, it defaults to :func:`time.monotonic`,
but it can be overridden by the user on a per-loop basis if necessary (read the asyncio docs for details).
This principle is valid for all other components of the library.


Media sub-layers
++++++++++++++++

Typically, a given concrete transport implementation would need to support multiple different lower-level
communication mediums for the sake of application flexibility.
Such lower-level implementation details fall outside of the scope of the UAVCAN transport model entirely,
but they are relevant for this library as we want to encourage consistent design across the codebase.
Such lower-level modules are called *media sub-layers*.

Media sub-layer implementations should be located under the submodule called ``media``,
which in turn should be located under its parent transport's submodule, i.e., ``pyuavcan.transport.*.media.*``.
The media interface class should be ``pyuavcan.transport.*.media.Media``;
derived concrete implementations should be suffixed with ``*Media``, e.g., ``SocketCANMedia``.
Users may implement their custom media drivers for use with the transport by subclassing ``Media`` as well.

Take the CAN media sub-layer for example; it contains the following classes (among others):

- :class:`pyuavcan.transport.can.media.socketcan.SocketCANMedia`
- :class:`pyuavcan.transport.can.media.pythoncan.PythonCANMedia`

Media sub-layer modules should not be auto-imported. Instead, the user should import the required media sub-modules
manually as necessary.
This is important because sub-layers may have specific dependency requirements which are not guaranteed
to be satisfied in all deployments;
also, unnecessary submodules slow down package initialization and increase the memory footprint of the application,
not to mention possible software reliability issues.

Some transport implementations may be entirely monolithic, without a dedicated media sub-layer.
For example, see :class:`pyuavcan.transport.serial.SerialTransport`.


Redundant pseudo-transport
++++++++++++++++++++++++++

The pseudo-transport :class:`pyuavcan.transport.redundant.RedundantTransport` is used to operate with
UAVCAN networks built with redundant transports.
In order to initialize it, the application should first initialize each of the physical transports and then
supply them to the redundant pseudo-transport instance.
Afterwards, the configured instance is used with the upper layers of the protocol stack, as shown on the diagram.

.. image:: /_static/arch-redundant.svg

The `UAVCAN Specification <https://uavcan.org/specification>`_ adds the following remark on redundant transports:

    Reassembly of transfers from redundant interfaces may be implemented either on the per-transport-frame level
    or on the per-transfer level.
    The former amounts to receiving individual transport frames from redundant interfaces which are then
    used for reassembly;
    it can be seen that this method requires that all transports in the redundant group use identical
    application-level MTU (i.e., same number of transfer pay-load bytes per frame).
    The latter can be implemented by treating each transport in the redundant group separately,
    so that each runs an independent transfer reassembly process, whose outputs are then deduplicated
    on the per-transfer level;
    this method may be more computationally complex but it provides greater flexibility.

Per this classification, PyUAVCAN implements *per-transfer* redundancy.


Advanced network diagnostics: sniffing/snooping, tracing, spoofing
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Packet capture (aka sniffing or snooping) and their further analysis (either real-time or postmortem)
are vital for advanced network diagnostics or debugging.
While existing general-purpose solutions like Wireshark, libpcap, npcap, SocketCAN, etc. are adequate for
low-level access, they are unsuitable for non-trivial use cases where comprehensive analysis is desired.

Certain scenarios require emitting spoofed traffic where some of its parameters are intentionally distorted
(like fake source address).
This may be useful for implementing complex end-to-end tests for UAVCAN-enabled equipment,
running HITL/SITL simulation, or validating devices for compliance against the UAVCAN Specification.

These capabilities are covered by the advanced network diagnostics API exposed by the transport layer:

- :meth:`pyuavcan.transport.Transport.begin_capture` --
  **capturing** on a transport refers to monitoring low-level network events and packets exchanged over the
  network even if they neither originate nor terminate at the local node.

- :meth:`pyuavcan.transport.Transport.make_tracer` --
  **tracing** refers to reconstructing high-level processes that transpire on the network from a sequence of
  captured low-level events.
  Tracing may take place in real-time (with PyUAVCAN connected to a live network) or offline
  (with events read from a black box recorder or from a log file).

- :meth:`pyuavcan.transport.Transport.spoof` --
  **spoofing** refers to faking network transactions as if they were coming from a different node
  (possibly a non-existent one) or whose parameters are significantly altered (e.g., out-of-sequence transfer-ID).

These advanced capabilities exist alongside the main communication logic using a separate set of API entities
because their semantics are incompatible with regular applications.


Virtualization
++++++++++++++

Some transports support virtual interfaces that can be used for testing and experimentation
instead of physical connections.
For example, the UAVCAN/CAN transport supports virtual CAN buses via SocketCAN,
and the serial transport supports TCP/IP tunneling and local loopback mode.


DSDL support
------------

The DSDL support module :mod:`pyuavcan.dsdl` is used for automatic generation of Python
classes from DSDL type definitions.
The auto-generated classes have a high-level application-facing API and built-in auto-generated
serialization and deserialization routines.

By default, DSDL-generated packages are stored in the current working directory.
This is convenient because the packages contained in the same directory are importable by default.
If a different directory is used, it has to be added to the import lookup path manually
either via the ``PYTHONPATH`` environment variable or via :data:`sys.path`.

The main API entries are:

- :func:`pyuavcan.dsdl.compile` -- transcompiles a DSDL namespace into a Python package.

- :func:`pyuavcan.dsdl.serialize` and :func:`pyuavcan.dsdl.deserialize` -- serialize and deserialize
  an instance of an autogenerated class.

- :class:`pyuavcan.dsdl.CompositeObject` and :class:`pyuavcan.dsdl.ServiceObject` -- base classes for
  Python classes generated from DSDL type definitions; message types and service types, respectively.

- :func:`pyuavcan.dsdl.to_builtin` and :func:`pyuavcan.dsdl.update_from_builtin` -- used to convert
  a DSDL object instance to/from a simplified representation using only built-in types such as :class:`dict`,
  :class:`list`, :class:`int`, :class:`float`, :class:`str`, and so on. These can be used as an intermediate
  representation for conversion to/from JSON, YAML, and other commonly used serialization formats.


Presentation layer
------------------

The presentation layer submodule :mod:`pyuavcan.presentation` is the first submodule among the reviewed so far that
depends on other submodules (aside from the utility submodule).
The internal dependency relations can be visualized as follows:

.. graphviz::
    :caption: Submodule interdependency

    digraph submodule_interdependency {
        graph   [bgcolor=transparent];
        node    [shape=box, style=filled, fontname="monospace"];

        dsdl            [fillcolor="#FF88FF", label="pyuavcan.dsdl"];
        transport       [fillcolor="#FFF2CC", label="pyuavcan.transport"];
        presentation    [fillcolor="#D9EAD3", label="pyuavcan.presentation"];
        application     [fillcolor="#C9DAF8", label="pyuavcan.application"];
        util            [fillcolor="#D3D3D3", label="pyuavcan.util"];

        dsdl            -> util;
        transport       -> util;
        presentation    -> {dsdl transport util};
        application     -> {dsdl transport presentation util};
    }

The only function of the presentation layer is to provide a high-level object-oriented interface on top of
the transport layer by invoking the DSDL serialization routines
(see :func:`pyuavcan.dsdl.serialize` and :func:`pyuavcan.dsdl.deserialize`).
A typical application is not expected to invoke the presentation-layer API directly;
instead, it should rely on the higher-level API entities provided by :mod:`pyuavcan.application`.


Application layer
-----------------

Submodule :mod:`pyuavcan.application` provides the top-level API for the application and implements certain
standard application-layer functions defined by the UAVCAN Specification (chapter 5 *Application layer*).
The **main entry point of the library** is :func:`pyuavcan.application.make_node`.

This submodule requires the standard DSDL namespace ``uavcan`` to be compiled first (see :func:`pyuavcan.dsdl.compile`),
so it is not auto-imported.
A typical usage scenario is to either distribute compiled DSDL namespaces together with the application,
or to generate them lazily before importing this submodule.

Chapter :ref:`demo` contains a complete usage example.


High-level functions
++++++++++++++++++++

There are several submodules under this one that implement various application-layer functions of the protocol.
Here is the full list them:

.. computron-injection::
   :filename: synth/application_module_summary.py

Excepting some basic functions that are always initialized by default (like heartbeat or the register API),
these modules are not auto-imported.


Utilities
---------

Submodule :mod:`pyuavcan.util` contains a loosely organized collection of minor utilizies and helpers that are
used by the library and are also available for reuse by the application.
