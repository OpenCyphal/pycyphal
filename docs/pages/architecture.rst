.. _architecture:

Architecture
============

Overview
--------

PyUAVCAN is a full-featured implementation of the `UAVCAN protocol stack <https://uavcan.org>`_
intended for non-embedded, user-facing applications such as GUI software, diagnostic tools,
automation scripts, prototypes, and various R&D cases.
PyUAVCAN consists of a Python library (package) and a simple :abbr:`CLI (command line interface)`
tool for basic diagnostics and shell script automation.

It is designed to support **GNU/Linux**, **MS Windows**, and **OS X** as first-class target platforms.

The reader should be familiar with the `UAVCAN specification <https://uavcan.org/specification>`_
and `asynchronous programming in Python <https://docs.python.org/3/library/asyncio-task.html>`_
to understand this documentation.
The latter is quite important because **PyUAVCAN provides a coroutine-based async API**.

The library consists of several loosely coupled submodules,
each implementing a well-segregated part of the protocol.
Each of the submodules can be used separately depending on the level of abstraction required by the application.

- :mod:`pyuavcan.dsdl` -- DSDL language support: code generation and object serialization.

- :mod:`pyuavcan.transport` -- the abstract UAVCAN transport layer model and several
  concrete transport implementations (CAN, serial, etc.).
  The transport layer exposes a low-level API where data is represented as unstructured blocks of bytes.
  Users may build custom concrete transports based on this module as well.

- :mod:`pyuavcan.presentation` -- this layer binds the transport layer together with DSDL serialization logic,
  providing a high-level object-oriented API.
  At this layer, data is represented as instances of well-structured Python classes
  auto-generated from their DSDL source definitions.

- :mod:`pyuavcan.application` -- this is an entirely optional layer containing very high-level convenience
  classes providing support for high-level protocol features such as node heartbeat broadcasting.
  This layer can be used to look up usage examples for the presentation and transport layer API.

- :mod:`pyuavcan.util` -- this is just a loosely organized collection of various utility functions and classes
  that are used across the library. User applications may benefit from them also.
  Every other submodule depends on this one.

The overall structure of the library and its mapping onto the UAVCAN protocol is shown on the following diagram:

.. image:: /_static/arch-non-redundant.svg

Every submodule is imported automatically, excepting application layer and concrete transport implementation
submodules -- those must be imported explicitly by the user.
For example::

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

The UAVCAN protocol itself is designed to support multiple transports such as CAN bus, UDP, serial, and so on.
Generally, a real-time safety-critical implementation of UAVCAN would support a limited subset of
transports defined by the protocol (often just one) in order to reduce the validation & verification efforts.
PyUAVCAN is different -- it is created for user-facing software rather than reliable deeply embedded systems;
that is, PyUAVCAN can't be put onboard a vehicle, but it can be put onto the computer of an engineer or a researcher
building said vehicle to help them implement, understand, validate, verify, and diagnose its onboard network.

Hence, PyUAVCAN trades off simplicity and constraindness (desirable for embedded systems)
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
This principle is valid for all other components of the library; for example, the presentation layer.


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

Take the CAN media sub-layer for example; it contains the following classes (the list may not be exhaustive,
this is just an example):

- :class:`pyuavcan.transport.can.media.socketcan.SocketCANMedia`
- :class:`pyuavcan.transport.can.media.pythoncan.PythonCANMedia`

Media sub-layer modules should not be auto-imported. Instead, the user should import the required media sub-modules
manually as necessary.
This is important because sub-layers may have specific dependency requirements which are not guaranteed
to be satisfied in all deployments; also, unnecessary submodules slow down package initialization
and increase the memory footprint of the application, not to mention possible software reliability issues.

Generally, what's been described can be seen as the transport layer model projected
one level further down the protocol stack.

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

For more information, please read the class API documentation.


Virtualization
++++++++++++++

Some transports support virtual interfaces that can be used for testing and experimentation
instead of real physical connections.
For example, the CAN transport supports virtual CAN buses via SocketCAN,
and the serial transport supports TCP/IP tunneling and local loopback mode.
Read the API documentation for more information about these features.


DSDL support
------------

The DSDL support module :mod:`pyuavcan.dsdl` is used for automatic generation of Python
classes from DSDL type definitions.
The auto-generated classes have a high-level application-facing API and built-in auto-generated
serialization and deserialization routines.

The main API functions are:

- :func:`pyuavcan.dsdl.generate_package` -- generates a Python package from a DSDL namespace.
- :func:`pyuavcan.dsdl.serialize` and :func:`pyuavcan.dsdl.deserialize` -- serialize and deserialize
  an instance of an autogenerated class.
- :class:`pyuavcan.dsdl.CompositeObject` and :class:`pyuavcan.dsdl.ServiceObject` -- base classes for
  Python classes generated from DSDL type definitions; message types and service types, respectively.
- :func:`pyuavcan.dsdl.to_builtin` and :func:`pyuavcan.dsdl.update_from_builtin` -- used to convert
  a DSDL object instance to/from a simplified representation using only built-in types such as :class:`dict`,
  :class:`list`, :class:`int`, :class:`float`, :class:`str`, and so on. These can be used as an intermediate
  representation for conversion to/from JSON, YAML, and other commonly used serialization formats.

Please read the module API documentation for more info.


Presentation layer
------------------

The presentation layer submodule :mod:`pyuavcan.presentation` is the first submodule among the reviewed so far that
depends on other submodules (barring the utility submodule, which is an implicit dependency so it's not mentioned).
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

The function of the presentation layer is to build high-level object-oriented interface on top of the transport
layer by invoking the DSDL serialization routines
(see :func:`pyuavcan.dsdl.serialize` and :func:`pyuavcan.dsdl.deserialize`).
This is the highest level of abstraction presented to the user of the library.
That is, when creating a new publisher or another network session, the calling code will interact
directly with the presentation layer (the application layer, if used, serves as a thin proxy
rather than adding any new abstraction on top).

The main entity of the presentation layer is the controller class :class:`pyuavcan.presentation.Presentation`;
specifically, the following methods form pretty much the core of the upper API:

- :meth:`pyuavcan.presentation.Presentation.make_publisher` -- constructs :class:`pyuavcan.presentation.Publisher`.
- :meth:`pyuavcan.presentation.Presentation.make_subscriber` -- constructs :class:`pyuavcan.presentation.Subscriber`.
- :meth:`pyuavcan.presentation.Presentation.make_client` -- constructs :class:`pyuavcan.presentation.Client`.
- :meth:`pyuavcan.presentation.Presentation.get_server` (sic!) -- constructs :class:`pyuavcan.presentation.Server`.
  The name and semantics are slightly different because servers are unlike other session objects.
  Read the docs for info.

The presentation layer is the main part of the library API.


Application layer
-----------------

The main entity of the application layer is the node class :class:`pyuavcan.application.Node`.
There are also several submodules containing implementations of various higher-level functions of the protocol,
one submodule per function; for example, :class:`pyuavcan.application.heartbeat_publisher`.

The node class is essentially a helper class, it does not provide any new abstractions.
The higher-level functions provided at this level are implemented directly on top of the presentation layer;
there are no internal/private APIs used.
Since the submodule relies exclusively on the public library API,
it can be studied as a solid collection of usage examples and best practices.

The application layer submodule is the only top-level submodule that is not auto-imported.
This is because it requires that the auto-generated Python package for the standard data types contained
in the DSDL root namespace ``uavcan`` is available for importing; by default it is not.
Another reason is that it is expected that some applications may choose to avoid reliance on the application
layer, so in that case importing this submodule at initialization time would be counter-productive.

As one might guess, if the submodule is imported before the ``uavcan`` root namespace package is generated,
an :class:`ImportError` is raised (with ``name='uavcan'``).
Applications may choose to catch that exception to implement lazy code generation.
For a hands-on guide on how to do that read the :ref:`basic_usage` chapter
and the API documentation for :mod:`pyuavcan.dsdl`.


Utilities
---------

The utilities module contains a loosely organized collection of functions and classes that are
used by the library and are also available for reuse by the application.

Functions :func:`pyuavcan.util.import_submodules` and :func:`pyuavcan.util.iter_descendants`
may come useful if automatic discovery of available transport and/or media implementations is needed.

For more information, read the API docs for :mod:`pyuavcan.util`.


Command-line tool
-----------------

The command-line tool named ``pyuavcan`` (like the library)
can be installed as described in the :ref:`installation` chapter.
Run ``pyuavcan --help`` to see the usage documentation, or read the :ref:`cli` chapter.

The tool can be used as a library usage demo along with the application layer module;
its entry point is located in the private submodule named ``_cli`` (it is not a part of the library API,
hence it's not public and is never imported).
