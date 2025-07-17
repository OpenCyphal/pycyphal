.. _changelog:

Changelog
=========

v1.24
-----

- Add ``Lockerfile`` to prevent race condition when compiling namespaces (`#357 <https://github.com/OpenCyphal/pycyphal/pull/357>`_)
- Add ``remove_import_hook``, rename ``install_import_hook`` to ``add_import_hook`` (`#356 <https://github.com/OpenCyphal/pycyphal/pull/356>`_)
- Choose the type width based on elements in the source array (`#358 <https://github.com/OpenCyphal/pycyphal/pull/358>`_)

- **v1.24.1:**

  - Install Graphviz 13.x.
    See (`#363 <https://github.com/OpenCyphal/pycyphal/pull/363>`_)

- **v1.24.2:**

  - Revert changes from 1.24.1. See Issue (`#321 <https://github.com/OpenCyphal/pycyphal/issues/321>`_)
  - Build Graphviz 13.x.
    See (`#363 <https://github.com/OpenCyphal/pycyphal/pull/365>`_)

v1.23
-----

- Add progress reporting to `pycyphal.application.file.FileClient2`
  (`#353 <https://github.com/OpenCyphal/pycyphal/pull/353>`_).

v1.22
-----

- Fix ``pycyphal.application.file.RemoteFileError`` (`#347 <https://github.com/OpenCyphal/pycyphal/pull/347>`_).
- Add support for the USBtingo CAN interface. This is only available when the
  `python-can-usbtingo <https://github.com/EmbedME/python-can-usbtingo>`_ package is installed
  (`#348 <https://github.com/OpenCyphal/pycyphal/pull/348>`_).

v1.21
-----

- Upgrade to require NumPy v2.2. NumPy v1 is still supported but no longer tested against.
- Add testing against Python 3.12 and 3.13. No changes to the library were needed to enable this.
- Drop testing against Python 3.8 and 3.9.
- Cyphal/UDP: The default node-ID is now None, not zero (`#323 <https://github.com/OpenCyphal/pycyphal/issues/323>`_).

v1.20
-----
- Add the ``py.typed`` annotation (`#343 <https://github.com/OpenCyphal/pycyphal/pull/343>`_).

v1.19
-----
- Implement configure_acceptance_filters for socketcan.
- **v1.19.1:**

  - Fix socketcan timestamp on newer 32 bit kernel.

v1.18
-----
- Add FileClient2 which reports errors by raising exceptions.
  See (`#327 <https://github.com/OpenCyphal/pycyphal/issues/327>`_) for rationale.

v1.17
-----
- Move to Nunavut Version 2
  See (`#318 <https://github.com/OpenCyphal/pycyphal/pull/318>`_) for details on the internal changes.

v1.16
-----

- Added support for the Socketcand interface. 
  See (`#306 <https://github.com/OpenCyphal/pycyphal/pull/306>`_) for details on the changes. 

v1.15
-----

- Made PythonCAN support better.

v1.14
-----

- Updated the Serial frame format to match the UDP frame format.
  See (`#266 <https://github.com/OpenCyphal/pycyphal/issues/266>`_) for a general description of the changes.

v1.13
-----

- Cyphal/UDP: Service transfer are also multicast now. Make sure to take into account the updated IP mapping specifications.
  See (`post <https://forum.opencyphal.org/t/cyphal-udp-architectural-issues-caused-by-the-dependency-between-the-nodes-ip-address-and-its-identity/1765>`_).

v1.12
-----

- Add :attr:`pycyphal.application.node_tracker.NodeTracker.get_info_priority`, to allow configuring the node tracker's
  GetInfo request priority.

v1.11
-----

- Add ``pycyphal.transport.can.CANTransport.spoof_frames()`` for compatibility with 3rd-party CAN protocols
  sharing the network interface with PyCyphal.

v1.10
-----

- Implement DSDL compilation via import hooks
  (`#236 <https://github.com/OpenCyphal/pycyphal/pull/236>`_).

v1.9
----

- Cyphal/CAN: Support GS USB adapter via PythonCAN
  (`#212 <https://github.com/OpenCyphal/pycyphal/pull/212>`_).

- Cyphal/CAN: Adjust SocketCAN socket behavior to avoid ENOBUFS
  (`#234 <https://github.com/OpenCyphal/pycyphal/pull/234>`_).

- Cyphal/CAN: Add :mod:``pycyphal.transport.can.media.candump`` media that reads standard CAN bus log files created
  by the candump utility (part of SocketCAN).

v1.8
----

- Subscription synchronizer added (`#65 <https://github.com/OpenCyphal/pycyphal/issues/65>`_).

- **v1.8.1:**
  Port factory methods in :class:`pycyphal.application.Node` that accept direct port-ID always update the registry.

- **v1.8.2:** Fix error handing in :meth:`pycyphal.transport.redundant.RedundantOutputSession.send`;
  see `#222 <https://github.com/OpenCyphal/pycyphal/issues/222>`_.

- **v1.8.3:**

  - ``DiagnosticPublisher``: do not instantiate the publisher if the local node is anonymous.
  - ``publish_soon()``: Do not log error if closed.
  - ``Client`` and ``Publisher``: fix edge cases related to ``PortClosedError`` when the interface becomes unavailable.
  - Fix assertion failure during register value coercion.
  - SocketCAN: close the media instance automatically on unrecoverable errors like ENODEV, ENXIO, EBADF, EBADFD, etc.

- **v1.8.4**:

  - Actualize the Demo (mostly Yakut-related).

v1.7
----

- :class:`pycyphal.application.Node` supports construction of ports (publishers, subscribers, clients, servers)
  with a directly specified port-ID, bypassing the registry.

- New presentation layer capabilities:

  - New overload :meth:`pycyphal.presentation.Client.__call__`

  - New method :meth:`pycyphal.presentation.Subscriber.get`

  - Support sync callbacks in :meth:`pycyphal.presentation.Subscriber.receive_in_background`

v1.5
----

- The library renamed from PyUAVCAN to PyCyphal and republished under the new name.

v1.4
----

- Behavior of the redundant output session changed:
  :meth:`pyuavcan.transport.redundant.RedundantOutputSession.send` returns as soon as at least one inferior is done
  transmitting, the slower ones keep transmitting in the background.
  In other words, the redundant transport now operates at the rate of the fastest inferior (used to be the slowest one).

- Implement the DSDL UX improvement described in `#147 <https://github.com/UAVCAN/pyuavcan/issues/147>`_.

- Fully adopt PEP 585 in generated code.

v1.3
----

- Support Python 3.10.

- Deprecate property ``pyuavcan.transport.Transport.loop`` and the corresponding constructor argument.
  The constructor argument is now ignored and the aforementioned property is an alias of ``asyncio.get_event_loop()``.

- Generated classes include convenience aliases of the newest minor version per major version
  (`Nunavut #193 <https://github.com/UAVCAN/nunavut/issues/193>`_).

- Remove the NumPy <= 0.17 version constraint.

- Improve type annotations in generated code thanks to the new NumPy typing support.

- Support assignment of ``memoryview`` to ``uint8``-typed arrays.

- Rename installation extras by replacing underscores with the minus character;
  e.g., ``transport_can_pythoncan`` --> ``transport-can-pythoncan``.

- Improve logging and error reporting.

- Fix issues related to UDP packet capture.

v1.2
----

- ``pyuavcan.transport.can``: Add Python-CAN media driver.
  The corresponding installation extra is ``transport_can_pythoncan``.

- Support packet capture and tracing for all transports (spoofing is implemented for all transports except UAVCAN/UDP).
  Refactor the unstable capture/tracing API to model the underlying protocols more accurately.

- Add ``pyuavcan.application.file.FileServer``/``FileClient`` implementing the standard file service ``uavcan.file``.

- Constructor parameter ``anonymous`` for ``UDPTransport`` has been deprecated in favor of ``local_node_id``.

- Refactor the Node API (`#154 <https://github.com/UAVCAN/pyuavcan/pull/154>`_):

  - Add factory function ``make_node()``.

  - Implement the UAVCAN Register API and add port construction factory methods that take port-ID from the registry.
    This is a major change that allows applications to avoid hard-coding any port-ID whatsoever.
    The respective configuration is now sourced from the registers, which in turn are read from environment variables
    and from persistent register files (i.e., configuration files).

  - Support context manager API (``__enter__``, ``__leave__``).

  - Rework the demo accordingly.

- In ``pyuavcan.dsdl``: rename ``generate_package`` into ``compile``, add ``compile_all``.

v1.1
----

First stable release. v1.0 was never released for legacy reasons.
