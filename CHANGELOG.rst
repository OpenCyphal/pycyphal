.. _changelog:

Changelog
=========


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
