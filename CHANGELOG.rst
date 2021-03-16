.. _changelog:

Changelog
=========

v1.2
----

- ``pyuavcan.transport.can``: Add Python-CAN media driver.
  The corresponding installation extra is ``transport_can_pythoncan``.

- Support packet capture and tracing for UAVCAN/CAN.
  Refactor the unstable capture/tracing API to model the underlying protocols more accurately.

- Constructor parameter ``anonymous`` for ``UDPTransport`` has been deprecated in favor of ``local_node_id``.

- Refactor the Node API:

  - Add factory function ``make_node()``.

  - Implement the UAVCAN Register API and add port construction factory methods that take port-ID from the registry.
    This is a major change that allows applications to avoid hard-coding any port-ID whatsoever.
    The respective configuration is now sourced from the registers, which in turn are read from environment variables
    and from persistent register files (i.e., configuration files).

  - Support context manager API (``__enter__``, ``__leave__``).

  - Rework the documentation accordingly.

- Fix minor issues with logging throughout.

- In ``pyuavcan.dsdl``: rename ``generate_package`` into ``compile``, add ``compile_all``.


v1.1
----

First stable release. v1.0 was never released for legacy reasons.
