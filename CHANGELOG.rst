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

v1.1
----

First stable release. v1.0 was never released for legacy reasons.
