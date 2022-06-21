Frequently asked questions
==========================

What is Cyphal?
    Cyphal is an open technology for real-time intravehicular distributed computing and communication
    based on modern networking standards (Ethernet, CAN FD, etc.).
    It was created to address the challenge of on-board deterministic computing and data distribution
    in next-generation intelligent vehicles: manned and unmanned aircraft, spacecraft, robots, and cars.
    The project was once known as `UAVCAN <https://forum.opencyphal.org/t/uavcan-v1-is-now-cyphal/1622>`_.


How can I deploy PyCyphal on my embedded system?
    PyCyphal is mostly designed for high-level user-facing software for R&D, diagnostic, and testing applications.
    We have Cyphal implementations in other programming languages that are built specifically for embedded systems;
    please find more info at `opencyphal.org <https://opencyphal.org>`_.


PyCyphal seems complex. Does that mean that Cyphal is a complex protocol?
    Cyphal is a very simple protocol.
    This particular implementation may appear convoluted because it is very generic and provides a very high-level API.
    For comparison, there is a minimal Cyphal-over-CAN implementation in C called ``libcanard``
    that is only ~1k SLoC large.


I am getting ``ModuleNotFoundError: No module named 'uavcan'``. Do I need to install additional packages?
    We no longer ship the public regulated DSDL definitions together with Cyphal implementations
    in order to simplify maintenance and integration;
    also, this underlines our commitment to make vendor-specific (or application-specific)
    data types first-class citizens in Cyphal.
    Please read the user documentation to learn how to generate Python packages from DSDL namespaces.


Imports fail with ``AttributeError: module 'uavcan...' has no attribute '...'``. What am I doing wrong?
    Remove the legacy library: ``pip uninstall -y uavcan``.
    Read the :ref:`installation` guide for details.
