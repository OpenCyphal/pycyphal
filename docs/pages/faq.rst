Frequently asked questions
==========================

What is UAVCAN?
    UAVCAN is an open technology for real-time intravehicular distributed computing and communication
    based on modern networking standards (Ethernet, CAN FD, etc.).
    It was created to address the challenge of on-board deterministic computing and data distribution
    in next-generation intelligent vehicles: manned and unmanned aircraft, spacecraft, robots, and cars.
    The name stands for *Uncomplicated Application-level Vehicular Computing And Networking*.


How can I deploy PyUAVCAN on my embedded system?
    Consider avoiding that.
    PyUAVCAN is designed for high-level user-facing software for R&D, diagnostic, and testing applications.
    We have UAVCAN implementations in other programming languages that are built specifically for embedded systems;
    please find more info at `uavcan.org <https://uavcan.org>`_.


PyUAVCAN seems complex. Does that mean that UAVCAN is a complex protocol?
    UAVCAN is a very simple protocol.
    This particular implementation may appear convoluted because it is very generic and provides a very high-level API.
    For comparison, there is a minimal UAVCAN-over-CAN implementation in C called ``libcanard``
    that is only ~1k SLoC large.


I am getting ``ModuleNotFoundError: No module named 'uavcan'``. Do I need to install additional packages?
    We no longer ship the public regulated DSDL definitions together with UAVCAN implementations
    in order to simplify maintenance and integration;
    also, this underlines our commitment to make vendor-specific (or application-specific)
    data types first-class citizens in UAVCAN v1.
    Please read the user documentation to learn how to generate Python packages from DSDL namespaces.


Imports fail with ``AttributeError: module 'uavcan...' has no attribute '...'``. What am I doing wrong?
    Remove the old library: ``pip uninstall -y uavcan``.
    Read the :ref:`installation` guide for details.
