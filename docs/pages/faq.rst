Frequently asked questions
==========================

What is UAVCAN?
    It's a data exchange solution for modern software-defined vehicles: drones, manned aircraft, robots, cars,
    and spacecraft. There is a `post on the UAVCAN forum <https://forum.uavcan.org/t/557>`_ explaining its
    principles and design goals.


How can I deploy PyUAVCAN on my embedded system?
    Consider avoiding that.
    PyUAVCAN is designed for high-level user-facing software for R&D, diagnostic, and testing applications.
    We have UAVCAN implementations in other programming languages that are built specifically for embedded systems;
    please find more info at `uavcan.org <https://uavcan.org>`_.


PyUAVCAN seems complex. Does that mean that UAVCAN is a complex protocol?
    UAVCAN is a very simple protocol. This particular implementation may appear convoluted because it is very
    generic and provides a very high-level API. For comparison, there is a full-featured UAVCAN-over-CAN
    implementation in C called ``libcanard`` that is only ~1k SLoC large.


Library import fails with ``ImportError``. Do I need additional packages to get it working?
    No. The missing packages (usually it's ``uavcan``) are supposed to be auto-generated from DSDL definitions.
    We no longer ship the public regulated DSDL definitions together with UAVCAN implementations
    in order to simplify maintenance and integration; also, this underlines our commitment to make
    vendor-specific (or application-specific) data types first-class citizens in UAVCAN v1.
    Please read the user documentation to learn how to generate Python packages from DSDL namespaces.
