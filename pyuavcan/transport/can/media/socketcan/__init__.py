# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

"""
The module is always importable but is functional only on GNU/Linux.

For testing or experimentation on a local machine it is often convenient to use a virtual CAN bus instead of a real one.
Using SocketCAN, one can set up a virtual CAN bus interface as follows::

    modprobe can
    modprobe can_raw
    modprobe vcan
    ip link add dev vcan0 type vcan
    ip link set vcan0 mtu 72         # Enable CAN FD by configuring the MTU of 64+8
    ip link set up vcan0

Where ``vcan0`` can be replaced with any other valid interface name.
Please read the SocketCAN documentation for more information.
"""

from sys import platform as _platform

if _platform == "linux":
    from ._socketcan import SocketCANMedia as SocketCANMedia
