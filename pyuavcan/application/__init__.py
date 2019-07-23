#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

# noinspection PyUnresolvedReferences
"""
The application module contains application-level protocol entities.
This module is not imported automatically because it depends on DSDL generated packages,
particularly on that of the standard root namespace ``uavcan``,
so the user must explicitly import ``pyuavcan.application`` after the environment is set up appropriately.
This is unlike all other top-level modules in the PyUAVCAN library, which are always imported automatically.

>>> import pyuavcan
>>> pyuavcan.transport   # Works.
<module ...>
>>> pyuavcan.application
Traceback (most recent call last):
    ...
AttributeError: module 'pyuavcan' has no attribute 'application'
>>> import pyuavcan.application  # Will fail unless the DSDL package "uavcan" is generated and importable.

Many classes contained here affect the state of the bus, e.g., by publishing data or responding to service
requests. It is expected that some applications may need to complete early initialization procedures before
they are ready to begin interaction with the outside world; hence, many classes in this module are equipped
with a method `start() -> None`, which must be invoked once in order to bring their instance into functional
state. The instance will remain operational until `close() -> None` is invoked on it. Both methods are idempotent.
"""

from ._node import Node as Node, NodeInfo as NodeInfo

from . import heartbeat_publisher as heartbeat_publisher
