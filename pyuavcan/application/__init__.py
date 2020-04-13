#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

# noinspection PyUnresolvedReferences
"""
The application module contains application-level protocol entities.
This module is not imported automatically because:

- Some applications may choose to rely on the presentation-level API directly instead of using this higher-level module,
  in which case having it imported would be counter-productive due to increased initialization time, memory footprint,
  and possible reliability issues. The presentation level is the main abstraction layer provided by this library.
  The functionality of this application-level module is built on top of the public user-facing API of the
  presentation layer (no internal interfaces between modules exist). See :mod:`pyuavcan.presentation`.

- The module depends on DSDL generated packages, particularly on that of the standard root namespace ``uavcan``,
  so this module cannot be imported until the required code is generated.

>>> import pyuavcan
>>> pyuavcan.transport   # Works.
<module ...>
>>> pyuavcan.application
Traceback (most recent call last):
    ...
AttributeError: module 'pyuavcan' has no attribute 'application'
>>> import pyuavcan.application  # Will fail unless the DSDL package "uavcan" is generated and importable.

Classes contained here affect the state of the bus by publishing data, responding to service requests, or otherwise.
It is expected that some applications may need to complete early initialization procedures before
they are ready to begin interaction with the outside world.
Hence, many classes in this module are equipped with a method ``start()``,
which must be invoked once to bring the instance into a functional state.
The instance will remain operational until ``close()`` is invoked on it.
Both methods are idempotent.

The main entity of this module is the class :class:`pyuavcan.application.Node`.
There are several nested submodules,
each dedicated to a particular *application-level function* of the UAVCAN protocol,
such as :mod:`pyuavcan.application.heartbeat_publisher`;
each such module shall be imported explicitly.
Read the UAVCAN specification for background.
"""

from ._node import Node as Node, NodeInfo as NodeInfo
