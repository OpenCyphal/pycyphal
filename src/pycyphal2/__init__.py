"""
PyCyphal2 -- Python implementation of the Cyphal v1.1 session layer.

All public entities of the library are directly exposed at the top level, here;
implementations of the public interfaces are hidden and private.
One exception applies to transport-specific direct submodules like ``pycyphal2.udp``, ``pycyphal2.can`` --
the user should import only the needed transports manually.

To start using the library, the application will invoke ``pycyphal2.Node.new()`` after constructing a transport,
such as ``pycyphal2.udp.UDPTransport.new()``.
"""

from __future__ import annotations

from ._api import *
from ._transport import Transport as Transport
from ._transport import TransportArrival as TransportArrival
from ._transport import SubjectWriter as SubjectWriter

__version__ = "2.0.0.dev0"
