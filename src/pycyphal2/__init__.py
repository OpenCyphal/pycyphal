"""
`Cyphal <https://opencyphal.org>`_ in Python —
decentralized real-time pub/sub with tunable reliability, service discovery, and zero configuration.
Works anywhere, `even baremetal MCUs <https://github.com/OpenCyphal-Garage/cy>`_.

Supports various transports such as Ethernet (UDP) and CAN FD with optional redundancy.
Set up a transport, make a node, publish and subscribe:

```python
from pycyphal2 import Node, Instant
from pycyphal2.udp import UDPTransport

async def main():
    node = Node.new(UDPTransport.new(), "my_node")

    pub = node.advertise("sensor/temperature")
    await pub(Instant.now() + 1.0, b"21.5")

    sub = node.subscribe("sensor/temperature")
    async for arrival in sub:
        print(arrival.message)
```

All public symbols live at the top level — just `import pycyphal2`.
Transport modules (`pycyphal2.udp`, `pycyphal2.can`) are imported separately
so that only the needed dependencies are pulled in.

The source repository contains a collection of runnable examples.

Environment variables control name remapping similar to ROS:

- `CYPHAL_NAMESPACE` — default namespace prepended to relative topic names.
- `CYPHAL_REMAP` — topic name remappings (`from=to` pairs, whitespace-separated).
"""

from __future__ import annotations

from ._api import *
from ._transport import Transport as Transport
from ._transport import TransportArrival as TransportArrival
from ._transport import SubjectWriter as SubjectWriter

__version__ = "2.0.0.dev0"

# pdoc needs __all__ to display re-exported members.
__all__ = [
    _k
    for _k, _v in vars().items()
    if not _k.startswith("_")
    and _k not in {"annotations", "TYPE_CHECKING"}
    and (getattr(_v, "__module__", None) or "").startswith(__name__)
]
