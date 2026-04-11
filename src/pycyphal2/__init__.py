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

Publication is best-effort by default. Pass ``reliable=True`` when publishing to retry delivery until
acknowledged by every known subscriber or until the deadline; if the remote side does not acknowledge in time,
:class:`DeliveryError` is raised.

```python
await pub(Instant.now() + 1.0, b"payload", reliable=True)
```

Subscriptions normally yield messages as soon as they arrive. Set ``reordering_window`` [seconds] on
:meth:`Node.subscribe` to allow delaying out-of-order messages to reconstruct the original publication order.
This is useful for sensor feeds and state estimators.

```python
sub = node.subscribe("sensor/temperature", reordering_window=0.1)
```

RPC is layered directly on top of pub/sub. Use :meth:`Publisher.request` to publish a message that expects
responses, and use :attr:`Arrival.breadcrumb` on the subscriber side to send a unicast reply back to the requester.
One request may yield responses from multiple subscribers.

```python
stream = await pub.request(Instant.now() + 1.0, 0.5, b"read")
async for response in stream:
    print(response.message)
```

Streaming is just repeated replying on the same breadcrumb. The requester consumes such replies through
:class:`ResponseStream`; each responder numbers its own responses from zero upward.

```python
await arrival.breadcrumb(Instant.now() + 1.0, b"chunk-1", reliable=True)
await arrival.breadcrumb(Instant.now() + 1.0, b"chunk-2", reliable=True)
```

Cyphal does not define a serialization format. Previous versions used to define the DSDL format but it has been
extracted into an independent project, and Cyphal was made serialization-agnostic in v1.1+.
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
