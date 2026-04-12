"""
[Cyphal](https://opencyphal.org) in Python —
decentralized real-time pub/sub with tunable reliability, service discovery, and zero configuration.
Works anywhere, [including baremetal MCUs](https://github.com/OpenCyphal-Garage/cy).

Supports various transports such as Ethernet (UDP) and CAN FD with optional redundancy.

## Installation

Optional features inside the brackets can be removed if not needed; see `pyproject.toml` for the full list:

```
pip install pycyphal2[udp,pythoncan]
```

## Usage

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

### Name resolution

The topic naming system shares many similarities with ROS.
A valid name contains printable ASCII characters except space (ASCII codes [33, 126]).
Normalized names do not have leading or trailing segment separators `/` and do not have consecutive separators.
Every node should have a unique name, which is called its *home*; home substitution is done via `~/`.

| Input name        | Namespace | Home | Remap              | Resolved name         | Note                             |
| ----------------- | --------- | ---- | ------------------ | --------------------- | -------------------------------- |
| `foo/bar`         | `ns`      | `me` |                    | `ns/foo/bar`          | Relative name                    |
| `/foo//bar/`      | `ns`      | `me` |                    | `foo/bar`             | Absolute name; namespace ignored |
| `~/foo/bar`       | `ns`      | `me` |                    | `me/foo/bar`          | Homeful name                     |
| `sensor/*/temp`   | `diag`    | `me` |                    | `diag/sensor/*/temp`  | Pattern with `*`                 |
| `/sensor/>`       | `diag`    | `me` |                    | `sensor/>`            | Pattern with trailing `>`        |
| `foo/bar`         | `ns`      | `me` | `foo/bar=~/zoo`    | `me/zoo`              | Remap first, then resolve        |

Only exact `~` or `~/...` is homeful; `~ns` is literal. A matching remap overrides pinning.
Pins are allowed only on verbatim names, not on patterns.

Environment variables that control name remapping:

- `CYPHAL_NAMESPACE` — default namespace prepended to relative topic names.
- `CYPHAL_REMAP` — topic name remappings (`from=to` pairs, whitespace-separated).

See also :meth:`Node.remap`.

### Publish

Publication is best-effort by default. Pass `reliable=True` when publishing to retry delivery until
acknowledged by every known subscriber or until the deadline; if the remote side does not acknowledge in time,
:class:`DeliveryError` is raised.

```python
pub = node.advertise("sensor/temperature")
await pub(Instant.now() + 1.0, b"payload", reliable=True)
```

### Subscribe

Subscriptions normally yield messages as soon as they arrive. Set `reordering_window` [seconds] on
:meth:`Node.subscribe` to allow delaying out-of-order messages to reconstruct the original publication order.
This is useful for sensor feeds and state estimators.

```python
sub = node.subscribe("sensor/temperature", reordering_window=0.1)
```

Pattern matching is supported: use `*` to match one name segment (e.g., `sensor/*/temperature`)
and a trailing `>` to match zero or more trailing segments (e.g., `sensor/>`).
Pattern subscribers automatically join matching topics as they appear, and unsubscribe as they disappear.

```python
sub = node.subscribe("sensor/*/temperature")
async for arrival in sub:
    topic = arrival.breadcrumb.topic
    captures = sub.substitutions(topic)
    print(topic.name, captures)  # [('engine', 1)], where 1 is the pattern segment index
```

### RPC & streaming

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

### Topic pinning

Topics may be pinned to a specific subject-ID using `name#1234` to bypass automatic assignment.
This is useful for applications where a high degree of determinism is required and for Cyphal/CAN v1.0 interoperability.
Pattern names (e.g., `sensor/*/temperature/>`) cannot be pinned.

To join a Cyphal/CAN v1.0 subject, use topic name of the form `subject_id#subject_id`; e.g., `7509#7509`.

```python
pub = node.advertise("motor/status#1234")
sub = node.subscribe("1234#1234")
```

Old Cyphal/CAN v1.0 nodes do not participate in the topic discovery protocol,
so topics joined only by such nodes are not discoverable by pattern subscribers.

## Remarks

Cyphal does not define a serialization format. Previous versions used to define the DSDL format but it has been
extracted into an independent project, and Cyphal was made serialization-agnostic in v1.1+.

PyCyphal v2 is published on PyPI as [`pycyphal2`](https://pypi.org/project/pycyphal2/)
to enable coexistence with the original [`pycyphal` v1](https://pypi.org/project/pycyphal/)
in the same Python environment.
The two packages have radically different APIs but are wire-compatible on Cyphal/CAN.
The maintenance of the original `pycyphal` package will eventually cease;
existing applications leveraging `pycyphal` should upgrade to the new API of `pycyphal2`.
"""

from __future__ import annotations

from ._api import *
from ._transport import Transport as Transport
from ._transport import TransportArrival as TransportArrival
from ._transport import SubjectWriter as SubjectWriter

__version__ = "2.0.0.dev1"

# pdoc needs __all__ to display re-exported members.
__all__ = [
    _k
    for _k, _v in vars().items()
    if not _k.startswith("_")
    and _k not in {"annotations", "TYPE_CHECKING"}
    and (getattr(_v, "__module__", None) or "").startswith(__name__)
]
