<div align="center">

<img src="https://opencyphal.org/favicon-192.png" width="60px">

<h1>Cyphal in Python</h1>

_pub/sub without steroids_

[![Website](https://img.shields.io/badge/website-opencyphal.org-black?color=1700b3)](https://opencyphal.org/)
[![Forum](https://img.shields.io/discourse/https/forum.opencyphal.org/users.svg?logo=discourse&color=1700b3)](https://forum.opencyphal.org)

</div>

-----

Python implementation of the [Cyphal](https://opencyphal.org) stack that runs on GNU/Linux, Windows, and macOS.

**WORK IN PROGRESS**: The work on v2 is still ongoing and the new version is not yet ready for production use.
Users seeking stability should continue using PyCyphal v1. The two versions are wire-compatible on Cyphal/CAN.

## Usage

```
pip install pycyphal2
```

```python
import pycyphal2
import pycyphal2.udp

async def main():
    # Set up the local node.
    transport = pycyphal2.udp.new()
    node = pycyphal2.new(transport, home="my_node")
    
    # Subscribe to a topic.
    sub = node.subscribe("my/topic")
    async for arrival in sub:
        print(
            f"Received message at {arrival.timestamp.s:.3f} published by node {arrival.breadcrumb.remote_id:016x} "
            f"on topic {arrival.breadcrumb.topic.name} with contents:",
            arrival.message.hex(),
            sep="\n"
        )
```

See `examples/` for complete runnable usage examples.
