"""
Cyphal/CAN transport — real-time reliable pub/sub over Classic CAN and CAN FD.
Supports various backends such as SocketCAN and Python-CAN.

```python
from pycyphal2.can import CANTransport
# Import the backend you need.
# Beware: optional dependencies may be needed, check pyproject.toml.
from pycyphal2.can.socketcan import SocketCANInterface

transport = CANTransport.new(SocketCANInterface("can0"))
```

Pass the transport to `pycyphal2.Node.new()` to start a node.

For the available dependencies see the submodules such as `socketcan` et al.
"""

from __future__ import annotations

from ._interface import Filter as Filter
from ._interface import Frame as Frame
from ._interface import Interface as Interface
from ._interface import State as State
from ._interface import TimestampedFrame as TimestampedFrame
from ._transport import CANTransport as CANTransport

__all__ = ["CANTransport", "Frame", "TimestampedFrame", "Filter", "State", "Interface"]
