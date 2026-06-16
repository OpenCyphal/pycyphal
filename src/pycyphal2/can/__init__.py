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

Python-CAN is useful when the application runs not on GNU/Linux or already uses `python-can` or needs
[one of its *many* hardware backends](https://python-can.readthedocs.io/en/stable/interfaces.html)
-- GS-USB, SLCAN, PCAN, etc:

```python
import can
from pycyphal2.can import CANTransport
from pycyphal2.can.pythoncan import PythonCANInterface

bus = can.ThreadSafeBus(interface="socketcan", channel="can0")
transport = CANTransport.new(PythonCANInterface(bus))
```

Pass the transport to `pycyphal2.Node.new()` to start a node.

For the available dependencies see the submodules such as `socketcan` et al.
"""

from __future__ import annotations

from ._interface import Filter as Filter
from ._interface import Frame as Frame
from ._interface import Interface as Interface
from ._interface import TimestampedFrame as TimestampedFrame
from ._transport import CANTransport as CANTransport

# Backend submodules importable via pycyphal2.can.pythoncan / pycyphal2.can.socketcan;
# they are not eagerly imported here because they pull in optional dependencies.

__all__ = ["CANTransport", "Frame", "TimestampedFrame", "Filter", "Interface"]
