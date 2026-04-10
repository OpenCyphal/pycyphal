#!/usr/bin/env python3
"""
Publish the current wall-clock time on a Cyphal topic once per second.
Usage:
    python examples/publish_time.py demo/time
    python examples/publish_time.py demo/time --reliable
    python examples/publish_time.py demo/time --count 5
    python examples/publish_time.py demo/time --transport socketcan:vcan0
"""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

from pycyphal2 import Node, Instant, Transport

PUBLISH_TIMEOUT = 10.0
NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.


async def run(transport_spec: str, topic: str, reliable: bool, count: int) -> None:
    # Construct a transport -- this part determines how the node connects to the network.
    if transport_spec == "udp":
        from pycyphal2.udp import UDPTransport

        transport: Transport = UDPTransport.new()
    elif transport_spec.startswith("socketcan:"):
        from pycyphal2.can import CANTransport
        from pycyphal2.can.socketcan import SocketCANInterface

        transport = CANTransport.new(SocketCANInterface(transport_spec.split(":", 1)[1]))
    else:
        raise ValueError(f"Unknown transport {transport_spec!r}")

    node = Node.new(transport, NAME)
    pub = node.advertise(topic)
    logging.info("Publishing on %r via %s (reliable=%s)", topic, transport, reliable)
    try:
        published = 0
        while count == 0 or published < count:
            payload = json.dumps({"t": round(time.time(), 6)}).encode()
            deadline = Instant.now() + PUBLISH_TIMEOUT
            await pub(deadline, payload, reliable=reliable)
            published += 1
            logging.debug("Published #%d: %s", published, payload.decode())
            if count == 0 or published < count:
                await asyncio.sleep(1.0)
    finally:
        pub.close()
        node.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish current time on a Cyphal topic.")
    parser.add_argument("topic", help="Topic name to publish on, e.g. demo/time")
    parser.add_argument("--reliable", action="store_true", help="Use reliable (acknowledged) delivery")
    parser.add_argument("--count", type=int, default=0, help="Number of messages to publish (0 = infinite)")
    parser.add_argument("--transport", default="udp", help="Transport: 'udp' (default) or 'socketcan:<iface>'")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.transport, args.topic, args.reliable, args.count))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
