#!/usr/bin/env python3
"""
Subscribe to a Cyphal topic and print received messages as JSONL to stdout.
Usage:
    python examples/subscribe_demo.py demo/time
    python examples/subscribe_demo.py demo/time --timeout 5.0
    python examples/subscribe_demo.py demo/time --transport socketcan:vcan0
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
from pathlib import Path

from pycyphal2 import Node, LivenessError, Transport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.


async def run(transport_spec: str, topic: str, timeout: float) -> None:
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
    sub = node.subscribe(topic)
    if timeout > 0:
        sub.timeout = timeout
    logging.info("Subscribed to %r on %s", topic, transport)
    try:
        async for arrival in sub:
            line = json.dumps(
                {
                    "ts": round(arrival.timestamp.s, 6),
                    "remote_id": arrival.breadcrumb.remote_id,
                    "topic": arrival.breadcrumb.topic.name,
                    "message_b64": base64.b64encode(arrival.message).decode(),
                },
            )
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
            # You can send a response (best-effort or reliable) to the publisher like:
            #   await arrival.breadcrumb(Instant.now() + 1.0, b"payload", reliable=True)
    except LivenessError:
        logging.info("Liveness timeout — no messages for %.1f s", timeout)
    finally:
        sub.close()
        node.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Subscribe to a Cyphal topic and print JSONL to stdout.")
    parser.add_argument("topic", help="Topic name to subscribe to, e.g. demo/time")
    parser.add_argument("--timeout", type=float, default=0, help="Liveness timeout in seconds (0 = infinite)")
    parser.add_argument(
        "--transport",
        default="udp",
        help="Transport: 'udp' (default) or 'socketcan:<iface>'",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.transport, args.topic, args.timeout))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
