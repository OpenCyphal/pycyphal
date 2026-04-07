#!/usr/bin/env python3
"""
Subscribe to a Cyphal topic and print received messages as JSONL to stdout.
Usage:
    python examples/subscribe.py demo/time
    python examples/subscribe.py demo/time --timeout 5.0
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
from pathlib import Path

import pycyphal
import pycyphal.udp

HOME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.


async def run(topic: str, timeout: float) -> None:
    transport = pycyphal.udp.new()
    node = pycyphal.new(transport, home=HOME)
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
    except pycyphal.LivenessError:
        logging.info("Liveness timeout — no messages for %.1f s", timeout)
    finally:
        sub.close()
        node.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Subscribe to a Cyphal topic and print JSONL to stdout.")
    parser.add_argument("topic", help="Topic name to subscribe to, e.g. demo/time")
    parser.add_argument("--timeout", type=float, default=0, help="Liveness timeout in seconds (0 = infinite)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.topic, args.timeout))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
