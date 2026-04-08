#!/usr/bin/env python3
"""
Publish the current wall-clock time on a Cyphal topic once per second.
Usage:
    python examples/publish_time.py demo/time
    python examples/publish_time.py demo/time --reliable
    python examples/publish_time.py demo/time --count 5
"""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path

import pycyphal2
import pycyphal2.udp

PUBLISH_TIMEOUT = 10.0
HOME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.


async def run(topic: str, reliable: bool, count: int) -> None:
    transport = pycyphal2.udp.new()
    node = pycyphal2.new(transport, home=HOME)
    pub = node.advertise(topic)
    logging.info("Publishing on %r via %s (reliable=%s)", topic, transport, reliable)
    try:
        published = 0
        while count == 0 or published < count:
            payload = json.dumps({"t": round(time.time(), 6)}).encode()
            deadline = pycyphal2.Instant.now() + PUBLISH_TIMEOUT
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
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.topic, args.reliable, args.count))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
