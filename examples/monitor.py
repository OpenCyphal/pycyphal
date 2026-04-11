#!/usr/bin/env python3
"""
Discover all topics on the Cyphal network and display them in a live terminal view.
Usage:
    python examples/monitor.py
    python examples/monitor.py --transport socketcan:vcan0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

from pycyphal2 import Node, Topic, Transport

NAME = f"{Path(__file__).stem}/"
SCOUT_INTERVAL = 10.0
DISPLAY_INTERVAL = 2.0
EVICTION_TIMEOUT = 600.0


def make_node(transport_spec: str) -> Node:
    if transport_spec == "udp":
        from pycyphal2.udp import UDPTransport

        transport: Transport = UDPTransport.new()
    elif transport_spec.startswith("socketcan:"):
        from pycyphal2.can import CANTransport
        from pycyphal2.can.socketcan import SocketCANInterface

        transport = CANTransport.new(SocketCANInterface(transport_spec.split(":", 1)[1]))
    else:
        raise ValueError(f"Unknown transport {transport_spec!r}")

    return Node.new(transport, NAME)


async def run(transport_spec: str) -> None:
    # topic_name -> (topic_hash, last_seen_monotonic, gossip_count)
    topics: dict[str, tuple[int, float, int]] = {}

    def on_gossip(topic: Topic) -> None:
        name = topic.name
        prev = topics.get(name)
        count = (prev[2] + 1) if prev else 1
        topics[name] = (topic.hash, time.monotonic(), count)

    node = make_node(transport_spec)
    _mon = node.monitor(on_gossip)

    async def scout_loop() -> None:
        while True:
            try:
                await node.scout("/>")
            except Exception:
                logging.debug("Scout failed", exc_info=True)
            await asyncio.sleep(SCOUT_INTERVAL)

    async def display_loop() -> None:
        while True:
            await asyncio.sleep(DISPLAY_INTERVAL)
            now = time.monotonic()
            # Evict stale topics.
            for name in [n for n, (_, ts, _) in topics.items() if now - ts > EVICTION_TIMEOUT]:
                del topics[name]
            # Clear screen and home cursor.
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.write("#\tHASH\t\t\tCOUNT\tAGO\tNAME\n")
            for idx, name in enumerate(sorted(topics), 1):
                th, ts, count = topics[name]
                age = int(now - ts)
                sys.stdout.write(f"{idx}\t{th:016x}\t{count}\t{age // 60:02d}:{age % 60:02d}\t{name}\n")
            sys.stdout.flush()

    await asyncio.gather(scout_loop(), display_loop())


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor all topics on the Cyphal network.")
    parser.add_argument("--transport", default="udp", help="Transport: 'udp' (default) or 'socketcan:<iface>'")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.transport))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
