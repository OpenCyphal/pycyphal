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
from dataclasses import dataclass

from pycyphal2 import Node, Topic, Transport

SCOUT_INTERVAL = 10.0
DISPLAY_INTERVAL = 2.0
EVICTION_TIMEOUT = 600.0


@dataclass(frozen=True)
class TopicInfo:
    last_seen_monotonic: float
    topic: Topic


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

    return Node.new(transport, "monitor/")  # The trailing slash indicates that we want a unique ID at the end.


def _clear() -> str:
    return "\033[2J\033[H" if sys.stdout.isatty() else ("\n" * 3)


def _bright(text: str) -> str:
    return f"\033[1m{text}\033[0m" if sys.stdout.isatty() else text


async def run(transport_spec: str) -> None:
    topics: dict[str, TopicInfo] = {}
    node = make_node(transport_spec)
    subject_id_modulus = node.transport.subject_id_modulus

    def on_gossip(topic: Topic) -> None:
        topics[topic.name] = TopicInfo(last_seen_monotonic=time.monotonic(), topic=topic)

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
            for name in [n for n, info in topics.items() if now - info.last_seen_monotonic > EVICTION_TIMEOUT]:
                del topics[name]
            # Render the display.
            out = [
                _clear(),
                _bright(f"{'#':>3} {'HEARD':<5} {'HASH':<16} {'EVICTIONS':>10} {'SUBJECT-ID':>10} NAME\n"),
            ]
            for idx, name in enumerate(sorted(topics), 1):
                age = int(now - topics[name].last_seen_monotonic)
                age_fmt = f"{age // 60:02d}:{age % 60:02d}"
                t = topics[name].topic
                subject_id = t.subject_id(subject_id_modulus)
                out.append(f"{idx:>3} {age_fmt} {t.hash:016x} {t.evictions:>10} {subject_id:>10} {t.name}\n")
            print("".join(out), end="", flush=True)

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
