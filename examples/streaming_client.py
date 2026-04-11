#!/usr/bin/env python3
"""
Send one streaming request over Cyphal/UDP and print JSONL responses.
Usage:
    python examples/streaming_client.py
    python examples/streaming_client.py --count 3 --period 0.2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from pycyphal2 import DeliveryError, Instant, LivenessError, Node, Response, SendError
from pycyphal2.udp import UDPTransport

NAME = f"{Path(__file__).stem}/"  # The trailing separator ensures that a random ID will be added.
REQUEST_DEADLINE = 10.0


def _decode_response(response: Response) -> dict[str, object] | None:
    try:
        obj = json.loads(response.message.decode("utf8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logging.warning("dropping malformed response from %016x seq=%d", response.remote_id, response.seqno)
        return None
    if not isinstance(obj, dict):
        logging.warning("dropping malformed response from %016x seq=%d", response.remote_id, response.seqno)
        return None
    return obj


async def run(count: int, period: float, timeout: float) -> None:
    transport = UDPTransport.new()
    node = Node.new(transport, NAME)
    pub = node.advertise("demo/stream")
    stop_after = count if count <= 1 else count - 1
    stream = None
    logging.info("streaming client ready: count=%d period=%f", count, period)
    try:
        request = json.dumps({"count": count, "period": period}).encode("utf8")
        try:
            stream = await pub.request(Instant.now() + REQUEST_DEADLINE, timeout, request)
        except DeliveryError:
            logging.info("request delivery failed before the response stream started")
            return
        except SendError as ex:
            logging.warning("request send failed: %s", ex)
            return

        received = 0
        try:
            async for response in stream:
                payload = _decode_response(response)
                if payload is None:
                    continue
                line = {
                    "ts": round(response.timestamp.s, 6),
                    "remote_id": response.remote_id,
                    "seqno": response.seqno,
                    **payload,
                }
                sys.stdout.write(json.dumps(line) + "\n")
                sys.stdout.flush()
                received += 1
                if received >= stop_after:
                    if stop_after < count:
                        logging.info("closing stream early after %d response(s)", received)
                        stream.close()
                        await asyncio.sleep(max(1.0, 2.0 * period))
                    else:
                        logging.info("stream consumed: %d response(s)", received)
                        stream.close()
                    return
        except LivenessError:
            logging.info("response timeout after %d response(s)", received)
        except DeliveryError:
            logging.info("request delivery failed after %d response(s)", received)
        except SendError as ex:
            logging.warning("request send failed after %d response(s): %s", received, ex)
    finally:
        if stream is not None:
            stream.close()
        pub.close()
        node.close()
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Send one streaming request over Cyphal/UDP.")
    parser.add_argument("--count", type=int, default=10, help="Requested response count, default: 10")
    parser.add_argument("--period", type=float, default=0.5, help="Requested response period [second]")
    parser.add_argument(
        "--timeout", type=float, default=2.0, help="Max idle gap between responses, aka liveness timeout [second]"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s: %(message)s")
    try:
        asyncio.run(run(args.count, args.period, args.timeout))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
