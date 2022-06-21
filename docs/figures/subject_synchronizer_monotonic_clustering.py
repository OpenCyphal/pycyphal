#!/usr/bin/env python
#
# This script generates a diagram illustrating the operation of the monotonic clustering synchronizer.
# Pipe its output to "neato -T svg > result.svg" to obtain the diagram.
#
# We could run the script at every doc build but I don't want to make the doc build unnecessarily fragile,
# and this is not expected to be updated frequently.
# It is also possible to use an online tool like https://edotor.net.
#
# The reason we don't use hand-drawn diagrams is that they may not accurately reflect the behavior of the synchronizer.
#
# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
from typing import Any, Callable
import random
import asyncio
from pycyphal.transport.loopback import LoopbackTransport
from pycyphal.transport import TransferFrom
from pycyphal.presentation import Presentation
from pycyphal.presentation.subscription_synchronizer import get_timestamp_field
from pycyphal.presentation.subscription_synchronizer.monotonic_clustering import MonotonicClusteringSynchronizer
from uavcan.si.sample.mass import Scalar_1
from uavcan.time import SynchronizedTimestamp_1 as Ts1


async def main() -> None:
    print("digraph {")
    print("node[shape=circle,style=filled,fillcolor=black,fixedsize=1];")
    print("edge[arrowhead=none,penwidth=10,color=black];")

    pres = Presentation(LoopbackTransport(1234))

    pub_a = pres.make_publisher(Scalar_1, 2000)
    pub_b = pres.make_publisher(Scalar_1, 2001)
    pub_c = pres.make_publisher(Scalar_1, 2002)

    f_key = get_timestamp_field

    pres.make_subscriber(pub_a.dtype, pub_a.port_id).receive_in_background(_make_graphviz_printer("red", 0, f_key))
    pres.make_subscriber(pub_b.dtype, pub_b.port_id).receive_in_background(_make_graphviz_printer("green", 1, f_key))
    pres.make_subscriber(pub_c.dtype, pub_c.port_id).receive_in_background(_make_graphviz_printer("blue", 2, f_key))

    sub_a = pres.make_subscriber(pub_a.dtype, pub_a.port_id)
    sub_b = pres.make_subscriber(pub_b.dtype, pub_b.port_id)
    sub_c = pres.make_subscriber(pub_c.dtype, pub_c.port_id)

    synchronizer = MonotonicClusteringSynchronizer([sub_a, sub_b, sub_c], f_key, 0.5)

    def cb(a: Scalar_1, b: Scalar_1, c: Scalar_1) -> None:
        print(f'"{_represent("red", a)}"->"{_represent("green", b)}"->"{_represent("blue", c)}";')

    synchronizer.get_in_background(cb)

    reference = 0
    random_skew = (-0.2, -0.1, 0.0, +0.1, +0.2)

    def ts() -> Ts1:
        return Ts1(round(max(0.0, (reference + random.choice(random_skew))) * 1e6))

    async def advance(step: int = 1) -> None:
        nonlocal reference
        reference += step
        await asyncio.sleep(0.1)

    for _ in range(6):
        await pub_a.publish(Scalar_1(ts(), reference))
        await pub_b.publish(Scalar_1(ts(), reference))
        await pub_c.publish(Scalar_1(ts(), reference))
        await advance()

    for _ in range(10):
        if random.random() < 0.7:
            await pub_a.publish(Scalar_1(ts(), reference))
        if random.random() < 0.7:
            await pub_b.publish(Scalar_1(ts(), reference))
        if random.random() < 0.7:
            await pub_c.publish(Scalar_1(ts(), reference))
        await advance()

    for _ in range(3):
        await pub_a.publish(Scalar_1(ts(), reference))
        await pub_b.publish(Scalar_1(ts(), reference))
        await pub_c.publish(Scalar_1(ts(), reference))
        await advance(3)

    for i in range(22):
        await pub_a.publish(Scalar_1(ts(), reference))
        if i % 3 == 0:
            await pub_b.publish(Scalar_1(ts(), reference))
        if i % 2 == 0:
            await pub_c.publish(Scalar_1(ts(), reference))
        await advance(1)

    pres.close()
    await asyncio.sleep(0.1)
    print("}")


def _represent(color: str, msg: Any) -> str:
    return f"{color}{round(msg.timestamp.microsecond * 1e-6)}"


def _make_graphviz_printer(
    color: str,
    y_pos: float,
    f_key: Callable[[Any], float],
) -> Callable[[Any, TransferFrom], None]:
    def cb(msg: Any, meta: TransferFrom) -> None:
        print(f'"{_represent(color, msg)}"[label="",fillcolor="{color}",pos="{f_key((msg, meta))},{y_pos}!"];')

    return cb


if __name__ == "__main__":
    asyncio.run(main())
