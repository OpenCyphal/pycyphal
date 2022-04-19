# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import random
import asyncio
import pytest
import pycyphal
from pycyphal.transport.loopback import LoopbackTransport
from pycyphal.presentation import Presentation
from pycyphal.presentation.subscription_synchronizer import get_timestamp_field
from pycyphal.presentation.subscription_synchronizer.monotonic_clustering import MonotonicClusteringSynchronizer


async def _unittest_timestamped(compiled: list[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from uavcan.si.sample import force, power, angle
    from uavcan.time import SynchronizedTimestamp_1

    _ = compiled
    asyncio.get_running_loop().slow_callback_duration = 5.0

    pres = Presentation(LoopbackTransport(1234))

    pub_a = pres.make_publisher(force.Scalar_1, 2000)
    pub_b = pres.make_publisher(power.Scalar_1, 2001)
    pub_c = pres.make_publisher(angle.Scalar_1, 2002)

    sub_a = pres.make_subscriber(pub_a.dtype, pub_a.port_id)
    sub_b = pres.make_subscriber(pub_b.dtype, pub_b.port_id)
    sub_c = pres.make_subscriber(pub_c.dtype, pub_c.port_id)

    synchronizer = MonotonicClusteringSynchronizer([sub_a, sub_b, sub_c], get_timestamp_field, 0.1)
    assert synchronizer.tolerance == pytest.approx(0.1)
    synchronizer.tolerance = 0.5
    assert synchronizer.tolerance == pytest.approx(0.5)

    reference = 0
    cb_count = 0

    def cb(a: force.Scalar_1, b: power.Scalar_1, c: angle.Scalar_1) -> None:
        nonlocal cb_count
        cb_count += 1
        print(synchronizer.tolerance, a, b, c)
        assert reference == round(a.newton)
        assert reference == round(b.watt)
        assert reference == round(c.radian)

    synchronizer.get_in_background(cb)

    random_skew = (-0.2, -0.1, 0.0, +0.1, +0.2)

    def ts() -> SynchronizedTimestamp_1:
        return SynchronizedTimestamp_1(round((reference + random.choice(random_skew)) * 1e6))

    reference += 1
    await pub_a.publish(force.Scalar_1(ts(), reference))
    await pub_b.publish(power.Scalar_1(ts(), reference))
    await pub_c.publish(angle.Scalar_1(ts(), reference))
    await asyncio.sleep(0.1)
    assert 1 == cb_count

    reference += 1
    await pub_c.publish(angle.Scalar_1(ts(), reference))  # Reordered.
    await pub_b.publish(power.Scalar_1(ts(), reference))
    await pub_a.publish(force.Scalar_1(ts(), reference))
    await asyncio.sleep(0.1)
    assert 2 == cb_count

    reference += 1
    await pub_b.publish(power.Scalar_1(ts(), 999999999))  # Incorrect, will be overridden next.
    await pub_b.publish(power.Scalar_1(ts(), reference))  # Override the incorrect value.
    await asyncio.sleep(0.1)
    await pub_a.publish(force.Scalar_1(ts(), reference))
    await pub_c.publish(angle.Scalar_1(ts(), reference))
    await asyncio.sleep(0.1)
    assert 3 == cb_count

    reference += 1
    await pub_a.publish(force.Scalar_1(ts(), reference))
    # b skip
    await pub_c.publish(angle.Scalar_1(ts(), reference))
    await asyncio.sleep(0.1)
    assert 3 == cb_count

    reference += 1
    # a skip
    await pub_b.publish(power.Scalar_1(ts(), reference))
    await pub_c.publish(angle.Scalar_1(ts(), reference))
    await asyncio.sleep(0.1)
    assert 3 == cb_count

    for i in range(10):
        reference += 1
        await pub_a.publish(force.Scalar_1(ts(), reference))
        await pub_b.publish(power.Scalar_1(ts(), reference))
        await pub_c.publish(angle.Scalar_1(ts(), reference))
        await asyncio.sleep(0.1)
        assert 4 + i == cb_count

    pres.close()
    await asyncio.sleep(1.0)
