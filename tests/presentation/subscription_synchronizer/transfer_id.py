# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import asyncio
import pycyphal
from pycyphal.transport.loopback import LoopbackTransport
from pycyphal.presentation import Presentation
from pycyphal.presentation.subscription_synchronizer.transfer_id import TransferIDSynchronizer


async def _unittest_a(compiled: list[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from uavcan.si.unit import force, power, angle

    _ = compiled
    asyncio.get_running_loop().slow_callback_duration = 5.0

    pres = Presentation(LoopbackTransport(1234))

    pub_a = pres.make_publisher(force.Scalar_1, 2000)
    pub_b = pres.make_publisher(power.Scalar_1, 2001)
    pub_c = pres.make_publisher(angle.Scalar_1, 2002)

    sub_a = pres.make_subscriber(pub_a.dtype, pub_a.port_id)
    sub_b = pres.make_subscriber(pub_b.dtype, pub_b.port_id)
    sub_c = pres.make_subscriber(pub_c.dtype, pub_c.port_id)

    synchronizer = TransferIDSynchronizer([sub_a, sub_b, sub_c])

    reference = 0
    cb_count = 0

    def cb(a: force.Scalar_1, b: power.Scalar_1, c: angle.Scalar_1) -> None:
        nonlocal cb_count
        cb_count += 1
        print(a, b, c)
        assert reference == round(a.newton)
        assert reference == round(b.watt)
        assert reference == round(c.radian)

    synchronizer.get_in_background(cb)

    reference += 1
    await pub_a.publish(force.Scalar_1(reference))
    await pub_b.publish(power.Scalar_1(reference))
    await pub_c.publish(angle.Scalar_1(reference))
    await asyncio.sleep(0.1)
    assert 1 == cb_count

    reference += 1
    await pub_c.publish(angle.Scalar_1(reference))  # Reordered.
    await pub_b.publish(power.Scalar_1(reference))
    await pub_a.publish(force.Scalar_1(reference))
    await asyncio.sleep(0.1)
    assert 2 == cb_count

    reference += 1
    await pub_a.publish(force.Scalar_1(reference))
    # b skip
    await pub_c.publish(angle.Scalar_1(reference))
    await asyncio.sleep(0.1)
    assert 2 == cb_count

    pres.close()
    await asyncio.sleep(1.0)
