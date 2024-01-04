# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import asyncio
from typing import Any
import pytest
import pycyphal
from pycyphal.transport import TransferFrom
from pycyphal.transport.loopback import LoopbackTransport, LoopbackInputSession
from pycyphal.presentation import Presentation
from pycyphal.presentation.subscription_synchronizer.transfer_id import TransferIDSynchronizer
import nunavut_support


async def _unittest_basic(compiled: list[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
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


async def _unittest_different_sources(compiled: list[pycyphal.dsdl.GeneratedPackageInfo]) -> None:
    from uavcan.si.unit.force import Scalar_1

    _ = compiled
    asyncio.get_running_loop().slow_callback_duration = 5.0

    pres = Presentation(LoopbackTransport(None))
    sub_a = pres.make_subscriber(Scalar_1, 2000)
    sub_b = pres.make_subscriber(Scalar_1, 2001)

    synchronizer = TransferIDSynchronizer([sub_a, sub_b])

    # These are accepted because node-ID and transfer-ID match.
    await _inject(sub_a, Scalar_1(90), 100, 10)
    await _inject(sub_b, Scalar_1(91), 100, 10)
    await _inject(sub_a, Scalar_1(92), 101, 10)
    await _inject(sub_b, Scalar_1(93), 101, 10)
    await _inject(sub_a, Scalar_1(94), 100, 11)
    await _inject(sub_b, Scalar_1(95), 100, 11)
    # These are not accepted because of the differences.
    await _inject(sub_a, Scalar_1(), 101, 10)
    await _inject(sub_b, Scalar_1(), 102, 10)
    await _inject(sub_a, Scalar_1(), 101, 11)
    await _inject(sub_b, Scalar_1(), 101, 12)
    # These are not accepted because anonymous.
    await _inject(sub_a, Scalar_1(), None, 13)
    await _inject(sub_b, Scalar_1(), None, 14)

    # First successful group.
    res = await synchronizer.receive(asyncio.get_running_loop().time() + 1.0)
    assert res
    ((msg_a, meta_a), (msg_b, meta_b)) = res
    assert isinstance(msg_a, Scalar_1) and isinstance(msg_b, Scalar_1)
    assert isinstance(meta_a, TransferFrom) and isinstance(meta_b, TransferFrom)
    assert msg_a.newton == pytest.approx(90)
    assert msg_b.newton == pytest.approx(91)
    assert meta_a.source_node_id == meta_b.source_node_id == 100
    assert meta_a.transfer_id == meta_b.transfer_id == 10

    # Second successful group.
    res = await synchronizer.receive(asyncio.get_running_loop().time() + 1.0)
    assert res
    ((msg_a, meta_a), (msg_b, meta_b)) = res
    assert isinstance(msg_a, Scalar_1) and isinstance(msg_b, Scalar_1)
    assert isinstance(meta_a, TransferFrom) and isinstance(meta_b, TransferFrom)
    assert msg_a.newton == pytest.approx(92)
    assert msg_b.newton == pytest.approx(93)
    assert meta_a.source_node_id == meta_b.source_node_id == 101
    assert meta_a.transfer_id == meta_b.transfer_id == 10

    # Third successful group.
    res = await synchronizer.receive(asyncio.get_running_loop().time() + 1.0)
    assert res
    ((msg_a, meta_a), (msg_b, meta_b)) = res
    assert isinstance(msg_a, Scalar_1) and isinstance(msg_b, Scalar_1)
    assert isinstance(meta_a, TransferFrom) and isinstance(meta_b, TransferFrom)
    assert msg_a.newton == pytest.approx(94)
    assert msg_b.newton == pytest.approx(95)
    assert meta_a.source_node_id == meta_b.source_node_id == 100
    assert meta_a.transfer_id == meta_b.transfer_id == 11

    # Bad groups rejected.
    assert None is await synchronizer.receive(asyncio.get_running_loop().time() + 1.0)

    pres.close()
    await asyncio.sleep(1.0)


async def _inject(
    sub: pycyphal.presentation.Subscriber[Any],
    msg: Any,
    source_node_id: int | None,
    transfer_id: int,
) -> None:
    tran = TransferFrom(
        timestamp=pycyphal.transport.Timestamp.now(),
        priority=pycyphal.transport.Priority.NOMINAL,
        transfer_id=int(transfer_id),
        fragmented_payload=list(nunavut_support.serialize(msg)),
        source_node_id=source_node_id,
    )
    in_ses = sub.transport_session
    assert isinstance(in_ses, LoopbackInputSession)
    await in_ses.push(tran)
