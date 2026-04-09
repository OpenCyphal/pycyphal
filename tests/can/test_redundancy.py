from __future__ import annotations

import asyncio

import pycyphal2
from pycyphal2 import Instant, Priority
from pycyphal2.can import CANTransport
from tests.can._support import MockCANBus, MockCANInterface, wait_for


def _force_distinct_ids(a: CANTransport, b: CANTransport) -> None:
    if a.id != b.id:
        return
    b._local_node_id = (a.id % 127) + 1  # type: ignore[attr-defined]
    b._refresh_filters()  # type: ignore[attr-defined]


async def test_duplicate_ingress_is_delivered_once() -> None:
    bus = MockCANBus()
    pub = CANTransport.new([MockCANInterface(bus, "pa"), MockCANInterface(bus, "pb")])
    sub = CANTransport.new([MockCANInterface(bus, "sa"), MockCANInterface(bus, "sb")])
    _force_distinct_ids(pub, sub)
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(300, arrivals.append)
    writer = pub.subject_advertise(300)

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"redundant")
    await wait_for(lambda: len(arrivals) == 1)
    await asyncio.sleep(0.02)

    assert len(arrivals) == 1
    assert arrivals[0].message == b"redundant"
    writer.close()
    pub.close()
    sub.close()


async def test_duplicate_multiframe_ingress_is_delivered_once() -> None:
    bus = MockCANBus()
    pub = CANTransport.new([MockCANInterface(bus, "pa"), MockCANInterface(bus, "pb")])
    sub = CANTransport.new([MockCANInterface(bus, "sa"), MockCANInterface(bus, "sb")])
    _force_distinct_ids(pub, sub)
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(9001, arrivals.append)
    writer = pub.subject_advertise(9001)
    payload = bytes(range(40))

    await writer(Instant.now() + 1.0, Priority.HIGH, payload)
    await wait_for(lambda: len(arrivals) == 1)
    await asyncio.sleep(0.02)

    assert len(arrivals) == 1
    assert arrivals[0].message == payload
    writer.close()
    pub.close()
    sub.close()


async def test_publish_succeeds_when_one_interface_rejects_transiently() -> None:
    bus = MockCANBus()
    pub_a = MockCANInterface(bus, "pa")
    pub_b = MockCANInterface(bus, "pb", transient_enqueue_failures=1)
    sub = CANTransport.new(MockCANInterface(bus, "sub"))
    pub = CANTransport.new([pub_a, pub_b])
    _force_distinct_ids(pub, sub)
    arrivals: list[pycyphal2.TransportArrival] = []
    sub.subject_listen(302, arrivals.append)
    writer = pub.subject_advertise(302)

    await writer(Instant.now() + 1.0, Priority.NOMINAL, b"hi")
    await wait_for(lambda: len(arrivals) == 1)

    assert arrivals[0].message == b"hi"
    assert len(pub.interfaces) == 2
    writer.close()
    pub.close()
    sub.close()


async def test_receive_failure_evicts_one_interface_but_transport_survives() -> None:
    bus = MockCANBus()
    sub_a = MockCANInterface(bus, "sa", fail_receive=True)
    sub_b = MockCANInterface(bus, "sb")
    transport = CANTransport.new([sub_a, sub_b])

    await wait_for(lambda: len(transport.interfaces) == 1)

    assert transport.closed is False
    assert transport.interfaces[0] is sub_b
    transport.close()
