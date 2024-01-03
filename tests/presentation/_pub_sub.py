# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import asyncio
import pytest
import pycyphal
import nunavut_support
from .conftest import TransportFactory


_RX_TIMEOUT = 1.0

pytestmark = pytest.mark.asyncio


async def _unittest_slow_presentation_pub_sub_anon(
    compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo], transport_factory: TransportFactory
) -> None:
    assert compiled
    import uavcan.node
    from pycyphal.transport import Priority

    loop = asyncio.get_running_loop()
    loop.slow_callback_duration = 5.0

    tran_a, tran_b, transmits_anon = transport_factory(None, None)
    assert tran_a.local_node_id is None
    assert tran_b.local_node_id is None

    pres_a = pycyphal.presentation.Presentation(tran_a)
    pres_b = pycyphal.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    sub_heart = pres_b.make_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_client_with_fixed_service_id(uavcan.node.Heartbeat_1_0, 123)
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.get_server_with_fixed_service_id(uavcan.node.Heartbeat_1_0)

    if transmits_anon:
        pub_heart = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    else:
        with pytest.raises(pycyphal.transport.OperationNotDefinedForAnonymousNodeError):
            pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
        pres_a.close()
        pres_b.close()
        return  # The test ends here.

    assert pub_heart._maybe_impl is not None  # pylint: disable=protected-access
    assert pub_heart._maybe_impl.proxy_count == 1  # pylint: disable=protected-access
    pub_heart_new = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart_new._maybe_impl is not None  # pylint: disable=protected-access
    assert pub_heart is not pub_heart_new
    assert pub_heart._maybe_impl is pub_heart_new._maybe_impl  # pylint: disable=protected-access
    assert pub_heart._maybe_impl.proxy_count == 2  # pylint: disable=protected-access
    pub_heart_new.close()
    del pub_heart_new
    assert pub_heart._maybe_impl.proxy_count == 1  # pylint: disable=protected-access

    pub_heart_impl_old = pub_heart._maybe_impl  # pylint: disable=protected-access
    pub_heart.close()
    assert pub_heart_impl_old.proxy_count == 0

    pub_heart = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart._maybe_impl is not pub_heart_impl_old  # pylint: disable=protected-access

    assert pub_heart.transport_session.destination_node_id is None
    assert sub_heart.transport_session.specifier.data_specifier == pub_heart.transport_session.specifier.data_specifier
    assert pub_heart.port_id == nunavut_support.get_fixed_port_id(uavcan.node.Heartbeat_1_0)
    assert sub_heart.dtype is uavcan.node.Heartbeat_1_0

    heart = uavcan.node.Heartbeat_1_0(
        uptime=123456,
        health=uavcan.node.Health_1_0(uavcan.node.Health_1_0.CAUTION),
        mode=uavcan.node.Mode_1_0(uavcan.node.Mode_1_0.OPERATIONAL),
        vendor_specific_status_code=0xC0,
    )
    assert pub_heart.priority == pycyphal.presentation.DEFAULT_PRIORITY
    pub_heart.priority = Priority.SLOW
    assert pub_heart.priority == Priority.SLOW
    await pub_heart.publish(heart)

    item = await sub_heart.receive_for(1)
    assert item
    rx, transfer = item  # type: typing.Any, pycyphal.transport.TransferFrom
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id is None
    assert transfer.priority == Priority.SLOW
    assert transfer.transfer_id == 0

    stat = sub_heart.sample_statistics()
    # Remember that anonymous transfers over redundant transports are NOT deduplicated.
    # Hence, to support the case of redundant transports, we use 'greater or equal' here.
    assert stat.transport_session.transfers >= 1
    assert stat.transport_session.frames >= 1
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages >= 1

    pres_a.close()
    pres_a.close()  # Double-close has no effect
    pres_b.close()
    pres_b.close()  # Double-close has no effect

    # Make sure the transport sessions have been closed properly, this is supremely important.
    assert list(pres_a.transport.input_sessions) == []
    assert list(pres_b.transport.input_sessions) == []
    assert list(pres_a.transport.output_sessions) == []
    assert list(pres_b.transport.output_sessions) == []

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


async def _unittest_slow_presentation_pub_sub(
    compiled: typing.List[pycyphal.dsdl.GeneratedPackageInfo], transport_factory: TransportFactory
) -> None:
    assert compiled
    import uavcan.node
    from test_dsdl_namespace.numpy import Complex_254_255
    from pycyphal.transport import Priority

    loop = asyncio.get_running_loop()
    loop.slow_callback_duration = 5.0

    tran_a, tran_b, _ = transport_factory(123, 42)
    assert tran_a.local_node_id == 123
    assert tran_b.local_node_id == 42

    pres_a = pycyphal.presentation.Presentation(tran_a)
    pres_b = pycyphal.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    pub_heart = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    sub_heart = pres_b.make_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)

    pub_record = pres_b.make_publisher(Complex_254_255, 2222)
    sub_record = pres_a.make_subscriber(Complex_254_255, 2222)
    sub_record2 = pres_a.make_subscriber(Complex_254_255, 2222)
    sub_record3 = pres_a.make_subscriber(Complex_254_255, 2222)
    sub_record4 = pres_a.make_subscriber(Complex_254_255, 2222)

    heart = uavcan.node.Heartbeat_1_0(
        uptime=123456,
        health=uavcan.node.Health_1_0(uavcan.node.Health_1_0.CAUTION),
        mode=uavcan.node.Mode_1_0(uavcan.node.Mode_1_0.OPERATIONAL),
        vendor_specific_status_code=0xC0,
    )

    pub_heart.transfer_id_counter.override(23)
    await pub_heart.publish(heart)
    item = await sub_heart.receive(asyncio.get_running_loop().time() + 1)
    assert item
    rx, transfer = item  # type: typing.Any, pycyphal.transport.TransferFrom
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id == 123
    assert transfer.priority == Priority.NOMINAL
    assert transfer.transfer_id == 23

    stat = sub_heart.sample_statistics()
    assert stat.transport_session.transfers == 1
    assert stat.transport_session.frames >= 1  # 'greater' is needed to accommodate redundant transports.
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages == 1

    await pub_heart.publish(heart)
    item = await sub_heart.receive(asyncio.get_running_loop().time() + 1)
    assert item
    rx, _ = item
    assert repr(rx) == repr(heart)

    await pub_heart.publish(heart)
    rx = (await sub_heart.receive(asyncio.get_event_loop().time() + _RX_TIMEOUT))[0]  # type: ignore
    assert repr(rx) == repr(heart)
    rx = await sub_heart.get(_RX_TIMEOUT)
    assert rx is None

    sub_heart.close()
    sub_heart.close()  # Shall not raise.

    handler_output_async: typing.List[typing.Tuple[Complex_254_255, pycyphal.transport.TransferFrom]] = []
    handler_output_sync: typing.List[typing.Tuple[Complex_254_255, pycyphal.transport.TransferFrom]] = []

    async def handler_async(message: Complex_254_255, cb_transfer: pycyphal.transport.TransferFrom) -> None:
        print("HANDLER ASYNC:", message, cb_transfer)
        handler_output_async.append((message, cb_transfer))

    sub_record2.receive_in_background(handler_async)
    sub_record3.receive_in_background(lambda *a: handler_output_sync.append(a))

    record = Complex_254_255(bytes_=[1, 2, 3, 1])
    assert pub_record.priority == pycyphal.presentation.DEFAULT_PRIORITY
    pub_record.priority = Priority.NOMINAL
    assert pub_record.priority == Priority.NOMINAL
    with pytest.raises(TypeError, match=".*Heartbeat.*"):
        # noinspection PyTypeChecker
        await pub_heart.publish(record)  # type: ignore

    pub_record.publish_soon(record)
    await asyncio.sleep(0.1)  # Needed to make the deferred publication get the message out
    item2 = await sub_record.receive(asyncio.get_running_loop().time() + 1)
    assert item2
    rx, transfer = item2
    assert repr(rx) == repr(record)
    assert transfer.source_node_id == 42
    assert transfer.priority == Priority.NOMINAL
    assert transfer.transfer_id == 0

    msg4 = await sub_record4.get()
    assert msg4
    assert isinstance(msg4, Complex_254_255)
    assert repr(msg4) == repr(record)
    assert not await sub_record4.get()

    # Broken transfer
    stat = sub_record.sample_statistics()
    assert stat.transport_session.transfers == 1
    assert stat.transport_session.frames >= 1  # 'greater' is needed to accommodate redundant transports.
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages == 1

    await pub_record.transport_session.send(
        pycyphal.transport.Transfer(
            timestamp=pycyphal.transport.Timestamp.now(),
            priority=Priority.NOMINAL,
            transfer_id=12,
            fragmented_payload=[memoryview(b"\xFF" * 15)],  # Invalid union tag.
        ),
        loop.time() + 1.0,
    )
    assert (await sub_record.receive(asyncio.get_event_loop().time() + _RX_TIMEOUT)) is None

    stat = sub_record.sample_statistics()
    assert stat.transport_session.transfers == 2
    assert stat.transport_session.frames >= 2  # 'greater' is needed to accommodate redundant transports.
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 1
    assert stat.messages == 1

    # Close the objects explicitly and ensure that they are finalized. This also removes the warnings that some tasks
    # have been removed while pending.
    pub_heart.close()
    sub_record.close()
    sub_record2.close()
    sub_record3.close()
    sub_record4.close()
    pub_record.close()
    await asyncio.sleep(1.1)

    pres_a.close()
    pres_a.close()  # Double-close has no effect
    pres_b.close()
    pres_b.close()  # Double-close has no effect

    # Make sure the transport sessions have been closed properly, this is supremely important.
    assert list(pres_a.transport.input_sessions) == []
    assert list(pres_b.transport.input_sessions) == []
    assert list(pres_a.transport.output_sessions) == []
    assert list(pres_b.transport.output_sessions) == []

    assert len(handler_output_async) == 1
    assert repr(handler_output_async[0][0]) == repr(record)
    assert handler_output_async[0][1].source_node_id == 42
    assert handler_output_async[0][1].transfer_id == 0
    assert handler_output_async[0][1].priority == Priority.NOMINAL

    assert repr(handler_output_async) == repr(handler_output_sync), "Sync handler is not functional"

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.
