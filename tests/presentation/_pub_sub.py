#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio

import pytest

import pyuavcan
import pyuavcan.transport.can
import tests.transport.can


_RX_TIMEOUT = 10e-3


# noinspection PyProtectedMember
@pytest.mark.asyncio    # type: ignore
async def _unittest_slow_presentation_pub_sub(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) \
        -> None:
    assert generated_packages
    import uavcan.node
    import uavcan.time
    import uavcan.diagnostic
    from pyuavcan.transport import Priority

    bus: typing.Set[tests.transport.can.media.mock.MockMedia] = set()
    media_a = tests.transport.can.media.mock.MockMedia(bus, 8, 1)
    media_b = tests.transport.can.media.mock.MockMedia(bus, 64, 2)      # Look, a heterogeneous setup!
    assert bus == {media_a, media_b}

    tran_a = pyuavcan.transport.can.CANTransport(media_a)
    tran_b = pyuavcan.transport.can.CANTransport(media_b)

    pres_a = pyuavcan.presentation.Presentation(tran_a)
    pres_b = pyuavcan.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    pub_heart = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    sub_heart = pres_b.make_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)

    pub_record = pres_b.make_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
    sub_record = pres_a.make_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
    sub_record2 = pres_a.make_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_client_with_fixed_service_id(uavcan.node.Heartbeat_1_0, 123)  # type: ignore
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.get_server_with_fixed_service_id(uavcan.node.Heartbeat_1_0)  # type: ignore

    assert pub_heart._maybe_impl is not None
    assert pub_heart._maybe_impl.proxy_count == 1
    pub_heart_new = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart_new._maybe_impl is not None
    assert pub_heart is not pub_heart_new
    assert pub_heart._maybe_impl is pub_heart_new._maybe_impl
    assert pub_heart._maybe_impl.proxy_count == 2
    pub_heart_new.close()
    del pub_heart_new
    assert pub_heart._maybe_impl.proxy_count == 1

    pub_heart_impl_old = pub_heart._maybe_impl
    pub_heart.close()
    assert pub_heart_impl_old.proxy_count == 0

    pub_heart = pres_a.make_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    assert pub_heart._maybe_impl is not pub_heart_impl_old

    assert pub_heart.transport_session.destination_node_id is None
    assert sub_heart.transport_session.specifier.data_specifier == pub_heart.transport_session.specifier.data_specifier
    assert pub_heart.port_id == pyuavcan.dsdl.get_fixed_port_id(uavcan.node.Heartbeat_1_0)
    assert sub_heart.dtype is uavcan.node.Heartbeat_1_0

    heart = uavcan.node.Heartbeat_1_0(uptime=123456,
                                      health=uavcan.node.Heartbeat_1_0.HEALTH_CAUTION,
                                      mode=uavcan.node.Heartbeat_1_0.MODE_OPERATIONAL,
                                      vendor_specific_status_code=0xc0fe)
    assert pub_heart.priority == pyuavcan.presentation.DEFAULT_PRIORITY
    pub_heart.priority = Priority.SLOW
    assert pub_heart.priority == Priority.SLOW
    await pub_heart.publish(heart)
    rx, transfer = await sub_heart.receive_with_transfer()  # type: typing.Any, pyuavcan.transport.TransferFrom
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id is None
    assert transfer.priority == Priority.SLOW
    assert transfer.transfer_id == 0

    stat = sub_heart.sample_statistics()
    assert stat.transport_session.transfers == 1
    assert stat.transport_session.frames == 1
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages == 1

    tran_a.set_local_node_id(123)
    tran_b.set_local_node_id(42)

    pub_heart.transfer_id_counter.override(23)
    await pub_heart.publish(heart)
    rx, transfer = await sub_heart.receive_with_transfer()
    assert repr(rx) == repr(heart)
    assert transfer.source_node_id == 123
    assert transfer.priority == Priority.SLOW
    assert transfer.transfer_id == 23

    stat = sub_heart.sample_statistics()
    assert stat.transport_session.transfers == 2
    assert stat.transport_session.frames == 2
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages == 2

    await pub_heart.publish(heart)
    rx = await sub_heart.receive()
    assert repr(rx) == repr(heart)

    await pub_heart.publish(heart)
    rx = await sub_heart.receive_until(asyncio.get_event_loop().time() + _RX_TIMEOUT)
    assert repr(rx) == repr(heart)
    rx = await sub_heart.receive_for(_RX_TIMEOUT)
    assert rx is None

    sub_heart.close()
    sub_heart.close()       # Shall not raise.

    record_handler_output: typing.List[typing.Tuple[uavcan.diagnostic.Record_1_0, pyuavcan.transport.TransferFrom]] = []

    async def record_handler(message: uavcan.diagnostic.Record_1_0,
                             cb_transfer: pyuavcan.transport.TransferFrom) -> None:
        print('RECORD HANDLER:', message, cb_transfer)
        record_handler_output.append((message, cb_transfer))

    sub_record2.receive_in_background(record_handler)

    record = uavcan.diagnostic.Record_1_0(timestamp=uavcan.time.SynchronizedTimestamp_1_0(1234567890),
                                          severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.ALERT),
                                          text='Hello world!')
    assert pub_record.priority == pyuavcan.presentation.DEFAULT_PRIORITY
    pub_record.priority = Priority.NOMINAL
    assert pub_record.priority == Priority.NOMINAL
    with pytest.raises(ValueError, match='.*Heartbeat.*'):
        # noinspection PyTypeChecker
        await pub_heart.publish(record)  # type: ignore

    pub_record.publish_soon(record)
    await asyncio.sleep(0.1)                # Need to make the deferred publication get the message out
    rx, transfer = await sub_record.receive_with_transfer()
    assert repr(rx) == repr(record)
    assert transfer.source_node_id == 42
    assert transfer.priority == Priority.NOMINAL
    assert transfer.transfer_id == 0

    # Broken transfer
    stat = sub_record.sample_statistics()
    assert stat.transport_session.transfers == 1
    assert stat.transport_session.frames == 1
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 0
    assert stat.messages == 1

    await pub_record.transport_session.send_until(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=Priority.NOMINAL,
        transfer_id=12,
        fragmented_payload=[memoryview(b'Broken')],
    ), tran_a.loop.time() + 1.0)
    assert (await sub_record.receive_until(asyncio.get_event_loop().time() + _RX_TIMEOUT)) is None

    stat = sub_record.sample_statistics()
    assert stat.transport_session.transfers == 2
    assert stat.transport_session.frames == 2
    assert stat.transport_session.drops == 0
    assert stat.deserialization_failures == 1
    assert stat.messages == 1

    # Close the objects explicitly and ensure that they are finalized. This also removes the warnings that some tasks
    # have been removed while pending.
    pub_heart.close()
    sub_record.close()
    sub_record2.close()
    pub_record.close()
    await asyncio.sleep(1.1)
    assert pres_a.sessions == []
    assert pres_b.sessions == []

    pres_a.close()
    pres_a.close()  # Double-close has no effect
    pres_b.close()
    pres_b.close()  # Double-close has no effect

    # All disposed of?
    assert list(pres_a.sessions) == []
    assert list(pres_b.sessions) == []

    # Make sure the transport sessions have been closed properly, this is supremely important.
    assert list(pres_a.transport.input_sessions) == []
    assert list(pres_b.transport.input_sessions) == []
    assert list(pres_a.transport.output_sessions) == []
    assert list(pres_b.transport.output_sessions) == []

    assert len(record_handler_output) == 1
    assert repr(record_handler_output[0][0]) == repr(record)
    assert record_handler_output[0][1].source_node_id == 42
    assert record_handler_output[0][1].transfer_id == 0
    assert record_handler_output[0][1].priority == Priority.NOMINAL
