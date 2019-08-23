#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import xml.etree.ElementTree
import pytest
import serial
import pyuavcan.transport
# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.serial import SerialTransport, SerialStatistics, SerialFrame


@pytest.mark.asyncio    # type: ignore
async def _unittest_serial_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, SessionSpecifier, ProtocolParameters

    get_monotonic = asyncio.get_event_loop().time

    service_multiplication_factor = 2

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            single_frame_transfer_payload_capacity_bytes=1)

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            service_transfer_multiplier=10000)

    with pytest.raises(pyuavcan.transport.InvalidMediaConfigurationError):
        _ = SerialTransport(serial_port=serial.serial_for_url('loop://', do_not_open=True))

    tr = SerialTransport(serial_port='loop://')

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id is None
    assert tr.serial_port.is_open

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert list(xml.etree.ElementTree.fromstring(tr.descriptor).itertext()) == ['loop://']
    assert str(SerialTransport.DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES) in tr.descriptor

    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        node_id_set_cardinality=4096,
        single_frame_transfer_payload_capacity_bytes=SerialTransport
        .DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES,
    )

    assert tr.sample_statistics() == SerialStatistics()

    sft_capacity = SerialTransport.DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES

    payload_single = [_mem('qwertyui'), _mem('01234567')] * (sft_capacity // 16)
    assert sum(map(len, payload_single)) == sft_capacity

    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = sft_capacity * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    #
    # Instantiate session objects.
    #
    meta = PayloadMetadata(0x_bad_c0ffee_0dd_f00d, 10000)

    broadcaster = tr.get_output_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)
    assert broadcaster is tr.get_output_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)

    subscriber_promiscuous = tr.get_input_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)

    subscriber_selective = tr.get_input_session(SessionSpecifier(MessageDataSpecifier(12345), 3210), meta)
    assert subscriber_selective is tr.get_input_session(SessionSpecifier(MessageDataSpecifier(12345), 3210), meta)

    server_listener = tr.get_input_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)
    assert server_listener is tr.get_input_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    server_responder = tr.get_output_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)
    assert server_responder is tr.get_output_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)

    client_requester = tr.get_output_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)
    assert client_requester is tr.get_output_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)

    client_listener = tr.get_input_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)
    assert client_listener is tr.get_input_session(
        SessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)

    print('INPUTS:', tr.input_sessions)
    print('OUTPUTS:', tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}
    assert tr.sample_statistics() == SerialStatistics()

    #
    # Message exchange test.
    #
    assert await broadcaster.send_until(
        Transfer(timestamp=Timestamp.now(),
                 priority=Priority.LOW,
                 transfer_id=77777,
                 fragmented_payload=payload_single),
        monotonic_deadline=get_monotonic() + 5.0
    )

    rx_transfer = await subscriber_promiscuous.receive_until(get_monotonic() + 5.0)
    print('PROMISCUOUS SUBSCRIBER TRANSFER:', rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b''.join(payload_single)]  # type: ignore

    print(tr.sample_statistics())
    assert tr.sample_statistics().in_bytes >= 32 + sft_capacity + 2
    assert tr.sample_statistics().in_frames == 1
    assert tr.sample_statistics().in_unparsed_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 1
    assert tr.sample_statistics().out_incomplete == 0

    with pytest.raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        # Anonymous nodes can't send multiframe transfers.
        assert await broadcaster.send_until(
            Transfer(timestamp=Timestamp.now(),
                     priority=Priority.LOW,
                     transfer_id=77777,
                     fragmented_payload=payload_x3),
            monotonic_deadline=get_monotonic() + 5.0
        )

    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.1)
    assert None is await server_listener.receive_until(get_monotonic() + 0.1)
    assert None is await client_listener.receive_until(get_monotonic() + 0.1)

    #
    # Service exchange test.
    #
    with pytest.raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        # Anonymous nodes can't emit service transfers.
        assert await client_requester.send_until(
            Transfer(timestamp=Timestamp.now(),
                     priority=Priority.HIGH,
                     transfer_id=88888,
                     fragmented_payload=payload_single),
            monotonic_deadline=get_monotonic() + 5.0
        )

    with pytest.raises(ValueError):
        tr.set_local_node_id(2 ** 64)
    assert tr.local_node_id is None
    tr.set_local_node_id(3210)
    assert tr.local_node_id == 3210
    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        tr.set_local_node_id(42)
    assert tr.local_node_id == 3210

    assert await client_requester.send_until(
        Transfer(timestamp=Timestamp.now(),
                 priority=Priority.HIGH,
                 transfer_id=88888,
                 fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() + 5.0
    )

    rx_transfer = await server_listener.receive_until(get_monotonic() + 5.0)
    print('SERVER LISTENER TRANSFER:', rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.HIGH
    assert rx_transfer.transfer_id == 88888
    assert len(rx_transfer.fragmented_payload) == 3
    assert b''.join(rx_transfer.fragmented_payload) == b''.join(payload_x3)

    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.1)
    assert None is await server_listener.receive_until(get_monotonic() + 0.1)
    assert None is await client_listener.receive_until(get_monotonic() + 0.1)

    print(tr.sample_statistics())
    assert tr.sample_statistics().in_bytes >= \
        (32 + sft_capacity + 2) + (32 * 3 + payload_x3_size_bytes + 2) * service_multiplication_factor
    assert tr.sample_statistics().in_frames == 1 + 3 * service_multiplication_factor
    assert tr.sample_statistics().in_unparsed_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 1 + 3 * service_multiplication_factor
    assert tr.sample_statistics().out_incomplete == 0

    #
    # Write timeout test.
    #
    assert not await broadcaster.send_until(
        Transfer(timestamp=Timestamp.now(),
                 priority=Priority.IMMEDIATE,
                 transfer_id=99999,
                 fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() - 5.0    # The deadline is in the past.
    )

    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.1)
    assert None is await server_listener.receive_until(get_monotonic() + 0.1)
    assert None is await client_listener.receive_until(get_monotonic() + 0.1)

    print(tr.sample_statistics())
    assert tr.sample_statistics().in_bytes >= \
        (32 + sft_capacity + 2) + (32 * 3 + payload_x3_size_bytes + 2) * service_multiplication_factor
    assert tr.sample_statistics().in_frames == 1 + 3 * service_multiplication_factor
    assert tr.sample_statistics().in_unparsed_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 1 + 3 * service_multiplication_factor
    assert tr.sample_statistics().out_incomplete == 1   # INCREMENTED HERE

    #
    # Selective message exchange test.
    #
    assert await broadcaster.send_until(
        Transfer(timestamp=Timestamp.now(),
                 priority=Priority.IMMEDIATE,
                 transfer_id=99999,
                 fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() + 5.0
    )

    rx_transfer = await subscriber_promiscuous.receive_until(get_monotonic() + 5.0)
    print('PROMISCUOUS SUBSCRIBER TRANSFER:', rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.IMMEDIATE
    assert rx_transfer.transfer_id == 99999
    assert b''.join(rx_transfer.fragmented_payload) == b''.join(payload_x3)

    rx_transfer = await subscriber_selective.receive_until(get_monotonic() + 1.0)
    print('SELECTIVE SUBSCRIBER TRANSFER:', rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.IMMEDIATE
    assert rx_transfer.transfer_id == 99999
    assert b''.join(rx_transfer.fragmented_payload) == b''.join(payload_x3)

    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.1)
    assert None is await server_listener.receive_until(get_monotonic() + 0.1)
    assert None is await client_listener.receive_until(get_monotonic() + 0.1)

    #
    # Unparsed data test.
    #
    stats_reference = tr.sample_statistics()

    grownups = b"Aren't there any grownups at all? - No grownups!"

    # The frame delimiter is needed to force new frame in the state machine.
    tr.serial_port.write(grownups + bytes([SerialFrame.FRAME_DELIMITER_BYTE]))
    stats_reference.in_bytes += len(grownups) + 1
    stats_reference.in_unparsed_bytes += len(grownups)

    # Wait for the reader thread to catch up.
    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.2)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.2)
    assert None is await server_listener.receive_until(get_monotonic() + 0.2)
    assert None is await client_listener.receive_until(get_monotonic() + 0.2)

    print(tr.sample_statistics())
    assert tr.sample_statistics() == stats_reference

    # The frame delimiter is needed to force new frame in the state machine.
    tr.serial_port.write(bytes([0xFF, 0xFF, SerialFrame.FRAME_DELIMITER_BYTE]))
    stats_reference.in_bytes += 3
    stats_reference.in_unparsed_bytes += 2

    # Wait for the reader thread to catch up.
    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.2)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.2)
    assert None is await server_listener.receive_until(get_monotonic() + 0.2)
    assert None is await client_listener.receive_until(get_monotonic() + 0.2)

    print(tr.sample_statistics())
    assert tr.sample_statistics() == stats_reference

    #
    # Termination.
    #
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}

    subscriber_promiscuous.close()
    subscriber_promiscuous.close()  # Idempotency.

    assert set(tr.input_sessions) == {subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}

    broadcaster.close()
    broadcaster.close()  # Idempotency.

    assert set(tr.input_sessions) == {subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {server_responder, client_requester}

    tr.close()
    tr.close()  # Idempotency.

    assert not set(tr.input_sessions)
    assert not set(tr.output_sessions)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr.get_output_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr.get_input_session(SessionSpecifier(MessageDataSpecifier(12345), None), meta)


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
