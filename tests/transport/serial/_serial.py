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
import logging
import pyuavcan.transport
# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.serial import SerialTransport, SerialTransportStatistics, SerialFrame


@pytest.mark.asyncio    # type: ignore
async def _unittest_serial_transport(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    get_monotonic = asyncio.get_event_loop().time

    service_multiplication_factor = 2

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            local_node_id=None,
                            mtu=1)

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            local_node_id=None,
                            service_transfer_multiplier=10000)

    with pytest.raises(pyuavcan.transport.InvalidMediaConfigurationError):
        _ = SerialTransport(serial_port=serial.serial_for_url('loop://', do_not_open=True), local_node_id=None)

    tr = SerialTransport(serial_port='loop://', local_node_id=None, mtu=1024)

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id is None
    assert tr.serial_port.is_open

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert list(xml.etree.ElementTree.fromstring(tr.descriptor).itertext()) == ['loop://']

    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=4096,
        mtu=1024,
    )

    assert tr.sample_statistics() == SerialTransportStatistics()

    sft_capacity = 1024

    payload_single = [_mem('qwertyui'), _mem('01234567')] * (sft_capacity // 16)
    assert sum(map(len, payload_single)) == sft_capacity

    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = sft_capacity * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    #
    # Instantiate session objects.
    #
    meta = PayloadMetadata(10000)

    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None),
                                                          meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)
    assert subscriber_selective is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)
    assert server_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)
    assert client_requester is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)
    assert client_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)

    print('INPUTS:', tr.input_sessions)
    print('OUTPUTS:', tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, client_requester}
    assert tr.sample_statistics() == SerialTransportStatistics()

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
    assert rx_transfer.fragmented_payload == [b''.join(payload_single)]

    print(tr.sample_statistics())
    assert tr.sample_statistics().in_bytes >= 32 + sft_capacity + 2
    assert tr.sample_statistics().in_frames == 1
    assert tr.sample_statistics().in_out_of_band_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 1
    assert tr.sample_statistics().out_transfers == 1
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

    #
    # Replace the transport with a different one where the local node-ID is not None.
    #
    tr = SerialTransport(serial_port='loop://', local_node_id=3210, mtu=1024)
    assert tr.local_node_id == 3210

    #
    # Re-instantiate session objects because the transport instances have been replaced.
    #
    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)
    assert server_responder is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)

    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)
    assert client_requester is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta)

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)
    assert client_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta)

    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}
    assert tr.sample_statistics() == SerialTransportStatistics()

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
    assert tr.sample_statistics().in_bytes >= (32 * 3 + payload_x3_size_bytes + 2) * service_multiplication_factor
    assert tr.sample_statistics().in_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().in_out_of_band_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().out_transfers == 1 * service_multiplication_factor
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
    assert tr.sample_statistics().in_bytes >= (32 * 3 + payload_x3_size_bytes + 2) * service_multiplication_factor
    assert tr.sample_statistics().in_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().in_out_of_band_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().out_transfers == 1 * service_multiplication_factor
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
    # Out-of-band data test.
    #
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.serial.__name__):
        stats_reference = tr.sample_statistics()

        grownups = b"Aren't there any grownups at all? - No grownups!"

        # The frame delimiter is needed to force new frame into the state machine.
        tr.serial_port.write(grownups + bytes([SerialFrame.FRAME_DELIMITER_BYTE]))
        stats_reference.in_bytes += len(grownups) + 1
        stats_reference.in_out_of_band_bytes += len(grownups)

        # Wait for the reader thread to catch up.
        assert None is await subscriber_selective.receive_until(get_monotonic() + 0.2)
        assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.2)
        assert None is await server_listener.receive_until(get_monotonic() + 0.2)
        assert None is await client_listener.receive_until(get_monotonic() + 0.2)

        print(tr.sample_statistics())
        assert tr.sample_statistics() == stats_reference

        # The frame delimiter is needed to force new frame into the state machine.
        tr.serial_port.write(bytes([0xFF, 0xFF, SerialFrame.FRAME_DELIMITER_BYTE]))
        stats_reference.in_bytes += 3
        stats_reference.in_out_of_band_bytes += 2

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
        _ = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


@pytest.mark.asyncio    # type: ignore
async def _unittest_serial_transport_monitoring(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer
    from pyuavcan.transport import Priority, Timestamp, OutputSessionSpecifier
    from pyuavcan.transport.serial import SerialFrame

    get_monotonic = asyncio.get_event_loop().time

    tr = SerialTransport(serial_port='loop://', local_node_id=42, mtu=1024, service_transfer_multiplier=2)
    sft_capacity = 1024
    payload_single = [_mem('qwertyui'), _mem('01234567')] * (sft_capacity // 16)
    assert sum(map(len, payload_single)) == sft_capacity
    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = sft_capacity * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None),
                                        PayloadMetadata(10000))
    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210),
        PayloadMetadata(10000)
    )

    events: typing.List[object] = []
    events2: typing.List[object] = []
    assert not tr.monitoring_enabled
    tr.enable_monitoring(events.append)
    tr.enable_monitoring(events2.append)
    assert tr.monitoring_enabled
    assert events == []
    assert events2 == []

    #
    # Multi-frame message.
    #
    ts = Timestamp.now()
    assert await broadcaster.send_until(
        Transfer(timestamp=ts,
                 priority=Priority.LOW,
                 transfer_id=777,
                 fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() + 5.0
    )
    await asyncio.sleep(0.1)
    assert events == events2
    a, b, c, d = events                 # Send three -- one event, receive three -- three events.
    assert isinstance(a, SerialFrame)
    assert isinstance(b, SerialFrame)
    assert isinstance(c, SerialFrame)
    assert isinstance(d, tuple)
    tx_ts, (tx_a, tx_b, tx_c) = d
    assert isinstance(tx_ts, Timestamp)
    assert isinstance(tx_a, SerialFrame)
    assert isinstance(tx_b, SerialFrame)
    assert isinstance(tx_c, SerialFrame)

    assert a.transfer_id == 777
    assert b.transfer_id == 777
    assert c.transfer_id == 777
    assert a.timestamp.monotonic >= ts.monotonic
    assert b.timestamp.monotonic >= ts.monotonic
    assert c.timestamp.monotonic >= ts.monotonic
    assert a.index == 0
    assert b.index == 1
    assert c.index == 2
    assert not a.end_of_transfer
    assert not b.end_of_transfer
    assert c.end_of_transfer

    assert tx_ts.monotonic >= ts.monotonic
    assert tx_a.timestamp == ts
    assert tx_b.timestamp == ts
    assert tx_c.timestamp == ts
    assert tx_a.transfer_id == 777
    assert tx_b.transfer_id == 777
    assert tx_c.transfer_id == 777
    assert tx_a.timestamp.monotonic >= ts.monotonic
    assert tx_c.timestamp.monotonic >= ts.monotonic
    assert tx_b.timestamp.monotonic >= ts.monotonic
    assert tx_a.index == 0
    assert tx_b.index == 1
    assert tx_c.index == 2
    assert not tx_a.end_of_transfer
    assert not tx_b.end_of_transfer
    assert tx_c.end_of_transfer

    events.clear()
    events2.clear()

    #
    # Single-frame service request with dual frame duplication.
    #
    ts = Timestamp.now()
    assert await client_requester.send_until(
        Transfer(timestamp=ts,
                 priority=Priority.HIGH,
                 transfer_id=888,
                 fragmented_payload=payload_single),
        monotonic_deadline=get_monotonic() + 5.0
    )
    await asyncio.sleep(0.1)
    assert events == events2
    # Send two -- two events, receive two -- two events.
    # Sorting is required because the order of the two events in the middle is not defined: the arrival event
    # may or may not be registered before the emission event depending on how the serial loopback is operating.
    a, b, d, d2 = sorted(events, key=lambda x: not isinstance(x, SerialFrame))
    assert isinstance(a, SerialFrame)
    assert isinstance(b, SerialFrame)
    assert isinstance(d, tuple)
    assert isinstance(d2, tuple)
    tx_ts, (tx_x,) = d
    assert isinstance(tx_ts, Timestamp)
    assert isinstance(tx_x, SerialFrame)
    tx_ts2, (tx_y,) = d2
    assert isinstance(tx_ts2, Timestamp)
    assert isinstance(tx_y, SerialFrame)

    assert a.transfer_id == 888
    assert b.transfer_id == 888
    assert a.timestamp.monotonic >= ts.monotonic
    assert b.timestamp.monotonic >= ts.monotonic
    assert a.index == 0
    assert b.index == 0
    assert a.end_of_transfer
    assert b.end_of_transfer

    assert tx_ts.monotonic >= ts.monotonic
    assert tx_ts.monotonic >= ts.monotonic
    assert tx_x.timestamp == ts
    assert tx_y.timestamp == ts
    assert tx_x.transfer_id == 888
    assert tx_y.transfer_id == 888
    assert tx_x.timestamp.monotonic >= ts.monotonic
    assert tx_y.timestamp.monotonic >= ts.monotonic
    assert tx_x.index == 0
    assert tx_y.index == 0
    assert tx_x.end_of_transfer
    assert tx_y.end_of_transfer

    events.clear()
    events2.clear()

    #
    # Out-of-band data.
    #
    grownups = b"Aren't there any grownups at all? - No grownups!"
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.serial.__name__):
        # The frame delimiter is needed to force new frame into the state machine.
        tr.serial_port.write(grownups + bytes([SerialFrame.FRAME_DELIMITER_BYTE]))
        await asyncio.sleep(1)
    assert events == events2
    oob, = events
    assert isinstance(oob, memoryview)
    assert bytes(oob) == grownups  # The delimiter is (responsibly) consumed by the parser. Bye bye delimiter.

    events.clear()
    events2.clear()


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
