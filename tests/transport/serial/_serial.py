# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import logging
import pytest
import serial
import pyuavcan.transport

# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.serial import SerialTransport, SerialTransportStatistics, SerialFrame
from pyuavcan.transport.serial import SerialCapture

pytestmark = pytest.mark.asyncio


async def _unittest_serial_transport(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    get_monotonic = asyncio.get_event_loop().time

    service_multiplication_factor = 2

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port="loop://", local_node_id=None, mtu=1)

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port="loop://", local_node_id=None, service_transfer_multiplier=10000)

    with pytest.raises(pyuavcan.transport.InvalidMediaConfigurationError):
        _ = SerialTransport(serial_port=serial.serial_for_url("loop://", do_not_open=True), local_node_id=None)

    tr = SerialTransport(serial_port="loop://", local_node_id=None, mtu=1024)

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id is None
    assert tr.serial_port.is_open

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=4096,
        mtu=1024,
    )

    assert tr.sample_statistics() == SerialTransportStatistics()

    sft_capacity = 1024

    payload_single = [_mem("qwertyui"), _mem("01234567")] * (sft_capacity // 16)
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
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)
    assert subscriber_selective is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta
    )
    assert server_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta
    )

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )
    assert client_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )

    print("INPUTS:", tr.input_sessions)
    print("OUTPUTS:", tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster}
    assert tr.sample_statistics() == SerialTransportStatistics()

    #
    # Message exchange test.
    #
    assert await broadcaster.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=77777, fragmented_payload=payload_single
        ),
        monotonic_deadline=get_monotonic() + 5.0,
    )

    rx_transfer = await subscriber_promiscuous.receive(get_monotonic() + 5.0)
    print("PROMISCUOUS SUBSCRIBER TRANSFER:", rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b"".join(payload_single)]

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
        assert await broadcaster.send(
            Transfer(
                timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=77777, fragmented_payload=payload_x3
            ),
            monotonic_deadline=get_monotonic() + 5.0,
        )

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    #
    # Service exchange test.
    #
    with pytest.raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        # Anonymous nodes can't emit service transfers.
        tr.get_output_session(
            OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta
        )

    #
    # Replace the transport with a different one where the local node-ID is not None.
    #
    tr = SerialTransport(serial_port="loop://", local_node_id=3210, mtu=1024)
    assert tr.local_node_id == 3210

    #
    # Re-instantiate session objects because the transport instances have been replaced.
    #
    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta
    )

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )
    assert server_responder is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )

    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta
    )
    assert client_requester is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210), meta
    )

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )
    assert client_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 3210), meta
    )

    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}
    assert tr.sample_statistics() == SerialTransportStatistics()

    assert await client_requester.send(
        Transfer(timestamp=Timestamp.now(), priority=Priority.HIGH, transfer_id=88888, fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() + 5.0,
    )

    rx_transfer = await server_listener.receive(get_monotonic() + 5.0)
    print("SERVER LISTENER TRANSFER:", rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.HIGH
    assert rx_transfer.transfer_id == 88888
    assert len(rx_transfer.fragmented_payload) == 3
    assert b"".join(rx_transfer.fragmented_payload) == b"".join(payload_x3)

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

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
    assert not await broadcaster.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.IMMEDIATE, transfer_id=99999, fragmented_payload=payload_x3
        ),
        monotonic_deadline=get_monotonic() - 5.0,  # The deadline is in the past.
    )

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    print(tr.sample_statistics())
    assert tr.sample_statistics().in_bytes >= (32 * 3 + payload_x3_size_bytes + 2) * service_multiplication_factor
    assert tr.sample_statistics().in_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().in_out_of_band_bytes == 0
    assert tr.sample_statistics().out_bytes == tr.sample_statistics().in_bytes
    assert tr.sample_statistics().out_frames == 3 * service_multiplication_factor
    assert tr.sample_statistics().out_transfers == 1 * service_multiplication_factor
    assert tr.sample_statistics().out_incomplete == 1  # INCREMENTED HERE

    #
    # Selective message exchange test.
    #
    assert await broadcaster.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.IMMEDIATE, transfer_id=99999, fragmented_payload=payload_x3
        ),
        monotonic_deadline=get_monotonic() + 5.0,
    )

    rx_transfer = await subscriber_promiscuous.receive(get_monotonic() + 5.0)
    print("PROMISCUOUS SUBSCRIBER TRANSFER:", rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.IMMEDIATE
    assert rx_transfer.transfer_id == 99999
    assert b"".join(rx_transfer.fragmented_payload) == b"".join(payload_x3)

    rx_transfer = await subscriber_selective.receive(get_monotonic() + 1.0)
    print("SELECTIVE SUBSCRIBER TRANSFER:", rx_transfer)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.IMMEDIATE
    assert rx_transfer.transfer_id == 99999
    assert b"".join(rx_transfer.fragmented_payload) == b"".join(payload_x3)

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    #
    # Out-of-band data test.
    #
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.serial.__name__):
        stats_reference = tr.sample_statistics()

        # The frame delimiter is needed to force new frame into the state machine.
        grownups = b"Aren't there any grownups at all? - No grownups!\x00"
        tr.serial_port.write(grownups)
        stats_reference.in_bytes += len(grownups)
        stats_reference.in_out_of_band_bytes += len(grownups)

        # Wait for the reader thread to catch up.
        assert None is await subscriber_selective.receive(get_monotonic() + 0.2)
        assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.2)
        assert None is await server_listener.receive(get_monotonic() + 0.2)
        assert None is await client_listener.receive(get_monotonic() + 0.2)

        print(tr.sample_statistics())
        assert tr.sample_statistics() == stats_reference

        # The frame delimiter is needed to force new frame into the state machine.
        tr.serial_port.write(bytes([0xFF, 0xFF, SerialFrame.FRAME_DELIMITER_BYTE]))
        stats_reference.in_bytes += 3
        stats_reference.in_out_of_band_bytes += 3

        # Wait for the reader thread to catch up.
        assert None is await subscriber_selective.receive(get_monotonic() + 0.2)
        assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.2)
        assert None is await server_listener.receive(get_monotonic() + 0.2)
        assert None is await client_listener.receive(get_monotonic() + 0.2)

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


async def _unittest_serial_transport_capture(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer
    from pyuavcan.transport import Priority, Timestamp, OutputSessionSpecifier

    get_monotonic = asyncio.get_event_loop().time

    tr = SerialTransport(serial_port="loop://", local_node_id=42, mtu=1024, service_transfer_multiplier=2)
    sft_capacity = 1024
    payload_single = [_mem("qwertyui"), _mem("01234567")] * (sft_capacity // 16)
    assert sum(map(len, payload_single)) == sft_capacity
    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = sft_capacity * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    broadcaster = tr.get_output_session(
        OutputSessionSpecifier(MessageDataSpecifier(2345), None), PayloadMetadata(10000)
    )
    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 3210),
        PayloadMetadata(10000),
    )

    events: typing.List[SerialCapture] = []
    events2: typing.List[pyuavcan.transport.Capture] = []

    def append_events(cap: pyuavcan.transport.Capture) -> None:
        assert isinstance(cap, SerialCapture)
        events.append(cap)

    tr.begin_capture(append_events)
    tr.begin_capture(events2.append)
    assert events == []
    assert events2 == []

    #
    # Multi-frame message.
    #
    ts = Timestamp.now()
    assert await broadcaster.send(
        Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=777, fragmented_payload=payload_x3),
        monotonic_deadline=get_monotonic() + 5.0,
    )
    await asyncio.sleep(0.1)
    assert events == events2
    # Send three, receive three.
    # Sorting is required because the ordering of the events in the middle is not defined: arrival events
    # may or may not be registered before the emission event depending on how the serial loopback is operating.
    a, b, c, d, e, f = sorted(events, key=lambda x: not x.own)
    assert isinstance(a, SerialCapture) and a.own
    assert isinstance(b, SerialCapture) and b.own
    assert isinstance(c, SerialCapture) and c.own
    assert isinstance(d, SerialCapture) and not d.own
    assert isinstance(e, SerialCapture) and not e.own
    assert isinstance(f, SerialCapture) and not f.own

    def parse(x: SerialCapture) -> SerialFrame:
        out = SerialFrame.parse_from_cobs_image(x.fragment)
        assert out is not None
        return out

    assert parse(a).transfer_id == 777
    assert parse(b).transfer_id == 777
    assert parse(c).transfer_id == 777
    assert a.timestamp.monotonic >= ts.monotonic
    assert b.timestamp.monotonic >= ts.monotonic
    assert c.timestamp.monotonic >= ts.monotonic
    assert parse(a).index == 0
    assert parse(b).index == 1
    assert parse(c).index == 2
    assert not parse(a).end_of_transfer
    assert not parse(b).end_of_transfer
    assert parse(c).end_of_transfer

    assert a.fragment.tobytes().strip(b"\x00") == d.fragment.tobytes().strip(b"\x00")
    assert b.fragment.tobytes().strip(b"\x00") == e.fragment.tobytes().strip(b"\x00")
    assert c.fragment.tobytes().strip(b"\x00") == f.fragment.tobytes().strip(b"\x00")

    events.clear()
    events2.clear()

    #
    # Single-frame service request with dual frame duplication.
    #
    ts = Timestamp.now()
    assert await client_requester.send(
        Transfer(timestamp=ts, priority=Priority.HIGH, transfer_id=888, fragmented_payload=payload_single),
        monotonic_deadline=get_monotonic() + 5.0,
    )
    await asyncio.sleep(0.1)
    assert events == events2
    # Send two, receive two.
    # Sorting is required because the order of the two events in the middle is not defined: the arrival event
    # may or may not be registered before the emission event depending on how the serial loopback is operating.
    a, b, c, d = sorted(events, key=lambda x: not x.own)
    assert isinstance(a, SerialCapture) and a.own
    assert isinstance(b, SerialCapture) and b.own
    assert isinstance(c, SerialCapture) and not c.own
    assert isinstance(d, SerialCapture) and not d.own

    assert parse(a).transfer_id == 888
    assert parse(b).transfer_id == 888
    assert a.timestamp.monotonic >= ts.monotonic
    assert b.timestamp.monotonic >= ts.monotonic
    assert parse(a).index == 0
    assert parse(b).index == 0
    assert parse(a).end_of_transfer
    assert parse(b).end_of_transfer

    assert a.fragment.tobytes().strip(b"\x00") == c.fragment.tobytes().strip(b"\x00")
    assert b.fragment.tobytes().strip(b"\x00") == d.fragment.tobytes().strip(b"\x00")

    events.clear()
    events2.clear()

    #
    # Out-of-band data.
    #
    grownups = b"Aren't there any grownups at all? - No grownups!\x00"
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.serial.__name__):
        # The frame delimiter is needed to force new frame into the state machine.
        tr.serial_port.write(grownups)
        await asyncio.sleep(1)
    assert events == events2
    (oob,) = events
    assert isinstance(oob, SerialCapture)
    assert not oob.own
    assert bytes(oob.fragment) == grownups

    events.clear()
    events2.clear()


async def _unittest_serial_spoofing() -> None:
    from pyuavcan.transport import AlienTransfer, AlienSessionSpecifier, AlienTransferMetadata, Priority
    from pyuavcan.transport import MessageDataSpecifier

    tr = pyuavcan.transport.serial.SerialTransport("loop://", None, mtu=1024)

    mon_events: typing.List[pyuavcan.transport.Capture] = []
    assert not tr.capture_active
    tr.begin_capture(mon_events.append)
    assert tr.capture_active

    transfer = AlienTransfer(
        AlienTransferMetadata(
            Priority.IMMEDIATE, 0xBADC0FFEE0DDF00D, AlienSessionSpecifier(1234, None, MessageDataSpecifier(7777))
        ),
        fragmented_payload=[],
    )
    assert await tr.spoof(transfer, monotonic_deadline=asyncio.get_running_loop().time() + 5.0)
    await asyncio.sleep(1.0)
    cap_rx, cap_tx = sorted(mon_events, key=lambda x: typing.cast(SerialCapture, x).own)
    assert isinstance(cap_rx, SerialCapture)
    assert isinstance(cap_tx, SerialCapture)
    assert not cap_rx.own and cap_tx.own
    assert cap_tx.fragment.tobytes() == cap_rx.fragment.tobytes()
    assert 0xBADC0FFEE0DDF00D .to_bytes(8, "little") in cap_rx.fragment.tobytes()
    assert 1234 .to_bytes(2, "little") in cap_rx.fragment.tobytes()
    assert 7777 .to_bytes(2, "little") in cap_rx.fragment.tobytes()

    with pytest.raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError, match=r".*multi-frame.*"):
        transfer = AlienTransfer(
            AlienTransferMetadata(
                Priority.IMMEDIATE, 0xBADC0FFEE0DDF00D, AlienSessionSpecifier(None, None, MessageDataSpecifier(7777))
            ),
            fragmented_payload=[memoryview(bytes(range(256)))] * 5,
        )
        assert await tr.spoof(transfer, monotonic_deadline=asyncio.get_running_loop().time())


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
