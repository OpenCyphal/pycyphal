# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import ipaddress
import pytest
import pyuavcan.transport

# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.udp import UDPTransport, UDPTransportStatistics


@pytest.mark.asyncio  # type: ignore
async def _unittest_udp_transport_ipv4() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    asyncio.get_running_loop().slow_callback_duration = 5.0

    get_monotonic = asyncio.get_event_loop().time

    with pytest.raises(ValueError):
        _ = UDPTransport("127.0.0.111", mtu=10)

    with pytest.raises(ValueError):
        _ = UDPTransport("127.0.0.111", service_transfer_multiplier=100)

    tr = UDPTransport("127.0.0.111", mtu=9000)
    tr2 = UDPTransport("127.0.0.222", service_transfer_multiplier=2)

    assert tr.local_ip_address == ipaddress.ip_address("127.0.0.111")
    assert tr2.local_ip_address == ipaddress.ip_address("127.0.0.222")

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id == 111
    assert tr2.local_node_id == 222

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert "127.0.0.111" in repr(tr)
    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=65535,
        mtu=9000,
    )

    default_mtu = min(UDPTransport.VALID_MTU_RANGE)
    assert "127.0.0.222" in repr(tr2)
    assert tr2.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=65535,
        mtu=default_mtu,
    )

    assert tr.sample_statistics() == tr2.sample_statistics() == UDPTransportStatistics()

    payload_single = [_mem("qwertyui"), _mem("01234567")] * (default_mtu // 16)
    assert sum(map(len, payload_single)) == default_mtu

    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = default_mtu * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    #
    # Instantiate session objects.
    #
    meta = PayloadMetadata(10000)

    broadcaster = tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 123), meta)
    assert subscriber_selective is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 123), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), None), meta
    )
    assert server_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), None), meta
    )

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 222), meta
    )
    assert server_responder is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 222), meta
    )

    client_requester = tr2.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), 111), meta
    )
    assert client_requester is tr2.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), 111), meta
    )

    client_listener = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 111), meta
    )
    assert client_listener is tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 111), meta
    )

    print("tr :", tr.input_sessions, tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}

    print("tr2:", tr2.input_sessions, tr2.output_sessions)
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {broadcaster, client_requester}

    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)].accepted_datagrams == {}
    assert (
        tr.sample_statistics()
        .received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)]
        .accepted_datagrams
        == {}
    )

    assert (
        tr2.sample_statistics()
        .received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)]
        .accepted_datagrams
        == {}
    )

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

    print("tr :", tr.sample_statistics())
    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)].accepted_datagrams == {222: 1}
    assert (
        tr.sample_statistics()
        .received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)]
        .accepted_datagrams
        == {}
    )
    print("tr2:", tr2.sample_statistics())
    assert (
        tr2.sample_statistics()
        .received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)]
        .accepted_datagrams
        == {}
    )

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    #
    # Service exchange test.
    #
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

    print("tr :", tr.sample_statistics())
    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)].accepted_datagrams == {222: 1}
    assert tr.sample_statistics().received_datagrams[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)
    ].accepted_datagrams == {
        222: 3 * 2
    }  # Deterministic data loss mitigation is enabled, multiplication factor 2
    print("tr2:", tr2.sample_statistics())
    assert (
        tr2.sample_statistics()
        .received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)]
        .accepted_datagrams
        == {}
    )

    #
    # Termination.
    #
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {broadcaster, client_requester}

    subscriber_promiscuous.close()
    subscriber_promiscuous.close()  # Idempotency.

    assert set(tr.input_sessions) == {subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {broadcaster, client_requester}

    broadcaster.close()
    broadcaster.close()  # Idempotency.

    assert set(tr.input_sessions) == {subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {client_requester}

    tr.close()
    tr.close()  # Idempotency.
    tr2.close()
    tr2.close()  # Idempotency.

    assert not set(tr.input_sessions)
    assert not set(tr.output_sessions)
    assert not set(tr2.input_sessions)
    assert not set(tr2.output_sessions)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


@pytest.mark.asyncio  # type: ignore
async def _unittest_udp_transport_ipv4_capture() -> None:
    import socket
    from pyuavcan.transport.udp import UDPCapture, IPPacket
    from pyuavcan.transport import MessageDataSpecifier, PayloadMetadata, Transfer
    from pyuavcan.transport import Priority, Timestamp, OutputSessionSpecifier
    from pyuavcan.transport import Capture, AlienSessionSpecifier

    asyncio.get_running_loop().slow_callback_duration = 5.0

    tr_capture = UDPTransport("127.50.0.2", local_node_id=None)
    captures: typing.List[UDPCapture] = []

    def inhale(s: Capture) -> None:
        print("CAPTURED:", s)
        assert isinstance(s, UDPCapture)
        captures.append(s)

    tr_capture.begin_capture(inhale)
    await asyncio.sleep(1.0)

    tr = UDPTransport("127.50.0.111")
    meta = PayloadMetadata(10000)
    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(190), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(190), None), meta)

    # For reasons of Windows compatibility, we have to set up a dummy listener on the target multicast group.
    # Otherwise, we will not see any packets at all. This is Windows-specific.
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sink.bind(("", 11111))
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.50.0.190") + socket.inet_aton("127.0.0.1")
    )

    ts = Timestamp.now()
    assert len(captures) == 0  # Assuming here that there are no other entities that might create noise.
    await broadcaster.send(
        Transfer(
            timestamp=ts,
            priority=Priority.NOMINAL,
            transfer_id=9876543210,
            fragmented_payload=[_mem(bytes(range(256)))] * 4,
        ),
        monotonic_deadline=tr.loop.time() + 2.0,
    )
    await asyncio.sleep(1.0)  # Let the packet propagate.
    assert len(captures) == 1  # Ensure the packet is captured.
    tr_capture.close()  # Ensure the capture is stopped after the capturing transport is closed.
    await broadcaster.send(  # This one shall be ignored.
        Transfer(timestamp=Timestamp.now(), priority=Priority.HIGH, transfer_id=54321, fragmented_payload=[_mem(b"")]),
        monotonic_deadline=tr.loop.time() + 2.0,
    )
    await asyncio.sleep(1.0)
    assert len(captures) == 1  # Ignored?
    tr.close()
    sink.close()

    (pkt,) = captures
    assert isinstance(pkt, UDPCapture)
    assert (ts.monotonic - 1) <= pkt.timestamp.monotonic <= Timestamp.now().monotonic
    assert (ts.system - 1) <= pkt.timestamp.system <= Timestamp.now().system
    ip_pkt = IPPacket.parse(pkt.link_layer_packet)
    assert ip_pkt is not None
    assert [str(x) for x in ip_pkt.source_destination] == ["127.50.0.111", "239.50.0.190"]
    parsed = pkt.parse()
    assert parsed
    ses, frame = parsed
    assert isinstance(ses, AlienSessionSpecifier)
    assert ses.source_node_id == 111
    assert ses.destination_node_id is None
    assert ses.data_specifier == broadcaster.specifier.data_specifier
    assert frame.end_of_transfer
    assert frame.index == 0
    assert frame.transfer_id == 9876543210
    assert len(frame.payload) == 1024
    assert frame.priority == Priority.NOMINAL


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
