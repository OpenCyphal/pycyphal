# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import asyncio
import ipaddress
import pytest
import pycyphal.transport
from pycyphal.transport import OperationNotDefinedForAnonymousNodeError
from pycyphal.transport.udp import UDPTransportStatistics

# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pycyphal.transport.udp import UDPTransport

from pycyphal.transport.udp._session import PromiscuousUDPInputSessionStatistics, SelectiveUDPInputSessionStatistics


pytestmark = pytest.mark.asyncio


async def _unittest_udp_transport_ipv4() -> None:
    from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pycyphal.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pycyphal.transport import ProtocolParameters

    asyncio.get_running_loop().slow_callback_duration = 5.0

    get_monotonic = asyncio.get_event_loop().time

    with pytest.raises(ValueError):
        # Invalid MTU (not in range)
        _ = UDPTransport("127.0.0.1", local_node_id=111, mtu=10)

    with pytest.raises(ValueError):
        # Invalid service transfer multiplier (not in range)
        _ = UDPTransport("127.0.0.1", local_node_id=111, service_transfer_multiplier=100)

    # Instantiate UDPTransport

    tr = UDPTransport("127.0.0.1", local_node_id=111, mtu=9000)
    tr2 = UDPTransport("127.0.0.1", local_node_id=222, service_transfer_multiplier=2)
    anon_tr = UDPTransport("127.0.0.1", local_node_id=None)

    assert tr.local_ip_address == ipaddress.ip_address("127.0.0.1")
    assert tr2.local_ip_address == ipaddress.ip_address("127.0.0.1")
    assert anon_tr.local_ip_address == ipaddress.ip_address("127.0.0.1")

    assert tr.local_node_id == 111
    assert tr2.local_node_id == 222
    assert anon_tr.local_node_id is None

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert "127.0.0.1" in repr(tr)
    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2**64,
        max_nodes=65535,
        mtu=9000,
    )

    default_mtu = min(UDPTransport.VALID_MTU_RANGE)
    assert "127.0.0.1" in repr(tr2)
    assert tr2.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2**64,
        max_nodes=65535,
        mtu=default_mtu,
    )

    assert "127.0.0.1" in repr(anon_tr)
    assert anon_tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2**64,
        max_nodes=65535,
        mtu=default_mtu,
    )

    empty_stats = UDPTransportStatistics()
    assert tr.sample_statistics() == tr2.sample_statistics() == anon_tr.sample_statistics() == empty_stats

    payload_single = [_mem("qwertyui"), _mem("01234567")] * (default_mtu // 16)
    assert sum(map(len, payload_single)) == default_mtu

    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = default_mtu * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    #
    # Instantiate session objects.
    #
    # UDPOutputSession          UDPTransport(local_node_id) data_specifier(subject_id)  remote_node_id
    # ------------------------------------------------------------------------------------------------
    # broadcaster               tr2(222)                    MessageDataSpecifier(2345)  None
    # anon_broadcaster          anon_tr(None)               MessageDataSpecifier(2345)  None
    # server_responder          tr(111)                     ServiceDataSpecifier(444)   222
    # client_requester          tr2(222)                    ServiceDataSpecifier(444)   111
    #
    # UDPInputSession           UDPTransport(local_node_id) data_specifier(subject_id)  remote_node_id
    # ------------------------------------------------------------------------------------------------
    # subscriber_promiscuous    tr(111)                     MessageDataSpecifier(2345)  None
    # anon_sub_promiscuous      anon_tr(None)               MessageDataSpecifier(2345)  None
    # subscriber_selective      tr(111)                     MessageDataSpecifier(2345)  123
    # server_listener           tr(111)                     ServiceDataSpecifier(444)   None
    # client_listener           tr2(222)                    ServiceDataSpecifier(444)   111

    meta = PayloadMetadata(10000)

    broadcaster = tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    anon_broadcaster = anon_tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert anon_broadcaster is anon_tr.get_output_session(
        OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta
    )

    subscriber_promiscuous_specifier = InputSessionSpecifier(MessageDataSpecifier(2345), None)
    subscriber_promiscuous = tr.get_input_session(subscriber_promiscuous_specifier, meta)
    assert subscriber_promiscuous is tr.get_input_session(subscriber_promiscuous_specifier, meta)

    anon_sub_promiscuous_specifier = InputSessionSpecifier(MessageDataSpecifier(2345), None)
    anon_sub_promiscuous = anon_tr.get_input_session(anon_sub_promiscuous_specifier, meta)
    assert anon_sub_promiscuous is anon_tr.get_input_session(anon_sub_promiscuous_specifier, meta)

    # Anonymous UDP Transport cannot create non-promiscuous input session (only Message, no Service)
    faulthy_specifier = InputSessionSpecifier(MessageDataSpecifier(2345), 123)
    with pytest.raises(OperationNotDefinedForAnonymousNodeError):
        _ = anon_tr.get_input_session(faulthy_specifier, meta)

    subscriber_selective_specifier = InputSessionSpecifier(MessageDataSpecifier(2345), 123)
    subscriber_selective = tr.get_input_session(subscriber_selective_specifier, meta)
    assert subscriber_selective is tr.get_input_session(subscriber_selective_specifier, meta)

    server_listener_specifier = InputSessionSpecifier(
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), None
    )
    server_listener = tr.get_input_session(server_listener_specifier, meta)
    assert server_listener is tr.get_input_session(server_listener_specifier, meta)

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

    client_listener_specifier = InputSessionSpecifier(
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 111
    )
    client_listener = tr2.get_input_session(client_listener_specifier, meta)
    assert client_listener is tr2.get_input_session(client_listener_specifier, meta)

    print("tr :", tr.input_sessions, tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}

    print("tr2:", tr2.input_sessions, tr2.output_sessions)
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {broadcaster, client_requester}

    print("anon_tr :", anon_tr.input_sessions, anon_tr.output_sessions)
    assert set(anon_tr.input_sessions) == {anon_sub_promiscuous}
    assert set(anon_tr.output_sessions) == {anon_broadcaster}

    ## empty statistics [subscriber_promiscuous]
    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(subject_id=2345)] == [
        PromiscuousUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors_per_source_node_id={}
        ),
        SelectiveUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
        ),
    ]

    ## empty statistics [anon_sub_promiscuous]
    assert anon_tr.sample_statistics().received_datagrams[MessageDataSpecifier(subject_id=2345)] == [
        PromiscuousUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors_per_source_node_id={}
        ),
    ]

    ## empty statistics [subscriber_selective]
    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(subject_id=2345)] == [
        PromiscuousUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors_per_source_node_id={}
        ),
        SelectiveUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
        ),
    ]

    ## empty statistics [server_listener]
    assert tr.sample_statistics().received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)] == [
        PromiscuousUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors_per_source_node_id={}
        ),
    ]

    ## empty statistics [client_listener]
    assert tr2.sample_statistics().received_datagrams[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)
    ] == [
        SelectiveUDPInputSessionStatistics(
            transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
        ),
    ]

    #
    # Message exchange test.
    # send: broadcaster
    # receive: subscriber_promiscuous, anon_sub_promiscuous
    #
    assert await broadcaster.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=77777, fragmented_payload=payload_single
        ),
        monotonic_deadline=get_monotonic() + 5.0,
    )

    # subscriber_promiscuous
    rx_transfer = await subscriber_promiscuous.receive(get_monotonic() + 5.0)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b"".join(payload_single)]

    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=1200, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )

    # anon_sub_promiscuous
    rx_transfer = await anon_sub_promiscuous.receive(get_monotonic() + 5.0)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b"".join(payload_single)]

    assert anon_tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=1200, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )

    assert tr.sample_statistics().received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors_per_source_node_id={}
    )

    assert tr2.sample_statistics().received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)][
        0
    ] == SelectiveUDPInputSessionStatistics(
        transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
    )

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    #
    # Message exchange test.
    # send: anon_broadcaster
    # receive: anon_sub_promiscuous, subscriber_promiscuous
    #
    assert await anon_broadcaster.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=77777, fragmented_payload=payload_single
        ),
        monotonic_deadline=get_monotonic() + 5.0,
    )

    rx_transfer = await anon_sub_promiscuous.receive(get_monotonic() + 5.0)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b"".join(payload_single)]

    assert anon_tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=2, frames=2, payload_bytes=2400, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )

    rx_transfer = await subscriber_promiscuous.receive(get_monotonic() + 5.0)
    assert isinstance(rx_transfer, TransferFrom)
    assert rx_transfer.priority == Priority.LOW
    assert rx_transfer.transfer_id == 77777
    assert rx_transfer.fragmented_payload == [b"".join(payload_single)]

    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=2, frames=2, payload_bytes=2400, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )

    assert None is await subscriber_selective.receive(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive(get_monotonic() + 0.1)
    assert None is await server_listener.receive(get_monotonic() + 0.1)
    assert None is await client_listener.receive(get_monotonic() + 0.1)

    #
    # Service exchange test.
    # send: client_requester
    # receive: server_listener
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
    assert tr.sample_statistics().received_datagrams[MessageDataSpecifier(2345)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=2, frames=2, payload_bytes=2400, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )
    assert tr.sample_statistics().received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)][
        0
    ] == PromiscuousUDPInputSessionStatistics(
        transfers=1, frames=6, payload_bytes=3592, errors=0, drops=0, reassembly_errors_per_source_node_id={222: {}}
    )

    print("tr2:", tr2.sample_statistics())
    assert tr2.sample_statistics().received_datagrams[ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)][
        0
    ] == SelectiveUDPInputSessionStatistics(
        transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
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

    with pytest.raises(pycyphal.transport.ResourceClosedError):
        _ = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    with pytest.raises(pycyphal.transport.ResourceClosedError):
        _ = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


async def _unittest_udp_transport_ipv4_capture() -> None:
    import socket
    from pycyphal.transport.udp import UDPCapture, IPPacket
    from pycyphal.transport import MessageDataSpecifier, PayloadMetadata, Transfer
    from pycyphal.transport import Priority, Timestamp, OutputSessionSpecifier
    from pycyphal.transport import Capture, AlienSessionSpecifier

    asyncio.get_running_loop().slow_callback_duration = 5.0

    tr_capture = UDPTransport("127.0.0.1", local_node_id=None)
    captures: typing.List[UDPCapture] = []

    def inhale(s: Capture) -> None:
        print("CAPTURED:", s)
        assert isinstance(s, UDPCapture)
        captures.append(s)

    assert not tr_capture.capture_active
    tr_capture.begin_capture(inhale)
    assert tr_capture.capture_active
    await asyncio.sleep(1.0)

    tr = UDPTransport("127.0.0.1", local_node_id=456)
    meta = PayloadMetadata(10000)
    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(190), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(190), None), meta)

    # For reasons of Windows compatibility, we have to set up a dummy listener on the target multicast group.
    # Otherwise, we will not see any packets at all. This is Windows-specific.
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sink.bind(("", 11111))
    sink.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.0.0.190") + socket.inet_aton("127.0.0.1")
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
        monotonic_deadline=asyncio.get_running_loop().time() + 2.0,
    )
    await asyncio.sleep(1.0)  # Let the packet propagate.
    assert len(captures) == 1  # Ensure the packet is captured.
    tr_capture.close()  # Ensure the capture is stopped after the capturing transport is closed.
    await broadcaster.send(  # This one shall be ignored.
        Transfer(timestamp=Timestamp.now(), priority=Priority.HIGH, transfer_id=54321, fragmented_payload=[_mem(b"")]),
        monotonic_deadline=asyncio.get_running_loop().time() + 2.0,
    )
    await asyncio.sleep(1.0)
    assert len(captures) == 1  # Ignored?
    tr.close()
    sink.close()

    (pkt,) = captures
    assert isinstance(pkt, UDPCapture)
    assert (ts.monotonic - 1) <= pkt.timestamp.monotonic <= Timestamp.now().monotonic
    # assert (ts.system - 1) <= pkt.timestamp.system <= Timestamp.now().system
    ip_pkt = IPPacket.parse(pkt.link_layer_packet)
    assert ip_pkt is not None
    assert [str(x) for x in ip_pkt.source_destination] == ["127.0.0.1", "239.0.0.190"]
    parsed = pkt.parse()
    assert parsed
    ses, frame = parsed
    assert isinstance(ses, AlienSessionSpecifier)
    assert ses.source_node_id == 456
    # assert ses.destination_node_id is None
    assert ses.data_specifier == broadcaster.specifier.data_specifier
    assert frame.end_of_transfer
    assert frame.index == 0
    assert frame.transfer_id == 9876543210
    assert len(frame.payload) == 1024 + 4
    assert frame.priority == Priority.NOMINAL


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
