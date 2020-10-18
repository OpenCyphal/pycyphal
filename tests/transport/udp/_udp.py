#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import xml.etree.ElementTree
import pytest
import pyuavcan.transport
# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.udp import UDPTransport, UDPTransportStatistics


@pytest.mark.asyncio    # type: ignore
async def _unittest_udp_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    get_monotonic = asyncio.get_event_loop().time

    with pytest.raises(ValueError):
        _ = UDPTransport(ip_address='127.0.0.111/8',
                         mtu=10)

    with pytest.raises(ValueError):
        _ = UDPTransport(ip_address='127.0.0.111/8',
                         service_transfer_multiplier=100)

    tr = UDPTransport('127.0.0.111/8', mtu=9000)
    tr2 = UDPTransport('127.0.0.222/8', service_transfer_multiplier=2)

    assert tr.local_ip_address_with_netmask == '127.0.0.111/8'
    assert tr2.local_ip_address_with_netmask == '127.0.0.222/8'

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id == 111
    assert tr2.local_node_id == 222

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert list(xml.etree.ElementTree.fromstring(tr.descriptor).itertext()) == ['127.0.0.111/8']
    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=2 ** UDPTransport.NODE_ID_BIT_LENGTH,
        mtu=9000,
    )

    assert list(xml.etree.ElementTree.fromstring(tr2.descriptor).itertext()) == ['127.0.0.222/8']
    assert tr2.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=2 ** UDPTransport.NODE_ID_BIT_LENGTH,
        mtu=UDPTransport.DEFAULT_MTU,
    )

    assert tr.sample_statistics() == tr2.sample_statistics() == UDPTransportStatistics()

    payload_single = [_mem('qwertyui'), _mem('01234567')] * (UDPTransport.DEFAULT_MTU // 16)
    assert sum(map(len, payload_single)) == UDPTransport.DEFAULT_MTU

    payload_x3 = (payload_single * 3)[:-1]
    payload_x3_size_bytes = UDPTransport.DEFAULT_MTU * 3 - 8
    assert sum(map(len, payload_x3)) == payload_x3_size_bytes

    #
    # Instantiate session objects.
    #
    meta = PayloadMetadata(10000)

    broadcaster = tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(12345), None), meta)
    assert broadcaster is tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(12345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(12345), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(12345), None),
                                                          meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(12345), 123), meta)
    assert subscriber_selective is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(12345), 123), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), None), meta)
    assert server_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), None), meta)

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 222), meta)
    assert server_responder is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 222), meta)

    client_requester = tr2.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), 111), meta)
    assert client_requester is tr2.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST), 111), meta)

    client_listener = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 111), meta)
    assert client_listener is tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE), 111), meta)

    print('tr :', tr.input_sessions, tr.output_sessions)
    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener}
    assert set(tr.output_sessions) == {server_responder}

    print('tr2:', tr2.input_sessions, tr2.output_sessions)
    assert set(tr2.input_sessions) == {client_listener}
    assert set(tr2.output_sessions) == {broadcaster, client_requester}

    assert tr.sample_statistics().demultiplexer[
        MessageDataSpecifier(12345)
    ].accepted_datagrams == {}
    assert tr.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)
    ].accepted_datagrams == {}

    assert tr2.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)
    ].accepted_datagrams == {}

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

    print('tr :', tr.sample_statistics())
    assert tr.sample_statistics().demultiplexer[
        MessageDataSpecifier(12345)
    ].accepted_datagrams == {222: 1}
    assert tr.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)
    ].accepted_datagrams == {}
    print('tr2:', tr2.sample_statistics())
    assert tr2.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)
    ].accepted_datagrams == {}

    assert None is await subscriber_selective.receive_until(get_monotonic() + 0.1)
    assert None is await subscriber_promiscuous.receive_until(get_monotonic() + 0.1)
    assert None is await server_listener.receive_until(get_monotonic() + 0.1)
    assert None is await client_listener.receive_until(get_monotonic() + 0.1)

    #
    # Service exchange test.
    #
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

    print('tr :', tr.sample_statistics())
    assert tr.sample_statistics().demultiplexer[
        MessageDataSpecifier(12345)
    ].accepted_datagrams == {222: 1}
    assert tr.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.REQUEST)
    ].accepted_datagrams == {222: 3 * 2}  # Deterministic data loss mitigation is enabled, multiplication factor 2
    print('tr2:', tr2.sample_statistics())
    assert tr2.sample_statistics().demultiplexer[
        ServiceDataSpecifier(444, ServiceDataSpecifier.Role.RESPONSE)
    ].accepted_datagrams == {}

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
        _ = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(12345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):
        _ = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(12345), None), meta)

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
