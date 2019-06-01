#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import pytest
import pyuavcan.transport


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata
    from pyuavcan.transport import UnsupportedSessionConfigurationError
    from .media.mock import MockMedia

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        pyuavcan.transport.can.CANTransport(MockMedia(set(), 64, 0))

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        pyuavcan.transport.can.CANTransport(MockMedia(set(), 7, 16))

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 64, 1000)
    media2 = MockMedia(peers, 64, 3)
    assert len(peers) == 2

    tr = pyuavcan.transport.can.CANTransport(media)
    tr2 = pyuavcan.transport.can.CANTransport(media2)

    assert tr.protocol_parameters == pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        node_id_set_cardinality=128,
        single_frame_transfer_payload_capacity_bytes=63
    )
    assert tr.frame_payload_capacity == 63
    assert tr.local_node_id is None
    assert tr.protocol_parameters == tr2.protocol_parameters

    meta = PayloadMetadata(0x_bad_c0ffee_0dd_f00d, 123)

    with pytest.raises(UnsupportedSessionConfigurationError):                           # Can't broadcast service calls
        await tr.get_broadcast_output(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.SERVER), meta)

    with pytest.raises(UnsupportedSessionConfigurationError):                           # Can't unicast messages
        await tr.get_unicast_output(MessageDataSpecifier(1234), meta, 123)

    broadcaster = await tr.get_broadcast_output(MessageDataSpecifier(12345), meta)
    assert broadcaster is await tr.get_broadcast_output(MessageDataSpecifier(12345), meta)              # Same stuff

    subscriber_promiscuous = await tr.get_promiscuous_input(MessageDataSpecifier(2222), meta)
    assert subscriber_promiscuous is await tr.get_promiscuous_input(MessageDataSpecifier(2222), meta)

    subscriber_selective = await tr.get_selective_input(MessageDataSpecifier(2222), meta, 42)
    assert subscriber_selective is await tr.get_selective_input(MessageDataSpecifier(2222), meta, 42)

    server_listener = await tr.get_promiscuous_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta)
    assert server_listener is await tr.get_promiscuous_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta)

    server_responder = await tr.get_unicast_output(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta, 123)
    assert server_responder is await tr.get_unicast_output(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta, 123)

    client_requester = await tr.get_unicast_output(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 123)
    assert client_requester is await tr.get_unicast_output(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 123)

    client_listener = await tr.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 123)
    assert client_listener is await tr.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 123)

    base_ts = time.process_time()
    inputs = tr.inputs
    print(f'INPUTS (sampled in {time.process_time() - base_ts:.3f}s): {inputs}')
    assert set(inputs) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    del inputs

    print('OUTPUTS:', tr.outputs)
    assert set(tr.outputs) == {broadcaster, server_responder, client_requester}
