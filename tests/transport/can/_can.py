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
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, Timestamp
    from pyuavcan.transport import UnsupportedSessionConfigurationError, Priority, can, Statistics
    from pyuavcan.transport import InvalidTransportConfigurationError
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._identifier import MessageCANID, ServiceCANID
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._frame import UAVCANFrame
    from .media.mock import MockMedia, FrameCollector

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        can.CANTransport(MockMedia(set(), 64, 0))

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        can.CANTransport(MockMedia(set(), 7, 16))

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 64, 1000)
    media2 = MockMedia(peers, 64, 3)
    peeper = MockMedia(peers, 64, 1000)
    assert len(peers) == 3

    tr = can.CANTransport(media)
    tr2 = can.CANTransport(media2)

    assert tr.protocol_parameters == pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        node_id_set_cardinality=128,
        single_frame_transfer_payload_capacity_bytes=63
    )
    assert tr.frame_payload_capacity == 63
    assert tr.local_node_id is None
    assert tr.protocol_parameters == tr2.protocol_parameters

    #
    # Instantiate session objects
    #
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

    #
    # Basic exchange test, no one is listening
    #
    await media2.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])
    await peeper.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])

    collector = FrameCollector()
    peeper.set_received_frames_handler(collector.give)

    assert tr.sample_frame_counters() == can.CANFrameStatistics()
    assert tr2.sample_frame_counters() == can.CANFrameStatistics()

    ts = Timestamp.now()

    await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[
            _mem('abc'),
            _mem('def')
        ]
    ))
    assert broadcaster.sample_statistics() == Statistics(transfers=1, frames=1, bytes=6)

    assert tr.sample_frame_counters() == can.CANFrameStatistics(sent=1)
    assert tr2.sample_frame_counters() == can.CANFrameStatistics(received=1, received_uavcan=1)
    assert tr.sample_frame_counters().media_acceptance_filtering_efficiency == pytest.approx(1)
    assert tr2.sample_frame_counters().media_acceptance_filtering_efficiency == pytest.approx(0)
    assert tr.sample_frame_counters().lost_loopback == 0
    assert tr2.sample_frame_counters().lost_loopback == 0

    assert collector.pop().is_same_manifestation(UAVCANFrame(
        identifier=MessageCANID(Priority.IMMEDIATE, None, 12345).compile([_mem('abcdef')]),  # payload fragments joined
        padded_payload=_mem('abcdef'),
        transfer_id=11,
        start_of_transfer=True,
        end_of_transfer=True,
        toggle_bit=True,
        loopback=False
    ).compile())
    assert collector.empty

    with pytest.raises(InvalidTransportConfigurationError, match='.*anonymous.*'):
        await client_requester.send(Transfer(
            timestamp=ts, priority=Priority.IMMEDIATE, transfer_id=0, fragmented_payload=[]
        ))
    assert client_requester.sample_statistics() == Statistics()   # Not incremented!


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)
