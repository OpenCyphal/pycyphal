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
    from pyuavcan.transport import InvalidTransportConfigurationError, OperationNotDefinedForAnonymousNodeError
    from pyuavcan.transport import ResourceClosedError
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
    meta = PayloadMetadata(0x_bad_c0ffee_0dd_f00d, 10000)

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

    def is_timestamp_valid(timestamp: Timestamp) -> bool:
        mon = ts.monotonic < timestamp.monotonic < time.monotonic()
        sys = ts.system < timestamp.system < time.time()
        return mon and sys

    await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ))
    assert broadcaster.sample_statistics() == Statistics(transfers=1, frames=1, payload_bytes=6)

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

    # Can't send anonymous service transfers
    with pytest.raises(OperationNotDefinedForAnonymousNodeError):
        await client_requester.send(Transfer(
            timestamp=ts, priority=Priority.IMMEDIATE, transfer_id=0, fragmented_payload=[]
        ))
    assert client_requester.sample_statistics() == Statistics()   # Not incremented!

    # Can't send multiframe anonymous messages
    with pytest.raises(OperationNotDefinedForAnonymousNodeError):
        await broadcaster.send(Transfer(
            timestamp=ts,
            priority=Priority.SLOW,
            transfer_id=2,
            fragmented_payload=[_mem('qwe'), _mem('rty')] * 50  # Lots of data here, very multiframe
        ))

    #
    # Broadcast exchange with input dispatch test
    #
    selective_m12345_5 = await tr2.get_selective_input(MessageDataSpecifier(12345), meta, 5)
    selective_m12345_9 = await tr2.get_selective_input(MessageDataSpecifier(12345), meta, 9)
    promiscuous_m12345 = await tr2.get_promiscuous_input(MessageDataSpecifier(12345), meta)

    await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ))
    assert broadcaster.sample_statistics() == Statistics(transfers=2, frames=2, payload_bytes=12)

    assert tr.sample_frame_counters() == can.CANFrameStatistics(sent=2)
    assert tr2.sample_frame_counters() == can.CANFrameStatistics(
        received=2, received_uavcan=2, received_uavcan_accepted=1)

    received = await promiscuous_m12345.receive()
    assert received.transfer_id == 11
    assert received.source_node_id is None      # The sender is anonymous
    assert received.priority == Priority.IMMEDIATE
    assert is_timestamp_valid(received.timestamp)
    assert received.fragmented_payload == [_mem('abcdef')]

    assert selective_m12345_5.sample_statistics() == Statistics()       # Nothing
    assert selective_m12345_9.sample_statistics() == Statistics()       # Nothing
    assert promiscuous_m12345.sample_statistics() == Statistics(transfers=1, frames=1, payload_bytes=6)

    with pytest.raises(ValueError):
        await tr.set_local_node_id(128)
    with pytest.raises(ValueError):
        await tr.set_local_node_id(-1)
    await tr.set_local_node_id(5)
    with pytest.raises(InvalidTransportConfigurationError, match='.*once.*'):
        await tr.set_local_node_id(123)

    feedback_collector = _FeedbackCollector()

    broadcaster.enable_feedback(feedback_collector.give)
    await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.SLOW,
        transfer_id=2,
        fragmented_payload=[_mem('qwe'), _mem('rty')] * 50  # Lots of data here, very multiframe
    ))
    assert broadcaster.sample_statistics() == Statistics(transfers=3, frames=7, payload_bytes=312)
    broadcaster.disable_feedback()

    assert tr.sample_frame_counters() == can.CANFrameStatistics(sent=7, loopback_requested=1, loopback_returned=1)
    assert tr2.sample_frame_counters() == can.CANFrameStatistics(
        received=7, received_uavcan=7, received_uavcan_accepted=6)

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    assert is_timestamp_valid(fb.first_frame_transmission_timestamp)

    received = await promiscuous_m12345.receive()
    assert received.transfer_id == 2
    assert received.source_node_id == 5
    assert received.priority == Priority.SLOW
    assert is_timestamp_valid(received.timestamp)
    assert b''.join(received.fragmented_payload) == b'qwerty' * 50 + b'\x55' * 13  # The 0x55 at the end is padding

    await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.OPTIONAL,
        transfer_id=3,
        fragmented_payload=[_mem('qwe'), _mem('rty')]
    ))
    assert broadcaster.sample_statistics() == Statistics(transfers=4, frames=8, payload_bytes=318)

    received = await promiscuous_m12345.receive()
    assert received.transfer_id == 3
    assert received.source_node_id == 5
    assert received.priority == Priority.OPTIONAL
    assert is_timestamp_valid(received.timestamp)
    assert list(received.fragmented_payload) == [_mem('qwerty')]

    assert promiscuous_m12345.sample_statistics() == Statistics(transfers=3, frames=7, payload_bytes=325)

    assert tr.sample_frame_counters() == can.CANFrameStatistics(sent=8, loopback_requested=1, loopback_returned=1)
    assert tr2.sample_frame_counters() == can.CANFrameStatistics(
        received=8, received_uavcan=8, received_uavcan_accepted=7)

    await broadcaster.close()
    with pytest.raises(ResourceClosedError):
        await broadcaster.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4, fragmented_payload=[]))
    await broadcaster.close()   # Does nothing

    # Final checks for the broadcaster - make sure nothing is left in the queue
    assert (await promiscuous_m12345.try_receive(time.monotonic() + 1e-3)) is None

    # The selective listener was not supposed to pick up anything because it's selective for node 9, not 5
    assert (await selective_m12345_9.try_receive(time.monotonic() + 1e-3)) is None

    # Now, there are a bunch of items awaiting in the selective input for node 5, collect them and check the stats
    assert selective_m12345_5.source_node_id == 5

    received = await selective_m12345_5.receive()
    assert received.transfer_id == 2
    assert received.priority == Priority.SLOW
    assert is_timestamp_valid(received.timestamp)
    assert b''.join(received.fragmented_payload) == b'qwerty' * 50 + b'\x55' * 13  # The 0x55 at the end is padding

    received = await selective_m12345_5.receive()
    assert received.transfer_id == 3
    assert received.priority == Priority.OPTIONAL
    assert is_timestamp_valid(received.timestamp)
    assert list(received.fragmented_payload) == [_mem('qwerty')]

    assert selective_m12345_5.sample_statistics() == Statistics(transfers=2, frames=6, payload_bytes=319)

    #
    # Unicast exchange test
    #
    selective_server_s333_5 = await tr2.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta, 5)
    selective_server_s333_9 = await tr2.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta, 9)
    promiscuous_server_s333 = await tr2.get_promiscuous_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.SERVER), meta)

    selective_client_s333_5 = await tr2.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 5)
    selective_client_s333_9 = await tr2.get_selective_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta, 9)
    promiscuous_client_s333 = await tr2.get_promiscuous_input(
        ServiceDataSpecifier(333, ServiceDataSpecifier.Role.CLIENT), meta)

    # No one is listening, this one will be lost.
    await client_requester.send(Transfer(
        timestamp=ts,
        priority=Priority.FAST,
        transfer_id=11,
        fragmented_payload=[]
    ))
    assert client_requester.sample_statistics() == Statistics(transfers=1, frames=1, payload_bytes=0)

    assert (await selective_server_s333_5.try_receive(time.monotonic() + 1e-3)) is None
    assert (await selective_server_s333_9.try_receive(time.monotonic() + 1e-3)) is None
    assert (await promiscuous_server_s333.try_receive(time.monotonic() + 1e-3)) is None
    assert selective_server_s333_5.sample_statistics() == Statistics()
    assert selective_server_s333_9.sample_statistics() == Statistics()
    assert promiscuous_server_s333.sample_statistics() == Statistics()

    assert (await selective_client_s333_5.try_receive(time.monotonic() + 1e-3)) is None
    assert (await selective_client_s333_9.try_receive(time.monotonic() + 1e-3)) is None
    assert (await promiscuous_client_s333.try_receive(time.monotonic() + 1e-3)) is None
    assert selective_client_s333_5.sample_statistics() == Statistics()
    assert selective_client_s333_9.sample_statistics() == Statistics()
    assert promiscuous_client_s333.sample_statistics() == Statistics()

    await tr2.set_local_node_id(123)
    with pytest.raises(InvalidTransportConfigurationError):
        await tr2.set_local_node_id(10)
    assert tr2.local_node_id == 123

    client_requester.enable_feedback(feedback_collector.give)
    await client_requester.send(Transfer(
        timestamp=ts,
        priority=Priority.FAST,
        transfer_id=12,
        fragmented_payload=[
            _mem('Until philosophers are kings, or the kings and princes of this world have the spirit and power of '
                 'philosophy, and political greatness and wisdom meet in one, and those commoner natures who pursue '
                 'either to the exclusion of the other are compelled to stand aside, cities will never have rest from '
                 'their evils '),
            _mem('- no, nor the human race, as I believe - '),
            _mem('and then only will this our State have a possibility of life and behold the light of day.'),
        ]
    ))
    client_requester.disable_feedback()
    assert client_requester.sample_statistics() == Statistics(transfers=2, frames=8, payload_bytes=438)

    await client_requester.close()
    with pytest.raises(ResourceClosedError):
        await client_requester.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4, fragmented_payload=[]))

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    assert is_timestamp_valid(fb.first_frame_transmission_timestamp)

    received = await promiscuous_server_s333.receive()
    assert received.source_node_id == 5
    assert received.transfer_id == 12
    assert received.priority == Priority.FAST
    assert is_timestamp_valid(received.timestamp)
    assert len(received.fragmented_payload) == 7                    # Equals the number of frames
    assert sum(map(len, received.fragmented_payload)) == 438 + 1    # Padding also included
    assert b'Until philosophers are kings' in bytes(received.fragmented_payload[0])
    assert b'behold the light of day.' in bytes(received.fragmented_payload[-1])

    received = await selective_server_s333_5.receive()     # Same thing here
    assert received.transfer_id == 12
    assert received.priority == Priority.FAST
    assert is_timestamp_valid(received.timestamp)
    assert len(received.fragmented_payload) == 7                    # Equals the number of frames
    assert sum(map(len, received.fragmented_payload)) == 438 + 1    # Padding also included
    assert b'Until philosophers are kings' in bytes(received.fragmented_payload[0])
    assert b'behold the light of day.' in bytes(received.fragmented_payload[-1])

    # Nothing is received - non-matching node ID selector
    assert (await selective_server_s333_9.try_receive(time.monotonic() + 1e-3)) is None

    # Nothing is received - non-matching role (not server)
    assert (await selective_client_s333_5.try_receive(time.monotonic() + 1e-3)) is None
    assert (await selective_client_s333_9.try_receive(time.monotonic() + 1e-3)) is None
    assert (await promiscuous_client_s333.try_receive(time.monotonic() + 1e-3)) is None
    assert selective_client_s333_5.sample_statistics() == Statistics()
    assert selective_client_s333_9.sample_statistics() == Statistics()
    assert promiscuous_client_s333.sample_statistics() == Statistics()

    # Final transport stats check
    assert tr.sample_frame_counters() == can.CANFrameStatistics(sent=16, loopback_requested=2, loopback_returned=2)
    assert tr2.sample_frame_counters() == can.CANFrameStatistics(
        received=15, received_uavcan=15, received_uavcan_accepted=14)



def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)


class _FeedbackCollector:
    def __init__(self):
        self._item: typing.Optional[pyuavcan.transport.Feedback] = None

    def give(self, feedback: pyuavcan.transport.Feedback) -> None:
        assert self._item is None, 'Clear the old feedback first'
        self._item = feedback

    def take(self) -> pyuavcan.transport.Feedback:
        out = self._item
        self._item = None
        assert out is not None, 'Feedback is missing'
        return out
