#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import pytest
import pyuavcan.transport


_RX_TIMEOUT = 10e-3


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import UnsupportedSessionConfigurationError, Priority, can, Statistics, Timestamp
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
    media = MockMedia(peers, 64, 10)
    media2 = MockMedia(peers, 64, 3)
    peeper = MockMedia(peers, 64, 10)
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

    subscriber_selective = await tr.get_selective_input(MessageDataSpecifier(2222), meta, 123)
    assert subscriber_selective is await tr.get_selective_input(MessageDataSpecifier(2222), meta, 123)

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

    assert tr.sample_frame_statistics() == can.CANFrameStatistics()
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics()

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

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=1)
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(received=1, received_uavcan=1)
    assert tr.sample_frame_statistics().media_acceptance_filtering_efficiency == pytest.approx(1)
    assert tr2.sample_frame_statistics().media_acceptance_filtering_efficiency == pytest.approx(0)
    assert tr.sample_frame_statistics().lost_loopback == 0
    assert tr2.sample_frame_statistics().lost_loopback == 0

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

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=2)
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(
        received=2, received_uavcan=2, received_uavcan_accepted=1)

    received: Transfer = await promiscuous_m12345.receive()
    assert isinstance(received, TransferFrom)
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

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=7, loopback_requested=1, loopback_returned=1)
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(
        received=7, received_uavcan=7, received_uavcan_accepted=6)

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    assert is_timestamp_valid(fb.first_frame_transmission_timestamp)

    received = await promiscuous_m12345.receive()
    assert isinstance(received, TransferFrom)
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
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 3
    assert received.source_node_id == 5
    assert received.priority == Priority.OPTIONAL
    assert is_timestamp_valid(received.timestamp)
    assert list(received.fragmented_payload) == [_mem('qwerty')]

    assert promiscuous_m12345.sample_statistics() == Statistics(transfers=3, frames=7, payload_bytes=325)

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=8, loopback_requested=1, loopback_returned=1)
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(
        received=8, received_uavcan=8, received_uavcan_accepted=7)

    await broadcaster.close()
    with pytest.raises(ResourceClosedError):
        await broadcaster.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4, fragmented_payload=[]))
    await broadcaster.close()   # Does nothing

    # Final checks for the broadcaster - make sure nothing is left in the queue
    assert (await promiscuous_m12345.try_receive(time.monotonic() + _RX_TIMEOUT)) is None

    # The selective listener was not supposed to pick up anything because it's selective for node 9, not 5
    assert (await selective_m12345_9.try_receive(time.monotonic() + _RX_TIMEOUT)) is None

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

    assert (await selective_server_s333_5.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await selective_server_s333_9.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await promiscuous_server_s333.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert selective_server_s333_5.sample_statistics() == Statistics()
    assert selective_server_s333_9.sample_statistics() == Statistics()
    assert promiscuous_server_s333.sample_statistics() == Statistics()

    assert (await selective_client_s333_5.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await selective_client_s333_9.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await promiscuous_client_s333.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert selective_client_s333_5.sample_statistics() == Statistics()
    assert selective_client_s333_9.sample_statistics() == Statistics()
    assert promiscuous_client_s333.sample_statistics() == Statistics()

    await tr2.set_local_node_id(123)
    with pytest.raises(InvalidTransportConfigurationError):
        await tr2.set_local_node_id(10)
    assert tr2.local_node_id == 123

    client_requester.enable_feedback(feedback_collector.give)       # FEEDBACK ENABLED HERE

    # Will fail with an error; make sure it's counted properly. The feedback registry entry will remain pending!
    media.raise_on_send_once(RuntimeError('Induced failure'))
    with pytest.raises(RuntimeError, match='Induced failure'):
        await client_requester.send(Transfer(
            timestamp=ts,
            priority=Priority.FAST,
            transfer_id=12,
            fragmented_payload=[]
        ))
    assert client_requester.sample_statistics() == Statistics(transfers=1, frames=1, payload_bytes=0, errors=1)

    # Some malformed feedback frames which will be ignored
    media.inject_received([UAVCANFrame(
        identifier=ServiceCANID(priority=Priority.FAST,
                                source_node_id=5,
                                destination_node_id=123,
                                service_id=333,
                                request_not_response=True).compile([_mem('Ignored')]),
        padded_payload=_mem('Ignored'),
        start_of_transfer=False,        # Ignored because not start-of-frame
        end_of_transfer=False,
        toggle_bit=True,
        transfer_id=12,
        loopback=True).compile()
    ])

    media.inject_received([UAVCANFrame(
        identifier=ServiceCANID(priority=Priority.FAST,
                                source_node_id=5,
                                destination_node_id=123,
                                service_id=333,
                                request_not_response=True).compile([_mem('Ignored')]),
        padded_payload=_mem('Ignored'),
        start_of_transfer=True,
        end_of_transfer=False,
        toggle_bit=True,
        transfer_id=9,                  # Ignored because there is no such transfer-ID in the registry
        loopback=True).compile()
    ])

    # Now, this transmission will succeed, but a pending loopback registry entry will be overwritten, which will be
    # reflected in the error counter.
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
    assert client_requester.sample_statistics() == Statistics(transfers=2, frames=8, payload_bytes=438, errors=2)

    # The feedback is disabled, but we will send a valid loopback frame anyway to make sure it is silently ignored
    media.inject_received([UAVCANFrame(
        identifier=ServiceCANID(priority=Priority.FAST,
                                source_node_id=5,
                                destination_node_id=123,
                                service_id=333,
                                request_not_response=True).compile([_mem('Ignored')]),
        padded_payload=_mem('Ignored'),
        start_of_transfer=True,
        end_of_transfer=False,
        toggle_bit=True,
        transfer_id=12,
        loopback=True).compile()
    ])

    await client_requester.close()
    with pytest.raises(ResourceClosedError):
        await client_requester.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4, fragmented_payload=[]))

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    assert is_timestamp_valid(fb.first_frame_transmission_timestamp)

    received = await promiscuous_server_s333.receive()
    assert isinstance(received, TransferFrom)
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
    assert (await selective_server_s333_9.try_receive(time.monotonic() + _RX_TIMEOUT)) is None

    # Nothing is received - non-matching role (not server)
    assert (await selective_client_s333_5.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await selective_client_s333_9.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert (await promiscuous_client_s333.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert selective_client_s333_5.sample_statistics() == Statistics()
    assert selective_client_s333_9.sample_statistics() == Statistics()
    assert promiscuous_client_s333.sample_statistics() == Statistics()

    # Final transport stats check; additional loopback frames are due to our manual tests above
    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=16, loopback_requested=2, loopback_returned=5)
    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(
        received=15, received_uavcan=15, received_uavcan_accepted=14)

    #
    # Drop non-UAVCAN frames silently
    #
    media.inject_received([can.media.DataFrame(
        identifier=ServiceCANID(priority=Priority.FAST,
                                source_node_id=5,
                                destination_node_id=123,
                                service_id=333,
                                request_not_response=True).compile([_mem('')]),
        data=bytearray(b''),                # The CAN ID is valid for UAVCAN, but the payload is not - no tail byte
        format=can.media.FrameFormat.EXTENDED,
        loopback=False)
    ])

    media.inject_received([can.media.DataFrame(
        identifier=0,                       # The CAN ID is not valid for UAVCAN
        data=bytearray(b'123'),
        format=can.media.FrameFormat.BASE,
        loopback=False)
    ])

    media.inject_received([UAVCANFrame(
        identifier=ServiceCANID(priority=Priority.FAST,
                                source_node_id=5,
                                destination_node_id=123,
                                service_id=444,             # No such service
                                request_not_response=True).compile([_mem('Ignored')]),
        padded_payload=_mem('Ignored'),
        start_of_transfer=True,
        end_of_transfer=False,
        toggle_bit=True,
        transfer_id=12,
        loopback=True).compile()
    ])

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=16,
                                                                  received=2,
                                                                  loopback_requested=2,
                                                                  loopback_returned=6)

    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(received=15,
                                                                   received_uavcan=15,
                                                                   received_uavcan_accepted=14)

    #
    # Reception logic test.
    #
    pub_m2222 = await tr2.get_broadcast_output(MessageDataSpecifier(2222), meta)

    # Transfer ID timeout configuration - one of them will be configured very short for testing purposes
    subscriber_promiscuous.transfer_id_timeout = 1e-9       # Very low, basically zero timeout
    with pytest.raises(ValueError):
        subscriber_promiscuous.transfer_id_timeout = -1
    with pytest.raises(ValueError):
        subscriber_promiscuous.transfer_id_timeout = float('nan')
    assert subscriber_promiscuous.transfer_id_timeout == pytest.approx(1e-9)

    subscriber_selective.transfer_id_timeout = 1.0
    with pytest.raises(ValueError):
        subscriber_selective.transfer_id_timeout = -1
    with pytest.raises(ValueError):
        subscriber_selective.transfer_id_timeout = float('nan')
    assert subscriber_selective.transfer_id_timeout == pytest.approx(1.0)

    # Queue capacity configuration
    assert subscriber_selective.queue_capacity is None      # Unlimited by default
    subscriber_selective.queue_capacity = 2
    with pytest.raises(ValueError):
        subscriber_selective.queue_capacity = 0
    assert subscriber_selective.queue_capacity == 2

    await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.EXCEPTIONAL,
        transfer_id=7,
        fragmented_payload=[
            _mem('Finally, from so little sleeping and so much reading, '),
            _mem('his brain dried up and he went completely out of his mind.'),  # Two frames.
        ]
    ))

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=16,
                                                                  received=4,
                                                                  received_uavcan=2,
                                                                  received_uavcan_accepted=2,
                                                                  loopback_requested=2,
                                                                  loopback_returned=6)

    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(sent=2,
                                                                   received=15,
                                                                   received_uavcan=15,
                                                                   received_uavcan_accepted=14)

    received = await subscriber_promiscuous.receive()
    assert isinstance(received, TransferFrom)
    assert received.source_node_id == 123
    assert received.priority == Priority.EXCEPTIONAL
    assert received.transfer_id == 7
    assert is_timestamp_valid(received.timestamp)
    assert bytes(received.fragmented_payload[0]).startswith(b'Finally')
    assert bytes(received.fragmented_payload[-1]).rstrip(b'\x55').endswith(b'out of his mind.')

    received = await subscriber_selective.receive()
    assert received.priority == Priority.EXCEPTIONAL
    assert received.transfer_id == 7
    assert is_timestamp_valid(received.timestamp)
    assert bytes(received.fragmented_payload[0]).startswith(b'Finally')
    assert bytes(received.fragmented_payload[-1]).rstrip(b'\x55').endswith(b'out of his mind.')

    assert subscriber_selective.sample_statistics() == subscriber_promiscuous.sample_statistics()
    assert subscriber_promiscuous.sample_statistics() == Statistics(transfers=1,
                                                                    frames=2,
                                                                    payload_bytes=124)  # Includes padding!

    await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.NOMINAL,
        transfer_id=7,                  # Same transfer ID, will be accepted only by the instance with low TID timeout
        fragmented_payload=[]
    ))

    assert tr.sample_frame_statistics() == can.CANFrameStatistics(sent=16,
                                                                  received=5,
                                                                  received_uavcan=3,
                                                                  received_uavcan_accepted=3,
                                                                  loopback_requested=2,
                                                                  loopback_returned=6)

    assert tr2.sample_frame_statistics() == can.CANFrameStatistics(sent=3,
                                                                   received=15,
                                                                   received_uavcan=15,
                                                                   received_uavcan_accepted=14)

    received = await subscriber_promiscuous.receive()
    assert isinstance(received, TransferFrom)
    assert received.source_node_id == 123
    assert received.priority == Priority.NOMINAL
    assert received.transfer_id == 7
    assert is_timestamp_valid(received.timestamp)
    assert b''.join(received.fragmented_payload) == b''

    assert subscriber_promiscuous.sample_statistics() == Statistics(transfers=2,
                                                                    frames=3,
                                                                    payload_bytes=124)

    # Discarded because of the same transfer ID
    assert (await subscriber_selective.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert subscriber_selective.sample_statistics() == Statistics(transfers=1,
                                                                  frames=3,
                                                                  payload_bytes=124,
                                                                  errors=1)     # Error due to the repeated transfer ID

    await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.HIGH,
        transfer_id=8,
        fragmented_payload=[
            _mem('a' * 63),
            _mem('b' * 63),
            _mem('c' * 63),
            _mem('d' * 62),  # Tricky case - one of the CRC bytes spills over into the fifth frame
        ]
    ))

    # The promiscuous one is able to receive the transfer since its queue is large enough
    received = await subscriber_promiscuous.receive()
    assert received.priority == Priority.HIGH
    assert received.transfer_id == 8
    assert is_timestamp_valid(received.timestamp)
    assert list(map(bytes, received.fragmented_payload)) == [
        b'a' * 63,
        b'b' * 63,
        b'c' * 63,
        b'd' * 62,
    ]
    assert subscriber_promiscuous.sample_statistics() == Statistics(transfers=3,
                                                                    frames=8,
                                                                    payload_bytes=375)

    # The selective one is unable to do so since it's RX queue is too small; it is reflected in the error counter
    assert (await subscriber_selective.try_receive(time.monotonic() + _RX_TIMEOUT)) is None
    assert subscriber_selective.sample_statistics() == Statistics(transfers=1,
                                                                  frames=5,
                                                                  payload_bytes=124,
                                                                  errors=1,
                                                                  overruns=3)  # Overruns!

    #
    # Finalization.
    #
    print('str(CANTransport):', tr)
    print('str(CANTransport):', tr2)
    await client_listener.close()
    await server_listener.close()
    await subscriber_promiscuous.close()
    await subscriber_selective.close()
    await tr.close()
    await tr2.close()


def _mem(data: typing.Union[str, bytes, bytearray]) -> memoryview:
    return memoryview(data.encode() if isinstance(data, str) else data)


class _FeedbackCollector:
    def __init__(self) -> None:
        self._item: typing.Optional[pyuavcan.transport.Feedback] = None

    def give(self, feedback: pyuavcan.transport.Feedback) -> None:
        assert self._item is None, 'Clear the old feedback first'
        self._item = feedback

    def take(self) -> pyuavcan.transport.Feedback:
        out = self._item
        self._item = None
        assert out is not None, 'Feedback is missing'
        return out
