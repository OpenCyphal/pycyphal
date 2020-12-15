#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import pytest
import asyncio
import logging
import pyuavcan.transport
from pyuavcan.transport import can


_RX_TIMEOUT = 10e-3


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport_anon() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import UnsupportedSessionConfigurationError, Priority, SessionStatistics, Timestamp
    from pyuavcan.transport import OperationNotDefinedForAnonymousNodeError
    from pyuavcan.transport import InputSessionSpecifier, OutputSessionSpecifier
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._identifier import MessageCANID
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._frame import UAVCANFrame
    from .media.mock import MockMedia, FrameCollector

    asyncio.get_running_loop().slow_callback_duration = 5.0

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        can.CANTransport(MockMedia(set(), 64, 0), None)

    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        can.CANTransport(MockMedia(set(), 7, 16), None)

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 64, 10)
    media2 = MockMedia(peers, 64, 3)
    peeper = MockMedia(peers, 64, 10)
    assert len(peers) == 3

    tr = can.CANTransport(media, None)
    tr2 = can.CANTransport(media2, None)

    assert tr.protocol_parameters == pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        max_nodes=128,
        mtu=63
    )
    assert tr.local_node_id is None
    assert tr.protocol_parameters == tr2.protocol_parameters

    assert not media.automatic_retransmission_enabled
    assert not media2.automatic_retransmission_enabled

    #
    # Instantiate session objects
    #
    meta = PayloadMetadata(10000)

    with pytest.raises(Exception):                                                      # Can't broadcast service calls
        tr.get_output_session(OutputSessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.RESPONSE),
                                                     None),
                              meta)

    with pytest.raises(UnsupportedSessionConfigurationError):                           # Can't unicast messages
        tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(1234), 123), meta)

    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), 123), meta)
    assert subscriber_selective is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), 123), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)
    assert server_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)
    assert server_responder is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)

    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 123), meta)
    assert client_requester is tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 123), meta)

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)
    assert client_listener is tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)

    assert broadcaster.destination_node_id is None
    assert subscriber_promiscuous.source_node_id is None
    assert subscriber_selective.source_node_id == 123
    assert server_listener.source_node_id is None
    assert client_listener.source_node_id == 123

    base_ts = time.process_time()
    inputs = tr.input_sessions
    print(f'INPUTS (sampled in {time.process_time() - base_ts:.3f}s): {inputs}')
    assert set(inputs) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    del inputs

    print('OUTPUTS:', tr.output_sessions)
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}

    #
    # Basic exchange test, no one is listening
    #
    media2.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])
    peeper.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])

    collector = FrameCollector()
    peeper.start(collector.give, False)

    assert tr.sample_statistics() == can.CANTransportStatistics()
    assert tr2.sample_statistics() == can.CANTransportStatistics()

    ts = Timestamp.now()

    def validate_timestamp(timestamp: Timestamp) -> None:
        assert ts.monotonic_ns <= timestamp.monotonic_ns <= time.monotonic_ns()
        assert ts.system_ns <= timestamp.system_ns <= time.time_ns()

    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=6)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=1)
    assert tr2.sample_statistics() == can.CANTransportStatistics(in_frames=1, in_frames_uavcan=1)
    assert tr.sample_statistics().media_acceptance_filtering_efficiency == pytest.approx(1)
    assert tr2.sample_statistics().media_acceptance_filtering_efficiency == pytest.approx(0)
    assert tr.sample_statistics().lost_loopback_frames == 0
    assert tr2.sample_statistics().lost_loopback_frames == 0

    assert collector.pop()[1].frame == UAVCANFrame(
        identifier=MessageCANID(Priority.IMMEDIATE, None, 2345).compile([_mem('abcdef')]),  # payload fragments joined
        padded_payload=_mem('abcdef'),
        transfer_id=11,
        start_of_transfer=True,
        end_of_transfer=True,
        toggle_bit=True,
    ).compile()
    assert collector.empty

    # Can't send anonymous service transfers
    with pytest.raises(OperationNotDefinedForAnonymousNodeError):
        assert await client_requester.send(
            Transfer(timestamp=ts, priority=Priority.IMMEDIATE, transfer_id=0, fragmented_payload=[]),
            tr.loop.time() + 1.0,
        )
    assert client_requester.sample_statistics() == SessionStatistics()   # Not incremented!

    # Can't send multiframe anonymous messages
    with pytest.raises(OperationNotDefinedForAnonymousNodeError):
        assert await broadcaster.send(Transfer(
            timestamp=ts,
            priority=Priority.SLOW,
            transfer_id=2,
            fragmented_payload=[_mem('qwe'), _mem('rty')] * 50  # Lots of data here, very multiframe
        ), tr.loop.time() + 1.0)

    #
    # Broadcast exchange with input dispatch test
    #
    selective_m2345_5 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 5), meta)
    selective_m2345_9 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 9), meta)
    promiscuous_m2345 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=2, frames=2, payload_bytes=12)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=2)
    assert tr2.sample_statistics() == can.CANTransportStatistics(
        in_frames=2, in_frames_uavcan=2, in_frames_uavcan_accepted=1)

    received = await promiscuous_m2345.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 11
    assert received.source_node_id is None      # The sender is anonymous
    assert received.priority == Priority.IMMEDIATE
    validate_timestamp(received.timestamp)
    assert received.fragmented_payload == [_mem('abcdef')]

    assert selective_m2345_5.sample_statistics() == SessionStatistics()       # Nothing
    assert selective_m2345_9.sample_statistics() == SessionStatistics()       # Nothing
    assert promiscuous_m2345.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=6)

    assert not media.automatic_retransmission_enabled
    assert not media2.automatic_retransmission_enabled

    #
    # Finalization.
    #
    print('str(CANTransport):', tr)
    print('str(CANTransport):', tr2)
    client_listener.close()
    server_listener.close()
    subscriber_promiscuous.close()
    subscriber_selective.close()
    tr.close()
    tr2.close()
    # Double-close has no effect:
    client_listener.close()
    server_listener.close()
    subscriber_promiscuous.close()
    subscriber_selective.close()
    tr.close()
    tr2.close()


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport_non_anon(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import UnsupportedSessionConfigurationError, Priority, SessionStatistics, Timestamp
    from pyuavcan.transport import ResourceClosedError, InputSessionSpecifier, OutputSessionSpecifier
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._identifier import MessageCANID, ServiceCANID
    # noinspection PyProtectedMember
    from pyuavcan.transport.can._frame import UAVCANFrame
    from pyuavcan.transport.can.media import Envelope
    from .media.mock import MockMedia, FrameCollector

    asyncio.get_running_loop().slow_callback_duration = 5.0

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 64, 10)
    media2 = MockMedia(peers, 64, 3)
    peeper = MockMedia(peers, 64, 10)
    assert len(peers) == 3

    tr = can.CANTransport(media, 5)
    tr2 = can.CANTransport(media2, 123)

    assert tr.protocol_parameters == pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        max_nodes=128,
        mtu=63
    )
    assert tr.local_node_id == 5
    assert tr.protocol_parameters == tr2.protocol_parameters

    assert media.automatic_retransmission_enabled
    assert media2.automatic_retransmission_enabled

    #
    # Instantiate session objects
    #
    meta = PayloadMetadata(10000)

    with pytest.raises(Exception):                                                      # Can't broadcast service calls
        tr.get_output_session(OutputSessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.RESPONSE),
                                                     None),
                              meta)

    with pytest.raises(UnsupportedSessionConfigurationError):                           # Can't unicast messages
        tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(1234), 123), meta)

    broadcaster = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert broadcaster is tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    subscriber_promiscuous = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), None), meta)
    assert subscriber_promiscuous is tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), None), meta)

    subscriber_selective = tr.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2222), 123), meta)

    server_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    server_responder = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)

    client_requester = tr.get_output_session(
        OutputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 123), meta)

    client_listener = tr.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 123), meta)

    assert set(tr.input_sessions) == {subscriber_promiscuous, subscriber_selective, server_listener, client_listener}
    assert set(tr.output_sessions) == {broadcaster, server_responder, client_requester}

    #
    # Basic exchange test, no one is listening
    #
    media2.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])
    peeper.configure_acceptance_filters([can.media.FilterConfiguration.new_promiscuous()])

    collector = FrameCollector()
    peeper.start(collector.give, False)

    assert tr.sample_statistics() == can.CANTransportStatistics()
    assert tr2.sample_statistics() == can.CANTransportStatistics()

    ts = Timestamp.now()

    def validate_timestamp(timestamp: Timestamp) -> None:
        assert ts.monotonic_ns <= timestamp.monotonic_ns <= time.monotonic_ns()
        assert ts.system_ns <= timestamp.system_ns <= time.time_ns()

    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=6)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=1)
    assert tr2.sample_statistics() == can.CANTransportStatistics(in_frames=1, in_frames_uavcan=1)
    assert tr.sample_statistics().media_acceptance_filtering_efficiency == pytest.approx(1)
    assert tr2.sample_statistics().media_acceptance_filtering_efficiency == pytest.approx(0)
    assert tr.sample_statistics().lost_loopback_frames == 0
    assert tr2.sample_statistics().lost_loopback_frames == 0

    assert collector.pop()[1].frame == UAVCANFrame(
        identifier=MessageCANID(Priority.IMMEDIATE, 5, 2345).compile([_mem('abcdef')]),  # payload fragments joined
        padded_payload=_mem('abcdef'),
        transfer_id=11,
        start_of_transfer=True,
        end_of_transfer=True,
        toggle_bit=True,
    ).compile()
    assert collector.empty

    #
    # Broadcast exchange with input dispatch test
    #
    selective_m2345_5 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 5), meta)
    selective_m2345_9 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 9), meta)
    promiscuous_m2345 = tr2.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.IMMEDIATE,
        transfer_id=32 + 11,            # Modulus 11
        fragmented_payload=[_mem('abc'), _mem('def')]
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=2, frames=2, payload_bytes=12)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=2)
    assert tr2.sample_statistics() == can.CANTransportStatistics(
        in_frames=2, in_frames_uavcan=2, in_frames_uavcan_accepted=1)

    received = await promiscuous_m2345.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 11
    assert received.source_node_id == 5
    assert received.priority == Priority.IMMEDIATE
    validate_timestamp(received.timestamp)
    assert received.fragmented_payload == [_mem('abcdef')]

    assert selective_m2345_5.sample_statistics() == SessionStatistics()       # Nothing
    assert selective_m2345_9.sample_statistics() == SessionStatistics()       # Nothing
    assert promiscuous_m2345.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=6)

    assert media.automatic_retransmission_enabled
    assert media2.automatic_retransmission_enabled

    feedback_collector = _FeedbackCollector()

    broadcaster.enable_feedback(feedback_collector.give)
    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.SLOW,
        transfer_id=2,
        fragmented_payload=[_mem('qwe'), _mem('rty')] * 50  # Lots of data here, very multiframe
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=3, frames=7, payload_bytes=312)
    broadcaster.disable_feedback()

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=7,
                                                                out_frames_loopback=1,
                                                                in_frames_loopback=1)
    assert tr2.sample_statistics() == can.CANTransportStatistics(
        in_frames=7, in_frames_uavcan=7, in_frames_uavcan_accepted=6)

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    validate_timestamp(fb.first_frame_transmission_timestamp)

    received = await promiscuous_m2345.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 2
    assert received.source_node_id == 5
    assert received.priority == Priority.SLOW
    validate_timestamp(received.timestamp)
    assert b''.join(received.fragmented_payload) == b'qwerty' * 50 + b'\x00' * 13  # The 0x00 at the end is padding

    assert await broadcaster.send(Transfer(
        timestamp=ts,
        priority=Priority.OPTIONAL,
        transfer_id=3,
        fragmented_payload=[_mem('qwe'), _mem('rty')]
    ), tr.loop.time() + 1.0)
    assert broadcaster.sample_statistics() == SessionStatistics(transfers=4, frames=8, payload_bytes=318)

    received = await promiscuous_m2345.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 3
    assert received.source_node_id == 5
    assert received.priority == Priority.OPTIONAL
    validate_timestamp(received.timestamp)
    assert list(received.fragmented_payload) == [_mem('qwerty')]

    assert promiscuous_m2345.sample_statistics() == SessionStatistics(transfers=3, frames=7, payload_bytes=325)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=8,
                                                                out_frames_loopback=1,
                                                                in_frames_loopback=1)
    assert tr2.sample_statistics() == can.CANTransportStatistics(
        in_frames=8, in_frames_uavcan=8, in_frames_uavcan_accepted=7)

    broadcaster.close()
    with pytest.raises(ResourceClosedError):
        assert await broadcaster.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4,
                                               fragmented_payload=[]),
                                      tr.loop.time() + 1.0)
    broadcaster.close()   # Does nothing

    # Final checks for the broadcaster - make sure nothing is left in the queue
    assert (await promiscuous_m2345.receive(tr.loop.time() + _RX_TIMEOUT)) is None

    # The selective listener was not supposed to pick up anything because it's selective for node 9, not 5
    assert (await selective_m2345_9.receive(tr.loop.time() + _RX_TIMEOUT)) is None

    # Now, there are a bunch of items awaiting in the selective input for node 5, collect them and check the stats
    assert selective_m2345_5.source_node_id == 5

    received = await selective_m2345_5.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 11
    assert received.source_node_id == 5
    assert received.priority == Priority.IMMEDIATE
    validate_timestamp(received.timestamp)
    assert received.fragmented_payload == [_mem('abcdef')]

    received = await selective_m2345_5.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 2
    assert received.source_node_id == 5
    assert received.priority == Priority.SLOW
    validate_timestamp(received.timestamp)
    assert b''.join(received.fragmented_payload) == b'qwerty' * 50 + b'\x00' * 13  # The 0x00 at the end is padding

    received = await selective_m2345_5.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.transfer_id == 3
    assert received.source_node_id == 5
    assert received.priority == Priority.OPTIONAL
    validate_timestamp(received.timestamp)
    assert list(received.fragmented_payload) == [_mem('qwerty')]

    assert selective_m2345_5.sample_statistics() == promiscuous_m2345.sample_statistics()

    #
    # Unicast exchange test
    #
    selective_server_s333_5 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 5), meta)
    selective_server_s333_9 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), 9), meta)
    promiscuous_server_s333 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.REQUEST), None), meta)

    selective_client_s333_5 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 5), meta)
    selective_client_s333_9 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), 9), meta)
    promiscuous_client_s333 = tr2.get_input_session(
        InputSessionSpecifier(ServiceDataSpecifier(333, ServiceDataSpecifier.Role.RESPONSE), None), meta)

    assert await client_requester.send(Transfer(
        timestamp=ts,
        priority=Priority.FAST,
        transfer_id=11,
        fragmented_payload=[]
    ), tr.loop.time() + 1.0)
    assert client_requester.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=0)

    received = await selective_server_s333_5.receive(tr.loop.time() + 1.0)     # Same thing here
    assert received is not None
    assert received.transfer_id == 11
    assert received.priority == Priority.FAST
    validate_timestamp(received.timestamp)
    assert list(map(bytes, received.fragmented_payload)) == [b'']

    assert (await selective_server_s333_9.receive(tr.loop.time() + _RX_TIMEOUT)) is None

    received = await promiscuous_server_s333.receive(tr.loop.time() + 1.0)     # Same thing here
    assert received is not None
    assert received.transfer_id == 11
    assert received.priority == Priority.FAST
    validate_timestamp(received.timestamp)
    assert list(map(bytes, received.fragmented_payload)) == [b'']

    assert selective_server_s333_5.sample_statistics() == SessionStatistics(transfers=1, frames=1)
    assert selective_server_s333_9.sample_statistics() == SessionStatistics()
    assert promiscuous_server_s333.sample_statistics() == SessionStatistics(transfers=1, frames=1)

    assert (await selective_client_s333_5.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert (await selective_client_s333_9.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert (await promiscuous_client_s333.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert selective_client_s333_5.sample_statistics() == SessionStatistics()
    assert selective_client_s333_9.sample_statistics() == SessionStatistics()
    assert promiscuous_client_s333.sample_statistics() == SessionStatistics()

    client_requester.enable_feedback(feedback_collector.give)       # FEEDBACK ENABLED HERE

    # Will fail with an error; make sure it's counted properly. The feedback registry entry will remain pending!
    media.raise_on_send_once(RuntimeError('Induced failure'))
    with pytest.raises(RuntimeError, match='Induced failure'):
        assert await client_requester.send(Transfer(
            timestamp=ts,
            priority=Priority.FAST,
            transfer_id=12,
            fragmented_payload=[]
        ), tr.loop.time() + 1.0)
    assert client_requester.sample_statistics() == SessionStatistics(transfers=1, frames=1, payload_bytes=0, errors=1)

    # Some malformed feedback frames which will be ignored
    media.inject_received([Envelope(
        UAVCANFrame(identifier=ServiceCANID(priority=Priority.FAST,
                                            source_node_id=5,
                                            destination_node_id=123,
                                            service_id=333,
                                            request_not_response=True).compile([_mem('Ignored')]),
                    padded_payload=_mem('Ignored'),
                    start_of_transfer=False,        # Ignored because not start-of-frame
                    end_of_transfer=False,
                    toggle_bit=True,
                    transfer_id=12).compile(),
        loopback=True,
    )])
    media.inject_received([Envelope(
        UAVCANFrame(identifier=ServiceCANID(priority=Priority.FAST,
                                            source_node_id=5,
                                            destination_node_id=123,
                                            service_id=333,
                                            request_not_response=True).compile([_mem('Ignored')]),
                    padded_payload=_mem('Ignored'),
                    start_of_transfer=True,
                    end_of_transfer=False,
                    toggle_bit=True,
                    transfer_id=9).compile(),        # Ignored because there is no such transfer-ID in the registry
        loopback=True,
    )])

    # Now, this transmission will succeed, but a pending loopback registry entry will be overwritten, which will be
    # reflected in the error counter.
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.can.__name__):
        assert await client_requester.send(Transfer(
            timestamp=ts,
            priority=Priority.FAST,
            transfer_id=12,
            fragmented_payload=[
                _mem('Until philosophers are kings, or the kings and princes of this world have the spirit and power '
                     'of philosophy, and political greatness and wisdom meet in one, and those commoner natures who '
                     'pursue either to the exclusion of the other are compelled to stand aside, cities will never '
                     'have rest from their evils '),
                _mem('- no, nor the human race, as I believe - '),
                _mem('and then only will this our State have a possibility of life and behold the light of day.'),
            ]
        ), tr.loop.time() + 1.0)
    client_requester.disable_feedback()
    assert client_requester.sample_statistics() == SessionStatistics(transfers=2, frames=8, payload_bytes=438, errors=2)

    # The feedback is disabled, but we will send a valid loopback frame anyway to make sure it is silently ignored
    media.inject_received([Envelope(
        UAVCANFrame(identifier=ServiceCANID(priority=Priority.FAST,
                                            source_node_id=5,
                                            destination_node_id=123,
                                            service_id=333,
                                            request_not_response=True).compile([_mem('Ignored')]),
                    padded_payload=_mem('Ignored'),
                    start_of_transfer=True,
                    end_of_transfer=False,
                    toggle_bit=True,
                    transfer_id=12).compile(),
        loopback=True,
    )])

    client_requester.close()
    with pytest.raises(ResourceClosedError):
        assert await client_requester.send(Transfer(timestamp=ts, priority=Priority.LOW, transfer_id=4,
                                                    fragmented_payload=[]),
                                           tr.loop.time() + 1.0)

    fb = feedback_collector.take()
    assert fb.original_transfer_timestamp == ts
    validate_timestamp(fb.first_frame_transmission_timestamp)

    received = await promiscuous_server_s333.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.source_node_id == 5
    assert received.transfer_id == 12
    assert received.priority == Priority.FAST
    validate_timestamp(received.timestamp)
    assert len(received.fragmented_payload) == 7                    # Equals the number of frames
    assert sum(map(len, received.fragmented_payload)) == 438 + 1    # Padding also included
    assert b'Until philosophers are kings' in bytes(received.fragmented_payload[0])
    assert b'behold the light of day.' in bytes(received.fragmented_payload[-1])

    received = await selective_server_s333_5.receive(tr.loop.time() + 1.0)     # Same thing here
    assert received is not None
    assert received.transfer_id == 12
    assert received.priority == Priority.FAST
    validate_timestamp(received.timestamp)
    assert len(received.fragmented_payload) == 7                    # Equals the number of frames
    assert sum(map(len, received.fragmented_payload)) == 438 + 1    # Padding also included
    assert b'Until philosophers are kings' in bytes(received.fragmented_payload[0])
    assert b'behold the light of day.' in bytes(received.fragmented_payload[-1])

    # Nothing is received - non-matching node ID selector
    assert (await selective_server_s333_9.receive(tr.loop.time() + _RX_TIMEOUT)) is None

    # Nothing is received - non-matching role (not server)
    assert (await selective_client_s333_5.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert (await selective_client_s333_9.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert (await promiscuous_client_s333.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert selective_client_s333_5.sample_statistics() == SessionStatistics()
    assert selective_client_s333_9.sample_statistics() == SessionStatistics()
    assert promiscuous_client_s333.sample_statistics() == SessionStatistics()

    # Final transport stats check; additional loopback frames are due to our manual tests above
    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=16,
                                                                out_frames_loopback=2,
                                                                in_frames_loopback=5)
    assert tr2.sample_statistics() == can.CANTransportStatistics(in_frames=16,
                                                                 in_frames_uavcan=16,
                                                                 in_frames_uavcan_accepted=15)

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
        format=can.media.FrameFormat.EXTENDED)
    ])

    media.inject_received([can.media.DataFrame(
        identifier=0,                       # The CAN ID is not valid for UAVCAN
        data=bytearray(b'123'),
        format=can.media.FrameFormat.BASE)
    ])

    media.inject_received([Envelope(
        UAVCANFrame(identifier=ServiceCANID(priority=Priority.FAST,
                                            source_node_id=5,
                                            destination_node_id=123,
                                            service_id=444,             # No such service
                                            request_not_response=True).compile([_mem('Ignored')]),
                    padded_payload=_mem('Ignored'),
                    start_of_transfer=True,
                    end_of_transfer=False,
                    toggle_bit=True,
                    transfer_id=12).compile(),
        loopback=True,
    )])

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=16,
                                                                in_frames=2,
                                                                out_frames_loopback=2,
                                                                in_frames_loopback=6)

    assert tr2.sample_statistics() == can.CANTransportStatistics(in_frames=16,
                                                                 in_frames_uavcan=16,
                                                                 in_frames_uavcan_accepted=15)

    #
    # Reception logic test.
    #
    pub_m2222 = tr2.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2222), None), meta)

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
    assert subscriber_selective.frame_queue_capacity is None      # Unlimited by default
    subscriber_selective.frame_queue_capacity = 2
    with pytest.raises(ValueError):
        subscriber_selective.frame_queue_capacity = 0
    assert subscriber_selective.frame_queue_capacity == 2

    assert await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.EXCEPTIONAL,
        transfer_id=7,
        fragmented_payload=[
            _mem('Finally, from so little sleeping and so much reading, '),
            _mem('his brain dried up and he went completely out of his mind.'),  # Two frames.
        ]
    ), tr.loop.time() + 1.0)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=16,
                                                                in_frames=4,
                                                                in_frames_uavcan=2,
                                                                in_frames_uavcan_accepted=2,
                                                                out_frames_loopback=2,
                                                                in_frames_loopback=6)

    assert tr2.sample_statistics() == can.CANTransportStatistics(out_frames=2,
                                                                 in_frames=16,
                                                                 in_frames_uavcan=16,
                                                                 in_frames_uavcan_accepted=15)

    received = await subscriber_promiscuous.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.source_node_id == 123
    assert received.priority == Priority.EXCEPTIONAL
    assert received.transfer_id == 7
    validate_timestamp(received.timestamp)
    assert bytes(received.fragmented_payload[0]).startswith(b'Finally')
    assert bytes(received.fragmented_payload[-1]).rstrip(b'\x00').endswith(b'out of his mind.')

    received = await subscriber_selective.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert received.priority == Priority.EXCEPTIONAL
    assert received.transfer_id == 7
    validate_timestamp(received.timestamp)
    assert bytes(received.fragmented_payload[0]).startswith(b'Finally')
    assert bytes(received.fragmented_payload[-1]).rstrip(b'\x00').endswith(b'out of his mind.')

    assert subscriber_selective.sample_statistics() == subscriber_promiscuous.sample_statistics()
    assert subscriber_promiscuous.sample_statistics() == SessionStatistics(transfers=1,
                                                                           frames=2,
                                                                           payload_bytes=124)  # Includes padding!

    # Small delay is needed to make the small-TID instance certainly time out on Windows, where clock resolution is low.
    await asyncio.sleep(0.1)
    assert await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.NOMINAL,
        transfer_id=7,                  # Same transfer ID, will be accepted only by the instance with low TID timeout
        fragmented_payload=[]
    ), tr.loop.time() + 1.0)

    assert tr.sample_statistics() == can.CANTransportStatistics(out_frames=16,
                                                                in_frames=5,
                                                                in_frames_uavcan=3,
                                                                in_frames_uavcan_accepted=3,
                                                                out_frames_loopback=2,
                                                                in_frames_loopback=6)

    assert tr2.sample_statistics() == can.CANTransportStatistics(out_frames=3,
                                                                 in_frames=16,
                                                                 in_frames_uavcan=16,
                                                                 in_frames_uavcan_accepted=15)

    received = await subscriber_promiscuous.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert isinstance(received, TransferFrom)
    assert received.source_node_id == 123
    assert received.priority == Priority.NOMINAL
    assert received.transfer_id == 7
    validate_timestamp(received.timestamp)
    assert b''.join(received.fragmented_payload) == b''

    assert subscriber_promiscuous.sample_statistics() == SessionStatistics(transfers=2,
                                                                           frames=3,
                                                                           payload_bytes=124)

    # Discarded because of the same transfer ID
    assert (await subscriber_selective.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert subscriber_selective.sample_statistics() == SessionStatistics(
        transfers=1,
        frames=3,
        payload_bytes=124,
        errors=1            # Error due to the repeated transfer ID
    )

    assert await pub_m2222.send(Transfer(
        timestamp=ts,
        priority=Priority.HIGH,
        transfer_id=8,
        fragmented_payload=[
            _mem('a' * 63),
            _mem('b' * 63),
            _mem('c' * 63),
            _mem('d' * 62),  # Tricky case - one of the CRC bytes spills over into the fifth frame
        ]
    ), tr.loop.time() + 1.0)

    # The promiscuous one is able to receive the transfer since its queue is large enough
    received = await subscriber_promiscuous.receive(tr.loop.time() + 1.0)
    assert received is not None
    assert received.priority == Priority.HIGH
    assert received.transfer_id == 8
    validate_timestamp(received.timestamp)
    assert list(map(bytes, received.fragmented_payload)) == [
        b'a' * 63,
        b'b' * 63,
        b'c' * 63,
        b'd' * 62,
    ]
    assert subscriber_promiscuous.sample_statistics() == SessionStatistics(transfers=3,
                                                                           frames=8,
                                                                           payload_bytes=375)

    # The selective one is unable to do so since its RX queue is too small; it is reflected in the error counter
    assert (await subscriber_selective.receive(tr.loop.time() + _RX_TIMEOUT)) is None
    assert subscriber_selective.sample_statistics() == SessionStatistics(transfers=1,
                                                                         frames=5,
                                                                         payload_bytes=124,
                                                                         errors=1,
                                                                         drops=3)  # Overruns!

    #
    # Finalization.
    #
    print('str(CANTransport):', tr)
    print('str(CANTransport):', tr2)
    client_listener.close()
    server_listener.close()
    subscriber_promiscuous.close()
    subscriber_selective.close()
    tr.close()
    tr2.close()
    # Double-close has no effect:
    client_listener.close()
    server_listener.close()
    subscriber_promiscuous.close()
    subscriber_selective.close()
    tr.close()
    tr2.close()
    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


@pytest.mark.asyncio    # type: ignore
async def _unittest_issue_120() -> None:
    from pyuavcan.transport import MessageDataSpecifier, PayloadMetadata, Transfer
    from pyuavcan.transport import Priority, Timestamp, OutputSessionSpecifier
    from .media.mock import MockMedia

    asyncio.get_running_loop().slow_callback_duration = 5.0

    peers: typing.Set[MockMedia] = set()
    media = MockMedia(peers, 8, 10)
    tr = can.CANTransport(media, 42)
    assert tr.protocol_parameters.transfer_id_modulo == 32

    feedback_collector = _FeedbackCollector()

    ses = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), PayloadMetadata(1024))
    ses.enable_feedback(feedback_collector.give)
    for i in range(70):
        ts = Timestamp.now()
        assert await ses.send(Transfer(
            timestamp=ts,
            priority=Priority.SLOW,
            transfer_id=i,
            fragmented_payload=[_mem(str(i))] * 7  # Ensure both single- and multiframe
        ), tr.loop.time() + 1.0)
        await asyncio.sleep(0.1)
        fb = feedback_collector.take()
        assert fb.original_transfer_timestamp == ts

    num_frames = (10 * 1) + (60 * 3)                                    # 10 single-frame, 60 multi-frame
    assert 70 == ses.sample_statistics().transfers
    assert num_frames == ses.sample_statistics().frames
    assert 0 == tr.sample_statistics().in_frames                        # loopback not included here
    assert 70 == tr.sample_statistics().in_frames_loopback              # only first frame of each transfer
    assert num_frames == tr.sample_statistics().out_frames
    assert 70 == tr.sample_statistics().out_frames_loopback             # only first frame of each transfer
    assert 0 == tr.sample_statistics().lost_loopback_frames


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
