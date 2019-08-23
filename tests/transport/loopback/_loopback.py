#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import asyncio
import pytest

import pyuavcan.transport
import pyuavcan.transport.loopback


@pytest.mark.asyncio    # type: ignore
async def _unittest_loopback_transport() -> None:
    tr = pyuavcan.transport.loopback.LoopbackTransport()

    protocol_params = pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        node_id_set_cardinality=2 ** 64,
        single_frame_transfer_payload_capacity_bytes=2 ** 64 - 1,
    )
    tr.protocol_parameters = protocol_params
    assert tr.protocol_parameters == protocol_params

    assert tr.loop is asyncio.get_event_loop()

    assert tr.local_node_id is None
    with pytest.raises(ValueError):
        tr.set_local_node_id(-1)
    assert tr.local_node_id is None
    tr.set_local_node_id(42)
    assert tr.local_node_id == 42
    with pytest.raises(pyuavcan.transport.InvalidTransportConfigurationError):
        tr.set_local_node_id(123)

    payload_metadata = pyuavcan.transport.PayloadMetadata(0xdeadbeef0ddf00d, 1234)

    message_spec_123 = pyuavcan.transport.SessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    message_spec_42 = pyuavcan.transport.SessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 42)
    message_spec_any = pyuavcan.transport.SessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), None)

    out_123 = tr.get_output_session(specifier=message_spec_123, payload_metadata=payload_metadata)
    assert out_123 is tr.get_output_session(specifier=message_spec_123, payload_metadata=payload_metadata)

    last_feedback: typing.Optional[pyuavcan.transport.Feedback] = None

    def on_feedback(fb: pyuavcan.transport.Feedback) -> None:
        nonlocal last_feedback
        last_feedback = fb

    out_123.enable_feedback(on_feedback)

    ts = pyuavcan.transport.Timestamp.now()
    assert await out_123.send_until(pyuavcan.transport.Transfer(
        timestamp=ts,
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    out_123.disable_feedback()

    assert last_feedback is not None
    assert last_feedback.original_transfer_timestamp == ts
    assert last_feedback.first_frame_transmission_timestamp == ts
    del ts

    assert out_123.sample_statistics() == pyuavcan.transport.SessionStatistics(
        transfers=1,
        frames=1,
        payload_bytes=len('Hello world!'),
    )

    old_out = out_123
    out_123.close()
    out_123.close()  # Double close handled properly
    out_123 = tr.get_output_session(specifier=message_spec_123, payload_metadata=payload_metadata)
    assert out_123 is not old_out
    del old_out

    inp_123 = tr.get_input_session(specifier=message_spec_123, payload_metadata=payload_metadata)
    assert inp_123 is tr.get_input_session(specifier=message_spec_123, payload_metadata=payload_metadata)

    old_inp = inp_123
    inp_123.close()
    inp_123.close()  # Double close handled properly
    inp_123 = tr.get_input_session(specifier=message_spec_123, payload_metadata=payload_metadata)
    assert old_inp is not inp_123
    del old_inp

    assert None is await inp_123.receive_until(0)
    assert None is await inp_123.receive_until(tr.loop.time() + 1.0)

    # This one will be dropped because wrong target node 123 != 42
    assert await out_123.send_until(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    assert None is await inp_123.receive_until(0)
    assert None is await inp_123.receive_until(tr.loop.time() + 1.0)

    out_bc = tr.get_output_session(specifier=message_spec_any, payload_metadata=payload_metadata)
    assert out_123 is not out_bc

    inp_42 = tr.get_input_session(specifier=message_spec_42, payload_metadata=payload_metadata)
    assert inp_123 is not inp_42

    assert await out_bc.send_until(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    assert None is await inp_123.receive_until(0)
    assert None is await inp_123.receive_until(tr.loop.time() + 1.0)

    rx = await inp_42.receive_until(0)
    assert rx is not None
    assert rx.timestamp.monotonic <= time.monotonic()
    assert rx.timestamp.system <= time.time()
    assert rx.priority == pyuavcan.transport.Priority.IMMEDIATE
    assert rx.transfer_id == 27
    assert rx.fragmented_payload == [memoryview(b'Hello world!')]
    assert rx.source_node_id == tr.local_node_id

    assert inp_42.sample_statistics() == pyuavcan.transport.SessionStatistics(
        transfers=1,
        frames=1,
        payload_bytes=len('Hello world!'),
    )

    assert len(tr.input_sessions) == 2
    assert len(tr.output_sessions) == 2
    tr.close()
    assert len(tr.input_sessions) == 0
    assert len(tr.output_sessions) == 0


@pytest.mark.asyncio    # type: ignore
async def _unittest_loopback_transport_service() -> None:
    from pyuavcan.transport import ServiceDataSpecifier, SessionSpecifier

    payload_metadata = pyuavcan.transport.PayloadMetadata(0xdeadbeef0ddf00d, 1234)

    tr = pyuavcan.transport.loopback.LoopbackTransport()
    tr.set_local_node_id(1234)

    inp = tr.get_input_session(SessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST), 1234),
                               payload_metadata)

    out = tr.get_output_session(SessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST), 1234),
                                payload_metadata)

    assert await out.send_until(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)

    assert None is not await inp.receive_until(0)
