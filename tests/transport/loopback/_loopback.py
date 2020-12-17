#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import time
import typing
import asyncio
import logging
import pytest

import pyuavcan.transport
import pyuavcan.transport.loopback


@pytest.mark.asyncio    # type: ignore
async def _unittest_loopback_transport(caplog: typing.Any) -> None:
    tr = pyuavcan.transport.loopback.LoopbackTransport(None)
    protocol_params = pyuavcan.transport.ProtocolParameters(
        transfer_id_modulo=32,
        max_nodes=2 ** 64,
        mtu=2 ** 64 - 1,
    )
    tr.protocol_parameters = protocol_params
    assert tr.protocol_parameters == protocol_params
    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id is None

    tr = pyuavcan.transport.loopback.LoopbackTransport(42)
    tr.protocol_parameters = protocol_params
    assert 42 == tr.local_node_id

    payload_metadata = pyuavcan.transport.PayloadMetadata(1234)

    message_spec_123_in = pyuavcan.transport.InputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    message_spec_123_out = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    message_spec_42_in = pyuavcan.transport.InputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 42)
    message_spec_any_out = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), None)

    out_123 = tr.get_output_session(specifier=message_spec_123_out, payload_metadata=payload_metadata)
    assert out_123 is tr.get_output_session(specifier=message_spec_123_out, payload_metadata=payload_metadata)

    last_feedback: typing.Optional[pyuavcan.transport.Feedback] = None

    def on_feedback(fb: pyuavcan.transport.Feedback) -> None:
        nonlocal last_feedback
        last_feedback = fb

    out_123.enable_feedback(on_feedback)

    ts = pyuavcan.transport.Timestamp.now()
    assert await out_123.send(pyuavcan.transport.Transfer(
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
    out_123 = tr.get_output_session(specifier=message_spec_123_out, payload_metadata=payload_metadata)
    assert out_123 is not old_out
    del old_out

    inp_123 = tr.get_input_session(specifier=message_spec_123_in, payload_metadata=payload_metadata)
    assert inp_123 is tr.get_input_session(specifier=message_spec_123_in, payload_metadata=payload_metadata)

    old_inp = inp_123
    inp_123.close()
    inp_123.close()  # Double close handled properly
    inp_123 = tr.get_input_session(specifier=message_spec_123_in, payload_metadata=payload_metadata)
    assert old_inp is not inp_123
    del old_inp

    assert None is await inp_123.receive(0)
    assert None is await inp_123.receive(tr.loop.time() + 1.0)

    # This one will be dropped because wrong target node 123 != 42
    assert await out_123.send(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    assert None is await inp_123.receive(0)
    assert None is await inp_123.receive(tr.loop.time() + 1.0)

    out_bc = tr.get_output_session(specifier=message_spec_any_out, payload_metadata=payload_metadata)
    assert out_123 is not out_bc

    inp_42 = tr.get_input_session(specifier=message_spec_42_in, payload_metadata=payload_metadata)
    assert inp_123 is not inp_42

    assert await out_bc.send(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    assert None is await inp_123.receive(0)
    assert None is await inp_123.receive(tr.loop.time() + 1.0)

    rx = await inp_42.receive(0)
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

    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.loopback.__name__):
        out_bc.exception = RuntimeError('INTENDED EXCEPTION')
        with pytest.raises(ValueError):
            # noinspection PyTypeHints
            out_bc.exception = 123  # type: ignore
        with pytest.raises(RuntimeError, match='INTENDED EXCEPTION'):
            assert await out_bc.send(pyuavcan.transport.Transfer(
                timestamp=pyuavcan.transport.Timestamp.now(),
                priority=pyuavcan.transport.Priority.IMMEDIATE,
                transfer_id=123,        # mod 32 = 27
                fragmented_payload=[memoryview(b'Hello world!')],
            ), tr.loop.time() + 1.0)
        assert isinstance(out_bc.exception, RuntimeError)
        out_bc.exception = None
        assert out_bc.exception is None

    assert None is await inp_42.receive(0)

    mon_events: typing.List[pyuavcan.transport.Capture] = []
    mon_events2: typing.List[pyuavcan.transport.Capture] = []
    assert tr.capture_handlers == []
    tr.begin_capture(mon_events.append)
    assert len(tr.capture_handlers) == 1
    tr.begin_capture(mon_events2.append)
    assert len(tr.capture_handlers) == 2
    assert await out_bc.send(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=200,
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)
    rx = await inp_42.receive(0)
    assert rx is not None
    assert rx.transfer_id == 200 % 32
    ev, = mon_events
    assert isinstance(ev, pyuavcan.transport.loopback.LoopbackCapture)
    assert ev.timestamp == rx.timestamp
    assert ev.transfer.transfer_id == rx.transfer_id
    assert ev.transfer.session_specifier.source_node_id == tr.local_node_id
    assert ev.transfer.session_specifier.destination_node_id is None
    assert mon_events2 == mon_events

    assert len(tr.input_sessions) == 2
    assert len(tr.output_sessions) == 2
    tr.close()
    assert len(tr.input_sessions) == 0
    assert len(tr.output_sessions) == 0
    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


@pytest.mark.asyncio    # type: ignore
async def _unittest_loopback_transport_service() -> None:
    from pyuavcan.transport import ServiceDataSpecifier, InputSessionSpecifier, OutputSessionSpecifier

    payload_metadata = pyuavcan.transport.PayloadMetadata(1234)

    tr = pyuavcan.transport.loopback.LoopbackTransport(1234)

    inp = tr.get_input_session(InputSessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST),
                                                     1234),
                               payload_metadata)

    out = tr.get_output_session(OutputSessionSpecifier(ServiceDataSpecifier(123, ServiceDataSpecifier.Role.REQUEST),
                                                       1234),
                                payload_metadata)

    assert await out.send(pyuavcan.transport.Transfer(
        timestamp=pyuavcan.transport.Timestamp.now(),
        priority=pyuavcan.transport.Priority.IMMEDIATE,
        transfer_id=123,        # mod 32 = 27
        fragmented_payload=[memoryview(b'Hello world!')],
    ), tr.loop.time() + 1.0)

    assert None is not await inp.receive(0)


@pytest.mark.asyncio    # type: ignore
async def _unittest_loopback_tracer() -> None:
    from pyuavcan.transport import AlienTransfer, AlienSessionSpecifier, Timestamp, Priority
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, TransferTrace
    from pyuavcan.transport.loopback import LoopbackCapture

    tr = pyuavcan.transport.loopback.LoopbackTransport.make_tracer()
    ts = Timestamp.now()

    # MESSAGE
    msg = AlienTransfer(priority=Priority.IMMEDIATE,
                        session_specifier=AlienSessionSpecifier(1234, None, MessageDataSpecifier(7777)),
                        transfer_id=54321,
                        fragmented_payload=[])
    assert tr.update(LoopbackCapture(ts, msg)) == TransferTrace(
        timestamp=ts,
        transfer=msg,
        frames=[LoopbackCapture(ts, msg)],
        sibling=None,
    )

    # REQUEST (to be matched with later)
    req = AlienTransfer(
        priority=Priority.NOMINAL,
        session_specifier=AlienSessionSpecifier(321,
                                                123,
                                                ServiceDataSpecifier(222, ServiceDataSpecifier.Role.REQUEST)),
        transfer_id=333333333,
        fragmented_payload=[],
    )
    trace_req = tr.update(LoopbackCapture(ts, req))
    assert isinstance(trace_req, TransferTrace)
    assert trace_req == TransferTrace(
        timestamp=ts,
        transfer=req,
        frames=[LoopbackCapture(ts, req)],
        sibling=None,
    )

    # RESPONSE (mismatching)
    res = AlienTransfer(
        priority=Priority.NOMINAL,
        session_specifier=AlienSessionSpecifier(123,
                                                444,  # Wrong node-ID
                                                ServiceDataSpecifier(222, ServiceDataSpecifier.Role.RESPONSE)),
        transfer_id=333333333,
        fragmented_payload=[],
    )
    assert tr.update(LoopbackCapture(ts, res)) == TransferTrace(
        timestamp=ts,
        transfer=res,
        frames=[LoopbackCapture(ts, res)],
        sibling=None,
    )

    # RESPONSE (matching)
    res = AlienTransfer(
        priority=Priority.NOMINAL,
        session_specifier=AlienSessionSpecifier(123,
                                                321,
                                                ServiceDataSpecifier(222, ServiceDataSpecifier.Role.RESPONSE)),
        transfer_id=333333333,
        fragmented_payload=[],
    )
    assert tr.update(LoopbackCapture(ts, res)) == TransferTrace(
        timestamp=ts,
        transfer=res,
        frames=[LoopbackCapture(ts, res)],
        sibling=trace_req,
    )

    # Unknown capture types should yield None.
    assert tr.update(pyuavcan.transport.Capture(ts)) is None
