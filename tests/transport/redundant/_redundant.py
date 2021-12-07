# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import logging
import pytest
import pyuavcan.transport

# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.redundant import RedundantTransport, RedundantTransportStatistics
from pyuavcan.transport.redundant import InconsistentInferiorConfigurationError
from pyuavcan.transport.loopback import LoopbackTransport
from pyuavcan.transport.serial import SerialTransport
from pyuavcan.transport.udp import UDPTransport
from pyuavcan.transport.can import CANTransport
from tests.transport.serial import VIRTUAL_BUS_URI as SERIAL_URI


@pytest.mark.asyncio
async def _unittest_redundant_transport(caplog: typing.Any) -> None:
    from pyuavcan.transport import MessageDataSpecifier, PayloadMetadata, Transfer
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 5.0

    tr_a = RedundantTransport()
    tr_b = RedundantTransport(loop=loop)
    assert tr_a.sample_statistics() == RedundantTransportStatistics([])
    assert tr_a.inferiors == []
    assert tr_a.local_node_id is None
    assert tr_a.loop is asyncio.get_event_loop()
    assert tr_a.local_node_id is None
    assert tr_a.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=0,
        max_nodes=0,
        mtu=0,
    )
    assert tr_a.input_sessions == []
    assert tr_a.output_sessions == []

    assert tr_a.loop == tr_b.loop

    #
    # Instantiate session objects.
    #
    meta = PayloadMetadata(10_240)

    pub_a = tr_a.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    sub_any_a = tr_a.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert pub_a is tr_a.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    assert set(tr_a.input_sessions) == {sub_any_a}
    assert set(tr_a.output_sessions) == {pub_a}
    assert tr_a.sample_statistics() == RedundantTransportStatistics()

    pub_b = tr_b.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    sub_any_b = tr_b.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), None), meta)
    sub_sel_b = tr_b.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)
    assert sub_sel_b is tr_b.get_input_session(InputSessionSpecifier(MessageDataSpecifier(2345), 3210), meta)
    assert set(tr_b.input_sessions) == {sub_any_b, sub_sel_b}
    assert set(tr_b.output_sessions) == {pub_b}
    assert tr_b.sample_statistics() == RedundantTransportStatistics()

    #
    # Exchange test with no inferiors, expected to fail.
    #
    assert len(pub_a.inferiors) == 0
    assert len(sub_any_a.inferiors) == 0
    assert not await pub_a.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=1, fragmented_payload=[memoryview(b"abc")]
        ),
        monotonic_deadline=loop.time() + 1.0,
    )
    assert not await sub_any_a.receive(loop.time() + 0.1)
    assert not await sub_any_b.receive(loop.time() + 0.1)
    assert tr_a.sample_statistics() == RedundantTransportStatistics()
    assert tr_b.sample_statistics() == RedundantTransportStatistics()

    #
    # Adding inferiors - loopback, transport A only.
    #
    with pytest.raises(InconsistentInferiorConfigurationError, match="(?i).*loop.*"):
        tr_a.attach_inferior(LoopbackTransport(111, loop=asyncio.new_event_loop()))  # Wrong event loop.
    assert len(pub_a.inferiors) == 0
    assert len(sub_any_a.inferiors) == 0

    lo_mono_0 = LoopbackTransport(111)
    lo_mono_1 = LoopbackTransport(111)

    tr_a.attach_inferior(lo_mono_0)
    assert len(pub_a.inferiors) == 1
    assert len(sub_any_a.inferiors) == 1

    with pytest.raises(ValueError):
        tr_a.detach_inferior(lo_mono_1)  # Not a registered inferior (yet).

    tr_a.attach_inferior(lo_mono_1)
    assert len(pub_a.inferiors) == 2
    assert len(sub_any_a.inferiors) == 2

    with pytest.raises(ValueError):
        tr_a.attach_inferior(lo_mono_0)  # Double-add not allowed.

    with pytest.raises(InconsistentInferiorConfigurationError, match="(?i).*node-id.*"):
        tr_a.attach_inferior(LoopbackTransport(None))  # Wrong node-ID.

    with pytest.raises(InconsistentInferiorConfigurationError, match="(?i).*node-id.*"):
        tr_a.attach_inferior(LoopbackTransport(1230))  # Wrong node-ID.

    assert tr_a.inferiors == [lo_mono_0, lo_mono_1]
    assert len(pub_a.inferiors) == 2
    assert len(sub_any_a.inferiors) == 2

    assert tr_a.sample_statistics() == RedundantTransportStatistics(
        inferiors=[
            lo_mono_0.sample_statistics(),
            lo_mono_1.sample_statistics(),
        ]
    )
    assert tr_a.local_node_id == 111
    assert (
        repr(tr_a)
        == "RedundantTransport(LoopbackTransport(local_node_id=111, allow_anonymous_transfers=True),"
        + " LoopbackTransport(local_node_id=111, allow_anonymous_transfers=True))"
    )

    assert await pub_a.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=2, fragmented_payload=[memoryview(b"def")]
        ),
        monotonic_deadline=loop.time() + 1.0,
    )
    rx = await sub_any_a.receive(loop.time() + 1.0)
    assert rx is not None
    assert rx.fragmented_payload == [memoryview(b"def")]
    assert rx.transfer_id == 2
    assert not await sub_any_b.receive(loop.time() + 0.1)

    #
    # Incapacitate one inferior, ensure things are still OK.
    #
    with caplog.at_level(logging.CRITICAL, logger=pyuavcan.transport.redundant.__name__):
        for s in lo_mono_0.output_sessions:
            s.exception = RuntimeError("INTENDED EXCEPTION")

        assert await pub_a.send(
            Transfer(
                timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=3, fragmented_payload=[memoryview(b"qwe")]
            ),
            monotonic_deadline=loop.time() + 1.0,
        )
        rx = await sub_any_a.receive(loop.time() + 1.0)
        assert rx is not None
        assert rx.fragmented_payload == [memoryview(b"qwe")]
        assert rx.transfer_id == 3

    #
    # Remove old loopback transports. Configure new ones with cyclic TID.
    #
    lo_cyc_0 = LoopbackTransport(111)
    lo_cyc_1 = LoopbackTransport(111)
    cyc_proto_params = ProtocolParameters(
        transfer_id_modulo=32,  # Like CAN
        max_nodes=128,  # Like CAN
        mtu=63,  # Like CAN
    )
    lo_cyc_0.protocol_parameters = cyc_proto_params
    lo_cyc_1.protocol_parameters = cyc_proto_params
    assert lo_cyc_0.protocol_parameters == lo_cyc_1.protocol_parameters == cyc_proto_params

    assert tr_a.protocol_parameters.transfer_id_modulo >= 2 ** 56
    with pytest.raises(InconsistentInferiorConfigurationError, match="(?i).*transfer-id.*"):
        tr_a.attach_inferior(lo_cyc_0)  # Transfer-ID modulo mismatch

    tr_a.detach_inferior(lo_mono_0)
    tr_a.detach_inferior(lo_mono_1)
    del lo_mono_0  # Prevent accidental reuse.
    del lo_mono_1
    assert tr_a.inferiors == []  # All removed, okay.
    assert pub_a.inferiors == []
    assert sub_any_a.inferiors == []
    assert tr_a.local_node_id is None  # Back to the roots
    assert repr(tr_a) == "RedundantTransport()"

    # Now we can add our cyclic transports safely.
    tr_a.attach_inferior(lo_cyc_0)
    assert tr_a.protocol_parameters.transfer_id_modulo == 32
    tr_a.attach_inferior(lo_cyc_1)
    assert tr_a.protocol_parameters == cyc_proto_params, "Protocol parameter mismatch"
    assert tr_a.local_node_id == 111
    assert (
        repr(tr_a)
        == "RedundantTransport(LoopbackTransport(local_node_id=111, allow_anonymous_transfers=True),"
        + " LoopbackTransport(local_node_id=111, allow_anonymous_transfers=True))"
    )

    # Exchange test.
    assert await pub_a.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=4, fragmented_payload=[memoryview(b"rty")]
        ),
        monotonic_deadline=loop.time() + 1.0,
    )
    rx = await sub_any_a.receive(loop.time() + 1.0)
    assert rx is not None
    assert rx.fragmented_payload == [memoryview(b"rty")]
    assert rx.transfer_id == 4

    #
    # Real heterogeneous transport test.
    #
    tr_a.detach_inferior(lo_cyc_0)
    tr_a.detach_inferior(lo_cyc_1)
    del lo_cyc_0  # Prevent accidental reuse.
    del lo_cyc_1

    udp_a = UDPTransport("127.0.0.111")
    udp_b = UDPTransport("127.0.0.222")

    serial_a = SerialTransport(SERIAL_URI, 111)
    serial_b = SerialTransport(SERIAL_URI, 222, mtu=2048)  # Heterogeneous.

    tr_a.attach_inferior(udp_a)
    tr_a.attach_inferior(serial_a)

    tr_b.attach_inferior(udp_b)
    tr_b.attach_inferior(serial_b)

    assert tr_a.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=4096,
        mtu=udp_a.protocol_parameters.mtu,
    )
    assert tr_a.local_node_id == 111
    assert repr(tr_a) == f"RedundantTransport({udp_a}, {serial_a})"

    assert tr_b.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        max_nodes=4096,
        mtu=udp_b.protocol_parameters.mtu,
    )
    assert tr_b.local_node_id == 222

    assert await pub_a.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=5, fragmented_payload=[memoryview(b"uio")]
        ),
        monotonic_deadline=loop.time() + 1.0,
    )
    rx = await sub_any_b.receive(loop.time() + 1.0)
    assert rx is not None
    assert rx.fragmented_payload == [memoryview(b"uio")]
    assert rx.transfer_id == 5
    assert not await sub_any_a.receive(loop.time() + 0.1)
    assert not await sub_any_b.receive(loop.time() + 0.1)
    assert not await sub_sel_b.receive(loop.time() + 0.1)

    #
    # Construct new session with the transports configured.
    #
    pub_a_new = tr_a.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(255), None), meta)
    assert pub_a_new is tr_a.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(255), None), meta)
    assert set(tr_a.output_sessions) == {pub_a, pub_a_new}
    sub_b_new = tr_b.get_input_session(InputSessionSpecifier(MessageDataSpecifier(255), None), meta)

    assert await pub_a_new.send(
        Transfer(
            timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=6, fragmented_payload=[memoryview(b"asd")]
        ),
        monotonic_deadline=loop.time() + 1.0,
    )
    rx = await sub_b_new.receive(loop.time() + 1.0)
    assert rx is not None
    assert rx.fragmented_payload == [memoryview(b"asd")]
    assert rx.transfer_id == 6
    assert None is await sub_any_b.receive(loop.time() + 1.0)

    #
    # Termination.
    #
    tr_a.close()
    tr_a.close()  # Idempotency
    tr_b.close()
    tr_b.close()  # Idempotency

    with pytest.raises(pyuavcan.transport.ResourceClosedError):  # Make sure the inferiors are closed.
        udp_a.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):  # Make sure the inferiors are closed.
        serial_b.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(2345), None), meta)

    with pytest.raises(pyuavcan.transport.ResourceClosedError):  # Make sure the sessions are closed.
        await pub_a.send(
            Transfer(timestamp=Timestamp.now(), priority=Priority.LOW, transfer_id=100, fragmented_payload=[]),
            monotonic_deadline=loop.time() + 1.0,
        )

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


@pytest.mark.asyncio
async def _unittest_redundant_transport_capture() -> None:
    from threading import Lock
    from pyuavcan.transport import Capture, Trace, TransferTrace, Priority, ServiceDataSpecifier
    from pyuavcan.transport import AlienTransfer, AlienTransferMetadata, AlienSessionSpecifier
    from pyuavcan.transport.redundant import RedundantDuplicateTransferTrace, RedundantCapture
    from tests.transport.can.media.mock import MockMedia as CANMockMedia

    asyncio.get_event_loop().slow_callback_duration = 5.0

    tracer = RedundantTransport.make_tracer()
    traces: typing.List[typing.Optional[Trace]] = []
    lock = Lock()

    def handle_capture(cap: Capture) -> None:
        with lock:
            # Drop TX frames, they are not interesting for this test.
            assert isinstance(cap, RedundantCapture)
            if isinstance(cap.inferior, pyuavcan.transport.serial.SerialCapture) and cap.inferior.own:
                return
            if isinstance(cap.inferior, pyuavcan.transport.can.CANCapture) and cap.inferior.own:
                return
            print("CAPTURE:", cap)
            traces.append(tracer.update(cap))

    async def wait(how_many: int) -> None:
        for _ in range(10):
            await asyncio.sleep(0.1)
            with lock:
                if len(traces) >= 2:
                    return
        assert False, "No traces received"

    # Setup capture -- one is added before capture started, the other is added later.
    # Make sure they are treated identically.
    tr = RedundantTransport()
    inf_a: pyuavcan.transport.Transport = SerialTransport(SERIAL_URI, 1234)
    inf_b: pyuavcan.transport.Transport = SerialTransport(SERIAL_URI, 1234)
    tr.attach_inferior(inf_a)
    assert not tr.capture_active
    assert not inf_a.capture_active
    assert not inf_b.capture_active
    tr.begin_capture(handle_capture)
    assert tr.capture_active
    assert inf_a.capture_active
    assert not inf_b.capture_active
    tr.attach_inferior(inf_b)
    assert tr.capture_active
    assert inf_a.capture_active
    assert inf_b.capture_active

    # Send a transfer and make sure it is handled and deduplicated correctly.
    transfer = AlienTransfer(
        AlienTransferMetadata(
            priority=Priority.IMMEDIATE,
            transfer_id=1234,
            session_specifier=AlienSessionSpecifier(
                source_node_id=321,
                destination_node_id=222,
                data_specifier=ServiceDataSpecifier(77, ServiceDataSpecifier.Role.REQUEST),
            ),
        ),
        [memoryview(b"hello")],
    )
    assert await tr.spoof(transfer, monotonic_deadline=tr.loop.time() + 1.0)
    await wait(2)
    with lock:
        # Check the status of the deduplication process. We should get two: one transfer, one duplicate.
        assert len(traces) == 2
        trace = traces.pop(0)
        assert isinstance(trace, TransferTrace)
        assert trace.transfer == transfer
        # This is the duplicate.
        assert isinstance(traces.pop(0), RedundantDuplicateTransferTrace)
        assert not traces

    # Spoof the same thing again, get nothing out: transfers discarded by the inferior's own reassemblers.
    # WARNING: this will fail if too much time has passed since the previous transfer due to TID timeout.
    assert await tr.spoof(transfer, monotonic_deadline=tr.loop.time() + 1.0)
    await wait(2)
    with lock:
        assert None is traces.pop(0)
        assert None is traces.pop(0)
        assert not traces

    # But if we change ONLY destination, deduplication will not take place.
    transfer = AlienTransfer(
        AlienTransferMetadata(
            priority=Priority.IMMEDIATE,
            transfer_id=1234,
            session_specifier=AlienSessionSpecifier(
                source_node_id=321,
                destination_node_id=333,
                data_specifier=ServiceDataSpecifier(77, ServiceDataSpecifier.Role.REQUEST),
            ),
        ),
        [memoryview(b"hello")],
    )
    assert await tr.spoof(transfer, monotonic_deadline=tr.loop.time() + 1.0)
    await wait(2)
    with lock:
        # Check the status of the deduplication process. We should get two: one transfer, one duplicate.
        assert len(traces) == 2
        trace = traces.pop(0)
        assert isinstance(trace, TransferTrace)
        assert trace.transfer == transfer
        # This is the duplicate.
        assert isinstance(traces.pop(0), RedundantDuplicateTransferTrace)
        assert not traces

    # Change the inferior configuration and make sure it is handled properly.
    tr.detach_inferior(inf_a)
    tr.detach_inferior(inf_b)
    inf_a.close()
    inf_b.close()
    # The new inferiors use cyclic transfer-ID; the tracer should reconfigure itself automatically!
    can_peers: typing.Set[CANMockMedia] = set()
    inf_a = CANTransport(CANMockMedia(can_peers, 64, 2), 111)
    inf_b = CANTransport(CANMockMedia(can_peers, 64, 2), 111)
    tr.attach_inferior(inf_a)
    tr.attach_inferior(inf_b)
    # Capture should have been launched automatically.
    assert inf_a.capture_active
    assert inf_b.capture_active

    # Send transfer over CAN and observe that it is handled well.
    transfer = AlienTransfer(
        AlienTransferMetadata(
            priority=Priority.IMMEDIATE,
            transfer_id=19,
            session_specifier=AlienSessionSpecifier(
                source_node_id=111,
                destination_node_id=22,
                data_specifier=ServiceDataSpecifier(77, ServiceDataSpecifier.Role.REQUEST),
            ),
        ),
        [memoryview(b"hello")],
    )
    assert await tr.spoof(transfer, monotonic_deadline=tr.loop.time() + 1.0)
    await wait(2)
    with lock:
        # Check the status of the deduplication process. We should get two: one transfer, one duplicate.
        assert len(traces) == 2
        trace = traces.pop(0)
        assert isinstance(trace, TransferTrace)
        assert trace.transfer == transfer
        # This is the duplicate.
        assert isinstance(traces.pop(0), RedundantDuplicateTransferTrace)
        assert not traces

    # Dispose of everything.
    tr.close()
    await asyncio.sleep(1.0)


def _unittest_redundant_transport_reconfiguration() -> None:
    from pyuavcan.transport import OutputSessionSpecifier, MessageDataSpecifier, PayloadMetadata

    tr = RedundantTransport()
    tr.attach_inferior(LoopbackTransport(1234))
    ses = tr.get_output_session(OutputSessionSpecifier(MessageDataSpecifier(5555), None), PayloadMetadata(0))
    assert ses
    tr.detach_inferior(tr.inferiors[0])
    tr.attach_inferior(LoopbackTransport(1235))  # Different node-ID
    tr.detach_inferior(tr.inferiors[0])
    tr.attach_inferior(LoopbackTransport(None, allow_anonymous_transfers=True))  # Anonymous
    with pytest.raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        tr.attach_inferior(LoopbackTransport(None, allow_anonymous_transfers=False))
    assert len(tr.inferiors) == 1
