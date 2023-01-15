# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import asyncio
import ipaddress
from pycyphal.transport import Timestamp
from pycyphal.transport import Priority, PayloadMetadata
from pycyphal.transport import InputSessionSpecifier, MessageDataSpecifier
from pycyphal.transport.udp import UDPFrame
from pycyphal.transport.udp._session._input import PromiscuousUDPInputSession, SelectiveUDPInputSession
from pycyphal.transport.udp._session._input import PromiscuousUDPInputSessionStatistics
from pycyphal.transport.udp._session._input import SelectiveUDPInputSessionStatistics
from pycyphal.transport.udp._ip._endpoint_mapping import DESTINATION_PORT
from pycyphal.transport.udp._ip import IPv4SocketFactory

from pycyphal.transport.commons.high_overhead_transport import TransferReassembler
from pycyphal.transport.commons.crc import CRC32C

TransferCRC = CRC32C


async def _unittest_udp_input_session_uniframe() -> None:
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 5.0  # TODO use asyncio socket read and remove this thing.
    prom_finalized = False
    sel_finalized = False

    def do_finalize_prom() -> None:
        nonlocal prom_finalized
        prom_finalized = True

    def do_finalize_sel() -> None:
        nonlocal sel_finalized
        sel_finalized = True

    # SETUP

    sock_fac = IPv4SocketFactory(local_ip_addr=ipaddress.IPv4Address("127.0.0.1"))

    msg_sock_rx_1 = sock_fac.make_input_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))
    assert "239.0.0.123" == msg_sock_rx_1.getsockname()[0]
    assert DESTINATION_PORT == msg_sock_rx_1.getsockname()[1]

    msg_sock_rx_2 = sock_fac.make_input_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))
    assert "239.0.0.123" == msg_sock_rx_1.getsockname()[0]
    assert DESTINATION_PORT == msg_sock_rx_1.getsockname()[1]

    # create promiscuous input session, uses msg_sock_rx_1
    prom_in = PromiscuousUDPInputSession(
        specifier=InputSessionSpecifier(data_specifier=MessageDataSpecifier(123), remote_node_id=None),
        payload_metadata=PayloadMetadata(1024),
        sock=msg_sock_rx_1,
        finalizer=do_finalize_prom,
    )

    assert prom_in.specifier.data_specifier == MessageDataSpecifier(123)
    assert prom_in.specifier.remote_node_id == None

    # create selective input session, uses msg_sock_rx_2
    sel_in = SelectiveUDPInputSession(
        specifier=InputSessionSpecifier(data_specifier=MessageDataSpecifier(123), remote_node_id=10),
        payload_metadata=PayloadMetadata(1024),
        sock=msg_sock_rx_2,
        finalizer=do_finalize_sel,
    )

    assert sel_in.specifier.data_specifier == MessageDataSpecifier(123)
    assert sel_in.specifier.remote_node_id == 10

    # create output socket
    msg_sock_tx_1 = sock_fac.make_output_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))

    # 1. FRAME FOR THE PROMISCUOUS INPUT SESSION
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=11,  # different from renote_node_id selective session
                destination_node_id=1,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"Bitch I'm back out my coma" + TransferCRC.new(b"Bitch I'm back out my coma").value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )

    # promiscuous input session should receive the frame
    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 11
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"Bitch I'm back out my coma")

    assert not prom_finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=26, errors=0, drops=0, reassembly_errors_per_source_node_id={11: {}}
    )

    # selective input session should not receive the frame
    rx_data = await sel_in.receive(loop.time() + 1.0)
    assert rx_data == None

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=0, frames=0, payload_bytes=0, errors=0, drops=0, reassembly_errors={}
    )

    # 2. FRAME FOR THE SELECTIVE INPUT SESSION AND THE PROMISCUOUS INPUT SESSION
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=1,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"Waking up on your sofa" + TransferCRC.new(b"Waking up on your sofa").value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )

    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"Waking up on your sofa")

    assert not prom_finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=2,
        frames=2,
        payload_bytes=48,
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}},
    )

    rx_data = await sel_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"Waking up on your sofa")

    assert not sel_finalized
    assert sel_in.socket.fileno() > 0
    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=22, errors=0, drops=0, reassembly_errors={}
    )

    # 3. ANONYMOUS FRAME FOR THE PROMISCUOUS INPUT SESSION
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=None,
                destination_node_id=1,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"When I park my Range Rover" + TransferCRC.new(b"When I park my Range Rover").value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )

    # check that promiscuous has received the frame
    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == None
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"When I park my Range Rover")

    assert not prom_finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=74,
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={
            11: {},
            10: {},
        },  # Anonymous frames can't have reassembly errors (always single frame)
    )

    # check that selective has not received anything
    rx_data = await sel_in.receive(loop.time() + 1.0)
    assert rx_data is None

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=22, errors=0, drops=0, reassembly_errors={}
    )

    # 4. INVALID FRAME
    msg_sock_tx_1.send(b"Slightly scratch your Corolla")

    should_be_none = await prom_in.receive(loop.time() + 1.0)
    assert should_be_none is None
    should_be_none = await sel_in.receive(loop.time() + 1.0)
    assert should_be_none is None

    # check that errors has been updated in Statistics
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=74,
        errors=1,  # error on the invalid frame
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}},
    )

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=22, errors=1, drops=0, reassembly_errors={}
    )

    # 5. INVALID HEADER_CRC
    msg_sock_tx_1.send(
        b"".join(
            # from pycyphal/transport/udp/_frame.py
            (
                memoryview(
                    b"\x01"  # version
                    b"\x06"  # priority
                    b"\n\x00"  # source_node_id
                    b"\x02\x00"  # destination_node_id
                    b"\x03\x00"  # data_specifier_snm
                    b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
                    b"\x01\x00\x00\x80"  # index
                    b"\x00\x00"  # user_data
                    b"\xc9\x8f"  # header_crc is invalid, should be \xc8\x8f
                ),
                memoryview(
                    b"Okay, I smashed your Corolla" + TransferCRC.new(b"Okay, I smashed your Corolla").value_as_bytes
                ),
            )
        )
    )

    should_be_none = await prom_in.receive(loop.time() + 1.0)
    assert should_be_none is None
    should_be_none = await sel_in.receive(loop.time() + 1.0)
    assert should_be_none is None

    # check that errors has been updated in Statistics (Prmiscuous)
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=74,
        errors=2,  # error count increased
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}},
    )

    # check that errors has been updated in Statistics (Selective)
    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=22, errors=2, drops=0, reassembly_errors={}  # error count increased
    )

    # 6. INVALID PAYLOAD_CRC
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=2,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"I'm hanging on a hangover" + TransferCRC.new(b"I'm hanging on an INVALID hangover").value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )

    should_be_none = await prom_in.receive(loop.time() + 1.0)
    assert should_be_none is None
    should_be_none = await sel_in.receive(loop.time() + 1.0)
    assert should_be_none is None

    # check that errors has been updated in Statistics
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=4,
        payload_bytes=74,
        errors=3,  # error count increased
        drops=0,
        reassembly_errors_per_source_node_id={
            11: {},
            10: {TransferReassembler.Error.UNIFRAME_INTEGRITY_ERROR: 1},
        },
    )

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1,
        frames=2,
        payload_bytes=22,
        errors=3,  # error count increased
        drops=0,
        reassembly_errors={TransferReassembler.Error.UNIFRAME_INTEGRITY_ERROR: 1},
    )

    # 7. CLOSE THE PROMISCUOUS INPUT SESSION
    prom_in.close()
    assert prom_finalized is True

    # 8. CLOSE SELECTIVE INPUT SESSION
    sel_in.close()
    assert sel_finalized is True


async def _unittest_udp_input_session_multiframe() -> None:
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 5.0  # TODO use asyncio socket read and remove this thing.
    prom_finalized = False
    sel_finalized = False

    def do_finalize_prom() -> None:
        nonlocal prom_finalized
        prom_finalized = True

    def do_finalize_sel() -> None:
        nonlocal sel_finalized
        sel_finalized = True

    # SETUP

    sock_fac = IPv4SocketFactory(local_ip_addr=ipaddress.IPv4Address("127.0.0.1"))

    msg_sock_rx_1 = sock_fac.make_input_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))
    assert "239.0.0.123" == msg_sock_rx_1.getsockname()[0]
    assert DESTINATION_PORT == msg_sock_rx_1.getsockname()[1]

    msg_sock_rx_2 = sock_fac.make_input_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))
    assert "239.0.0.123" == msg_sock_rx_1.getsockname()[0]
    assert DESTINATION_PORT == msg_sock_rx_1.getsockname()[1]

    # create promiscuous input session, uses msg_sock_rx_1
    prom_in = PromiscuousUDPInputSession(
        specifier=InputSessionSpecifier(data_specifier=MessageDataSpecifier(123), remote_node_id=None),
        payload_metadata=PayloadMetadata(1024),
        sock=msg_sock_rx_1,
        finalizer=do_finalize_prom,
    )

    assert prom_in.specifier.data_specifier == MessageDataSpecifier(123)
    assert prom_in.specifier.remote_node_id == None

    # create selective input session, uses msg_sock_rx_2
    sel_in = SelectiveUDPInputSession(
        specifier=InputSessionSpecifier(data_specifier=MessageDataSpecifier(123), remote_node_id=10),
        payload_metadata=PayloadMetadata(1024),
        sock=msg_sock_rx_2,
        finalizer=do_finalize_sel,
    )

    assert sel_in.specifier.data_specifier == MessageDataSpecifier(123)
    assert sel_in.specifier.remote_node_id == 10

    # create output socket
    msg_sock_tx_1 = sock_fac.make_output_socket(remote_node_id=None, data_specifier=MessageDataSpecifier(123))

    # 1. VALID MULTIFRAME
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=2,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=False,
                user_data=0,
                payload=memoryview(b"I can hold my liquor"),
            ).compile_header_and_payload()
        )
    )
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=2,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=1,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"But this man can't handle his weed"
                    + TransferCRC.new(b"I can hold my liquor" + b"But this man can't handle his weed").value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )
    rx_data = await prom_in.receive(loop.time() + 1.0)
    assert rx_data is None
    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"I can hold my liquor")
    assert rx_data.fragmented_payload[1] == memoryview(b"But this man can't handle his weed")

    assert not prom_finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=1,  # +1
        frames=2,  # +2
        payload_bytes=54,  # +54
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={
            10: {},
        },
    )

    rx_data = await sel_in.receive(loop.time() + 1.0)
    assert rx_data is None
    rx_data = await sel_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"I can hold my liquor")
    assert rx_data.fragmented_payload[1] == memoryview(b"But this man can't handle his weed")

    assert not sel_finalized
    assert sel_in.socket.fileno() > 0
    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1,  # +1
        frames=2,  # +2
        payload_bytes=54,
        errors=0,
        drops=0,
        reassembly_errors={},
    )

    # 2. INVALID MULTIFRAME
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=2,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=False,
                user_data=0,
                payload=memoryview(b"Still ain't learn me no manners"),
            ).compile_header_and_payload()
        )
    )
    msg_sock_tx_1.send(
        b"".join(
            UDPFrame(
                priority=Priority.LOW,
                source_node_id=10,
                destination_node_id=2,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=1,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(
                    b"You love me when I ain't sober"
                    + TransferCRC.new(
                        b"Still ain't learn me no manners" + b"You love me when I ain't INVALID"
                    ).value_as_bytes
                ),
            ).compile_header_and_payload()
        )
    )
    rx_data = await prom_in.receive(loop.time() + 1.0)
    assert rx_data is None
    rx_data = await prom_in.receive(loop.time() + 1.0)
    assert rx_data is None

    assert not prom_finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=1,
        frames=4,  # +2
        payload_bytes=54,
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={
            10: {},  # This should show up as a reassembly error
        },
    )

    assert False
