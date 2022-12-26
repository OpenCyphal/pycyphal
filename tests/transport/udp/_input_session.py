# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import sys
import asyncio
import socket as socket_
import typing
import pytest
from pytest import raises
import pycyphal
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


#
# 1. FRAME FOR THE PROMISCUOUS INPUT SESSION
#   prom_in:
#       transfers=1
#       frames=1
#       payload_bytes=12
#       errors=0
#       drops=0
#       reassembly_errors_per_source_node_id={11: {}}
#   sel_in:
#       transfers=0
#       frames=0
#       payload_bytes=0
#       errors=0
#       drops=1 (correct?)
#       reassembly_errors={11: {}}
#
# 2. FRAME FOR THE SELECTIVE INPUT SESSION AND THE PROMISCUOUS INPUT SESSION
#   prom_in:
#       transfers=2
#       frames=2
#       payload_bytes=24
#       errors=0
#       drops=0
#       reassembly_errors_per_source_node_id={11: {}, 10: {}}
#   sel_in:
#       transfers=1
#       frames=1
#       payload_bytes=12
#       errors=0
#       drops=1
#       reassembly_errors={11: {}, 10: {}}
#
# 3. ANONYMOUS FRAME FOR THE PROMISCUOUS INPUT SESSION
#   prom_in:
#       transfers=3
#       frames=3
#       payload_bytes=50
#       errors=0
#       drops=0
#       reassembly_errors_per_source_node_id={11: {}, 10: {}, 65535: {}}
#   sel_in:
#       transfers=1
#       frames=1
#       payload_bytes=12
#       errors=0
#       drops=2 (correct?)
#       reassembly_errors={11: {}, 10: {}, 65535: {}}
#
# 4. INVALID FRAME
#   prom_in:
#       transfers=3
#       frames=3
#       payload_bytes=50
#       errors=1
#       drops=0
#       reassembly_errors_per_source_node_id={11: {}, 10: {}, 65535: {}}
#   sel_in:
#       transfers=1
#       frames=1
#       payload_bytes=12
#       errors=1
#       drops=2
#       reassembly_errors={11: {}, 10: {}, 65535: {}}
# OTHER TESTS?
#   - reassembly errors, wrong checksum of the transfer
#   - closure of the socket


async def _unittest_udp_input_session() -> None:
    ts = Timestamp.now()
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 5.0  # TODO use asyncio socket read and remove this thing.
    finalized = False

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

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
        finalizer=do_finalize,
    )

    assert prom_in.specifier.data_specifier == MessageDataSpecifier(123)
    assert prom_in.specifier.remote_node_id == None

    # create selective input session, uses msg_sock_rx_2
    sel_in = SelectiveUDPInputSession(
        specifier=InputSessionSpecifier(data_specifier=MessageDataSpecifier(123), remote_node_id=10),
        payload_metadata=PayloadMetadata(1024),
        sock=msg_sock_rx_2,
        finalizer=do_finalize,
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
                source_node_id=11,
                destination_node_id=1,
                data_specifier=MessageDataSpecifier(123),
                transfer_id=0x_DEAD_BEEF_C0FFEE,
                index=0,
                end_of_transfer=True,
                user_data=0,
                payload=memoryview(b"HOLDMYLIQUOR"),
            ).compile_header_and_payload()
        )
    )

    # promiscuous input session should receive the frame
    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 11
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"HOLDMYLIQUOR")

    assert not finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=12, errors=0, drops=0, reassembly_errors_per_source_node_id={11: {}}
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
                payload=memoryview(b"SCRATCHEDYOURCOROLLA"),
            ).compile_header_and_payload()
        )
    )

    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"SCRATCHEDYOURCOROLLA")

    assert not finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=2,
        frames=2,
        payload_bytes=32,
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}},
    )

    rx_data = await sel_in.receive(loop.time() + 1.0)  # Internal protocol violation

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 10
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"SCRATCHEDYOURCOROLLA")

    assert not finalized
    assert sel_in.socket.fileno() > 0
    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=20, errors=0, drops=0, reassembly_errors={}
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
                payload=memoryview(b"SMASHEDYOURCOROLLA"),
            ).compile_header_and_payload()
        )
    )

    # check that promiscuous has received the frame
    rx_data = await prom_in.receive(loop.time() + 1.0)

    assert rx_data.priority == Priority.LOW
    assert rx_data.source_node_id == 0xFFFF
    assert rx_data.transfer_id == 0x_DEAD_BEEF_C0FFEE
    assert rx_data.fragmented_payload[0] == memoryview(b"SMASHEDYOURCOROLLA")

    assert not finalized
    assert prom_in.socket.fileno() > 0
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=50,
        errors=0,
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}, None: {}},  # should be none
    )

    # check that selective has not received anything
    rx_data = await sel_in.receive(loop.time() + 1.0)
    assert rx_data is None

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=20, errors=0, drops=0, reassembly_errors={}
    )

    # 4. INVALID FRAME
    msg_sock_tx_1.send(b"INVALID")

    should_be_none = await prom_in.receive(loop.time() + 1.0)
    assert should_be_none is None
    should_be_none = await sel_in.receive(loop.time() + 1.0)
    assert should_be_none is None

    # check that errors has been updated in Statistics
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=50,
        errors=1,
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}, None: {}},
    )

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=20, errors=1, drops=0, reassembly_errors={}
    )

    # 5. INVALID HEADER_CRC
    msg_sock_tx_1.send(
        b"".join(
            # from pycyphal/transport/udp/_frame.py
            (
                memoryview(
                    b"\x01"  # version
                    b"\x06"  # priority
                    b"\x01\x00"  # source_node_id
                    b"\x02\x00"  # destination_node_id
                    b"\x03\x00"  # data_specifier_snm
                    b"\xee\xff\xc0\xef\xbe\xad\xde\x00"  # transfer_id
                    b"\x0d\xf0\xdd\x80"  # index
                    b"\x00\x00"  # user_data
                    b"\x94\xc8"  # header_crc is invalid, should be 0x94c9
                ),
                memoryview(b"Well, I got here the same way the coin did."),
            )
        )
    )

    should_be_none = await prom_in.receive(loop.time() + 1.0)
    assert should_be_none is None
    should_be_none = await sel_in.receive(loop.time() + 1.0)
    assert should_be_none is None

    # check that errors has been updated in Statistics
    assert prom_in.sample_statistics() == PromiscuousUDPInputSessionStatistics(
        transfers=3,
        frames=3,
        payload_bytes=50,
        errors=2,
        drops=0,
        reassembly_errors_per_source_node_id={11: {}, 10: {}, None: {}},
    )

    assert sel_in.sample_statistics() == SelectiveUDPInputSessionStatistics(
        transfers=1, frames=1, payload_bytes=20, errors=2, drops=0, reassembly_errors={}
    )

    # 6. INVALID PAYLOAD_CRC

    # 5. CLOSE THE PROMISCUOUS INPUT SESSION AND CHECK THAT THE SELECTIVE INPUT SESSION IS NOT AFFECTED

    # 6. MAKE SURE SELECTIVE INPUT SESSION DOES NOT RECEIVE OTHER NODES' FRAMES

    # 7. CLOSE SELECTIVE INPUT SESSION
