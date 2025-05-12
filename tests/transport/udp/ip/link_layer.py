# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

# pylint: disable=protected-access

from __future__ import annotations
import re
import sys
import time
import typing
import socket
import logging
import libpcap as pcap  # type: ignore
from pycyphal.transport import Timestamp
from pycyphal.transport.udp._ip._link_layer import LinkLayerCapture, LinkLayerSniffer, LinkLayerPacket, _get_codecs
from pycyphal.transport.udp._ip._endpoint_mapping import CYPHAL_PORT

_logger = logging.getLogger(__name__)


def _unittest_encode_decode_null() -> None:
    from socket import AddressFamily

    mv = memoryview

    enc, dec = _get_codecs()[pcap.DLT_NULL]
    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, sys.byteorder) + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b"abcd"
    assert re.match(
        r"LinkLayerPacket\(protocol=[^,]+, source=, destination=, payload=61626364\)",
        str(llp),
    )

    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, sys.byteorder)))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b""

    assert (
        enc(
            LinkLayerPacket(
                protocol=AddressFamily.AF_INET6,
                source=mv(b"\x11\x22"),
                destination=mv(b"\xaa\xbb\xcc"),
                payload=mv(b"abcd"),
            )
        )
        == AddressFamily.AF_INET6.to_bytes(4, sys.byteorder) + b"abcd"
    )

    assert dec(mv(b"")) is None


def _unittest_encode_decode_loop() -> None:
    from socket import AddressFamily

    mv = memoryview

    enc, dec = _get_codecs()[pcap.DLT_LOOP]
    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, "big") + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b"abcd"
    assert re.match(
        r"LinkLayerPacket\(protocol=[^,]+, source=, destination=, payload=61626364\)",
        str(llp),
    )

    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, "big")))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b""

    assert (
        enc(
            LinkLayerPacket(
                protocol=AddressFamily.AF_INET6,
                source=mv(b"\x11\x22"),
                destination=mv(b"\xaa\xbb\xcc"),
                payload=mv(b"abcd"),
            )
        )
        == AddressFamily.AF_INET6.to_bytes(4, "big") + b"abcd"
    )

    assert dec(mv(b"")) is None


def _unittest_encode_decode_ethernet() -> None:
    from socket import AddressFamily

    mv = memoryview

    enc, dec = _get_codecs()[pcap.DLT_EN10MB]
    llp = dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xaa\xbb\xcc\xdd\xee\xff" + b"\x08\x00" + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b"\x11\x22\x33\x44\x55\x66"
    assert llp.destination == b"\xaa\xbb\xcc\xdd\xee\xff"
    assert llp.payload == b"abcd"
    assert re.match(
        r"LinkLayerPacket\(protocol=[^,]+, source=112233445566, destination=aabbccddeeff, payload=61626364\)",
        str(llp),
    )

    llp = dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xaa\xbb\xcc\xdd\xee\xff" + b"\x08\x00"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.source == b"\x11\x22\x33\x44\x55\x66"
    assert llp.destination == b"\xaa\xbb\xcc\xdd\xee\xff"
    assert llp.payload == b""

    assert (
        enc(
            LinkLayerPacket(
                protocol=AddressFamily.AF_INET6,
                source=mv(b"\x11\x22"),
                destination=mv(b"\xaa\xbb\xcc"),
                payload=mv(b"abcd"),
            )
        )
        == b"\x00\x00\x00\x00\x11\x22" + b"\x00\x00\x00\xaa\xbb\xcc" + b"\x86\xdd" + b"abcd"
    )

    if sys.platform != "darwin":  # Darwin doesn't support AF_IRDA
        assert (
            enc(
                LinkLayerPacket(
                    protocol=AddressFamily.AF_IRDA,  # Unsupported encapsulation
                    source=mv(b"\x11\x22"),
                    destination=mv(b"\xaa\xbb\xcc"),
                    payload=mv(b"abcd"),
                )
            )
            is None
        )

    assert dec(mv(b"")) is None
    assert dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xaa\xbb\xcc\xdd\xee\xff" + b"\xaa\xaa" + b"abcdef")) is None
    # Bad ethertype/length
    assert dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xaa\xbb\xcc\xdd\xee\xff" + b"\x00\xff" + b"abcdef")) is None


def _unittest_find_devices() -> None:
    from pycyphal.transport.udp._ip._link_layer import _find_devices

    devices = _find_devices()
    print("Devices:", devices)
    assert len(devices) >= 1
    if sys.platform.startswith("linux"):
        assert "lo" in devices


def _unittest_sniff() -> None:
    ts_last = Timestamp.now()
    sniffs: typing.List[LinkLayerPacket] = []

    def callback(lls: LinkLayerCapture) -> None:
        nonlocal ts_last
        nonlocal sniffs
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= lls.timestamp.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= lls.timestamp.system_ns <= now.system_ns
        ts_last = lls.timestamp
        sniffs.append(lls.packet)

    is_linux = sys.platform.startswith("linux") or sys.platform.startswith("darwin")

    filter_expression = "udp and ip dst net 239.0.0.0/15"
    sn = LinkLayerSniffer(filter_expression, callback)
    assert sn.is_stable
    assert sn._filter_expr == "udp and ip dst net 239.0.0.0/15"

    # output socket
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    a.bind(("127.0.0.1", 0))  # Bind to a random port
    a.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.0.0.1"))
    # The sink socket is needed for compatibility with Windows. On Windows, an attempt to transmit to a loopback
    # multicast group for which there are no receivers may fail with the following errors:
    #   OSError: [WinError 10051]   A socket operation was attempted to an unreachable network
    #   OSError: [WinError 1231]    The network location cannot be reached. For information about network
    #                               troubleshooting, see Windows Help
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sink.bind(("239.0.1.200" * is_linux, CYPHAL_PORT))
        sink.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton("239.2.1.200") + socket.inet_aton("127.0.0.1"),
        )
        sink.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton("239.0.1.199") + socket.inet_aton("127.0.0.1"),
        )
        sink.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton("239.0.1.200") + socket.inet_aton("127.0.0.1"),
        )
        sink.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton("239.0.1.201") + socket.inet_aton("127.0.0.1"),
        )

        for i in range(10):  # Some random noise on an adjacent multicast group
            a.sendto(f"{i:04x}".encode(), ("239.2.1.200", CYPHAL_PORT))  # Ignored multicast
            time.sleep(0.1)

        time.sleep(1)
        assert sniffs == []  # Make sure we are not picking up any noise.

        # a.bind(("127.0.0.1", 0))
        a.sendto(b"\xaa\xaa\xaa\xaa", ("239.0.1.199", CYPHAL_PORT))  # Accepted multicast
        a.sendto(b"\xbb\xbb\xbb\xbb", ("239.0.1.200", CYPHAL_PORT))  # Accepted multicast
        a.sendto(b"\xcc\xcc\xcc\xcc", ("239.0.1.201", CYPHAL_PORT))  # Accepted multicast

        time.sleep(3)

        # Validate the received callbacks.
        print(sniffs[0])
        print(sniffs[1])
        print(sniffs[2])
        assert len(sniffs) == 3
        # Assume the packets are not reordered (why would they be?)
        assert b"\xaa\xaa\xaa\xaa" in bytes(sniffs[0].payload)
        assert b"\xbb\xbb\xbb\xbb" in bytes(sniffs[1].payload)
        assert b"\xcc\xcc\xcc\xcc" in bytes(sniffs[2].payload)

        sniffs.clear()
        sn.close()

        # Test that the sniffer is terminated.
        time.sleep(1)
        a.sendto(b"d", ("239.0.1.200", CYPHAL_PORT))
        time.sleep(1)
        assert sniffs == []  # Should be terminated.
    finally:
        sn.close()
        a.close()
        # b.close()
        sink.close()


def _unittest_sniff_errors() -> None:
    from pytest import raises

    from pycyphal.transport.udp._ip._link_layer import LinkLayerCaptureError

    with raises(LinkLayerCaptureError, match=r".*filter expression.*"):
        LinkLayerSniffer("invalid filter expression", lambda x: None)
