# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

# pylint: disable=protected-access

from __future__ import annotations
import sys
import time
import typing
import socket
from pyuavcan.transport import Timestamp

# noinspection PyProtectedMember
from pyuavcan.transport.udp._ip._link_layer import LinkLayerCapture, LinkLayerSniffer, LinkLayerPacket


def _unittest_encode_decode_null() -> None:
    import libpcap as pcap
    from socket import AddressFamily

    mv = memoryview

    enc, dec = LinkLayerPacket.get_codecs()[pcap.DLT_NULL]
    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, sys.byteorder) + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b"abcd"
    assert str(llp) == "LinkLayerPacket(protocol=AddressFamily.AF_INET, source=, destination=, payload=61626364)"

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
                destination=mv(b"\xAA\xBB\xCC"),
                payload=mv(b"abcd"),
            )
        )
        == AddressFamily.AF_INET6.to_bytes(4, sys.byteorder) + b"abcd"
    )

    assert dec(mv(b"")) is None


def _unittest_encode_decode_loop() -> None:
    import libpcap as pcap
    from socket import AddressFamily

    mv = memoryview

    enc, dec = LinkLayerPacket.get_codecs()[pcap.DLT_LOOP]
    llp = dec(mv(AddressFamily.AF_INET.to_bytes(4, "big") + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b""
    assert llp.destination == b""
    assert llp.payload == b"abcd"
    assert str(llp) == "LinkLayerPacket(protocol=AddressFamily.AF_INET, source=, destination=, payload=61626364)"

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
                destination=mv(b"\xAA\xBB\xCC"),
                payload=mv(b"abcd"),
            )
        )
        == AddressFamily.AF_INET6.to_bytes(4, "big") + b"abcd"
    )

    assert dec(mv(b"")) is None


def _unittest_encode_decode_ethernet() -> None:
    import libpcap as pcap
    from socket import AddressFamily

    mv = memoryview

    enc, dec = LinkLayerPacket.get_codecs()[pcap.DLT_EN10MB]
    llp = dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xAA\xBB\xCC\xDD\xEE\xFF" + b"\x08\x00" + b"abcd"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.protocol == AddressFamily.AF_INET
    assert llp.source == b"\x11\x22\x33\x44\x55\x66"
    assert llp.destination == b"\xAA\xBB\xCC\xDD\xEE\xFF"
    assert llp.payload == b"abcd"
    assert str(llp) == (
        "LinkLayerPacket(protocol=AddressFamily.AF_INET, "
        + "source=112233445566, destination=aabbccddeeff, payload=61626364)"
    )

    llp = dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xAA\xBB\xCC\xDD\xEE\xFF" + b"\x08\x00"))
    assert isinstance(llp, LinkLayerPacket)
    assert llp.source == b"\x11\x22\x33\x44\x55\x66"
    assert llp.destination == b"\xAA\xBB\xCC\xDD\xEE\xFF"
    assert llp.payload == b""

    assert (
        enc(
            LinkLayerPacket(
                protocol=AddressFamily.AF_INET6,
                source=mv(b"\x11\x22"),
                destination=mv(b"\xAA\xBB\xCC"),
                payload=mv(b"abcd"),
            )
        )
        == b"\x00\x00\x00\x00\x11\x22" + b"\x00\x00\x00\xAA\xBB\xCC" + b"\x86\xDD" + b"abcd"
    )

    assert (
        enc(
            LinkLayerPacket(
                protocol=AddressFamily.AF_IRDA,  # Unsupported encapsulation
                source=mv(b"\x11\x22"),
                destination=mv(b"\xAA\xBB\xCC"),
                payload=mv(b"abcd"),
            )
        )
        is None
    )

    assert dec(mv(b"")) is None
    assert dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xAA\xBB\xCC\xDD\xEE\xFF" + b"\xAA\xAA" + b"abcdef")) is None
    # Bad ethertype/length
    assert dec(mv(b"\x11\x22\x33\x44\x55\x66" + b"\xAA\xBB\xCC\xDD\xEE\xFF" + b"\x00\xFF" + b"abcdef")) is None


def _unittest_find_devices() -> None:
    # noinspection PyProtectedMember
    from pyuavcan.transport.udp._ip._link_layer import _find_devices

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
        now = Timestamp.now()
        assert ts_last.monotonic_ns <= lls.timestamp.monotonic_ns <= now.monotonic_ns
        assert ts_last.system_ns <= lls.timestamp.system_ns <= now.system_ns
        ts_last = lls.timestamp
        sniffs.append(lls.packet)

    filter_expression = "udp and src net 127.66.0.0/16"
    sn = LinkLayerSniffer(filter_expression, callback)
    assert sn.is_stable

    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # The sink socket is needed for compatibility with Windows. On Windows, an attempt to transmit to a loopback
    # multicast group for which there are no receivers may fail with the following errors:
    #   OSError: [WinError 10051]   A socket operation was attempted to an unreachable network
    #   OSError: [WinError 1231]    The network location cannot be reached. For information about network
    #                               troubleshooting, see Windows Help
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sink.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sink.bind(("", 4444))
        sink.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton("239.66.1.200") + socket.inet_aton("127.42.0.123"),
        )

        b.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.42.0.123"))
        b.bind(("127.42.0.123", 0))  # Some random noise on an adjacent subnet.
        for i in range(10):
            b.sendto(f"{i:04x}".encode(), ("127.66.1.200", 4444))  # Ignored unicast
            b.sendto(f"{i:04x}".encode(), ("239.66.1.200", 4444))  # Ignored multicast
            time.sleep(0.1)

        time.sleep(1)
        assert sniffs == []  # Make sure we are not picking up any noise.

        a.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("127.66.33.44"))
        a.bind(("127.66.33.44", 0))  # This one is on our local subnet, it should be heard.
        a.sendto(b"\xAA\xAA\xAA\xAA", ("127.66.1.200", 4444))  # Accepted unicast inside subnet
        a.sendto(b"\xBB\xBB\xBB\xBB", ("127.33.1.200", 4444))  # Accepted unicast outside subnet
        a.sendto(b"\xCC\xCC\xCC\xCC", ("239.66.1.200", 4444))  # Accepted multicast

        b.sendto(b"x", ("127.66.1.200", 4444))  # Ignored unicast
        b.sendto(b"y", ("239.66.1.200", 4444))  # Ignored multicast

        time.sleep(3)

        # Validate the received callbacks.
        print(sniffs[0])
        print(sniffs[1])
        print(sniffs[2])
        assert len(sniffs) == 3
        # Assume the packets are not reordered (why would they be?)
        assert b"\xAA\xAA\xAA\xAA" in bytes(sniffs[0].payload)
        assert b"\xBB\xBB\xBB\xBB" in bytes(sniffs[1].payload)
        assert b"\xCC\xCC\xCC\xCC" in bytes(sniffs[2].payload)

        sniffs.clear()
        sn.close()

        time.sleep(1)
        a.sendto(b"d", ("127.66.1.100", 4321))
        time.sleep(1)
        assert sniffs == []  # Should be terminated.
    finally:
        sn.close()
        a.close()
        b.close()
        sink.close()


def _unittest_sniff_errors() -> None:
    from pytest import raises

    # noinspection PyProtectedMember
    from pyuavcan.transport.udp._ip._link_layer import LinkLayerCaptureError

    with raises(LinkLayerCaptureError, match=r".*filter expression.*"):
        LinkLayerSniffer("invalid filter expression", lambda x: None)
