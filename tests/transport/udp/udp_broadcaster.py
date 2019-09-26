#!/usr/bin/env python3
#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import time
import typing
import socket
import logging
import threading
import ipaddress


_logger = logging.getLogger(__name__.replace('__', ''))


class UDPBroadcaster:
    """
    This class is created out of desperation:
    https://stackoverflow.com/questions/58080648/broadcast-packets-do-not-propagate-through-a-linux-veth-tunnel
    It should only be used on a local system and only for debugging and testing purposes.
    If you have a real physical network, you won't need it.

    Its purpose is to listen for UDP/IP packets addressed to the broadcast address of the specified subnet
    (normally 127.x.x.x), and for each received packet do the following:

    - Ensure that the source address belongs to the subnet, otherwise drop the packet.
    - Ensure that the packet is a UDP/IP packet, otherwise drop it.
    - Erase the UDP checksum and for each host address in the subnet:

        - Replace the destination address in the IP packet with the address of the host.
        - Send the packet.

    The class operates at OSI L3 so superuser privileges may be required.
    On GNU/Linux, consider setting the process capabilities instead::

        sudo setcap 'CAP_NET_RAW+eip CAP_NET_ADMIN+eip' "$(python -c 'import sys; print(sys.executable)')"

    Observe that due to IP spoofing the original sender of the packet may be receiving
    ICMP messages intended for the broadcaster.

    Normally, this thing shouldn't exist. If you have a solution for the above described problem,
    please post a reply there and I will happily delete this pizdets and forget that it ever existed.
    """

    #: Max jumbo frame size is 9 KiB
    _MTU = 1024 * 9

    def __init__(self, network: str):
        """
        :param network: Given '127.0.0.0/24', the class will bind its socket to 127.0.0.255
            and accept only packets that originate from [127.0.0.1, 127.0.0.254].
            Every received packet will be forwarded to every host inside the subnet.
            For receiving hosts the packets will look as if they are sent by the original sender.
        """
        self._closed = False
        self._network = ipaddress.ip_network(network)
        self._hosts = list(self._network.hosts())
        if len(self._hosts) > 2 ** 15:  # pragma: no cover
            raise ValueError(f'The subnet {self._network} is too large')

        self._datagrams_by_source: typing.Dict[str, int] = {}
        self._lock = threading.RLock()

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_UDP)
        self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        self._sock.bind((str(self._network.broadcast_address), 0))

        try:  # pragma: no cover
            # noinspection PyTypeChecker
            self._sock.ioctl(socket.SIO_RCVALL, socket.RCVALL_ON)  # type: ignore
        except AttributeError:
            pass  # Okay, this is not Windows. Who would have thought.

        self._thread = threading.Thread(target=self._thread_function, name=f'udp_broadcaster_worker', daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._closed = True
        self._sock.close()

    def sample_statistics(self) -> typing.Dict[str, int]:
        with self._lock:
            return self._datagrams_by_source.copy()

    def _thread_function(self) -> None:
        while not self._closed:
            try:
                data, endpoint = self._sock.recvfrom(self._MTU)
                if ipaddress.ip_address(endpoint[0]) not in self._network:
                    _logger.debug('%r: %d bytes from %s dropped - wrong subnet', self, len(data), endpoint[0])
                    continue

                _logger.debug('%r: Got %d bytes from %s; header: %r', self, len(data), endpoint[0], data[:48])
                stat_key = endpoint[0]
                with self._lock:
                    try:
                        self._datagrams_by_source[stat_key] += 1
                    except LookupError:
                        self._datagrams_by_source[stat_key] = 1

                self._broadcast(bytearray(data))
            except Exception as ex:  # pragma: no cover
                if self._closed:
                    _logger.debug('%r: Worker exception ignored because the closure flag is set: %s', ex)
                else:
                    _logger.exception('%r: Worker exception: %s. Will continue after a short nap.', self, ex)
                    time.sleep(1.0)

    def _broadcast(self, ip_frame: bytearray) -> None:
        ip_version = ip_frame[0] >> 4
        ip_header_size = (ip_frame[0] & 0x0F) * 4
        if ip_version == 4:
            is_udp = ip_frame[9] == socket.IPPROTO_UDP
            is_fragmented = (int.from_bytes(ip_frame[6:8], 'big') & 0b_101_1_1111_1111_1111) != 0
            if is_udp and not is_fragmented:
                # Computing the UDP checksum? Pfft, I can't be bothered. Thankfully, in UDP/IPv4 it's optional.
                udp_checksum_offset = ip_header_size + 6
                ip_frame[udp_checksum_offset:2 + udp_checksum_offset] = b'\x00\x00'
                for dst_ip in self._hosts:
                    ip_frame[16:20] = int(dst_ip).to_bytes(4, 'big')
                    self._sock.sendto(ip_frame, (str(dst_ip), 0))
            else:
                _logger.info('%r: Frame dropped (is_udp=%r, is_fragmented=%r): %r',
                             self, is_udp, is_fragmented, ip_frame)
        else:
            _logger.error('%r: Unsupported IP version: %d', self, ip_version)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(network={self._network})'


def _main() -> int:  # pragma: no cover
    try:
        network = sys.argv[1]
        verbose = '-v' in sys.argv
    except LookupError:
        print(f'Usage: {sys.argv[0]} <subnet> [-v]', file=sys.stderr)
        return 1

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)s: %(message)s',
                        level=logging.DEBUG if verbose else logging.INFO)

    ub = UDPBroadcaster(network)
    try:
        while True:
            time.sleep(10.0)
            _logger.info('UDPBroadcaster statistics: %s', ub.sample_statistics())
    except KeyboardInterrupt:
        ub.close()  # Close before printing the stats to ensure finality.
        _logger.info('UDPBroadcaster final statistics: %s', ub.sample_statistics())
    finally:
        ub.close()

    return 0


def _unittest_udp_broadcaster() -> None:
    import pytest

    try:
        ub = UDPBroadcaster('127.100.0.0/24')
    except PermissionError:  # pragma: no cover
        pytest.skip('UDP broadcaster will not be tested due to lack of permissions')
        raise  # This is unreachable but necessary for the linter to shut up

    def make_socket(bind_host: str, bind_port: int) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((bind_host, bind_port))
        s.settimeout(1.0)
        return s

    def recvfrom(s: socket.socket) -> typing.Optional[typing.Tuple[bytes, str]]:
        try:
            data, endpoint = s.recvfrom(10 * 1024)
            return data, endpoint[0]
        except socket.timeout:
            return None

    a0 = make_socket('127.100.0.10', 0)
    a1 = make_socket('127.100.0.42', a0.getsockname()[1])
    b0 = make_socket('127.100.0.10', 0)
    b1 = make_socket('127.100.0.42', b0.getsockname()[1])
    c = make_socket('127.100.0.222', 0)

    print('a0', a0)
    print('b0', b0)
    print('UDPBroadcaster:', ub.sample_statistics())

    c.sendto(b'Blin', ('127.100.0.255', a0.getsockname()[1]))
    assert recvfrom(a0) == (b'Blin', '127.100.0.222')
    assert recvfrom(a1) == (b'Blin', '127.100.0.222')
    assert recvfrom(b0) is None
    assert recvfrom(b1) is None

    c.sendto(b'Opa', ('127.100.0.255', b0.getsockname()[1]))
    assert recvfrom(a0) is None
    assert recvfrom(a1) is None
    assert recvfrom(b0) == (b'Opa', '127.100.0.222')
    assert recvfrom(b1) == (b'Opa', '127.100.0.222')

    # Wrong subnet.
    d = make_socket('127.0.0.222', 0)
    d.sendto(b'Luk', ('127.100.0.255', a0.getsockname()[1]))
    assert recvfrom(a0) is None
    assert recvfrom(a1) is None
    assert recvfrom(b0) is None
    assert recvfrom(b1) is None

    ub.close()
    ub.close()  # Idempotency

    for sock in (a0, a1, b0, b1, c, d):
        sock.close()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(_main())
