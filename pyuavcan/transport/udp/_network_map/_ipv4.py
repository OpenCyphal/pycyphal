#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import re
import errno
import typing
import socket
import logging
import pyuavcan
from ._network_map import NetworkMap


_logger = logging.getLogger(__name__)


class NetworkMapIPv4(NetworkMap):
    """
    In IPv4 networks, the node-ID of zero cannot be used because it represents the subnet address;
    the maximum node-ID can only be used if it is not the same as the broadcast address for the subnet.
    """

    def __init__(self, ip_address_with_mask: str):
        self._local = IPv4Address.parse(ip_address_with_mask)
        if self._local.netmask == 0 or self._local.hostmask == 0:
            raise ValueError(f'Invalid subnet mask in {ip_address_with_mask}')

        self._max_nodes = min(2 ** self.NODE_ID_BIT_LENGTH, self._local.hostmask)

        self._local_node_id = int(self._local) - int(self._local.subnet_address)
        assert (int(self._local.subnet_address) + self._local_node_id) in self._local
        assert self._local_node_id < self._max_nodes

        # Test the address configuration to detect configuration errors early.
        # I suppose we could also set up a pair of sockets and send a test datagram just for extra paranoia?
        for s in [
            self.make_output_socket(None, 65535),
            self.make_output_socket(1, 65535),
            self.make_input_socket(0),
        ]:
            # This invariant is supposed to be upheld by the OS, so we use an assertion check.
            assert IPv4Address.parse(s.getsockname()[0]) == self._local.host_address
            s.close()

    @property
    def max_nodes(self) -> int:
        return self._max_nodes

    @property
    def local_node_id(self) -> int:
        return self._local_node_id

    def map_ip_address_to_node_id(self, ip: str) -> typing.Optional[int]:
        a = IPv4Address.parse(ip)  # This is likely to be a bottleneck!
        if a in self._local:
            out = int(a) - int(self._local.subnet_address)
            assert out >= 0
            if out < self._max_nodes:
                return out
        return None

    def make_output_socket(self, remote_node_id: typing.Optional[int], remote_port: int) -> socket.socket:
        s = self._make_socket(0)    # Bind to an ephemeral port.
        # Specify the fixed remote end. The port is always fixed; the host is unicast or broadcast.
        if remote_node_id is None:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.connect((str(self._local.broadcast_address), remote_port))
        elif 0 <= remote_node_id < self._max_nodes:
            ip = IPv4Address(int(self._local.subnet_address) + remote_node_id)
            assert ip in self._local
            s.connect((str(ip), remote_port))
        else:
            raise ValueError(f'Cannot map the node-ID value {remote_node_id} to an IP address. '
                             f'The range of valid node-ID values is [0, {self._max_nodes})')
        _logger.debug('New output socket %r connected to remote node %r, remote port %r',
                      s, remote_node_id, remote_port)
        return s

    def make_input_socket(self, local_port: int) -> socket.socket:
        s = self._make_socket(local_port)
        # Allow other applications and other instances to listen to multicast/broadcast traffic.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _logger.debug('New input socket %r, local port %r', s, local_port)
        return s

    def _make_socket(self, local_port: int) -> socket.socket:
        bind_to = self._local.host_address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Output sockets shall be bound, too, in order to ensure that outgoing packets have the correct
            # source IP address specified. This is particularly important for localhost; an unbound socket
            # there emits all packets from 127.0.0.1 which is certainly not what we need.
            s.bind((str(bind_to), local_port))
        except OSError as ex:
            if ex.errno == errno.EADDRNOTAVAIL:
                raise pyuavcan.transport.InvalidMediaConfigurationError(
                    f'Bad IP configuration: cannot bind socket to {bind_to} [{errno.errorcode[ex.errno]}]'
                ) from None
            raise  # pragma: no cover
        return s

    def __str__(self) -> str:
        return str(self._local)


def _unittest_network_map_ipv4() -> None:
    from pytest import raises

    with raises(ValueError):
        NetworkMap.new('127.0.0.1')  # No network mask specified.

    with raises(pyuavcan.transport.InvalidMediaConfigurationError):
        NetworkMap.new('10.0.254.254/24')  # Suppose that the test machine does not have such interface.

    nm = NetworkMap.new(' 127.123.1.0/16\t')
    assert str(nm) == '127.123.1.0/16'
    assert nm.max_nodes == 2 ** NetworkMap.NODE_ID_BIT_LENGTH  # Full capacity available.
    assert nm.local_node_id == 256
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1
    assert nm.map_ip_address_to_node_id('127.123.254.254') is None
    assert nm.map_ip_address_to_node_id('127.254.254.254') is None

    nm = NetworkMap.new('127.123.0.123/24')
    assert str(nm) == '127.123.0.123/24'
    assert nm.max_nodes == 255  # Capacity limited because 255 would be the broadcast IP address.
    assert nm.local_node_id == 123
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1
    assert nm.map_ip_address_to_node_id('127.254.254.254') is None

    with raises(ValueError):
        assert nm.make_output_socket(4095, 65535)  # The node-ID cannot be mapped.

    out = nm.make_output_socket(nm.local_node_id, 12345)
    inp = nm.make_input_socket(12345)

    # Ensure the source IP address is specified correctly in outgoing UDP frames.
    out.send(b'Well, I got here the same way the coin did.')
    data, sockaddr = inp.recvfrom(1024)
    assert data == b'Well, I got here the same way the coin did.'
    assert sockaddr[0] == '127.123.0.123'


class IPv4Address:
    """
    This class models the IPv4 address of a particular host along with the subnet it is contained in.
    For example:

    - 192.168.1.200/24:
        - Host:      192.168.1.200
        - Network:   192.168.1.0
        - Broadcast: 192.168.1.255

    All properties are stored, so the class is suitable for frequent querying.
    """
    _REGEXP = re.compile(r'^(\d+)\.(\d+)\.(\d+)\.(\d+)(?:/(\d+))?$')

    BIT_LENGTH = 32

    def __init__(self, address: int, netmask_width: typing.Optional[int] = None):
        """
        The default netmask width is 32 bits.
        """
        self._address = int(address)
        self._netmask_width = int(netmask_width) if netmask_width is not None else self.BIT_LENGTH

        if not (0 <= self._address < 2 ** self.BIT_LENGTH):
            raise ValueError(f'Invalid IPv4 address: 0x{self._address:08x}')

        if not (0 <= self._netmask_width <= self.BIT_LENGTH):
            raise ValueError(f'Invalid netmask width: {self._netmask_width}')

    @property
    def netmask(self) -> int:
        netmask = (2 ** self._netmask_width - 1) << (self.BIT_LENGTH - self._netmask_width)
        assert 0 <= netmask < 2 ** self.BIT_LENGTH
        return netmask

    @property
    def hostmask(self) -> int:
        return self.netmask ^ (2 ** self.BIT_LENGTH - 1)

    @property
    def host_address(self) -> IPv4Address:
        return IPv4Address(self._address)

    @property
    def subnet_address(self) -> IPv4Address:
        return IPv4Address(self._address & self.netmask)

    @property
    def broadcast_address(self) -> IPv4Address:
        return IPv4Address(self._address | self.hostmask)

    def __contains__(self, item: typing.Union[int, IPv4Address]) -> bool:
        if isinstance(item, (int, IPv4Address)):
            return int(self.subnet_address) <= int(item) <= int(self.broadcast_address)
        else:
            return NotImplemented  # pragma: no cover

    def __eq__(self, other: typing.Union[int, IPv4Address]) -> bool:
        if isinstance(other, int):
            return other == self._address
        elif isinstance(other, IPv4Address):
            return other._address == self._address and other._netmask_width == self._netmask_width
        else:
            return NotImplemented  # pragma: no cover

    def __int__(self) -> int:
        return self._address

    def __str__(self) -> str:
        return '.'.join(map(str, self._address.to_bytes(4, 'big'))) + \
            (f'/{self._netmask_width}' if self._netmask_width < self.BIT_LENGTH else '')

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, str(self))

    @staticmethod
    def parse(text: str) -> IPv4Address:
        match = IPv4Address._REGEXP.match(text.strip())
        if not match:
            raise ValueError(f'Malformed IPv4 address: {text!r}; the expected format is "A.B.C.D/M"')
        # The matched text is guaranteed to contain valid integers. Enforced by the regexp.
        parts = list(map(int, filter(None, match.groups())))
        try:
            octets = bytes(parts[:4])
            assert len(octets) == 4
            address = int.from_bytes(octets, 'big')
            assert 0 <= address < 2 ** IPv4Address.BIT_LENGTH
        except ValueError:
            raise ValueError(f'The IPv4 address {text!r} contains invalid octet(s)') from None

        try:
            netmask_width = parts[4]
        except IndexError:
            netmask_width = None

        return IPv4Address(address, netmask_width)


def _unittest_ipv4() -> None:
    from pytest import raises

    with raises(ValueError):
        IPv4Address.parse('192.168.1.20x/24')

    with raises(ValueError):
        IPv4Address.parse('192.168.1.256/24')

    with raises(ValueError):
        IPv4Address.parse('192.168.1.200/-1')

    with raises(ValueError):
        IPv4Address.parse('192.168.1.200/33')

    with raises(ValueError):
        IPv4Address(-1)

    with raises(ValueError):
        IPv4Address(2 ** 32)

    with raises(ValueError):
        IPv4Address(123456789, -1)

    with raises(ValueError):
        IPv4Address(123456789, 33)

    ip = IPv4Address.parse(' 192.168.1.200/24\n')
    assert isinstance(ip, IPv4Address)

    assert str(ip) == '192.168.1.200/24'
    assert int(ip) == 0xC0A801C8

    assert ip == 0xC0A801C8
    assert ip == IPv4Address.parse('192.168.1.200/24')
    assert ip != IPv4Address.parse('192.168.1.200/16')

    assert ip.netmask == 0xFFFFFF00
    assert ip.hostmask == 0x000000FF
    assert ip.host_address == IPv4Address.parse('192.168.1.200')
    assert ip.subnet_address == IPv4Address.parse('192.168.1.0')
    assert ip.broadcast_address == IPv4Address.parse('192.168.1.255')

    assert IPv4Address.parse('192.168.1.0') in ip
    assert IPv4Address.parse('192.168.1.1') in ip
    assert IPv4Address.parse('192.168.1.200') in ip
    assert IPv4Address.parse('192.168.1.254') in ip
    assert IPv4Address.parse('192.168.1.255') in ip
    assert IPv4Address.parse('192.168.2.1') not in ip
    assert IPv4Address.parse('192.168.200.1') not in ip
    assert 0xC0A801C8 in ip

    assert ip.host_address.netmask == 0x_FFFF_FFFF
    assert ip.subnet_address.netmask == 0x_FFFF_FFFF
    assert ip.broadcast_address.netmask == 0x_FFFF_FFFF

    assert ip.host_address.hostmask == 0
    assert ip.subnet_address.hostmask == 0
    assert ip.broadcast_address.hostmask == 0

    print(ip)
    print(ip.host_address)
    print(ip.subnet_address)
    print(ip.broadcast_address)
