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

    def __init__(self, ip_address: str):
        self._local = IPv4Address.parse(ip_address)
        if self._local.netmask == 0 or self._local.hostmask == 0:
            raise ValueError(f'The subnet mask in {ip_address} is invalid or missing. '
                             f'It is needed to determine the subnet broadcast address.')

        # Say, if the node-ID bit length is 12, the maximum node-ID would be 4095,
        # but it won't be usable if the subnet mask is 20 bits wide because it would be the
        # broadcast address for the subnet. In this example, we can use the full range of node-ID
        # values if the subnet mask is 19 bits wide or less.
        self._max_nodes: int = min(2 ** self.NODE_ID_BIT_LENGTH, self._local.hostmask)

        maybe_local_node_id = int(self._local) - int(self._local.subnet_address)
        if maybe_local_node_id < self._max_nodes:
            self._local_node_id: typing.Optional[int] = maybe_local_node_id
            assert (int(self._local.subnet_address) + self._local_node_id) in self._local
            assert self._local_node_id < self._max_nodes
            # Test the address configuration to detect configuration errors early.
            # These checks are only valid if the local node is non-anonymous.
            for s in [
                self.make_output_socket(None, 65535),
                self.make_output_socket(1, 65535),
                self.make_input_socket(0, False),
            ]:
                # This invariant is supposed to be upheld by the OS, so we use an assertion check.
                assert IPv4Address.parse(s.getsockname()[0]) == self._local.host_address, \
                    'Socket API invariant violation'
                s.close()
        else:
            self._local_node_id = None

        # Test the address configuration to detect configuration errors early.
        # These checks are valid regardless of whether the local node is anonymous.
        self.make_input_socket(0, True).close()

        self._ip_to_nid_cache: typing.Dict[str, typing.Optional[int]] = {}

    @property
    def max_nodes(self) -> int:
        return self._max_nodes

    @property
    def local_node_id(self) -> typing.Optional[int]:
        return self._local_node_id

    def map_ip_address_to_node_id(self, ip: str) -> typing.Optional[int]:
        # Unclean strings will decrease the cache performance. Do we want to strip() them beforehand?
        try:
            return self._ip_to_nid_cache[ip]
        except LookupError:
            a = IPv4Address.parse(ip)
            node_id: typing.Optional[int] = None
            if a in self._local:
                candidate = int(a) - int(self._local.subnet_address)
                assert candidate >= 0
                if candidate < self._max_nodes:
                    node_id = candidate

            _logger.debug('%r: New IP to node-ID mapping: %r --> %s', self, ip, node_id)
            self._ip_to_nid_cache[ip] = node_id
            return node_id

    def make_output_socket(self, remote_node_id: typing.Optional[int], remote_port: int) -> socket.socket:
        if self.local_node_id is None:
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                f'Anonymous UDP/IP nodes cannot emit transfers, they can only listen. '
                f'The local IP address is {self._local}.'
            )

        bind_to = self._local.host_address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setblocking(False)
        try:
            # Output sockets shall be bound, too, in order to ensure that outgoing packets have the correct
            # source IP address specified. This is particularly important for localhost; an unbound socket
            # there emits all packets from 127.0.0.1 which is certainly not what we need.
            s.bind((str(bind_to), 0))  # Bind to an ephemeral port.
        except OSError as ex:
            if ex.errno == errno.EADDRNOTAVAIL:
                raise pyuavcan.transport.InvalidMediaConfigurationError(
                    f'Bad IP configuration: cannot bind output socket to {bind_to} [{errno.errorcode[ex.errno]}]'
                ) from None
            raise  # pragma: no cover

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

        _logger.debug('%r: New output socket %r connected to remote node %r, remote port %r',
                      self, s, remote_node_id, remote_port)
        return s

    def make_input_socket(self, local_port: int, expect_broadcast: bool) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setblocking(False)

        # Allow other applications and other instances to listen to broadcast traffic.
        # This option shall be set before the socket is bound.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if expect_broadcast or self.local_node_id is None:
            # The socket MUST BE BOUND TO INADDR_ANY IN ORDER TO RECEIVE BROADCAST DATAGRAMS.
            # The user will have to filter out irrelevant datagrams in user space.
            # Please read https://stackoverflow.com/a/58118503/1007777
            # We also bind to this address if the local node is anonymous because in that case the local
            # IP address may be impossible to bind to.
            s.bind(('', local_port))
        else:
            # We are not interested in broadcast traffic, so it is safe to bind to a specific address.
            # On some operating systems this may not reject broadcast traffic, but we don't care.
            # The motivation for this is to allow multiple nodes running on localhost to bind to the same
            # service port. If they all were binding to that port at INADDR_ANY, on some OS (like GNU/Linux)
            # only the last-bound node would receive unicast data, making the service dysfunctional on all
            # other (earlier bound) nodes. Jeez, the socket API is kind of a mess.
            bind_to = self._local.host_address
            try:
                s.bind((str(bind_to), local_port))
            except OSError as ex:
                if ex.errno == errno.EADDRNOTAVAIL:
                    raise pyuavcan.transport.InvalidMediaConfigurationError(
                        f'Bad IP configuration: cannot bind input socket to {bind_to} [{errno.errorcode[ex.errno]}]'
                    ) from None
                raise  # pragma: no cover

        # Man 7 IP says that SO_BROADCAST should be set in order to receive broadcast datagrams.
        # The behavior I am observing does not match that, but we do it anyway because man says so.
        # If the call fails, ignore because it may not be necessary depending on the OS in use.
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except Exception as ex:  # pragma: no cover
            _logger.exception('%r: Could not set SO_BROADCAST on %r: %s', self, s, ex)

        _logger.debug('%r: New input socket %r, local port %r, supports broadcast: %r',
                      self, s, local_port, expect_broadcast)
        return s

    def __str__(self) -> str:
        return str(self._local)


def _unittest_network_map_ipv4() -> None:
    from pytest import raises

    with raises(ValueError):
        NetworkMap.new('127.0.0.1/32')          # Bad network mask.

    with raises(ValueError):
        NetworkMap.new('127.0.0.1/0')           # Bad network mask.

    with raises(ValueError):
        NetworkMap.new('127.0.0.1')             # No network mask.

    with raises(pyuavcan.transport.InvalidMediaConfigurationError):
        NetworkMap.new('10.0.254.254/24')       # Suppose that the test machine does not have such interface.

    nm = NetworkMap.new('127.254.254.254/8')    # Maps to an invalid local node-ID which means anonymous.
    assert nm.local_node_id is None

    nm = NetworkMap.new('192.168.0.255/24')    # Maps to an invalid local node-ID which means anonymous.
    assert nm.local_node_id is None

    nm = NetworkMap.new(' 127.123.1.0/16\t')
    assert str(nm) == '127.123.1.0/16'
    assert nm.max_nodes == 2 ** NetworkMap.NODE_ID_BIT_LENGTH  # Full capacity available.
    assert nm.local_node_id == 256
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1  # From cache
    assert nm.map_ip_address_to_node_id('127.123.254.254') is None
    assert nm.map_ip_address_to_node_id('127.254.254.254') is None

    nm = NetworkMap.new(' 127.123.2.0/16')
    assert str(nm) == '127.123.2.0/16'
    assert nm.max_nodes == 2 ** NetworkMap.NODE_ID_BIT_LENGTH  # Full capacity available.
    assert nm.local_node_id == 512
    assert nm.map_ip_address_to_node_id('127.123.2.1') == 513
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1
    assert nm.map_ip_address_to_node_id('127.122.0.1') is None
    assert nm.map_ip_address_to_node_id('127.123.254.254') is None
    assert nm.map_ip_address_to_node_id('127.124.0.1') is None

    nm = NetworkMap.new('127.123.0.123/24')
    assert str(nm) == '127.123.0.123/24'
    assert nm.max_nodes == 255  # Capacity limited because 255 would be the broadcast IP address.
    assert nm.local_node_id == 123
    assert nm.map_ip_address_to_node_id('127.123.0.1') == 1
    assert nm.map_ip_address_to_node_id('127.254.254.254') is None

    with raises(ValueError):
        assert nm.make_output_socket(4095, 65535)  # The node-ID cannot be mapped.

    out = nm.make_output_socket(nm.local_node_id, 2345)
    inp = nm.make_input_socket(2345, True)

    # Ensure the source IP address is specified correctly in outgoing UDP frames.
    out.send(b'Well, I got here the same way the coin did.')
    data, sockaddr = inp.recvfrom(1024)
    assert data == b'Well, I got here the same way the coin did.'
    assert sockaddr[0] == '127.123.0.123'

    out.close()
    inp.close()


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
    DEFAULT_NETMASK_WIDTH = BIT_LENGTH

    def __init__(self, address: int, netmask_width: int = DEFAULT_NETMASK_WIDTH):
        """
        The default netmask width is 32 bits.
        """
        self._address = int(address)
        self._netmask_width = int(netmask_width)

        if not (0 <= self._address < 2 ** self.BIT_LENGTH):
            raise ValueError(f'Invalid IPv4 address: 0x{self._address:08x}')

        if not (0 <= self._netmask_width <= self.BIT_LENGTH):
            raise ValueError(f'Invalid netmask width: {self._netmask_width}')

    @property
    def netmask(self) -> int:
        netmask = (2 ** self._netmask_width - 1) << (self.BIT_LENGTH - self._netmask_width)
        assert 0 <= netmask < 2 ** self.BIT_LENGTH
        assert isinstance(netmask, int)
        return netmask

    @property
    def hostmask(self) -> int:
        hostmask = self.netmask ^ (2 ** self.BIT_LENGTH - 1)
        assert isinstance(hostmask, int)
        return hostmask

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

    def __eq__(self, other: object) -> bool:
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
    def parse(text: str, default_netmask_width: int = DEFAULT_NETMASK_WIDTH) -> IPv4Address:
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
            netmask_width = default_netmask_width

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
        IPv4Address.parse('192.168.1.200', -1)

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

    ip = IPv4Address.parse(' 192.168.1.200\n', 24)
    assert isinstance(ip, IPv4Address)

    assert str(ip) == f'192.168.1.200/24'
    assert int(ip) == 0xC0A801C8

    assert ip == 0xC0A801C8
    assert ip == IPv4Address.parse(f'192.168.1.200/24')
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
