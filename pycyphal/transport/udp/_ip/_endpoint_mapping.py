# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import typing
import ipaddress
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier

IPAddress = typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
"""
I wonder why the common base class of IPv4Address and IPv6Address is not public?
"""

IP_ADDRESS_NODE_ID_MASK = 0xFFFF
"""
Masks the least significant bits of the IP address (v4/v6) that represent the node-ID.
Does not apply to multicast group address.
"""

MULTICAST_GROUP_SUBJECT_ID_MASK = 0xFFFF
"""
Masks the least significant bits of the multicast group address (v4/v6) that represent the subject-ID.
"""

SUBJECT_PORT = 16383
"""
All subjects use the same fixed destination UDP port number.
Subjects are differentiated by the IP multicast group address instead.
"""

SERVICE_BASE_PORT = 16384
"""
Service transfers use unicast IP packets.
Services are differentiated by the destination UDP port number.
See :func:`get_service_udp_port`.
"""


def node_id_to_unicast_ip(local_ip_address: IPAddress, node_id: int) -> IPAddress:
    """
    The local IP address is needed to deduce the subnet that the Cyphal/UDP transport is operating on.
    The function simply combines the most significant bits from the first argument with the second argument.

    >>> from ipaddress import ip_address
    >>> str(node_id_to_unicast_ip(ip_address('127.42.11.22'), 123))
    '127.42.0.123'
    >>> str(node_id_to_unicast_ip(ip_address('127.42.11.22'), 456))
    '127.42.1.200'
    >>> str(node_id_to_unicast_ip(ip_address('239.42.11.22'), 456))
    Traceback (most recent call last):
      ...
    ValueError: The local address shall be a unicast address, not multicast: 239.42.11.22
    >>> str(node_id_to_unicast_ip(ip_address('127.42.11.22'), 65536))
    Traceback (most recent call last):
      ...
    ValueError: Invalid node-ID...
    """
    if node_id > IP_ADDRESS_NODE_ID_MASK:
        raise ValueError(f"Invalid node-ID: {node_id} is larger than {IP_ADDRESS_NODE_ID_MASK}")
    ty: type
    if isinstance(local_ip_address, ipaddress.IPv4Address):
        mask = 2**ipaddress.IPV4LENGTH - 1
        ty = ipaddress.IPv4Address
    elif isinstance(local_ip_address, ipaddress.IPv6Address):
        mask = 2**ipaddress.IPV6LENGTH - 1
        ty = ipaddress.IPv6Address
    else:
        assert False
    if local_ip_address.is_multicast:
        raise ValueError(f"The local address shall be a unicast address, not multicast: {local_ip_address}")
    return ty((int(local_ip_address) & (mask ^ IP_ADDRESS_NODE_ID_MASK)) | node_id)  # type: ignore


def unicast_ip_to_node_id(local_ip_address: IPAddress, node_ip_address: IPAddress) -> typing.Optional[int]:
    """
    Returns the node-ID if the node IP address and the local IP address belong to the same subnet.
    Returns None if the node is not a member of the local subnet.
    Raises a value error if either address is a multicast group address.

    >>> from ipaddress import ip_address
    >>> unicast_ip_to_node_id(ip_address('127.42.1.1'), ip_address('127.42.1.200'))
    456
    >>> unicast_ip_to_node_id(ip_address('127.0.0.99'), ip_address('127.0.0.99'))
    99
    >>> unicast_ip_to_node_id(ip_address('127.99.1.1'), ip_address('127.42.1.200'))  # Returns None
    >>> unicast_ip_to_node_id(ip_address('239.42.1.1'), ip_address('127.42.1.200'))
    Traceback (most recent call last):
      ...
    ValueError: Multicast group address cannot be a local IP address...
    >>> unicast_ip_to_node_id(ip_address('127.42.1.1'), ip_address('239.42.1.200'))
    Traceback (most recent call last):
      ...
    ValueError: Multicast group address cannot be mapped to a node-ID...
    """
    if local_ip_address.is_multicast:
        raise ValueError(f"Multicast group address cannot be a local IP address: {local_ip_address}")
    if node_ip_address.is_multicast:
        raise ValueError(f"Multicast group address cannot be mapped to a node-ID: {node_ip_address}")
    if (int(local_ip_address) | IP_ADDRESS_NODE_ID_MASK) == (int(node_ip_address) | IP_ADDRESS_NODE_ID_MASK):
        return int(node_ip_address) & IP_ADDRESS_NODE_ID_MASK
    return None


def message_data_specifier_to_multicast_group(
    local_ip_address: IPAddress, data_specifier: MessageDataSpecifier
) -> IPAddress:
    r"""
    The local IP address is needed to deduce the subnet that the Cyphal/UDP transport is operating on.
    For IPv4, the resulting address is constructed as follows::

       11101111.0ddddddd.ssssssss.ssssssss
                 \_____/ \_______________/
                subnet-ID    subject-ID

    Where the subnet-ID is taken from the local IP address::

       xxxxxxxx.xddddddd.nnnnnnnn.nnnnnnnn
                 \_____/
                subnet-ID

    >>> from pycyphal.transport import MessageDataSpecifier
    >>> from ipaddress import ip_address
    >>> str(message_data_specifier_to_multicast_group(ip_address('127.42.11.22'), MessageDataSpecifier(123)))
    '239.42.0.123'
    >>> str(message_data_specifier_to_multicast_group(ip_address('192.168.11.22'), MessageDataSpecifier(456)))
    '239.40.1.200'
    >>> str(message_data_specifier_to_multicast_group(ip_address('239.168.11.22'), MessageDataSpecifier(456)))
    Traceback (most recent call last):
      ...
    ValueError: The local address shall be a unicast address, not multicast: 239.168.11.22
    """
    assert data_specifier.subject_id <= MULTICAST_GROUP_SUBJECT_ID_MASK, "Protocol design error"
    ty: type
    if isinstance(local_ip_address, ipaddress.IPv4Address):
        ty = ipaddress.IPv4Address
        fix = 0b_11101111_00000000_00000000_00000000
        sub = 0b_00000000_01111111_00000000_00000000 & int(local_ip_address)
        msb = fix | sub
    elif isinstance(local_ip_address, ipaddress.IPv6Address):
        raise NotImplementedError("IPv6 is not yet supported; please, submit patches!")
    else:
        assert False
    if local_ip_address.is_multicast:
        raise ValueError(f"The local address shall be a unicast address, not multicast: {local_ip_address}")
    return ty(msb | data_specifier.subject_id)  # type: ignore


def multicast_group_to_message_data_specifier(
    local_ip_address: IPAddress, multicast_group: IPAddress
) -> typing.Optional[MessageDataSpecifier]:
    """
    The inverse of :func:`message_data_specifier_to_multicast_group`.
    The local IP address is needed to ensure that the multicast group belongs to the correct Cyphal/UDP subnet.
    The return value is None if the multicast group is not valid per the current Cyphal/UDP specification
    or if it belongs to a different Cyphal/UDP subnet.

    >>> from ipaddress import ip_address
    >>> multicast_group_to_message_data_specifier(ip_address('127.42.11.22'), ip_address('239.42.1.200'))
    MessageDataSpecifier(subject_id=456)
    >>> multicast_group_to_message_data_specifier(ip_address('127.42.11.22'), ip_address('239.43.1.200'))    # -> None
    >>> multicast_group_to_message_data_specifier(ip_address('127.42.11.22'), ip_address('239.42.255.200'))  # -> None
    """
    try:
        candidate = MessageDataSpecifier(int(multicast_group) & MULTICAST_GROUP_SUBJECT_ID_MASK)
    except ValueError:
        return None
    if message_data_specifier_to_multicast_group(local_ip_address, candidate) == multicast_group:
        return candidate
    return None


def service_data_specifier_to_udp_port(ds: ServiceDataSpecifier) -> int:
    """
    For request transfers, the destination port is computed as
    :data:`SERVICE_BASE_PORT` plus service-ID multiplied by two.
    For response transfers, it is as above plus one.

    >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.REQUEST))
    16384
    >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE))
    16385
    >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.REQUEST))
    17406
    >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.RESPONSE))
    17407
    """
    request = SERVICE_BASE_PORT + ds.service_id * 2
    if ds.role == ServiceDataSpecifier.Role.REQUEST:
        return request
    if ds.role == ServiceDataSpecifier.Role.RESPONSE:
        return request + 1
    assert False


def udp_port_to_service_data_specifier(port: int) -> typing.Optional[ServiceDataSpecifier]:
    """
    The inverse of :func:`service_data_specifier_to_udp_port`. Returns None for invalid ports.

    >>> udp_port_to_service_data_specifier(16384)
    ServiceDataSpecifier(service_id=0, role=...REQUEST...)
    >>> udp_port_to_service_data_specifier(16385)
    ServiceDataSpecifier(service_id=0, role=...RESPONSE...)
    >>> udp_port_to_service_data_specifier(17406)
    ServiceDataSpecifier(service_id=511, role=...REQUEST...)
    >>> udp_port_to_service_data_specifier(17407)
    ServiceDataSpecifier(service_id=511, role=...RESPONSE...)
    >>> udp_port_to_service_data_specifier(50000)  # Returns None
    >>> udp_port_to_service_data_specifier(10000)  # Returns None
    """
    out: typing.Optional[ServiceDataSpecifier] = None
    try:
        if port >= SERVICE_BASE_PORT:
            role = ServiceDataSpecifier.Role.REQUEST if port % 2 == 0 else ServiceDataSpecifier.Role.RESPONSE
            out = ServiceDataSpecifier((port - SERVICE_BASE_PORT) // 2, role)
    except ValueError:
        out = None
    return out
