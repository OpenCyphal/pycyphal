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

SUBJECT_ID_MASK = 2**13 - 1
"""
Masks the 13 least significant bits of the multicast group address (v4/v6) that represent the message-ID. (Message)
"""

NODE_ID_MASK = 0xFFFF
"""
Masks the 16 least significant bits of the multicast group address (v4/v6) that represent the node-ID. (Service)
"""

DOMAIN_ID_MASK = 0b_00000000_01111100_00000000_00000000
"""
Masks the 5 bits of the multicast group address that represent the domain-ID.
"""

DATASPECIFIER_BIT_MASK = 0b_00000000_00000001_00000000_00000000
"""
Masks the bit that determines whether the address represents a Message (=0) or Service (=1)
"""

SUBJECT_PORT = 16383
"""
All subjects use the same fixed destination UDP port number.
Subjects are differentiated by the IP multicast group address.
(Message)
"""

SERVICE_BASE_PORT = 16384
"""
Service transfers use multicast IP packets.
Services are differentiated by the destination UDP port number.
See :func:`service_data_specifier_to_udp_port`.
"""


def service_data_specifier_to_multicast_group(domain_id: int, node_id: int, ipv6_addr: bool = False) -> IPAddress:
    """
    Takes a domain_id and node_id; returns the corresponding multicast address (for services).
    For IPv4, the resulting address is constructed as follows::

        fixed          service
       (9 bits)  res.  selector
       ________      ||
      /        \     vv
      11101111.0ddddd01.nnnnnnnn.nnnnnnnn
      \__/      \___/   \_______________/
    (4 bits)   (5 bits)     (16 bits)
      IPv4     domain-ID     node-ID
    multicast
     prefix

    >>> from ipaddress import ip_address
    >>> str(service_data_specifier_to_multicast_group(0, 123))
    '239.1.0.123'
    >>> str(service_data_specifier_to_multicast_group(13, 456))
    '239.53.1.200'
    >>> str(service_data_specifier_to_multicast_group(32, 456))
    Traceback (most recent call last):
      ...
    ValueError: Invalid domain-ID...
    >>> str(service_data_specifier_to_multicast_group(13, 65536))
    Traceback (most recent call last):
      ...
    ValueError: Invalid node-ID...
    >>> srvc_ip = service_data_specifier_to_multicast_group(0, 123)
    >>> assert (int(srvc_ip) & DATASPECIFIER_BIT_MASK) == DATASPECIFIER_BIT_MASK, "Dataspecifier bit is 1 for service"
    """
    if domain_id >= (2**5):
        raise ValueError(f"Invalid domain-ID: {domain_id} is larger than 31")
    if node_id > NODE_ID_MASK:
        raise ValueError(f"Invalid node-ID: {node_id} is larger than {NODE_ID_MASK}")
    ty: type
    if not ipv6_addr:
        ty = ipaddress.IPv4Address
        fix = 0b_11101111_00000000_00000000_00000000  # multicast prefix
        sub = DOMAIN_ID_MASK & (domain_id << 18)  # domain-ID
        msb = fix | sub | DATASPECIFIER_BIT_MASK  # service selector
    else:
        raise NotImplementedError("IPv6 is not yet supported; please, submit patches!")
    return ty(msb | node_id)  # type: ignore


def service_multicast_group_to_node_id(domain_id: int, multicast_group: IPAddress) -> typing.Optional[int]:
    """
    The inverse of :func:`service_data_specifier_to_multicast_group`.
    The domain_id is needed to ensure that the multicast group belongs to the correct Cyphal/UDP domain.
    The return value is None if the multicast group is not valid per the current Cyphal/UDP specification
    or if it belongs to a different Cyphal/UDP domain.

    >>> from ipaddress import ip_address
    >>> service_multicast_group_to_node_id(13, ip_address('239.53.1.200'))
    456
    >>> service_multicast_group_to_node_id(13, ip_address('239.52.1.200')) # -> None (message, not service)
    >>> service_multicast_group_to_node_id(14, ip_address('239.53.1.200')) # -> None (different domain)
    >>> service_multicast_group_to_node_id(13, ip_address('255.53.1.200')) # -> None (multicast prefix is wrong)
    >>> str(service_multicast_group_to_node_id(32, ip_address('255.53.1.200')))
    Traceback (most recent call last):
      ...
    ValueError: Invalid domain-ID...
    """
    if domain_id >= (2**5):
        raise ValueError(f"Invalid domain-ID: {domain_id} is larger than 31")
    candidate = int(multicast_group) & NODE_ID_MASK
    if service_data_specifier_to_multicast_group(domain_id, candidate) == multicast_group:
        return candidate
    return None


def message_data_specifier_to_multicast_group(
    domain_id: int, data_specifier: MessageDataSpecifier, ipv6_addr: bool = False
) -> IPAddress:
    r"""
    Takes a domain_id and data_specifier; returns the corresponding multicast address (for messages).
    For IPv4, the resulting address is constructed as follows::

        fixed   message  reserved
       (9 bits) select.  (3 bits)
       ________   res.|  _
      /        \     vv / \
      11101111.0ddddd00.000sssss.ssssssss
      \__/      \___/      \____________/
    (4 bits)   (5 bits)       (13 bits)
      IPv4     domain-ID      subject-ID
    multicast
     prefix

    >>> from pycyphal.transport import MessageDataSpecifier
    >>> from ipaddress import ip_address
    >>> str(message_data_specifier_to_multicast_group(0, MessageDataSpecifier(123)))
    '239.0.0.123'
    >>> str(message_data_specifier_to_multicast_group(13, MessageDataSpecifier(456)))
    '239.52.1.200'
    >>> str(message_data_specifier_to_multicast_group(32, MessageDataSpecifier(456)))
    Traceback (most recent call last):
      ...
    ValueError: Invalid domain-ID...
    >>> msg_ip = message_data_specifier_to_multicast_group(13, MessageDataSpecifier(123))
    >>> assert (int(msg_ip) & DATASPECIFIER_BIT_MASK) != DATASPECIFIER_BIT_MASK, "Dataspecifier bit is 0 for message"
    """
    if domain_id >= (2**5):
        raise ValueError(f"Invalid domain-ID: {domain_id} is larger than 31")
    if data_specifier.subject_id > SUBJECT_ID_MASK:
        raise ValueError(f"Invalid node-ID: {data_specifier.subject_id} is larger than {SUBJECT_ID_MASK}")
    ty: type
    if not ipv6_addr:
        ty = ipaddress.IPv4Address
        fix = 0b_11101111_00000000_00000000_00000000  # multicast prefix
        sub = DOMAIN_ID_MASK & (domain_id << 18)  # domain-ID
        msb = fix | sub & ~(DATASPECIFIER_BIT_MASK)  # message selector
    else:
        assert False
    if local_ip_address.is_multicast:
        raise ValueError(f"The local address shall be a unicast address, not multicast: {local_ip_address}")
    return ty(msb | data_specifier.subject_id)


def multicast_group_to_message_data_specifier(
    domain_id: int, multicast_group: IPAddress
) -> typing.Optional[MessageDataSpecifier]:
    """
    The inverse of :func:`message_data_specifier_to_multicast_group`.
    The domain_id is needed to ensure that the multicast group belongs to the correct Cyphal/UDP subnet.
    The return value is None if the multicast group is not valid per the current Cyphal/UDP specification
    or if it belongs to a different Cyphal/UDP subnet.

    >>> from ipaddress import ip_address
    >>> multicast_group_to_message_data_specifier(13, ip_address('239.52.1.200'))
    MessageDataSpecifier(subject_id=456)
    >>> multicast_group_to_message_data_specifier(13, ip_address('239.53.1.200'))    # -> None (service, not message)
    >>> multicast_group_to_message_data_specifier(14, ip_address('239.52.1.200'))  # -> None (different domain)
    >>> multicast_group_to_message_data_specifier(13, ip_address('255.52.1.200'))  # -> None (multicast prefix is wrong)
    """
    if domain_id >= (2**5):
        raise ValueError(f"Invalid domain-ID: {domain_id} is larger than 31")
    try:
        candidate = MessageDataSpecifier(int(multicast_group) & SUBJECT_ID_MASK)
    except ValueError:
        return None
    if message_data_specifier_to_multicast_group(domain_id, candidate) == multicast_group:
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
