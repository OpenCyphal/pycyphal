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

MULTICAST_PREFIX = 0b_11101111_00000000_00000000_00000000
"""
IPv4 address multicast prefix
"""

SUBJECT_ID_MASK = 2**14 - 1
"""
Masks the 14 least significant bits of the multicast group address (v4/v6) that represent the subject-ID. (Message)
"""

DESTINATION_NODE_ID_MASK = 0xFFFF
"""
Masks the 16 least significant bits of the multicast group address (v4/v6) that represent the destination node-ID. (Service)
"""

SNM_BIT_MASK = 0b_00000000_00000001_00000000_00000000
"""
Service, Not Message: Masks the bit that determines whether the address represents a Message (=0) or Service (=1)
"""

CYPHAL_UDP_IPV4_ADDRESS_VERSION = 0b_00000000_00100000_00000000_00000000
"""
Cyphal/UDP uses this bit to isolate IP header version 0 traffic
(note that the IP header version is not, necessarily, the same as the Cyphal Header version)
to the 239.0.0.0/10 scope but we can enable the 239.64.0.0/10 scope in the future.
"""

DESTINATION_PORT = 9382
"""
All Cyphal traffic uses this port.
This is a temporary UDP port. We'll register an official one later.
"""


def service_node_id_to_multicast_group(destination_node_id: int | None, ipv6_addr: bool = False) -> IPAddress:
    """
    Takes a destination node_id; returns the corresponding multicast address (for Service).
    For IPv4, the resulting address is constructed as follows::

            fixed
          (15 bits)     
       ______________   
      /              \  
      11101111.0000000x.nnnnnnnn.nnnnnnnn
      \__/      ^     ^ \_______________/
    (4 bits)  Cyphal snm     (16 bits)
      IPv4     UDP           destination node-ID (Service)
    multicast address
     prefix   version

    >>> from ipaddress import ip_address
    >>> str(service_node_id_to_multicast_group(123))
    '239.1.0.123'
    >>> str(service_node_id_to_multicast_group(456))
    '239.1.1.200'
    >>> str(service_node_id_to_multicast_group(None))
    '239.1.255.255'
    >>> str(service_node_id_to_multicast_group(int(0xFFFF)))
    Traceback (most recent call last):
      ...
    ValueError: Invalid node-ID...
    >>> str(service_node_id_to_multicast_group(65536))
    Traceback (most recent call last):
      ...
    ValueError: Invalid node-ID...
    >>> srvc_ip = service_node_id_to_multicast_group(123)
    >>> assert (int(srvc_ip) & SNM_BIT_MASK) == SNM_BIT_MASK, "SNM bit is 1 for service"
    """
    if destination_node_id is not None and not (0 <= destination_node_id < DESTINATION_NODE_ID_MASK):
        raise ValueError(f"Invalid node-ID: {destination_node_id} is larger than {DESTINATION_NODE_ID_MASK}")
    if destination_node_id is None:
        destination_node_id = int(0xFFFF)
    ty: type
    if not ipv6_addr:
        ty = ipaddress.IPv4Address
        msb = MULTICAST_PREFIX | SNM_BIT_MASK  
    else:
        raise NotImplementedError("IPv6 is not yet supported; please, submit patches!")
    return ty(msb | destination_node_id)  # type: ignore


def service_multicast_group_to_node_id(multicast_group: IPAddress) -> typing.Optional[int]:
    """
    The inverse of :func:`service_node_id_to_multicast_group`.
    The return value is None if:
    - is a broadcast multicast group or
    - the multicast group is not valid per the current Cyphal/UDP specification.

    >>> from ipaddress import ip_address
    >>> service_multicast_group_to_node_id(13, ip_address('239.1.0.123'))
    123
    >>> service_multicast_group_to_node_id(13, ip_address('239.1.1.200'))
    456
    >>> service_multicast_group_to_node_id(13, ip_address('239.52.1.200')) # -> None (broadcast)
    """

    candidate = int(multicast_group) & DESTINATION_NODE_ID_MASK
    if candidate == DESTINATION_NODE_ID_MASK:
        candidate = None
    if service_node_id_to_multicast_group(candidate) == multicast_group:
        return candidate
    return None


def message_data_specifier_to_multicast_group(
    subnet_id: int, data_specifier: MessageDataSpecifier, ipv6_addr: bool = False
) -> IPAddress:
    r"""
    Takes a (Message) data_specifier; returns the corresponding multicast address.
    For IPv4, the resulting address is constructed as follows::

            fixed            subject-ID (Service)
          (15 bits)     res. (15 bits)
       ______________   || _____________ 
      /              \  vv/             \ 
      11101111.0000000x.zznnnnnn.nnnnnnnn
      \__/      ^     ^
    (4 bits)  Cyphal snm
      IPv4     UDP
    multicast address
     prefix   version

    >>> from pycyphal.transport import MessageDataSpecifier
    >>> from ipaddress import ip_address
    >>> str(message_data_specifier_to_multicast_group(0, MessageDataSpecifier(123)))
    '239.0.0.123'
    >>> str(message_data_specifier_to_multicast_group(13, MessageDataSpecifier(456)))
    '239.52.1.200'
    >>> str(message_data_specifier_to_multicast_group(32, MessageDataSpecifier(456)))
    Traceback (most recent call last):
      ...
    ValueError: Invalid subnet-ID...
    >>> str(message_data_specifier_to_multicast_group(13, MessageDataSpecifier(2**13)))
    Traceback (most recent call last):
      ...
    ValueError: Invalid subject-ID...
    >>> msg_ip = message_data_specifier_to_multicast_group(13, MessageDataSpecifier(123))
    >>> assert (int(msg_ip) & DATASPECIFIER_BIT_MASK) != DATASPECIFIER_BIT_MASK, "Dataspecifier bit is 0 for message"
    """
    if subnet_id >= (2**5):
        raise ValueError(f"Invalid subnet-ID: {subnet_id} is larger than 31")
    if data_specifier.subject_id > SUBJECT_ID_MASK:
        raise ValueError(f"Invalid subject-ID: {data_specifier.subject_id} is larger than {SUBJECT_ID_MASK}")
    ty: type
    if not ipv6_addr:
        ty = ipaddress.IPv4Address
        fix = MULTICAST_PREFIX
        sub = SUBNET_ID_MASK & (subnet_id << 18)  # subnet-ID
        msb = fix | sub & ~(DATASPECIFIER_BIT_MASK)  # message selector
    else:
        raise NotImplementedError("IPv6 is not yet supported; please, submit patches!")
    return ty(msb | data_specifier.subject_id)  # type: ignore


def multicast_group_to_message_data_specifier(
    multicast_group: IPAddress
) -> typing.Optional[MessageDataSpecifier]:
    """
    The inverse of :func:`message_data_specifier_to_multicast_group`.
    The return value is None if the multicast group is not valid per the current Cyphal/UDP specification.

    >>> from ipaddress import ip_address
    >>> multicast_group_to_message_data_specifier(13, ip_address('239.52.1.200'))
    MessageDataSpecifier(subject_id=456)
    >>> multicast_group_to_message_data_specifier(13, ip_address('239.53.1.200'))    # -> None (service, not message)
    >>> multicast_group_to_message_data_specifier(14, ip_address('239.52.1.200'))  # -> None (different subnet)
    >>> multicast_group_to_message_data_specifier(13, ip_address('255.52.1.200'))  # -> None (multicast prefix is wrong)
    """
    if subnet_id >= (2**5):
        raise ValueError(f"Invalid subnet-ID: {subnet_id} is larger than 31")
    try:
        candidate = MessageDataSpecifier(int(multicast_group) & SUBJECT_ID_MASK)
    except ValueError:
        return None
    if message_data_specifier_to_multicast_group(subnet_id, candidate) == multicast_group:
        return candidate
    return None


# def service_data_specifier_to_udp_port(ds: ServiceDataSpecifier) -> int:
#     """
#     For request transfers, the destination port is computed as
#     :data:`SERVICE_BASE_PORT` plus service-ID multiplied by two.
#     For response transfers, it is as above plus one.

#     >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.REQUEST))
#     16384
#     >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE))
#     16385
#     >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.REQUEST))
#     17406
#     >>> service_data_specifier_to_udp_port(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.RESPONSE))
#     17407
#     """
#     request = SERVICE_BASE_PORT + ds.service_id * 2
#     if ds.role == ServiceDataSpecifier.Role.REQUEST:
#         return request
#     if ds.role == ServiceDataSpecifier.Role.RESPONSE:
#         return request + 1
#     assert False


# def udp_port_to_service_data_specifier(port: int) -> typing.Optional[ServiceDataSpecifier]:
#     """
#     The inverse of :func:`service_data_specifier_to_udp_port`. Returns None for invalid ports.

#     >>> udp_port_to_service_data_specifier(16384)
#     ServiceDataSpecifier(service_id=0, role=...REQUEST...)
#     >>> udp_port_to_service_data_specifier(16385)
#     ServiceDataSpecifier(service_id=0, role=...RESPONSE...)
#     >>> udp_port_to_service_data_specifier(17406)
#     ServiceDataSpecifier(service_id=511, role=...REQUEST...)
#     >>> udp_port_to_service_data_specifier(17407)
#     ServiceDataSpecifier(service_id=511, role=...RESPONSE...)
#     >>> udp_port_to_service_data_specifier(50000)  # Returns None
#     >>> udp_port_to_service_data_specifier(10000)  # Returns None
#     """
#     out: typing.Optional[ServiceDataSpecifier] = None
#     try:
#         if port >= SERVICE_BASE_PORT:
#             role = ServiceDataSpecifier.Role.REQUEST if port % 2 == 0 else ServiceDataSpecifier.Role.RESPONSE
#             out = ServiceDataSpecifier((port - SERVICE_BASE_PORT) // 2, role)
#     except ValueError:
#         out = None
#     return out

# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------

def _unittest_udp_endpoint_mapping() -> None:
    from pytest import raises
    from ipaddress import ip_address
    
    ### service_data_specifier_to_multicast_group
    # valid service IDs
    assert '239.1.0.123' == str(service_node_id_to_multicast_group(destination_node_id=123))
    assert '239.1.1.200' == str(service_node_id_to_multicast_group(destination_node_id=456))
    assert '239.1.255.255' == str(service_node_id_to_multicast_group(destination_node_id=None))

    # invalid destination_node_id
    with raises(ValueError):
        _ = service_node_id_to_multicast_group(destination_node_id=int(0xFFFF))

    # SNM bit is set
    srvc_ip = service_node_id_to_multicast_group(destination_node_id=123)
    assert (int(srvc_ip) & SNM_BIT_MASK) == SNM_BIT_MASK

    ### multicast_group_to_service_data_specifier
    # valid multicast group
    assert 123 == service_multicast_group_to_node_id(ip_address('239.1.0.123'))
    assert 456 == service_multicast_group_to_node_id(ip_address('239.1.1.200'))
    assert None == service_multicast_group_to_node_id(ip_address('239.1.255.255'))

    # invalid multicast group
    assert None == service_multicast_group_to_node_id(ip_address('255.1.0.123'))

    ### message_data_specifier_to_multicast_group

    ### multicast_group_to_message_data_specifier