# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import ipaddress
from pycyphal.transport import MessageDataSpecifier

IPAddress = typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
"""
I wonder why the common base class of IPv4Address and IPv6Address is not public?
"""

MULTICAST_PREFIX = 0b_11101111_00000000_00000000_00000000
"""
IPv4 address multicast prefix
"""

FIXED_MASK_PREFIX = 0b_11111111_11111111_00000000_00000000
"""
Masks the 16 most significant bits of the multicast group address. To check whether the address is Cyphal/UDP.
"""

SUBJECT_ID_MASK = 2**15 - 1
"""
Masks the 14 least significant bits of the multicast group address (v4/v6) that represent the subject-ID. (Message)
"""

DESTINATION_NODE_ID_MASK = 0xFFFF
"""
Masks the 16 least significant bits of the multicast group address (v4/v6) that represent the destination node-ID.
(Service)
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

CYPHAL_PORT = 9382
"""
All Cyphal traffic uses this port.
This is a temporary UDP port. We'll register an official one later.
"""


def service_node_id_to_multicast_group(
    destination_node_id: int | None, ipv6_addr: bool = False, cy_addr_version: int = 0
) -> IPAddress:
    r"""
    Takes a destination node_id; returns the corresponding multicast address (for Service).
    For IPv4, the resulting address is constructed as follows::

                fixed
              (15 bits)
           ______________
          /              \
          11101111.00000001.nnnnnnnn.nnnnnnnn
          \__/      ^     ^ \_______________/
        (4 bits)  Cyphal SNM     (16 bits)
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
    if cy_addr_version != 0:
        raise NotImplementedError("Only Cyphal address version 0 is currently in use")
    return ty(msb | destination_node_id)


def message_data_specifier_to_multicast_group(
    data_specifier: MessageDataSpecifier, ipv6_addr: bool = False, cy_addr_version: int = 0
) -> IPAddress:
    r"""
    Takes a (Message) data_specifier; returns the corresponding multicast address.
    For IPv4, the resulting address is constructed as follows::

                fixed          subject-ID (Service)
            (15 bits)     res. (15 bits)
         ______________   | _____________
        /              \  v/             \
        11101111.00000000.znnnnnnn.nnnnnnnn
        \__/      ^     ^
      (4 bits)  Cyphal SNM
        IPv4     UDP
      multicast address
       prefix   version

    >>> from pycyphal.transport import MessageDataSpecifier
    >>> from ipaddress import ip_address
    >>> str(message_data_specifier_to_multicast_group(MessageDataSpecifier(123)))
    '239.0.0.123'
    >>> str(message_data_specifier_to_multicast_group(MessageDataSpecifier(456)))
    '239.0.1.200'
    >>> str(message_data_specifier_to_multicast_group(MessageDataSpecifier(2**14)))
    Traceback (most recent call last):
      ...
    ValueError: Invalid subject-ID...
    >>> msg_ip = message_data_specifier_to_multicast_group(MessageDataSpecifier(123))
    >>> assert (int(msg_ip) & SNM_BIT_MASK) != SNM_BIT_MASK, "SNM bit is 0 for message"
    """
    if data_specifier.subject_id > SUBJECT_ID_MASK:
        raise ValueError(f"Invalid subject-ID: {data_specifier.subject_id} is larger than {SUBJECT_ID_MASK}")
    ty: type
    if not ipv6_addr:
        ty = ipaddress.IPv4Address
        msb = MULTICAST_PREFIX & ~(SNM_BIT_MASK)
    else:
        raise NotImplementedError("IPv6 is not yet supported; please, submit patches!")
    if cy_addr_version != 0:
        raise NotImplementedError("Only Cyphal address version 0 is currently in use")
    return ty(msb | data_specifier.subject_id)


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_udp_endpoint_mapping() -> None:
    from pytest import raises

    ### service_node_id_to_multicast_group
    # valid service IDs
    assert "239.1.0.123" == str(service_node_id_to_multicast_group(destination_node_id=123))
    assert "239.1.1.200" == str(service_node_id_to_multicast_group(destination_node_id=456))
    assert "239.1.255.255" == str(service_node_id_to_multicast_group(destination_node_id=None))

    # invalid destination_node_id
    with raises(ValueError):
        _ = service_node_id_to_multicast_group(destination_node_id=int(0xFFFF))

    # invalid Cyphal address version
    with raises(NotImplementedError):
        _ = service_node_id_to_multicast_group(destination_node_id=123, cy_addr_version=1)

    # SNM bit is set
    srvc_ip = service_node_id_to_multicast_group(destination_node_id=123)
    assert (int(srvc_ip) & SNM_BIT_MASK) == SNM_BIT_MASK

    ### message_data_specifier_to_multicast_group
    # valid data_specifier
    assert "239.0.0.123" == str(message_data_specifier_to_multicast_group(MessageDataSpecifier(123)))
    assert "239.0.1.200" == str(message_data_specifier_to_multicast_group(MessageDataSpecifier(456)))

    # invalid data_specifier
    with raises(ValueError):
        _ = message_data_specifier_to_multicast_group(MessageDataSpecifier(2**14))

    # invalid Cyphal address version
    with raises(NotImplementedError):
        _ = message_data_specifier_to_multicast_group(MessageDataSpecifier(123), cy_addr_version=1)

    # SNM bit is not set
    msg_ip = message_data_specifier_to_multicast_group(MessageDataSpecifier(123))
    assert (int(msg_ip) & SNM_BIT_MASK) == 0
