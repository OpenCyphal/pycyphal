#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
from pyuavcan.transport import DataSpecifier, MessageDataSpecifier, ServiceDataSpecifier


SUBJECT_ID_OFFSET = 16384


def udp_port_from_data_specifier(ds: DataSpecifier) -> int:
    """
    Maps the data specifier to the UDP port number.

    For subjects, the UDP port number equals the subject-ID plus ``2**14 = 16384``.
    The offset is chosen so that UAVCAN ports do not conflict with the IANA-reserved range for ephemeral ports
    and commonly-used lower port numbers.

    UDP port numbers for services grow downward from the same offset;
    even ports for requests, odd ports for responses.
    Services grow downward to allow for a possible future extension of the service-ID
    range without breaking the existing mapping.

    >>> udp_port_from_data_specifier(MessageDataSpecifier(0))
    16384
    >>> udp_port_from_data_specifier(MessageDataSpecifier(8191))
    24575
    >>> udp_port_from_data_specifier(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.REQUEST))
    16382
    >>> udp_port_from_data_specifier(ServiceDataSpecifier(0, ServiceDataSpecifier.Role.RESPONSE))
    16383
    >>> udp_port_from_data_specifier(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.REQUEST))
    15360
    >>> udp_port_from_data_specifier(ServiceDataSpecifier(511, ServiceDataSpecifier.Role.RESPONSE))
    15361
    """
    if isinstance(ds, MessageDataSpecifier):
        return ds.subject_id + SUBJECT_ID_OFFSET

    if isinstance(ds, ServiceDataSpecifier):
        request = SUBJECT_ID_OFFSET - 2 - ds.service_id * 2
        if ds.role == ServiceDataSpecifier.Role.REQUEST:
            return request
        if ds.role == ServiceDataSpecifier.Role.RESPONSE:
            return request + 1

    raise ValueError(f'Unsupported data specifier: {ds}')  # pragma: no cover


def udp_port_to_data_specifier(port: int) -> typing.Optional[DataSpecifier]:
    """
    The inverse of :func:`udp_port_from_data_specifier`. Returns None for invalid ports.

    >>> udp_port_to_data_specifier(16384)
    MessageDataSpecifier(subject_id=0)
    >>> udp_port_to_data_specifier(24575)
    MessageDataSpecifier(subject_id=8191)
    >>> udp_port_to_data_specifier(16382)
    ServiceDataSpecifier(service_id=0, role=...REQUEST...)
    >>> udp_port_to_data_specifier(16383)
    ServiceDataSpecifier(service_id=0, role=...RESPONSE...)
    >>> udp_port_to_data_specifier(15360)
    ServiceDataSpecifier(service_id=511, role=...REQUEST...)
    >>> udp_port_to_data_specifier(15361)
    ServiceDataSpecifier(service_id=511, role=...RESPONSE...)
    """
    out: DataSpecifier
    try:
        if port >= SUBJECT_ID_OFFSET:
            out = MessageDataSpecifier(port - SUBJECT_ID_OFFSET)
        else:
            role = ServiceDataSpecifier.Role.REQUEST if port % 2 == 0 else ServiceDataSpecifier.Role.RESPONSE
            out = ServiceDataSpecifier((SUBJECT_ID_OFFSET - 1 - port) // 2, role)
    except ValueError:
        return None
    return out
