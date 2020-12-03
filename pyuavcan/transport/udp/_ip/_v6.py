#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import ipaddress
from ._socket_factory import SocketFactory


# noinspection PyAbstractClass
class SocketFactoryIPv6(SocketFactory):
    def __init__(self, local_ip_address: ipaddress.IPv6Address):
        assert local_ip_address
        raise NotImplementedError('Sorry, IPv6 is not yet supported. Please submit patches!')
