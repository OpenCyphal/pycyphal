#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import logging
from ._network_map import NetworkMap


_logger = logging.getLogger(__name__)


class NetworkMapIPv6(NetworkMap):
    def __init__(self, network: str):
        raise NotImplementedError('Sorry, IPv6 is not yet supported. Please submit patches!')
