#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import pyuavcan.transport


class Deduplicator(abc.ABC):
    @abc.abstractmethod
    def should_accept_transfer(self,
                               iface_index:         int,
                               transfer_id_timeout: float,
                               transfer:            pyuavcan.transport.TransferFrom) -> bool:
        raise NotImplementedError
