# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import abc
import pyuavcan.transport


class Deduplicator(abc.ABC):
    @abc.abstractmethod
    def should_accept_transfer(
        self, iface_id: int, transfer_id_timeout: float, transfer: pyuavcan.transport.TransferFrom
    ) -> bool:
        """
        The iface-ID is an arbitrary integer that is unique within the redundant group identifying the transport
        instance the transfer was received from.
        It could be the index of the redundant interface (e.g., 0, 1, 2 for a triply-redundant transport),
        or it could be something else like a memory address of a related object.
        Embedded applications usually use indexes, whereas in PyUAVCAN it may be more convenient to use ``id()``.

        The transfer-ID timeout is specified in seconds. It is used to handle the case of a node restart.
        """
        raise NotImplementedError
