# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import abc
import typing
import pycyphal.transport


class Deduplicator(abc.ABC):
    """
    The abstract class implementing the transfer-wise deduplication strategy.
    **Users of redundant transports do not need to deduplicate their transfers manually
    as it will be done automatically.**
    Please read the module documentation for further details.
    """

    MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD = int(2**48)
    """
    An inferior transport whose transfer-ID modulo is less than this value is expected to experience
    transfer-ID overflows routinely during its operation. Otherwise, the transfer-ID is not expected to
    overflow for centuries.

    A transfer-ID counter that is expected to overflow is called "cyclic", otherwise it's "monotonic".
    Read https://forum.opencyphal.org/t/alternative-transport-protocols/324.
    See :meth:`new`.
    """

    @staticmethod
    def new(transfer_id_modulo: int) -> Deduplicator:
        """
        A helper factory that constructs a :class:`MonotonicDeduplicator` if the argument is not less than
        :attr:`MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD`, otherwise constructs a :class:`CyclicDeduplicator`.
        """
        from . import CyclicDeduplicator, MonotonicDeduplicator

        if transfer_id_modulo >= Deduplicator.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
            return MonotonicDeduplicator()
        return CyclicDeduplicator(transfer_id_modulo)

    @abc.abstractmethod
    def should_accept_transfer(
        self,
        *,
        iface_id: int,
        transfer_id_timeout: float,
        timestamp: pycyphal.transport.Timestamp,
        source_node_id: typing.Optional[int],
        transfer_id: int,
    ) -> bool:
        """
        The iface-ID is an arbitrary integer that is unique within the redundant group identifying the transport
        instance the transfer was received from.
        It could be the index of the redundant interface (e.g., 0, 1, 2 for a triply-redundant transport),
        or it could be something else like a memory address of a related object.
        Embedded applications usually use indexes, whereas in PyCyphal it may be more convenient to use :func:`id`.

        The transfer-ID timeout is specified in seconds. It is used to handle the case of a node restart.
        """
        raise NotImplementedError
