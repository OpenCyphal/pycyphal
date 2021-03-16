# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import dataclasses
import pyuavcan.transport.loopback
from pyuavcan.transport import Trace, TransferTrace, Capture


@dataclasses.dataclass(frozen=True)
class LoopbackCapture(pyuavcan.transport.Capture):
    """
    Since the loopback transport is not really a transport, its capture events contain entire transfers.
    """

    transfer: pyuavcan.transport.AlienTransfer

    @staticmethod
    def get_transport_type() -> typing.Type[pyuavcan.transport.loopback.LoopbackTransport]:
        return pyuavcan.transport.loopback.LoopbackTransport


class LoopbackTracer(pyuavcan.transport.Tracer):
    """
    Since loopback transport does not have frames to trace, this tracer simply returns the transfer
    from the capture object.
    """

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if isinstance(cap, LoopbackCapture):
            return TransferTrace(cap.timestamp, cap.transfer, transfer_id_timeout=0)
        return None
