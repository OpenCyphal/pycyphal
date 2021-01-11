# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import dataclasses
import pyuavcan.transport
from pyuavcan.transport import Trace, TransferTrace, Capture


@dataclasses.dataclass(frozen=True)
class LoopbackCapture(pyuavcan.transport.Capture):
    """
    Since the loopback transport is not really a transport, its capture events contain entire transfers.
    """

    transfer: pyuavcan.transport.AlienTransfer


class LoopbackTracer(pyuavcan.transport.Tracer):
    """
    Since loopback transport does not have frames to trace, this tracer simply returns the transfer
    from the capture object.
    """

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if isinstance(cap, LoopbackCapture):
            return TransferTrace(cap.timestamp, cap.transfer, transfer_id_timeout=0)
        return None
