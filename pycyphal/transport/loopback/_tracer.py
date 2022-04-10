# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import dataclasses
import pycyphal.transport.loopback
from pycyphal.transport import Trace, TransferTrace, Capture


@dataclasses.dataclass(frozen=True)
class LoopbackCapture(pycyphal.transport.Capture):
    """
    Since the loopback transport is not really a transport, its capture events contain entire transfers.
    """

    transfer: pycyphal.transport.AlienTransfer

    @staticmethod
    def get_transport_type() -> typing.Type[pycyphal.transport.loopback.LoopbackTransport]:
        return pycyphal.transport.loopback.LoopbackTransport


class LoopbackTracer(pycyphal.transport.Tracer):
    """
    Since loopback transport does not have frames to trace, this tracer simply returns the transfer
    from the capture object.
    """

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if isinstance(cap, LoopbackCapture):
            return TransferTrace(cap.timestamp, cap.transfer, transfer_id_timeout=0)
        return None
