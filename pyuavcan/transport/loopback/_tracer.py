# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import pyuavcan.transport
import dataclasses
from collections import OrderedDict
from pyuavcan.transport import Trace, TransferTrace, Capture, ServiceDataSpecifier, AlienSessionSpecifier
from pyuavcan.transport import AlienTransfer


@dataclasses.dataclass(frozen=True)
class LoopbackCapture(pyuavcan.transport.Capture):
    """
    Since the loopback transport is not really a transport, its capture events contain entire transfers.
    """
    transfer: pyuavcan.transport.AlienTransfer


class LoopbackTracer(pyuavcan.transport.Tracer):
    """
    Since loopback transport does not have frames to trace, this tracer simply returns the transfer
    from the capture object (matched with its sibling in case of service response).
    """

    _HISTORY_DEPTH = 100

    def __init__(self) -> None:
        self._recent: typing.Dict[AlienSessionSpecifier, OrderedDict[int, TransferTrace]] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        if isinstance(cap, LoopbackCapture):
            sibling = self._get_sibling(cap.transfer)
            out = TransferTrace(timestamp=cap.timestamp,
                                transfer=cap.transfer,
                                frames=[cap],
                                sibling=sibling)
            self._record(out)
            return out

        return None

    def _get_sibling(self, tr: AlienTransfer) -> typing.Optional[TransferTrace]:
        ds = tr.session_specifier.data_specifier
        if isinstance(ds, ServiceDataSpecifier) and ds.role == ServiceDataSpecifier.Role.RESPONSE:
            request_ss = AlienSessionSpecifier(
                source_node_id=tr.session_specifier.destination_node_id,
                destination_node_id=tr.session_specifier.source_node_id,
                data_specifier=ServiceDataSpecifier(ds.service_id, ServiceDataSpecifier.Role.REQUEST),
            )
            try:
                return self._recent[request_ss][tr.transfer_id]
            except LookupError:
                pass
        return None

    def _record(self, tr: TransferTrace) -> None:
        ds = tr.transfer.session_specifier.data_specifier
        if isinstance(ds, ServiceDataSpecifier) and ds.role == ServiceDataSpecifier.Role.REQUEST:
            d = self._recent.setdefault(tr.transfer.session_specifier, OrderedDict())
            d[tr.transfer.transfer_id] = tr
            if len(d) > self._HISTORY_DEPTH:
                d.popitem(False)
